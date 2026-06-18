"""
rag_clinical_chatbot.py
=======================
AI PRODUCT 1 — RAG-Based Clinical Knowledge Chatbot

Provides natural language Q&A over:
  - Clinical coverage policies
  - Drug formularies and prior authorization rules
  - HEDIS measure specifications
  - Provider network information
  - Member benefits and eligibility

Architecture:
  1. Vector store built from clinical documents (PDF, HTML, structured data)
  2. Query → embedding → semantic search → top-k retrieval
  3. Retrieved context + query → LLM → grounded answer
  4. Citations provided for every answer (audit trail)

Built on Databricks:
  - Vector store: Databricks Vector Search
  - Embeddings: BGE-large / text-embedding-ada-002
  - LLM: Databricks DBRX / Llama-3 / Azure OpenAI
  - Serving: Databricks Model Serving endpoint
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
import json
import re

logger = logging.getLogger("HealthcarePlatform.RAG")


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class Document:
    """A clinical knowledge document for the RAG knowledge base."""
    doc_id: str
    content: str
    source: str              # policy | formulary | hedis | provider | benefits
    metadata: Dict[str, Any] = field(default_factory=dict)
    category: str = ""


@dataclass
class ChatMessage:
    """A single message in a conversation."""
    role: str    # user | assistant | system
    content: str
    timestamp: str = ""
    citations: List[str] = field(default_factory=list)


@dataclass
class RAGResponse:
    """Complete RAG response with answer, citations, and confidence."""
    answer: str
    citations: List[Dict[str, str]]
    confidence_score: float
    retrieved_docs: List[str]
    model_used: str
    latency_ms: float
    query_id: str
    timestamp: str


# =============================================================================
# Vector Store Builder
# =============================================================================

class ClinicalVectorStoreBuilder:
    """
    Builds and maintains the RAG knowledge base vector store.
    Indexes clinical documents for semantic retrieval.
    """

    SUPPORTED_SOURCES = {
        "coverage_policies": "Clinical coverage and medical necessity policies",
        "drug_formulary":    "Drug formulary tiers, prior auth requirements, step therapy",
        "hedis_specs":       "HEDIS technical measure specifications (NCQA)",
        "provider_network":  "In-network provider and facility information",
        "member_benefits":   "Plan benefits, cost-sharing, and coverage summaries",
        "clinical_guidelines": "Evidence-based clinical practice guidelines"
    }

    def __init__(self, spark: SparkSession, catalog: str, embedding_model: str):
        self.spark = spark
        self.catalog = catalog
        self.embedding_model = embedding_model

    def build_from_delta(self, source_table: str, doc_type: str,
                         content_col: str, id_col: str) -> int:
        """
        Build vector store from a Delta table of clinical documents.
        Returns number of documents indexed.
        """
        logger.info(f"[VECTOR] Building index from {source_table}")

        # Load documents from Delta
        docs_df = self.spark.table(source_table) \
            .filter(F.col("is_active") == True) \
            .select(
                F.col(id_col).alias("doc_id"),
                F.col(content_col).alias("content"),
                F.lit(doc_type).alias("source"),
                F.current_timestamp().alias("indexed_at")
            )

        # Chunk long documents (max 512 tokens per chunk)
        chunked = self._chunk_documents(docs_df)

        # Generate embeddings using Databricks AI Functions
        embedded = chunked.withColumn(
            "embedding",
            F.expr(f"ai_embed_text(content, '{self.embedding_model}')")
        )

        # Write to Vector Search index table
        index_table = f"{self.catalog}.ai.clinical_knowledge_index"
        embedded.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(index_table)

        count = embedded.count()
        logger.info(f"[VECTOR] Indexed {count:,} document chunks from {source_table}")
        return count

    def _chunk_documents(self, docs_df, max_chunk_size: int = 512,
                          overlap: int = 50):
        """
        Split long documents into overlapping chunks for better retrieval.
        Uses Spark UDF for distributed chunking.
        """
        from pyspark.sql.functions import udf
        from pyspark.sql.types import ArrayType, StructType, StructField, StringType

        ChunkSchema = ArrayType(StructType([
            StructField("chunk_id", StringType()),
            StructField("chunk_text", StringType()),
            StructField("chunk_index", StringType()),
        ]))

        @udf(ChunkSchema)
        def chunk_text(doc_id, content, max_size, ov):
            """Split content into overlapping word chunks."""
            if not content:
                return []
            words = content.split()
            chunks = []
            i = 0
            chunk_idx = 0
            while i < len(words):
                chunk_words = words[i:i + max_size]
                chunk_text = " ".join(chunk_words)
                chunks.append({
                    "chunk_id": f"{doc_id}_chunk_{chunk_idx}",
                    "chunk_text": chunk_text,
                    "chunk_index": str(chunk_idx)
                })
                i += max_size - ov
                chunk_idx += 1
            return chunks

        from pyspark.sql.functions import explode
        chunked = docs_df \
            .withColumn("chunks", chunk_text(
                F.col("doc_id"), F.col("content"),
                F.lit(max_chunk_size), F.lit(overlap)
            )) \
            .withColumn("chunk", explode(F.col("chunks"))) \
            .withColumn("chunk_id", F.col("chunk.chunk_id")) \
            .withColumn("content", F.col("chunk.chunk_text")) \
            .drop("chunks", "chunk")

        return chunked


# =============================================================================
# RAG Query Engine
# =============================================================================

class ClinicalRAGEngine:
    """
    Core RAG query engine for clinical knowledge retrieval.

    Workflow:
      1. Receive user query
      2. Safety check (PHI, inappropriate content)
      3. Generate query embedding
      4. Retrieve top-k similar document chunks
      5. Build augmented prompt with retrieved context
      6. Call LLM for grounded answer
      7. Log interaction for audit and quality monitoring
    """

    # Healthcare-specific prompt template
    SYSTEM_PROMPT = """You are a knowledgeable healthcare data assistant for a health plan.
You help members, providers, and staff with questions about:
- Coverage policies and medical necessity criteria
- Drug formulary and prior authorization requirements
- Quality measures (HEDIS) and care gaps
- Provider network information
- Benefits and cost-sharing

Guidelines:
- Only answer based on the provided context documents
- Always cite your sources with document IDs
- For clinical decisions, always recommend consulting a clinician
- Never provide specific medical advice — redirect to clinical staff
- If the context doesn't contain the answer, say so clearly
- Keep responses concise and accurate

Context documents:
{context}

Respond in a clear, professional tone appropriate for healthcare settings."""

    def __init__(self, spark: SparkSession, catalog: str,
                 llm_endpoint: str, top_k: int = 5):
        self.spark = spark
        self.catalog = catalog
        self.llm_endpoint = llm_endpoint
        self.top_k = top_k
        self.index_table = f"{catalog}.ai.clinical_knowledge_index"

    def query(self, user_question: str, session_id: str,
              filters: Optional[Dict] = None) -> RAGResponse:
        """
        Process a user question through the full RAG pipeline.
        Returns structured response with citations.
        """
        import time
        start = time.time()
        query_id = f"q_{session_id}_{int(start)}"

        logger.info(f"[RAG] Processing query: {query_id}")

        # Step 1: Safety & PHI check
        self._check_query_safety(user_question)

        # Step 2: Retrieve relevant documents
        retrieved_chunks = self._retrieve_context(user_question, filters)

        # Step 3: Build augmented prompt
        context_text = self._format_context(retrieved_chunks)
        augmented_prompt = self.SYSTEM_PROMPT.format(context=context_text)

        # Step 4: Call LLM
        answer = self._call_llm(augmented_prompt, user_question)

        # Step 5: Extract citations
        citations = self._extract_citations(retrieved_chunks)

        # Step 6: Calculate confidence score
        confidence = self._calculate_confidence(retrieved_chunks)

        latency = round((time.time() - start) * 1000, 1)

        response = RAGResponse(
            answer=answer,
            citations=citations,
            confidence_score=confidence,
            retrieved_docs=[c["chunk_id"] for c in retrieved_chunks],
            model_used=self.llm_endpoint,
            latency_ms=latency,
            query_id=query_id,
            timestamp=str(datetime.now())
        )

        # Step 7: Log for audit trail and quality monitoring
        self._log_interaction(query_id, session_id, user_question, response)

        return response

    def _check_query_safety(self, query: str) -> None:
        """
        Screen query for PHI and inappropriate content.
        Raises ValueError if query contains PHI patterns.
        """
        # PHI patterns that should not be in queries
        phi_patterns = [
            r"\b\d{3}-\d{2}-\d{4}\b",           # SSN
            r"\b\d{10,}\b",                       # Long numbers (member IDs, MRN)
            r"\b[A-Z]\d{8}\b",                    # Driver license patterns
        ]
        for pattern in phi_patterns:
            if re.search(pattern, query):
                logger.warning(f"[SAFETY] Query may contain PHI — sanitizing")
                # Don't log the query itself (may contain PHI)
                raise ValueError("Query appears to contain PHI. Please rephrase without personal identifiers.")

    def _retrieve_context(self, query: str,
                          filters: Optional[Dict] = None) -> List[Dict]:
        """
        Semantic search against vector index.
        Returns top-k most relevant document chunks.
        """
        # Use Databricks Vector Search for similarity retrieval
        # In production: use databricks.vector_search.client
        result_df = self.spark.sql(f"""
            SELECT
                chunk_id,
                content,
                source,
                metadata,
                vector_search_score(embedding, ai_embed_text('{query}', 'bge-large-en')) AS score
            FROM {self.index_table}
            {self._build_filter_clause(filters)}
            ORDER BY score DESC
            LIMIT {self.top_k}
        """)

        return [row.asDict() for row in result_df.collect()]

    def _format_context(self, chunks: List[Dict]) -> str:
        """Format retrieved chunks into LLM context string."""
        context_parts = []
        for i, chunk in enumerate(chunks, 1):
            source_label = chunk.get("source", "unknown")
            context_parts.append(
                f"[Document {i} | Source: {source_label} | ID: {chunk['chunk_id']}]\n"
                f"{chunk['content']}\n"
            )
        return "\n---\n".join(context_parts)

    def _call_llm(self, system_prompt: str, user_query: str) -> str:
        """Call the configured LLM endpoint via Databricks AI Functions."""
        response_df = self.spark.sql(f"""
            SELECT ai_query(
                '{self.llm_endpoint}',
                '{user_query}',
                systemPrompt => '{system_prompt.replace("'", "''")}'
            ) AS answer
        """)
        result = response_df.first()
        return result["answer"] if result else "Unable to generate response."

    def _extract_citations(self, chunks: List[Dict]) -> List[Dict[str, str]]:
        """Build citation list from retrieved documents."""
        return [
            {
                "doc_id": chunk["chunk_id"],
                "source": chunk.get("source", ""),
                "relevance_score": str(round(chunk.get("score", 0.0), 3))
            }
            for chunk in chunks
        ]

    def _calculate_confidence(self, chunks: List[Dict]) -> float:
        """
        Estimate answer confidence based on retrieval scores.
        High confidence = top retrieved doc is highly relevant.
        """
        if not chunks:
            return 0.0
        scores = [chunk.get("score", 0.0) for chunk in chunks]
        avg_score = sum(scores) / len(scores)
        # Normalize to 0-100
        return round(min(avg_score * 100, 100.0), 1)

    def _build_filter_clause(self, filters: Optional[Dict]) -> str:
        """Build SQL WHERE clause from filter dict."""
        if not filters:
            return ""
        conditions = [f"source = '{v}'" for k, v in filters.items() if k == "source"]
        return "WHERE " + " AND ".join(conditions) if conditions else ""

    def _log_interaction(self, query_id: str, session_id: str,
                          question: str, response: RAGResponse) -> None:
        """Log Q&A interaction for audit trail and quality monitoring."""
        # Never log the actual question content (may trigger PHI concerns)
        # Log metadata only
        log_record = self.spark.createDataFrame([{
            "query_id":        query_id,
            "session_id":      session_id,
            "query_hash":      str(hash(question)),  # Hash, not raw query
            "confidence_score": response.confidence_score,
            "docs_retrieved":  len(response.retrieved_docs),
            "latency_ms":      response.latency_ms,
            "model_used":      response.model_used,
            "timestamp":       response.timestamp
        }])
        log_record.write.format("delta").mode("append") \
            .saveAsTable(f"{self.catalog}.ai.rag_interaction_log")


# =============================================================================
# RAG Quality Evaluation
# =============================================================================

class RAGEvaluator:
    """
    Evaluates RAG system quality using LLM-as-judge approach.
    Runs automated evaluations on sample Q&A pairs.
    """

    def evaluate_batch(self, spark: SparkSession,
                        eval_dataset_table: str, engine: ClinicalRAGEngine,
                        catalog: str) -> Dict[str, float]:
        """
        Run batch evaluation on labeled Q&A pairs.
        Returns metrics: faithfulness, relevancy, correctness.
        """
        eval_df = spark.table(eval_dataset_table)
        questions = [row["question"] for row in eval_df.select("question").collect()]

        faithfulness_scores = []
        relevancy_scores = []

        for q in questions[:50]:  # Evaluate sample of 50
            try:
                response = engine.query(q, session_id="eval_run")
                # LLM judge for faithfulness (answer grounded in retrieved docs)
                faith_score = self._judge_faithfulness(
                    spark, q, response.answer, response.citations
                )
                faithfulness_scores.append(faith_score)
                relevancy_scores.append(response.confidence_score)
            except Exception:
                faithfulness_scores.append(0.0)

        metrics = {
            "faithfulness":  round(sum(faithfulness_scores) / max(len(faithfulness_scores), 1), 2),
            "avg_confidence": round(sum(relevancy_scores) / max(len(relevancy_scores), 1), 2),
            "questions_evaluated": len(questions[:50])
        }

        logger.info(f"[EVAL] RAG Metrics: {metrics}")
        return metrics

    def _judge_faithfulness(self, spark: SparkSession,
                             question: str, answer: str,
                             citations: List[Dict]) -> float:
        """Use LLM to judge if answer is faithful to retrieved sources."""
        citation_ids = [c["doc_id"] for c in citations]
        prompt = f"""Rate the faithfulness of this answer (0.0-1.0):
        Question: {question}
        Answer: {answer}
        Citations: {citation_ids}
        
        Faithfulness means the answer is fully supported by the cited sources.
        Return only a number between 0.0 and 1.0."""

        result = spark.sql(f"SELECT ai_query('databricks-llm', '{prompt}') AS score")
        try:
            return float(result.first()["score"])
        except Exception:
            return 0.5
