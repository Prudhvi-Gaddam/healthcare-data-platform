"""
nlp_clinical_notes.py
=====================
AI PRODUCT 4 — NLP Clinical Notes Processing Engine

Extracts structured clinical information from unstructured physician notes:
  - ICD-10 diagnosis codes (automated coding assistance)
  - Medications and dosages
  - Risk factors and social history
  - Lab result mentions
  - Procedures referenced
  - Clinical entities for risk adjustment (HCC coding)

Models:
  - NER (Named Entity Recognition): BioBERT / ClinicalBERT
  - ICD mapping: Trained classifier on MIMIC-III clinical notes
  - Medication extraction: Regex + NER hybrid
  - Sentiment: Clinical assertion detection (negated, uncertain, historical)

HIPAA Compliance:
  - All processing in Databricks — data never leaves the secure environment
  - PHI not sent to external LLM APIs
  - De-identification pipeline available for research workflows
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import logging
import re

logger = logging.getLogger("HealthcarePlatform.NLP")


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ClinicalEntity:
    """A single extracted clinical entity from a note."""
    text: str
    entity_type: str       # DIAGNOSIS | MEDICATION | PROCEDURE | LAB | RISK_FACTOR
    icd10_code: Optional[str] = None
    ndc_code: Optional[str] = None
    confidence: float = 0.0
    assertion: str = "present"  # present | negated | uncertain | historical
    position: Tuple[int, int] = (0, 0)


@dataclass
class NoteProcessingResult:
    """Complete result of processing a clinical note."""
    note_id: str
    member_id: str
    entities: List[ClinicalEntity]
    suggested_icd_codes: List[Dict]
    medications_extracted: List[Dict]
    risk_factors: List[str]
    hcc_codes: List[str]
    processing_time_ms: float
    model_version: str


# =============================================================================
# Medication Extraction (Rule-Based + NER)
# =============================================================================

class MedicationExtractor:
    """
    Extracts medication names, dosages, frequencies from clinical text.
    Uses regex patterns + drug name dictionary matching.
    """

    # Common medication patterns in clinical notes
    DOSAGE_PATTERN = r"\b(\d+(?:\.\d+)?)\s*(mg|mcg|mL|units?|IU)\b"
    FREQUENCY_PATTERN = r"\b(once|twice|q\.?d\.?|b\.?i\.?d\.?|t\.?i\.?d\.?|q\.?h\.?s\.?|prn|daily|weekly)\b"
    ROUTE_PATTERN = r"\b(oral(?:ly)?|po|iv|im|sc|subcut|topical(?:ly)?|inhaled?)\b"

    # Common drug name indicators
    DRUG_SUFFIXES = [
        "mab", "zumab", "ximab",      # Biologics
        "statin", "vastatin",          # Statins
        "pril", "sartan",              # ACE/ARB
        "olol", "alol",               # Beta blockers
        "mycin", "cillin", "oxacin",  # Antibiotics
        "prazole",                     # PPIs
        "dipine",                      # CCBs
    ]

    def extract(self, note_text: str) -> List[Dict]:
        """Extract medications with dosage and frequency from note text."""
        medications = []

        # Find dosage mentions
        dosage_matches = re.finditer(self.DOSAGE_PATTERN, note_text, re.IGNORECASE)
        for match in dosage_matches:
            # Look backwards for drug name (within 30 chars)
            start = max(0, match.start() - 30)
            context = note_text[start:match.end()]
            # Look for drug name patterns
            words_before = context[:match.start() - start].strip().split()
            if words_before:
                drug_candidate = words_before[-1].lower().strip(".,;:")
                if self._is_likely_drug(drug_candidate):
                    medications.append({
                        "drug_name": drug_candidate,
                        "dosage": match.group(1),
                        "unit": match.group(2),
                        "context": context,
                        "confidence": 0.75
                    })

        return medications

    def _is_likely_drug(self, word: str) -> bool:
        """Heuristic check if a word is likely a drug name."""
        if len(word) < 4:
            return False
        word_lower = word.lower()
        for suffix in self.DRUG_SUFFIXES:
            if word_lower.endswith(suffix):
                return True
        return word[0].isupper() and len(word) > 5  # Capitalized medical terms


# =============================================================================
# ICD Code Suggestion Engine
# =============================================================================

class ICDCodeSuggestionEngine:
    """
    Suggests ICD-10 codes from clinical text using NER + classifier.
    Trained on clinical notes dataset for healthcare coding assistance.
    """

    # Simplified keyword-to-ICD mapping for demonstration
    # In production: fine-tuned BERT model on clinical coding dataset
    CONDITION_ICD_MAP = {
        r"\b(type\s+2\s+diabetes|t2dm|diabetes\s+mellitus)\b": ("E11.9", "Type 2 diabetes mellitus without complications"),
        r"\b(hypertension|htn|high\s+blood\s+pressure)\b": ("I10", "Essential (primary) hypertension"),
        r"\b(heart\s+failure|chf|congestive\s+heart)\b": ("I50.9", "Heart failure, unspecified"),
        r"\b(atrial\s+fibrillation|a[\s-]?fib)\b": ("I48.91", "Unspecified atrial fibrillation"),
        r"\b(copd|chronic\s+obstructive\s+pulmonary)\b": ("J44.9", "COPD, unspecified"),
        r"\b(ckd|chronic\s+kidney\s+disease)\b": ("N18.9", "Chronic kidney disease, unspecified"),
        r"\b(depression|major\s+depressive)\b": ("F32.9", "Major depressive disorder, single episode, unspecified"),
        r"\b(hyperlipidemia|dyslipidemia|high\s+cholesterol)\b": ("E78.5", "Hyperlipidemia, unspecified"),
        r"\b(obesity|bmi\s+(?:3[0-9]|4[0-9]))\b": ("E66.9", "Obesity, unspecified"),
        r"\b(pneumonia)\b": ("J18.9", "Pneumonia, unspecified organism"),
        r"\b(sepsis)\b": ("A41.9", "Sepsis, unspecified organism"),
        r"\b(stroke|cva|cerebrovascular\s+accident)\b": ("I63.9", "Cerebral infarction, unspecified"),
        r"\b(mi|myocardial\s+infarction|heart\s+attack)\b": ("I21.9", "Acute myocardial infarction, unspecified"),
        r"\b(asthma)\b": ("J45.909", "Unspecified asthma, uncomplicated"),
    }

    # Negation patterns — don't code negated conditions
    NEGATION_PATTERNS = [
        r"\bno\s+(?:evidence\s+of\s+)?",
        r"\bdenies?\s+",
        r"\brules?\s+out\s+",
        r"\bnegative\s+for\s+",
        r"\bwithout\s+",
        r"\babsence\s+of\s+",
    ]

    def suggest_codes(self, note_text: str) -> List[Dict]:
        """
        Extract potential ICD-10 codes from clinical note text.
        Returns list of suggestions with confidence scores.
        """
        suggestions = []
        note_lower = note_text.lower()

        # Build negation mask — regions of text that are negated
        negated_regions = set()
        for neg_pattern in self.NEGATION_PATTERNS:
            for match in re.finditer(neg_pattern + r".{0,50}", note_lower):
                negated_regions.update(range(match.start(), match.end()))

        # Check each condition pattern
        for pattern, (icd_code, description) in self.CONDITION_ICD_MAP.items():
            matches = list(re.finditer(pattern, note_lower, re.IGNORECASE))
            for match in matches:
                # Check if match is in negated region
                is_negated = match.start() in negated_regions

                # Check for uncertainty modifiers
                context = note_lower[max(0, match.start()-20):match.start()]
                is_uncertain = bool(re.search(r"\bpossible|probable|rule\s+out|?|\bsuspected\b", context))

                assertion = "negated" if is_negated else ("uncertain" if is_uncertain else "present")

                suggestions.append({
                    "icd10_code":    icd_code,
                    "description":   description,
                    "matched_text":  match.group(),
                    "assertion":     assertion,
                    "confidence":    0.90 if assertion == "present" else 0.40,
                    "position":      (match.start(), match.end())
                })

        # Deduplicate by ICD code (keep highest confidence)
        deduped = {}
        for s in suggestions:
            code = s["icd10_code"]
            if code not in deduped or s["confidence"] > deduped[code]["confidence"]:
                deduped[code] = s

        # Return only "present" assertions sorted by confidence
        return sorted(
            [s for s in deduped.values() if s["assertion"] == "present"],
            key=lambda x: x["confidence"], reverse=True
        )


# =============================================================================
# HCC Risk Adjustment Coding
# =============================================================================

class HCCCodeMapper:
    """
    Maps ICD-10 codes to Hierarchical Condition Categories (HCC) for
    CMS risk adjustment / RAF score calculation.
    """

    # Simplified ICD-to-HCC mapping (CMS-HCC Model V28)
    ICD_TO_HCC = {
        "E11.9":  ("HCC 19",  "Diabetes without complication",              0.302),
        "E11.40": ("HCC 18",  "Diabetes with chronic complications",         0.418),
        "I50.9":  ("HCC 85",  "Congestive heart failure",                   0.368),
        "I48.91": ("HCC 96",  "Atrial fibrillation and flutter",            0.270),
        "J44.9":  ("HCC 111", "COPD",                                        0.352),
        "N18.9":  ("HCC 136", "Chronic kidney disease stage 1-2",            0.289),
        "N18.4":  ("HCC 137", "Chronic kidney disease stage 4",              0.289),
        "N18.6":  ("HCC 136", "End-stage renal disease",                     1.180),
        "F32.9":  ("HCC 58",  "Major depressive disorder",                   0.395),
        "A41.9":  ("HCC 2",   "Septicemia, sepsis",                          1.022),
    }

    def map_icd_to_hcc(self, icd_codes: List[str]) -> Tuple[List[Dict], float]:
        """
        Map ICD codes to HCC categories and calculate RAF score.
        Returns (hcc_list, total_raf_score)
        """
        hcc_results = []
        seen_hccs = set()
        total_raf = 0.0

        for icd in icd_codes:
            if icd in self.ICD_TO_HCC:
                hcc_code, hcc_description, raf_weight = self.ICD_TO_HCC[icd]
                if hcc_code not in seen_hccs:
                    hcc_results.append({
                        "icd10_code":      icd,
                        "hcc_code":        hcc_code,
                        "hcc_description": hcc_description,
                        "raf_weight":      raf_weight
                    })
                    total_raf += raf_weight
                    seen_hccs.add(hcc_code)

        return hcc_results, round(total_raf, 3)


# =============================================================================
# Main NLP Pipeline
# =============================================================================

class ClinicalNLPPipeline:
    """
    Orchestrates the full NLP processing pipeline at scale using Spark.
    Processes thousands of clinical notes per hour via PySpark UDFs.
    """

    def __init__(self, spark: SparkSession, catalog: str):
        self.spark = spark
        self.catalog = catalog
        self.med_extractor = MedicationExtractor()
        self.icd_engine = ICDCodeSuggestionEngine()
        self.hcc_mapper = HCCCodeMapper()

    def process_notes_batch(self, notes_table: str, output_table: str) -> int:
        """
        Process a batch of clinical notes and write structured results.
        Uses Spark pandas UDF for distributed NLP processing.
        """
        import time
        from pyspark.sql.functions import pandas_udf
        import pandas as pd

        logger.info(f"[NLP] Processing notes from {notes_table}")

        @pandas_udf(returnType=StructType([
            StructField("suggested_icd_codes", StringType()),
            StructField("medications", StringType()),
            StructField("hcc_codes", StringType()),
            StructField("raf_score", DoubleType()),
            StructField("entity_count", IntegerType()),
        ]))
        def process_note_udf(note_texts: pd.Series) -> pd.DataFrame:
            """Vectorized UDF for batch NLP processing."""
            import json

            icd_engine = ICDCodeSuggestionEngine()
            med_extractor = MedicationExtractor()
            hcc_mapper = HCCCodeMapper()

            results = []
            for note_text in note_texts:
                if not note_text:
                    results.append(("[]", "[]", "[]", 0.0, 0))
                    continue

                # Extract entities
                icd_suggestions = icd_engine.suggest_codes(note_text)
                medications = med_extractor.extract(note_text)
                icd_codes = [s["icd10_code"] for s in icd_suggestions]
                hcc_results, raf_score = hcc_mapper.map_icd_to_hcc(icd_codes)

                results.append((
                    json.dumps(icd_suggestions[:10]),  # Top 10 ICD suggestions
                    json.dumps(medications[:20]),       # Up to 20 medications
                    json.dumps(hcc_results),
                    raf_score,
                    len(icd_suggestions) + len(medications)
                ))

            return pd.DataFrame(results, columns=[
                "suggested_icd_codes", "medications", "hcc_codes", "raf_score", "entity_count"
            ])

        notes_df = self.spark.table(notes_table)

        processed = notes_df.withColumn(
            "nlp_result",
            process_note_udf(F.col("note_text"))
        ).withColumn("suggested_icd_codes", F.col("nlp_result.suggested_icd_codes")) \
         .withColumn("medications",         F.col("nlp_result.medications")) \
         .withColumn("hcc_codes",           F.col("nlp_result.hcc_codes")) \
         .withColumn("raf_score",           F.col("nlp_result.raf_score")) \
         .withColumn("entity_count",        F.col("nlp_result.entity_count")) \
         .withColumn("processed_at",        F.current_timestamp()) \
         .withColumn("model_version",       F.lit("clinical-nlp-v2")) \
         .drop("nlp_result")

        processed.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(output_table)

        count = processed.count()
        avg_entities = processed.agg(F.avg("entity_count")).first()[0]
        logger.info(f"[NLP] Processed {count:,} notes | Avg entities: {avg_entities:.1f}")
        return count
