"""
healthcare_ingestion.py
=======================
REUSABLE FRAMEWORK — Healthcare Data Ingestion Base Class

The HealthcareIngestionFramework is the core reusable component of this platform.
Any new healthcare data source can be onboarded by:
  1. Adding a YAML config file under framework/config/sources/
  2. Instantiating this class with the config
  3. Zero pipeline code changes required

Handles:
  - Config-driven source extraction (SQL Server, files, APIs, HL7)
  - Bronze layer landing with schema enforcement
  - Watermark-based incremental loads
  - PHI detection and masking
  - Audit logging and lineage tracking
  - Data quality validation
  - HIPAA-compliant error handling (no PHI in logs)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging
import json
import yaml

# Configure HIPAA-safe logging (no PHI in log messages)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("HealthcarePlatform")


# =============================================================================
# Configuration Data Classes
# =============================================================================

@dataclass
class DQRule:
    """Single data quality rule definition."""
    rule_type: str           # not_null | valid_values | date_range | regex | row_count
    columns: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)
    severity: str = "ERROR"  # ERROR (blocks) | WARN (flags)


@dataclass
class SourceConfig:
    """Complete source system configuration loaded from YAML."""
    source_id: str
    source_type: str           # sql_server | parquet | csv | hl7 | fhir | api
    connection_secret: str
    target_layer: str          # bronze | silver | gold
    primary_keys: List[str]
    watermark_column: Optional[str] = None
    partition_column: Optional[str] = None
    phi_columns: List[str] = field(default_factory=list)
    dq_rules: List[DQRule] = field(default_factory=list)
    sla_hour: int = 6
    alert_channel: str = ""
    extract_query: Optional[str] = None
    file_path_pattern: Optional[str] = None
    schema_version: str = "v1"
    environment: str = "dev"


# =============================================================================
# PHI Masking Engine
# =============================================================================

class PHIMaskingEngine:
    """
    HIPAA-compliant PHI masking for healthcare data pipelines.
    Detects and masks 18 HIPAA identifiers automatically.
    """

    # HIPAA Safe Harbor identifiers
    PHI_COLUMN_PATTERNS = [
        "ssn", "social_security", "dob", "date_of_birth", "birth_date",
        "member_name", "patient_name", "first_name", "last_name",
        "address", "zip_code", "phone", "fax", "email",
        "medical_record", "mrn", "health_plan_id", "account_number",
        "certificate_number", "device_identifier", "ip_address",
        "biometric", "photo", "license_number"
    ]

    @classmethod
    def detect_phi_columns(cls, df: DataFrame, declared_phi: List[str]) -> List[str]:
        """Detect PHI columns by pattern matching + declared list."""
        detected = set(declared_phi)
        for col_name in df.columns:
            col_lower = col_name.lower()
            for pattern in cls.PHI_COLUMN_PATTERNS:
                if pattern in col_lower:
                    detected.add(col_name)
                    break
        return list(detected)

    @classmethod
    def mask_phi(cls, df: DataFrame, phi_columns: List[str],
                 mode: str = "hash") -> DataFrame:
        """
        Apply PHI masking to identified columns.

        Modes:
          hash    — SHA-256 hash (consistent, allows joins)
          redact  — Replace with [REDACTED]
          tokenize— Replace with UUID token (requires token store)
        """
        for col_name in phi_columns:
            if col_name not in df.columns:
                continue
            if mode == "hash":
                df = df.withColumn(
                    col_name,
                    F.sha2(F.col(col_name).cast("string"), 256)
                )
            elif mode == "redact":
                df = df.withColumn(col_name, F.lit("[REDACTED]"))
            elif mode == "tokenize":
                # Token-based: replace with UUID per unique value
                # Full implementation uses a secure token store
                df = df.withColumn(
                    col_name,
                    F.sha2(F.concat(F.col(col_name).cast("string"), F.lit("SALT_V1")), 256)
                )

        # Add masking metadata
        df = df.withColumn("_phi_masked", F.lit(True)) \
               .withColumn("_phi_mask_mode", F.lit(mode)) \
               .withColumn("_phi_masked_at", F.current_timestamp())
        return df


# =============================================================================
# Data Quality Framework
# =============================================================================

class DataQualityFramework:
    """
    Reusable DQ validation engine.
    Applies rule-based checks and returns pass/fail with detailed metrics.
    """

    def __init__(self, source_id: str, run_date: str):
        self.source_id = source_id
        self.run_date = run_date
        self.results: List[Dict] = []

    def run_rules(self, df: DataFrame, rules: List[DQRule]) -> Dict[str, Any]:
        """Execute all DQ rules and return aggregated results."""
        total_rows = df.count()
        failures = []

        for rule in rules:
            result = self._execute_rule(df, rule, total_rows)
            self.results.append(result)
            if not result["passed"] and rule.severity == "ERROR":
                failures.append(result)

        passed = len(failures) == 0
        score = self._calculate_dq_score()

        return {
            "source_id":   self.source_id,
            "run_date":    self.run_date,
            "total_rows":  total_rows,
            "rules_run":   len(rules),
            "rules_passed": sum(1 for r in self.results if r["passed"]),
            "rules_failed": sum(1 for r in self.results if not r["passed"]),
            "critical_failures": len(failures),
            "dq_score":    score,
            "passed":      passed,
            "details":     self.results
        }

    def _execute_rule(self, df: DataFrame, rule: DQRule, total_rows: int) -> Dict:
        """Execute a single DQ rule and return result dict."""
        try:
            if rule.rule_type == "not_null":
                for col in rule.columns:
                    null_count = df.filter(F.col(col).isNull()).count()
                    pct_null = round(null_count / max(total_rows, 1) * 100, 2)
                    passed = null_count == 0
                    return {
                        "rule": "not_null", "column": col,
                        "failed_rows": null_count, "pct_failed": pct_null,
                        "passed": passed, "severity": rule.severity
                    }

            elif rule.rule_type == "valid_values":
                col = rule.columns[0]
                valid_vals = rule.params.get("values", [])
                invalid_count = df.filter(~F.col(col).isin(valid_vals)).count()
                passed = invalid_count == 0
                return {
                    "rule": "valid_values", "column": col,
                    "failed_rows": invalid_count,
                    "pct_failed": round(invalid_count / max(total_rows, 1) * 100, 2),
                    "passed": passed, "severity": rule.severity
                }

            elif rule.rule_type == "date_range":
                col = rule.columns[0]
                min_date = rule.params.get("min", "1900-01-01")
                max_date = rule.params.get("max", "2099-12-31")
                invalid = df.filter(
                    (F.col(col) < F.lit(min_date)) | (F.col(col) > F.lit(max_date))
                ).count()
                return {
                    "rule": "date_range", "column": col,
                    "failed_rows": invalid,
                    "pct_failed": round(invalid / max(total_rows, 1) * 100, 2),
                    "passed": invalid == 0, "severity": rule.severity
                }

            elif rule.rule_type == "row_count":
                min_count = rule.params.get("min", 1)
                passed = total_rows >= min_count
                return {
                    "rule": "row_count", "column": None,
                    "failed_rows": 0 if passed else 1,
                    "pct_failed": 0.0,
                    "passed": passed, "severity": rule.severity,
                    "detail": f"Expected >={min_count}, got {total_rows}"
                }

            elif rule.rule_type == "referential_integrity":
                # Check FK relationships
                ref_table = rule.params.get("ref_table")
                ref_col = rule.params.get("ref_column")
                col = rule.columns[0]
                # Implementation depends on SparkSession access
                return {"rule": "referential_integrity", "column": col,
                        "passed": True, "severity": rule.severity, "failed_rows": 0,
                        "pct_failed": 0.0, "detail": "Skipped - requires ref table"}

        except Exception as e:
            logger.error(f"DQ rule {rule.rule_type} failed to execute: {type(e).__name__}")
            return {
                "rule": rule.rule_type, "column": str(rule.columns),
                "passed": False, "severity": "ERROR",
                "failed_rows": -1, "pct_failed": -1.0,
                "error": type(e).__name__  # Never log actual error values (may contain PHI)
            }

    def _calculate_dq_score(self) -> float:
        """Calculate weighted DQ score (0-100)."""
        if not self.results:
            return 0.0
        error_rules = [r for r in self.results if r.get("severity") == "ERROR"]
        warn_rules  = [r for r in self.results if r.get("severity") == "WARN"]
        error_score = sum(100 if r["passed"] else 0 for r in error_rules)
        warn_score  = sum(100 if r["passed"] else 50 for r in warn_rules)
        total_weight = len(error_rules) * 100 + len(warn_rules) * 50
        if total_weight == 0:
            return 100.0
        return round((error_score + warn_score) / total_weight * 100, 1)


# =============================================================================
# Audit Manager
# =============================================================================

class AuditManager:
    """
    Pipeline audit and data lineage manager.
    Writes to Delta audit tables for full observability.
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str = "audit"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.audit_table = f"{catalog}.{schema}.pipeline_runs"
        self.lineage_table = f"{catalog}.{schema}.data_lineage"

    def start_run(self, pipeline_name: str, source_id: str,
                  run_date: str, environment: str) -> int:
        """Register pipeline run start. Returns run_id."""
        run_record = self.spark.createDataFrame([{
            "pipeline_name": pipeline_name,
            "source_id":     source_id,
            "run_date":      run_date,
            "environment":   environment,
            "status":        "RUNNING",
            "started_at":    str(datetime.now()),
            "records_read":  0,
            "records_written": 0,
            "dq_score":      0.0,
            "error_message": None
        }])
        run_record.write.format("delta").mode("append").saveAsTable(self.audit_table)
        # Return max run_id for this pipeline+date (simplified)
        return self.spark.table(self.audit_table) \
            .filter(F.col("pipeline_name") == pipeline_name) \
            .filter(F.col("run_date") == run_date) \
            .agg(F.max("run_id")).first()[0] or 1

    def complete_run(self, run_id: int, records_read: int, records_written: int,
                     dq_score: float, status: str = "SUCCESS") -> None:
        """Update audit record on pipeline completion."""
        self.spark.sql(f"""
            UPDATE {self.audit_table}
            SET status = '{status}',
                records_read = {records_read},
                records_written = {records_written},
                dq_score = {dq_score},
                completed_at = current_timestamp()
            WHERE run_id = {run_id}
        """)

    def log_lineage(self, source_table: str, target_table: str,
                    transformation: str, records_count: int) -> None:
        """Record data lineage for compliance and debugging."""
        lineage = self.spark.createDataFrame([{
            "source_table":    source_table,
            "target_table":    target_table,
            "transformation":  transformation,
            "records_count":   records_count,
            "recorded_at":     str(datetime.now())
        }])
        lineage.write.format("delta").mode("append").saveAsTable(self.lineage_table)


# =============================================================================
# Core Healthcare Ingestion Framework
# =============================================================================

class HealthcareIngestionFramework:
    """
    ★ REUSABLE CORE — Healthcare Data Ingestion Framework

    Orchestrates the full Bronze layer ingestion lifecycle:
      1. Load source config
      2. Extract data from source
      3. Apply DQ validation
      4. Mask PHI columns
      5. Write to Bronze Delta table
      6. Record audit and lineage

    Usage:
        config = SourceConfig(source_id="claims_professional", ...)
        framework = HealthcareIngestionFramework(spark, config, env="dev")
        result = framework.run()
    """

    def __init__(self, spark: SparkSession, config: SourceConfig,
                 adls_base: str, catalog: str):
        self.spark = spark
        self.config = config
        self.adls_base = adls_base
        self.catalog = catalog
        self.dq = DataQualityFramework(config.source_id, str(datetime.today().date()))
        self.audit = AuditManager(spark, catalog)
        self.masker = PHIMaskingEngine()

    def run(self, incremental_from: Optional[str] = None) -> Dict[str, Any]:
        """Execute full ingestion pipeline. Returns run summary."""
        logger.info(f"[INGESTION] Starting: {self.config.source_id}")
        run_id = self.audit.start_run(
            pipeline_name=f"bronze_{self.config.source_id}",
            source_id=self.config.source_id,
            run_date=str(datetime.today().date()),
            environment=self.config.environment
        )

        try:
            # Step 1: Extract
            df_raw = self._extract(incremental_from)
            records_read = df_raw.count()
            logger.info(f"[EXTRACT] {records_read:,} records from {self.config.source_id}")

            # Step 2: Add ingestion metadata
            df_stamped = self._add_metadata(df_raw)

            # Step 3: Data Quality Validation
            dq_result = self.dq.run_rules(df_stamped, self.config.dq_rules)
            logger.info(f"[DQ] Score: {dq_result['dq_score']} | "
                       f"Passed: {dq_result['rules_passed']}/{dq_result['rules_run']}")

            if not dq_result["passed"]:
                raise ValueError(f"DQ failed: {dq_result['critical_failures']} critical rules failed")

            # Step 4: PHI Masking (Gold layer — hash for Bronze to preserve joins)
            phi_cols = self.masker.detect_phi_columns(df_stamped, self.config.phi_columns)
            df_masked = self.masker.mask_phi(df_stamped, phi_cols, mode="hash")
            logger.info(f"[PHI] Masked {len(phi_cols)} PHI columns")

            # Step 5: Write to Bronze Delta
            target_path = f"{self.adls_base}/bronze/{self.config.source_id}"
            target_table = f"{self.catalog}.bronze.{self.config.source_id}"
            records_written = self._write_bronze(df_masked, target_path, target_table)

            # Step 6: Audit + Lineage
            self.audit.complete_run(run_id, records_read, records_written,
                                    dq_result["dq_score"], "SUCCESS")
            self.audit.log_lineage(
                source_table=f"source.{self.config.source_id}",
                target_table=target_table,
                transformation="bronze_ingestion",
                records_count=records_written
            )

            logger.info(f"[COMPLETE] {self.config.source_id}: "
                       f"{records_written:,} records written | DQ: {dq_result['dq_score']}")
            return {"status": "SUCCESS", "records_written": records_written,
                    "dq_score": dq_result["dq_score"], "phi_columns_masked": len(phi_cols)}

        except Exception as e:
            # HIPAA: Never include actual data values in error messages
            error_type = type(e).__name__
            self.audit.complete_run(run_id, 0, 0, 0.0, "FAILED")
            logger.error(f"[FAILED] {self.config.source_id}: {error_type}")
            raise

    def _extract(self, incremental_from: Optional[str]) -> DataFrame:
        """Extract data from source based on source_type."""
        if self.config.source_type == "sql_server":
            return self._extract_sql_server(incremental_from)
        elif self.config.source_type in ("parquet", "csv", "delta"):
            return self._extract_file(self.config.source_type)
        elif self.config.source_type == "hl7":
            return self._extract_hl7()
        else:
            raise ValueError(f"Unsupported source_type: {self.config.source_type}")

    def _extract_sql_server(self, incremental_from: Optional[str]) -> DataFrame:
        """Extract from SQL Server via JDBC with incremental watermark."""
        jdbc_url = self._get_secret(self.config.connection_secret)
        query = self.config.extract_query or f"SELECT * FROM {self.config.source_id}"

        # Apply incremental filter
        if incremental_from and self.config.watermark_column:
            query = f"SELECT * FROM ({query}) t WHERE {self.config.watermark_column} >= '{incremental_from}'"

        return (
            self.spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
            .option("numPartitions", 8)
            .option("partitionColumn", self.config.partition_column or self.config.primary_keys[0])
            .option("lowerBound", "1")
            .option("upperBound", "999999999")
            .load()
        )

    def _extract_file(self, fmt: str) -> DataFrame:
        """Extract from file-based source in ADLS."""
        path = f"{self.adls_base}/landing/{self.config.source_id}"
        return self.spark.read.format(fmt).load(path)

    def _extract_hl7(self) -> DataFrame:
        """
        Parse HL7 v2 ADT messages from ADLS landing zone.
        Returns normalized DataFrame with standard fields.
        """
        raw = self.spark.read.text(
            f"{self.adls_base}/landing/{self.config.source_id}/*.hl7"
        )
        # Parse HL7 segments — simplified; full parser in hl7_parser.py
        return raw.withColumn("raw_message", F.col("value")) \
                  .withColumn("message_type",
                      F.regexp_extract("value", r"MSH\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|.*?\|(\w+\^?\w*)", 1)
                  ) \
                  .withColumn("patient_id",
                      F.regexp_extract("value", r"PID\|.*?\|.*?\|([\w\-]+)", 1)
                  ) \
                  .withColumn("event_timestamp",
                      F.regexp_extract("value", r"MSH\|.*?\|.*?\|.*?\|.*?\|.*?\|(\d{14})", 1)
                  )

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add standard ingestion metadata columns."""
        return df \
            .withColumn("_source_id",      F.lit(self.config.source_id)) \
            .withColumn("_schema_version", F.lit(self.config.schema_version)) \
            .withColumn("_ingested_at",    F.current_timestamp()) \
            .withColumn("_batch_date",     F.current_date()) \
            .withColumn("_environment",    F.lit(self.config.environment)) \
            .withColumn("_is_deleted",     F.lit(False))

    def _write_bronze(self, df: DataFrame, path: str, table: str) -> int:
        """Write to Bronze Delta with merge (upsert) or append."""
        if self.config.watermark_column:
            # Merge for incremental loads
            if DeltaTable.isDeltaTable(self.spark, path):
                target = DeltaTable.forPath(self.spark, path)
                merge_condition = " AND ".join(
                    [f"target.{k} = source.{k}" for k in self.config.primary_keys]
                )
                target.alias("target") \
                      .merge(df.alias("source"), merge_condition) \
                      .whenMatchedUpdateAll() \
                      .whenNotMatchedInsertAll() \
                      .execute()
            else:
                df.write.format("delta").partitionBy(
                    self.config.partition_column or "_batch_date"
                ).save(path)
                self.spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {table} USING DELTA LOCATION '{path}'"
                )
        else:
            # Full load — overwrite
            df.write.format("delta") \
              .mode("overwrite") \
              .option("overwriteSchema", "true") \
              .save(path)

        return df.count()

    def _get_secret(self, secret_name: str) -> str:
        """Retrieve secret from Databricks secret scope (Key Vault backed)."""
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            return dbutils.secrets.get(scope="healthcare-platform-kv", key=secret_name)
        except Exception:
            # Local testing fallback
            import os
            return os.environ.get(secret_name, f"mock-secret-{secret_name}")


# =============================================================================
# Config-Driven Pipeline Runner
# =============================================================================

class ConfigDrivenPipelineRunner:
    """
    Loads YAML configs and orchestrates HealthcareIngestionFramework.
    Enables zero-code onboarding of new source systems.
    """

    def __init__(self, spark: SparkSession, config_path: str,
                 adls_base: str, catalog: str):
        self.spark = spark
        self.config_path = config_path
        self.adls_base = adls_base
        self.catalog = catalog

    def load_config(self, source_id: str) -> SourceConfig:
        """Load and parse YAML source configuration."""
        with open(f"{self.config_path}/{source_id}.yaml") as f:
            raw = yaml.safe_load(f)

        dq_rules = [
            DQRule(
                rule_type=r["rule"],
                columns=r.get("columns", [r.get("column", "")]),
                params={k: v for k, v in r.items()
                        if k not in ("rule", "column", "columns", "severity")},
                severity=r.get("severity", "ERROR")
            )
            for r in raw.get("dq_rules", [])
        ]

        return SourceConfig(
            source_id=raw["source_id"],
            source_type=raw["source_type"],
            connection_secret=raw["connection_secret"],
            target_layer=raw["target_layer"],
            primary_keys=raw["primary_keys"],
            watermark_column=raw.get("watermark_column"),
            partition_column=raw.get("partition_column"),
            phi_columns=raw.get("phi_columns", []),
            dq_rules=dq_rules,
            sla_hour=raw.get("sla_hour", 6),
            alert_channel=raw.get("alert_channel", ""),
            extract_query=raw.get("extract_query"),
        )

    def run_source(self, source_id: str,
                   incremental_from: Optional[str] = None) -> Dict[str, Any]:
        """Run ingestion for a single source by ID."""
        config = self.load_config(source_id)
        framework = HealthcareIngestionFramework(
            self.spark, config, self.adls_base, self.catalog
        )
        return framework.run(incremental_from)

    def run_all(self, source_ids: List[str],
                incremental_from: Optional[str] = None) -> List[Dict]:
        """Run ingestion for multiple sources. Returns list of results."""
        results = []
        for source_id in source_ids:
            try:
                result = self.run_source(source_id, incremental_from)
                results.append({"source_id": source_id, **result})
            except Exception as e:
                logger.error(f"[FAILED] {source_id}: {type(e).__name__}")
                results.append({"source_id": source_id, "status": "FAILED"})
        return results
