"""
test_healthcare_ingestion.py
============================
Unit tests for the reusable Healthcare Ingestion Framework.
Tests PHI masking, DQ validation, and ingestion logic.
Run: pytest tests/unit/test_healthcare_ingestion.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from unittest.mock import MagicMock, patch
from datetime import date
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../framework'))
from ingestion.healthcare_ingestion import (
    PHIMaskingEngine, DataQualityFramework, DQRule, SourceConfig
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("TestHealthcarePlatform")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


@pytest.fixture
def sample_claims(spark):
    """Synthetic claims data (no real PHI)."""
    schema = StructType([
        StructField("claim_id",        StringType()),
        StructField("member_id",       StringType()),
        StructField("member_name",     StringType()),   # PHI
        StructField("ssn",             StringType()),   # PHI
        StructField("service_date",    StringType()),
        StructField("claim_status",    StringType()),
        StructField("allowed_amount",  DoubleType()),
        StructField("procedure_code",  StringType()),
    ])
    data = [
        ("CLM001", "MBR001", "John Doe",   "123-45-6789", "2026-01-15", "PAID",    150.00, "99213"),
        ("CLM002", "MBR002", "Jane Smith", "987-65-4321", "2026-02-10", "DENIED",  200.00, "99214"),
        ("CLM003", "MBR003", None,          None,          "2026-03-05", "PAID",    300.00, "99215"),
        ("CLM004", "MBR004", "Bob Jones",  "555-44-3333", "2026-01-20", "PENDING", None,   "99213"),
        ("CLM005", None,      "Alice Brown","111-22-3333", "2026-02-28", "PAID",    175.00, "99213"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_source_config():
    return SourceConfig(
        source_id="claims_professional",
        source_type="sql_server",
        connection_secret="kv-claims-conn",
        target_layer="bronze",
        primary_keys=["claim_id"],
        watermark_column="service_date",
        partition_column="service_date",
        phi_columns=["member_name", "ssn"],
        dq_rules=[
            DQRule("not_null",    ["claim_id", "member_id"], severity="ERROR"),
            DQRule("valid_values", ["claim_status"],
                   params={"values": ["PAID", "DENIED", "PENDING", "ADJUSTED"]},
                   severity="ERROR"),
            DQRule("not_null", ["allowed_amount"], severity="WARN"),
        ]
    )


# =============================================================================
# PHI MASKING TESTS
# =============================================================================

class TestPHIMaskingEngine:

    def test_detect_phi_columns_from_declared_list(self, sample_claims):
        """Should detect declared PHI columns."""
        masker = PHIMaskingEngine()
        phi_cols = masker.detect_phi_columns(sample_claims, ["member_name", "ssn"])
        assert "member_name" in phi_cols
        assert "ssn" in phi_cols

    def test_detect_phi_columns_by_pattern(self, sample_claims):
        """Should auto-detect PHI columns by name pattern."""
        masker = PHIMaskingEngine()
        # 'ssn' and 'member_name' should be auto-detected from patterns
        phi_cols = masker.detect_phi_columns(sample_claims, [])
        assert "ssn" in phi_cols, "ssn column should be auto-detected as PHI"
        assert "member_name" in phi_cols, "member_name should be auto-detected as PHI"

    def test_hash_masking_produces_fixed_length_hash(self, spark, sample_claims):
        """Hash masking should produce 64-char SHA-256 hashes."""
        masker = PHIMaskingEngine()
        masked = masker.mask_phi(sample_claims, ["member_name", "ssn"], mode="hash")

        # Check hash format for non-null values
        first_row = masked.filter(F.col("member_name").isNotNull()).first()
        assert len(first_row["member_name"]) == 64, \
            f"SHA-256 hash should be 64 chars, got {len(first_row['member_name'])}"

    def test_hash_masking_is_consistent(self, spark, sample_claims):
        """Same input should produce same hash (deterministic)."""
        masker = PHIMaskingEngine()
        masked1 = masker.mask_phi(sample_claims, ["member_name"], mode="hash")
        masked2 = masker.mask_phi(sample_claims, ["member_name"], mode="hash")

        hash1 = masked1.filter(F.col("claim_id") == "CLM001").first()["member_name"]
        hash2 = masked2.filter(F.col("claim_id") == "CLM001").first()["member_name"]
        assert hash1 == hash2, "Hashing must be deterministic for same input"

    def test_redact_masking_replaces_with_placeholder(self, sample_claims):
        """Redact mode should replace PHI with [REDACTED]."""
        masker = PHIMaskingEngine()
        masked = masker.mask_phi(sample_claims, ["member_name"], mode="redact")

        first_row = masked.filter(F.col("member_name").isNotNull()).first()
        assert first_row["member_name"] == "[REDACTED]", \
            "Redact mode should replace with [REDACTED]"

    def test_masking_adds_metadata_columns(self, sample_claims):
        """Masking should add _phi_masked, _phi_mask_mode, _phi_masked_at columns."""
        masker = PHIMaskingEngine()
        masked = masker.mask_phi(sample_claims, ["ssn"], mode="hash")

        assert "_phi_masked" in masked.columns
        assert "_phi_mask_mode" in masked.columns
        assert "_phi_masked_at" in masked.columns
        assert masked.first()["_phi_masked"] == True

    def test_original_values_not_in_masked_output(self, sample_claims):
        """Original PHI values must not appear in any masked output."""
        masker = PHIMaskingEngine()
        masked = masker.mask_phi(sample_claims, ["member_name", "ssn"], mode="hash")

        # Collect all values in masked columns
        names = [row["member_name"] for row in masked.collect() if row["member_name"]]
        ssns  = [row["ssn"] for row in masked.collect() if row["ssn"]]

        known_phi_names = ["John Doe", "Jane Smith", "Bob Jones", "Alice Brown"]
        known_phi_ssns  = ["123-45-6789", "987-65-4321", "555-44-3333", "111-22-3333"]

        for phi in known_phi_names:
            assert phi not in names, f"Original PHI '{phi}' found in masked output!"
        for phi in known_phi_ssns:
            assert phi not in ssns, f"Original PHI '{phi}' found in masked output!"

    def test_null_phi_handled_gracefully(self, sample_claims):
        """Null PHI values should remain null after masking (not error)."""
        masker = PHIMaskingEngine()
        masked = masker.mask_phi(sample_claims, ["member_name"], mode="hash")

        null_rows = masked.filter(F.col("member_name").isNull()).count()
        # CLM003 has null member_name
        assert null_rows >= 1, "Null PHI values should remain null, not raise errors"


# =============================================================================
# DATA QUALITY FRAMEWORK TESTS
# =============================================================================

class TestDataQualityFramework:

    def test_not_null_rule_detects_nulls(self, sample_claims):
        """not_null rule should detect null member_id and claim_id."""
        dq = DataQualityFramework("claims_test", str(date.today()))
        rules = [DQRule("not_null", ["member_id"], severity="ERROR")]
        result = dq.run_rules(sample_claims, rules)

        # CLM005 has null member_id
        assert result["rules_failed"] == 1
        assert result["passed"] == False

    def test_not_null_rule_passes_clean_data(self, spark):
        """not_null rule should pass when all required fields are present."""
        clean_df = spark.createDataFrame([
            ("CLM001", "MBR001", "PAID"),
            ("CLM002", "MBR002", "DENIED"),
        ], ["claim_id", "member_id", "claim_status"])

        dq = DataQualityFramework("claims_clean", str(date.today()))
        rules = [DQRule("not_null", ["claim_id", "member_id"], severity="ERROR")]
        result = dq.run_rules(clean_df, rules)

        assert result["passed"] == True
        assert result["rules_failed"] == 0

    def test_valid_values_rule_detects_invalid(self, sample_claims):
        """valid_values rule should detect claim_status not in allowed list."""
        dq = DataQualityFramework("claims_test", str(date.today()))
        # 'PENDING' is in the list — only test with a stricter set
        rules = [DQRule(
            "valid_values", ["claim_status"],
            params={"values": ["PAID", "DENIED"]},
            severity="ERROR"
        )]
        result = dq.run_rules(sample_claims, rules)

        # PENDING is not in [PAID, DENIED]
        assert result["rules_failed"] >= 1

    def test_date_range_rule_detects_out_of_range(self, spark):
        """date_range rule should detect service dates outside valid range."""
        df_with_old_date = spark.createDataFrame([
            ("CLM001", "1990-01-15"),  # Too old
            ("CLM002", "2026-01-15"),  # Valid
        ], ["claim_id", "service_date"])

        dq = DataQualityFramework("claims_test", str(date.today()))
        rules = [DQRule(
            "date_range", ["service_date"],
            params={"min": "2015-01-01", "max": "2030-12-31"},
            severity="ERROR"
        )]
        result = dq.run_rules(df_with_old_date, rules)
        assert result["rules_failed"] >= 1

    def test_dq_score_100_for_all_pass(self, spark):
        """DQ score should be 100 when all ERROR rules pass."""
        clean_df = spark.createDataFrame([
            ("CLM001", "MBR001", "PAID", "2026-01-15"),
        ], ["claim_id", "member_id", "claim_status", "service_date"])

        dq = DataQualityFramework("clean_source", str(date.today()))
        rules = [
            DQRule("not_null", ["claim_id", "member_id"], severity="ERROR"),
            DQRule("valid_values", ["claim_status"],
                   params={"values": ["PAID", "DENIED", "PENDING"]},
                   severity="ERROR"),
        ]
        result = dq.run_rules(clean_df, rules)
        assert result["dq_score"] == 100.0

    def test_warn_severity_does_not_block_pipeline(self, sample_claims):
        """WARN-severity failures should not set passed=False."""
        dq = DataQualityFramework("claims_test", str(date.today()))
        rules = [
            DQRule("not_null", ["claim_id"], severity="ERROR"),  # Should pass
            DQRule("not_null", ["allowed_amount"], severity="WARN"),  # CLM004 null — WARN
        ]
        result = dq.run_rules(sample_claims, rules)
        # Should still pass despite WARN failure on allowed_amount
        assert result["passed"] == True
        assert result["critical_failures"] == 0

    def test_row_count_rule(self, sample_claims):
        """row_count rule should pass when records >= min threshold."""
        dq = DataQualityFramework("claims_test", str(date.today()))
        rules = [DQRule("row_count", [], params={"min": 3}, severity="ERROR")]
        result = dq.run_rules(sample_claims, rules)
        assert result["passed"] == True  # 5 rows >= 3

    def test_row_count_rule_fails_below_threshold(self, spark):
        """row_count rule should fail when records < min threshold."""
        tiny_df = spark.createDataFrame([("CLM001",)], ["claim_id"])
        dq = DataQualityFramework("tiny_source", str(date.today()))
        rules = [DQRule("row_count", [], params={"min": 100}, severity="ERROR")]
        result = dq.run_rules(tiny_df, rules)
        assert result["passed"] == False


# =============================================================================
# INTEGRATION SMOKE TEST
# =============================================================================

class TestFrameworkIntegration:

    def test_full_pipeline_simulation(self, spark, sample_claims, sample_source_config):
        """Smoke test: simulate full ingestion pipeline (mocked writes)."""
        masker = PHIMaskingEngine()
        dq = DataQualityFramework(
            sample_source_config.source_id,
            str(date.today())
        )

        # Step 1: DQ (subset of rules — not_null on claim_id only for this test)
        rules = [DQRule("not_null", ["claim_id"], severity="ERROR")]
        dq_result = dq.run_rules(sample_claims, rules)
        assert dq_result["passed"] == True

        # Step 2: PHI masking
        phi_cols = masker.detect_phi_columns(sample_claims, sample_source_config.phi_columns)
        masked = masker.mask_phi(sample_claims, phi_cols, mode="hash")

        assert "_phi_masked" in masked.columns
        assert masked.count() == sample_claims.count()  # Row count preserved

        # Step 3: Metadata stamping
        stamped = masked \
            .withColumn("_source_id", F.lit(sample_source_config.source_id)) \
            .withColumn("_batch_date", F.current_date())

        assert "_source_id" in stamped.columns
        assert "_batch_date" in stamped.columns
        assert stamped.count() == sample_claims.count()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
