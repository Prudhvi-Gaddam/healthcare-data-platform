"""
test_data_quality_suite.py
==========================
Great Expectations data quality test suite for Healthcare Data Platform.
Tests validate Bronze and Gold layer data against expected quality rules.
Run: pytest tests/data-quality/test_data_quality_suite.py -v
"""

import pytest
import great_expectations as ge
from great_expectations.core import ExpectationSuite
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("GEHealthcareDQTests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture
def sample_claims_df(spark):
    """Synthetic claims data for DQ testing — no real PHI."""
    schema = StructType([
        StructField("claim_id",               StringType()),
        StructField("member_id",              StringType()),
        StructField("service_date",           DateType()),
        StructField("claim_status",           StringType()),
        StructField("allowed_amount",         DoubleType()),
        StructField("plan_paid_amount",       DoubleType()),
        StructField("member_deductible_amt",  DoubleType()),
        StructField("member_copay_amt",       DoubleType()),
        StructField("member_coinsurance_amt", DoubleType()),
        StructField("procedure_code",         StringType()),
        StructField("primary_diagnosis_code", StringType()),
        StructField("billed_amount",          DoubleType()),
    ])
    data = [
        ("CLM001","MBR001", date(2026,1,15), "PAID",    500.0,  400.0, 50.0, 20.0, 30.0, "99213", "E11.9",  600.0),
        ("CLM002","MBR002", date(2026,2,10), "DENIED",  200.0,    0.0,  0.0,  0.0,  0.0, "99214", "I10",    250.0),
        ("CLM003","MBR003", date(2026,3,5),  "ADJUSTED",300.0,  250.0, 25.0, 10.0, 15.0, "99215", "J44.9",  350.0),
        ("CLM004","MBR004", date(2026,1,20), "PENDING",  75.0,    0.0,  0.0,  0.0,  0.0, "99212", "I50.9",   90.0),
        ("CLM005","MBR005", date(2026,4,1),  "PAID",   1000.0,  800.0,100.0, 50.0, 50.0, "45378", "Z12.11",1100.0),
    ]
    return spark.createDataFrame(data, schema)


class TestClaimsBronzeDQ:
    """Great Expectations style DQ tests for Bronze claims data."""

    def test_no_null_claim_id(self, sample_claims_df):
        """claim_id must never be null."""
        null_count = sample_claims_df.filter(F.col("claim_id").isNull()).count()
        assert null_count == 0, f"Found {null_count} null claim_id values"

    def test_no_null_member_id(self, sample_claims_df):
        """member_id must never be null (is hashed, but must exist)."""
        null_count = sample_claims_df.filter(F.col("member_id").isNull()).count()
        assert null_count == 0, f"Found {null_count} null member_id values"

    def test_no_null_service_date(self, sample_claims_df):
        """service_date must never be null."""
        null_count = sample_claims_df.filter(F.col("service_date").isNull()).count()
        assert null_count == 0

    def test_valid_claim_status_values(self, sample_claims_df):
        """claim_status must be one of the approved values."""
        valid_statuses = ["PAID", "DENIED", "PENDING", "ADJUSTED", "VOID", "SUSPENDED"]
        invalid = sample_claims_df.filter(
            ~F.col("claim_status").isin(valid_statuses)
        ).count()
        assert invalid == 0, f"{invalid} records with invalid claim_status"

    def test_service_date_not_in_future(self, sample_claims_df):
        """service_date must not be in the future."""
        future = sample_claims_df.filter(
            F.col("service_date") > F.current_date()
        ).count()
        assert future == 0, f"{future} claims have future service dates"

    def test_service_date_after_2015(self, sample_claims_df):
        """service_date must be after 2015-01-01 (data migration start)."""
        too_old = sample_claims_df.filter(
            F.col("service_date") < F.lit("2015-01-01").cast("date")
        ).count()
        assert too_old == 0, f"{too_old} claims before 2015"

    def test_allowed_amount_non_negative(self, sample_claims_df):
        """allowed_amount must be >= 0."""
        negative = sample_claims_df.filter(
            F.col("allowed_amount").isNotNull() &
            (F.col("allowed_amount") < 0)
        ).count()
        assert negative == 0, f"{negative} claims with negative allowed_amount"

    def test_plan_paid_lte_allowed(self, sample_claims_df):
        """plan_paid_amount must not exceed allowed_amount."""
        overpaid = sample_claims_df.filter(
            F.col("plan_paid_amount") > F.col("allowed_amount") + F.lit(0.01)
        ).count()
        assert overpaid == 0, f"{overpaid} claims where plan paid > allowed"

    def test_billed_gte_allowed(self, sample_claims_df):
        """billed_amount must be >= allowed_amount."""
        invalid = sample_claims_df.filter(
            F.col("billed_amount").isNotNull() &
            F.col("allowed_amount").isNotNull() &
            (F.col("billed_amount") < F.col("allowed_amount") - F.lit(0.01))
        ).count()
        assert invalid == 0, f"{invalid} claims where billed < allowed"

    def test_member_cost_sharing_lte_allowed(self, sample_claims_df):
        """Total member cost sharing must not exceed allowed amount."""
        excess = sample_claims_df.filter(
            (F.col("member_deductible_amt") +
             F.col("member_copay_amt") +
             F.col("member_coinsurance_amt")) >
            F.col("allowed_amount") + F.lit(1.0)
        ).count()
        assert excess == 0, f"{excess} claims with cost sharing > allowed"

    def test_procedure_code_format(self, sample_claims_df):
        """procedure_code must match CPT (5 digits) or HCPCS (letter + 4 digits)."""
        invalid_format = sample_claims_df.filter(
            F.col("procedure_code").isNotNull() &
            ~F.col("procedure_code").rlike(r"^\d{5}$|^[A-Z]\d{4}$")
        ).count()
        assert invalid_format == 0, f"{invalid_format} claims with invalid procedure code format"

    def test_icd10_format(self, sample_claims_df):
        """primary_diagnosis_code must match ICD-10-CM format."""
        invalid_icd = sample_claims_df.filter(
            F.col("primary_diagnosis_code").isNotNull() &
            ~F.col("primary_diagnosis_code").rlike(r"^[A-Z]\d{2}\.?\d*$")
        ).count()
        assert invalid_icd == 0, f"{invalid_icd} claims with invalid ICD-10 format"

    def test_minimum_row_count(self, sample_claims_df):
        """Claims dataset must have at least 1 record."""
        count = sample_claims_df.count()
        assert count >= 1, f"Expected at least 1 claim record, got {count}"

    def test_no_duplicate_claim_ids(self, sample_claims_df):
        """claim_id must be unique."""
        total = sample_claims_df.count()
        distinct = sample_claims_df.select("claim_id").distinct().count()
        assert total == distinct, f"Found {total - distinct} duplicate claim_ids"


class TestEligibilityDQ:
    """DQ tests for member eligibility data."""

    @pytest.fixture
    def sample_eligibility(self, spark):
        schema = StructType([
            StructField("member_id",         StringType()),
            StructField("plan_id",           StringType()),
            StructField("metal_tier",        StringType()),
            StructField("effective_date",    DateType()),
            StructField("termination_date",  DateType()),
        ])
        data = [
            ("MBR001","PLAN001","GOLD",   date(2026,1,1), date(2026,12,31)),
            ("MBR002","PLAN002","SILVER", date(2026,1,1), None),
            ("MBR003","PLAN001","BRONZE", date(2026,3,1), date(2026,12,31)),
        ]
        return spark.createDataFrame(data, schema)

    def test_no_null_member_plan(self, sample_eligibility):
        null_count = sample_eligibility.filter(
            F.col("member_id").isNull() | F.col("plan_id").isNull()
        ).count()
        assert null_count == 0

    def test_valid_metal_tier(self, sample_eligibility):
        valid_tiers = ["BRONZE","SILVER","GOLD","PLATINUM","CATASTROPHIC"]
        invalid = sample_eligibility.filter(
            F.col("metal_tier").isNotNull() &
            ~F.col("metal_tier").isin(valid_tiers)
        ).count()
        assert invalid == 0

    def test_termination_after_effective(self, sample_eligibility):
        """termination_date must be >= effective_date."""
        invalid = sample_eligibility.filter(
            F.col("termination_date").isNotNull() &
            (F.col("termination_date") < F.col("effective_date"))
        ).count()
        assert invalid == 0, f"{invalid} records where termination < effective"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
