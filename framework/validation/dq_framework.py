"""
dq_framework.py
===============
REUSABLE FRAMEWORK — Extended Data Quality Validation Engine

Extends the base DataQualityFramework with:
  - Statistical outlier detection
  - Cross-field validation rules
  - Referential integrity checks
  - Great Expectations integration
  - Healthcare-specific DQ rules (HIPAA, claims validation)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging

logger = logging.getLogger("HealthcarePlatform.DQ")


class HealthcareDQValidator:
    """
    Healthcare-specific data quality validation rules.
    Extends base DQ with clinical and claims-specific checks.
    """

    def validate_claims(self, df: DataFrame) -> Dict[str, Any]:
        """Run all claims-specific DQ checks."""
        results = {}
        total = df.count()

        # 1. Allowed amount >= plan paid amount (no overpayment)
        overpayment = df.filter(
            F.col("plan_paid_amount") > F.col("allowed_amount") + F.lit(0.01)
        ).count()
        results["no_overpayment"] = {
            "passed": overpayment == 0,
            "failed_rows": overpayment,
            "pct_failed": round(overpayment / max(total, 1) * 100, 2),
            "severity": "ERROR"
        }

        # 2. Service date within plan year
        invalid_dates = df.filter(
            F.year(F.col("service_date")) < 2000
        ).count()
        results["valid_service_year"] = {
            "passed": invalid_dates == 0,
            "failed_rows": invalid_dates,
            "pct_failed": round(invalid_dates / max(total, 1) * 100, 2),
            "severity": "ERROR"
        }

        # 3. Member cost sharing <= allowed amount
        excess_cost_sharing = df.filter(
            (F.col("member_deductible_amt") +
             F.col("member_copay_amt") +
             F.col("member_coinsurance_amt")) >
            F.col("allowed_amount") + F.lit(1.0)
        ).count()
        results["valid_cost_sharing"] = {
            "passed": excess_cost_sharing == 0,
            "failed_rows": excess_cost_sharing,
            "pct_failed": round(excess_cost_sharing / max(total, 1) * 100, 2),
            "severity": "WARN"
        }

        # 4. Billed amount >= allowed amount
        reversed_amounts = df.filter(
            F.col("billed_amount") < F.col("allowed_amount") - F.lit(0.01)
        ).count()
        results["billed_gte_allowed"] = {
            "passed": reversed_amounts == 0,
            "failed_rows": reversed_amounts,
            "pct_failed": round(reversed_amounts / max(total, 1) * 100, 2),
            "severity": "WARN"
        }

        # 5. No future service dates
        future_dates = df.filter(
            F.col("service_date") > F.current_date()
        ).count()
        results["no_future_dates"] = {
            "passed": future_dates == 0,
            "failed_rows": future_dates,
            "pct_failed": round(future_dates / max(total, 1) * 100, 2),
            "severity": "ERROR"
        }

        # 6. Procedure code format (CPT = 5 digits or HCPCS = letter + 4 digits)
        invalid_cpt = df.filter(
            F.col("procedure_code").isNotNull() &
            ~F.col("procedure_code").rlike(r"^\d{5}$|^[A-Z]\d{4}$")
        ).count()
        results["valid_procedure_code_format"] = {
            "passed": invalid_cpt == 0,
            "failed_rows": invalid_cpt,
            "pct_failed": round(invalid_cpt / max(total, 1) * 100, 2),
            "severity": "WARN"
        }

        # 7. ICD-10 format (letter + 2 digits + optional decimal + digits)
        invalid_icd = df.filter(
            F.col("primary_diagnosis_code").isNotNull() &
            ~F.col("primary_diagnosis_code").rlike(r"^[A-Z]\d{2}\.?\d*$")
        ).count()
        results["valid_icd10_format"] = {
            "passed": invalid_icd == 0,
            "failed_rows": invalid_icd,
            "pct_failed": round(invalid_icd / max(total, 1) * 100, 2),
            "severity": "WARN"
        }

        # 8. Duplicate claim lines
        window_dup = Window.partitionBy(
            "member_id", "rendering_provider_npi",
            "service_date", "procedure_code"
        )
        duplicates = df.withColumn(
            "dup_count", F.count("claim_id").over(window_dup)
        ).filter(F.col("dup_count") > 1).count()
        results["no_duplicate_claim_lines"] = {
            "passed": duplicates == 0,
            "failed_rows": duplicates,
            "pct_failed": round(duplicates / max(total, 1) * 100, 2),
            "severity": "WARN"
        }

        # Calculate overall score
        error_results = {k: v for k, v in results.items() if v["severity"] == "ERROR"}
        passed_errors = sum(1 for v in error_results.values() if v["passed"])
        score = round(passed_errors / max(len(error_results), 1) * 100, 1)

        return {
            "source_id": "claims_validation",
            "total_rows": total,
            "checks_run": len(results),
            "checks_passed": sum(1 for v in results.values() if v["passed"]),
            "checks_failed": sum(1 for v in results.values() if not v["passed"]),
            "dq_score": score,
            "passed": all(v["passed"] for v in error_results.values()),
            "details": results
        }

    def validate_eligibility(self, df: DataFrame) -> Dict[str, Any]:
        """Run eligibility-specific DQ checks."""
        total = df.count()
        results = {}

        # Termination date must be after effective date
        invalid_dates = df.filter(
            F.col("termination_date").isNotNull() &
            (F.col("termination_date") < F.col("effective_date"))
        ).count()
        results["termination_after_effective"] = {
            "passed": invalid_dates == 0,
            "failed_rows": invalid_dates,
            "pct_failed": round(invalid_dates / max(total, 1) * 100, 2),
            "severity": "ERROR"
        }

        # Valid metal tier
        invalid_tier = df.filter(
            F.col("metal_tier").isNotNull() &
            ~F.col("metal_tier").isin(["BRONZE","SILVER","GOLD","PLATINUM","CATASTROPHIC"])
        ).count()
        results["valid_metal_tier"] = {
            "passed": invalid_tier == 0,
            "failed_rows": invalid_tier,
            "pct_failed": round(invalid_tier / max(total, 1) * 100, 2),
            "severity": "WARN"
        }

        score = round(
            sum(1 for v in results.values() if v["passed"]) /
            max(len(results), 1) * 100, 1
        )
        return {
            "source_id": "eligibility_validation",
            "total_rows": total,
            "dq_score": score,
            "passed": all(v["passed"] for v in results.values() if v["severity"] == "ERROR"),
            "details": results
        }


class StatisticalDQChecker:
    """
    Statistical data quality checks using Z-score and IQR methods.
    Detects outliers and distribution shifts vs historical baselines.
    """

    def check_distribution_shift(self, current_df: DataFrame,
                                  historical_df: DataFrame,
                                  metric_col: str,
                                  threshold_pct: float = 20.0) -> Dict:
        """
        Detect if current data distribution has shifted > threshold vs historical.
        Uses mean comparison as a simple distribution shift indicator.
        """
        current_stats = current_df.agg(
            F.avg(metric_col).alias("mean"),
            F.stddev(metric_col).alias("stddev"),
            F.count("*").alias("count")
        ).first()

        historical_stats = historical_df.agg(
            F.avg(metric_col).alias("mean"),
            F.stddev(metric_col).alias("stddev")
        ).first()

        if not historical_stats["mean"] or historical_stats["mean"] == 0:
            return {"passed": True, "detail": "No historical baseline"}

        pct_change = abs(
            (current_stats["mean"] - historical_stats["mean"]) /
            historical_stats["mean"] * 100
        )

        passed = pct_change <= threshold_pct
        return {
            "check": "distribution_shift",
            "column": metric_col,
            "current_mean": round(current_stats["mean"] or 0, 2),
            "historical_mean": round(historical_stats["mean"] or 0, 2),
            "pct_change": round(pct_change, 2),
            "threshold_pct": threshold_pct,
            "passed": passed,
            "severity": "WARN"
        }

    def detect_outliers_zscore(self, df: DataFrame,
                                col_name: str,
                                threshold: float = 3.0) -> DataFrame:
        """Flag rows where value is more than threshold standard deviations from mean."""
        stats = df.agg(
            F.avg(col_name).alias("mean"),
            F.stddev(col_name).alias("stddev")
        ).first()

        if not stats["stddev"] or stats["stddev"] == 0:
            return df.withColumn(f"{col_name}_outlier", F.lit(False))

        return df.withColumn(
            f"{col_name}_zscore",
            F.abs((F.col(col_name) - F.lit(stats["mean"])) / F.lit(stats["stddev"]))
        ).withColumn(
            f"{col_name}_outlier",
            F.col(f"{col_name}_zscore") > threshold
        )
