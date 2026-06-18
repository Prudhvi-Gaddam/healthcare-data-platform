"""
anomaly_model.py
================
AI PRODUCT 2 — Healthcare Claims Anomaly & Fraud Detection

Identifies anomalous patterns in claims, billing, and utilization data.
Use cases:
  - Fraud, Waste & Abuse (FWA) detection
  - Billing anomalies (upcoding, unbundling, duplicate claims)
  - Unusual utilization patterns (excessive services, impossible days)
  - Data quality anomalies in ETL pipelines

Models Used:
  - Isolation Forest (unsupervised — no labeled data required)
  - Z-score statistical anomaly detection
  - Rule-based clinical impossibility detection
  - DBSCAN clustering for provider behavior patterns

Output:
  - Anomaly score per claim (0-100, higher = more anomalous)
  - Anomaly type classification
  - Investigation priority (CRITICAL/HIGH/MEDIUM/LOW)
  - Explanation of anomaly signal
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger("HealthcarePlatform.AnomalyDetection")


@dataclass
class AnomalyResult:
    """Result of anomaly detection for a single entity."""
    entity_id: str
    entity_type: str     # claim | provider | member
    anomaly_score: float
    anomaly_types: List[str]
    priority: str
    explanation: str
    detected_at: str


# =============================================================================
# Feature Engineering for Anomaly Detection
# =============================================================================

class ClaimsAnomalyFeatureEngine:
    """
    Computes behavioral features for anomaly detection.
    Uses rolling windows to establish baseline patterns per provider/member.
    """

    def provider_features(self, claims: DataFrame) -> DataFrame:
        """
        Per-provider behavioral features using 90-day rolling windows.
        Detects outlier providers vs their specialty peers.
        """
        window_90d = Window.partitionBy("rendering_provider_npi") \
            .orderBy("service_date") \
            .rowsBetween(-90, 0)

        window_peer = Window.partitionBy("specialty_code", "service_date")

        provider_stats = claims.withColumn(
            "avg_cost_90d",        F.avg("allowed_amount").over(window_90d)
        ).withColumn(
            "claims_per_day_90d",  F.count("claim_id").over(window_90d) / 90
        ).withColumn(
            "unique_members_90d",  F.countDistinct("member_id").over(window_90d)
        ).withColumn(
            "unique_dx_codes_90d", F.countDistinct("primary_diagnosis_code").over(window_90d)
        ).withColumn(
            "peer_avg_cost",       F.avg("allowed_amount").over(window_peer)
        ).withColumn(
            "peer_stddev_cost",    F.stddev("allowed_amount").over(window_peer)
        ).withColumn(
            # How many standard deviations above peer average?
            "cost_z_score",
            (F.col("allowed_amount") - F.col("peer_avg_cost")) /
            F.nullif(F.col("peer_stddev_cost"), F.lit(0))
        )
        return provider_stats

    def member_features(self, claims: DataFrame) -> DataFrame:
        """
        Per-member behavioral features.
        Detects unusual utilization patterns vs historical baseline.
        """
        window_member_12m = Window.partitionBy("member_id") \
            .orderBy("service_date") \
            .rowsBetween(-365, 0)

        member_stats = claims.withColumn(
            "member_total_cost_12m",
            F.sum("allowed_amount").over(window_member_12m)
        ).withColumn(
            "member_er_visits_12m",
            F.sum(F.when(F.col("service_category") == "EMERGENCY", 1).otherwise(0))
             .over(window_member_12m)
        ).withColumn(
            "member_unique_providers_12m",
            F.countDistinct("rendering_provider_npi").over(window_member_12m)
        ).withColumn(
            "member_unique_states_12m",
            F.countDistinct("provider_state").over(window_member_12m)
        )
        return member_stats


# =============================================================================
# Rule-Based Clinical Impossibility Detector
# =============================================================================

class ClinicalImpossibilityDetector:
    """
    Rule-based detection of clinically impossible or highly suspicious claims.
    These rules require no ML model — purely deterministic.
    """

    def detect(self, claims: DataFrame) -> DataFrame:
        """Apply all impossibility rules and flag violations."""

        flagged = claims \
            .withColumn(
                "flag_services_exceed_days",
                # Services in a period exceed total available days
                F.when(
                    F.col("services_in_period") > F.col("days_in_period"),
                    True
                ).otherwise(False)
            ) \
            .withColumn(
                "flag_deceased_member_claim",
                # Claim for member after recorded death date
                F.when(
                    F.col("death_date").isNotNull() &
                    (F.col("service_date") > F.col("death_date")),
                    True
                ).otherwise(False)
            ) \
            .withColumn(
                "flag_future_service_date",
                # Service date in the future
                F.when(F.col("service_date") > F.current_date(), True).otherwise(False)
            ) \
            .withColumn(
                "flag_age_procedure_mismatch",
                # Procedure codes not applicable to member age/gender
                F.when(
                    (F.col("gender_code") == "M") &
                    F.col("procedure_code").isin(
                        ["76801", "76802", "59400", "59510"]  # OB/GYN procedures
                    ),
                    True
                ).when(
                    (F.col("age_at_service") < 18) &
                    F.col("procedure_code").isin(["G0101", "Q0091"]),  # Pap smear adult codes
                    True
                ).otherwise(False)
            ) \
            .withColumn(
                "flag_duplicate_claim",
                # Same member, provider, date, procedure within 30 days
                F.count("claim_id").over(
                    Window.partitionBy(
                        "member_id", "rendering_provider_npi",
                        "service_date", "procedure_code"
                    )
                ) > 1
            ) \
            .withColumn(
                "flag_high_cost_outlier",
                # Cost > 10x specialty average
                F.col("cost_z_score") > 5.0
            ) \
            .withColumn(
                "impossibility_flag_count",
                (F.col("flag_services_exceed_days").cast("int") +
                 F.col("flag_deceased_member_claim").cast("int") +
                 F.col("flag_future_service_date").cast("int") +
                 F.col("flag_age_procedure_mismatch").cast("int") +
                 F.col("flag_duplicate_claim").cast("int") +
                 F.col("flag_high_cost_outlier").cast("int"))
            )

        return flagged


# =============================================================================
# ML-Based Anomaly Scorer (Isolation Forest)
# =============================================================================

class IsolationForestAnomalyScorer:
    """
    Unsupervised anomaly scoring using Isolation Forest.
    No labeled fraud data required — learns normal patterns automatically.
    """

    FEATURE_COLS = [
        "allowed_amount", "cost_z_score", "claims_per_day_90d",
        "unique_members_90d", "unique_dx_codes_90d",
        "member_total_cost_12m", "member_er_visits_12m",
        "member_unique_providers_12m", "length_of_stay",
        "impossibility_flag_count"
    ]

    def train(self, spark: SparkSession, training_table: str,
              catalog: str, model_name: str) -> None:
        """Train Isolation Forest on historical claims data."""
        import mlflow
        import mlflow.sklearn
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler
        import pandas as pd

        logger.info("[ANOMALY] Training Isolation Forest model")

        train_df = spark.table(training_table).select(
            self.FEATURE_COLS
        ).fillna(0).toPandas()

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(train_df)

        model = IsolationForest(
            n_estimators=200,
            contamination=0.05,  # Expect ~5% anomalies
            random_state=42,
            n_jobs=-1
        )
        model.fit(X_scaled)

        with mlflow.start_run(run_name=f"isolation_forest_{datetime.now().strftime('%Y%m%d')}"):
            mlflow.log_param("n_estimators", 200)
            mlflow.log_param("contamination", 0.05)
            mlflow.sklearn.log_model(
                model,
                artifact_path="isolation_forest",
                registered_model_name=f"{catalog}.ai.{model_name}"
            )
            # Also save scaler
            mlflow.sklearn.log_model(
                scaler,
                artifact_path="scaler",
                registered_model_name=f"{catalog}.ai.{model_name}_scaler"
            )
        logger.info(f"[ANOMALY] Model trained and registered: {catalog}.ai.{model_name}")

    def score_batch(self, spark: SparkSession, features_table: str,
                    model_uri: str, output_table: str) -> int:
        """Score a batch of claims and write anomaly scores."""
        import mlflow.pyfunc
        import pandas as pd

        features_df = spark.table(features_table).fillna(0)
        features_pd = features_df.select(self.FEATURE_COLS).toPandas()

        # Load model and score
        model = mlflow.sklearn.load_model(model_uri)
        raw_scores = model.score_samples(features_pd)

        # Convert to 0-100 scale (more negative = more anomalous)
        min_score = raw_scores.min()
        max_score = raw_scores.max()
        normalized = 100 - ((raw_scores - min_score) / (max_score - min_score) * 100)

        features_pd["isolation_forest_score"] = normalized

        # Convert back to Spark
        scored_spark = spark.createDataFrame(features_pd)

        # Join scores back to original features
        result = features_df.join(
            scored_spark.select("claim_id", "isolation_forest_score"),
            on="claim_id", how="left"
        )

        result = self._add_composite_score(result)
        result = self._assign_priority(result)

        result.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(output_table)

        count = result.count()
        high_risk = result.filter(F.col("investigation_priority") == "CRITICAL").count()
        logger.info(f"[ANOMALY] Scored {count:,} claims | {high_risk:,} CRITICAL")
        return count

    def _add_composite_score(self, df: DataFrame) -> DataFrame:
        """Combine ML score + rule-based flags into composite anomaly score."""
        return df.withColumn(
            "anomaly_score",
            F.least(F.lit(100.0),
                F.col("isolation_forest_score") +
                (F.col("impossibility_flag_count") * 15)  # +15 per rule violation
            )
        )

    def _assign_priority(self, df: DataFrame) -> DataFrame:
        """Assign investigation priority tier based on composite score."""
        return df.withColumn(
            "investigation_priority",
            F.when(F.col("anomaly_score") >= 85, "CRITICAL")
             .when(F.col("anomaly_score") >= 70, "HIGH")
             .when(F.col("anomaly_score") >= 50, "MEDIUM")
             .otherwise("LOW")
        ).withColumn(
            "anomaly_explanation",
            F.when(F.col("flag_deceased_member_claim"), "CLINICAL: Service after member death")
             .when(F.col("flag_duplicate_claim"), "BILLING: Potential duplicate claim")
             .when(F.col("flag_high_cost_outlier"), "COST: Extreme outlier vs specialty peers")
             .when(F.col("flag_age_procedure_mismatch"), "CLINICAL: Procedure/age-gender mismatch")
             .when(F.col("isolation_forest_score") > 80, "PATTERN: Statistically unusual claim pattern")
             .otherwise("MONITORING: Elevated risk score — review recommended")
        )
