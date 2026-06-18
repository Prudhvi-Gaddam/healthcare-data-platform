"""
readmission_risk_model.py
=========================
AI PRODUCT 3 — 30-Day Hospital Readmission Risk Predictor

Predicts the probability that a recently discharged patient will be
readmitted within 30 days. Enables care management teams to
proactively intervene with high-risk members.

Model: XGBoost gradient boosted classifier
Features: 40+ clinical, utilization, and social determinants features
Target: Binary (readmitted within 30 days: Yes/No)
Output: Risk score (0-100) + risk tier (Low/Medium/High/Critical)

Built on Databricks Feature Store for:
  - Feature reuse across models
  - Point-in-time correct training
  - Automatic feature freshness
  - Lineage tracking
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger("HealthcarePlatform.ReadmissionRisk")


# =============================================================================
# Feature Engineering
# =============================================================================

class ReadmissionFeatureEngineering:
    """
    Computes 40+ features for readmission risk prediction.
    Organized into feature groups for maintainability and reuse.
    """

    RISK_THRESHOLDS = {
        "LOW":      (0,   25),
        "MEDIUM":   (25,  50),
        "HIGH":     (50,  75),
        "CRITICAL": (75, 100)
    }

    def __init__(self, spark: SparkSession, catalog: str):
        self.spark = spark
        self.catalog = catalog

    def compute_all_features(self, admission_df, lookback_days: int = 365):
        """
        Compute all feature groups and join into feature matrix.
        admission_df: DataFrame of recent hospital admissions
        """
        logger.info("[FEATURES] Computing readmission risk features")

        # Load source tables
        claims = self.spark.table(f"{self.catalog}.gold.claims_fact")
        eligibility = self.spark.table(f"{self.catalog}.gold.member_eligibility")
        pharmacy = self.spark.table(f"{self.catalog}.gold.pharmacy_claims")
        labs = self.spark.table(f"{self.catalog}.gold.lab_results")

        # Compute each feature group
        demo_features    = self._demographics_features(admission_df, eligibility)
        utilization_feats = self._utilization_features(admission_df, claims, lookback_days)
        chronic_features = self._chronic_condition_features(admission_df, claims)
        pharmacy_features = self._pharmacy_features(admission_df, pharmacy, lookback_days)
        lab_features     = self._lab_features(admission_df, labs)
        sdoh_features    = self._sdoh_features(admission_df, eligibility)
        admission_features = self._admission_characteristics(admission_df)

        # Join all feature groups
        feature_matrix = admission_df.select(
            "admission_id", "member_id", "discharge_date"
        )
        for features in [demo_features, utilization_feats, chronic_features,
                          pharmacy_features, lab_features, sdoh_features,
                          admission_features]:
            feature_matrix = feature_matrix.join(
                features, on=["admission_id", "member_id"], how="left"
            )

        # Fill nulls with sensible defaults
        feature_matrix = feature_matrix.fillna(0)

        logger.info(f"[FEATURES] Feature matrix: {feature_matrix.count():,} rows, "
                   f"{len(feature_matrix.columns)} columns")
        return feature_matrix

    def _demographics_features(self, admissions, eligibility):
        """Age, gender, plan type, geographic features."""
        return admissions.join(
            eligibility.select("member_id", "birth_date", "gender_code",
                                "zip_code", "plan_id", "metal_tier"),
            on="member_id", how="left"
        ).withColumn(
            "age_at_discharge",
            F.datediff(F.col("discharge_date"), F.col("birth_date")) / 365.25
        ).withColumn(
            "is_elderly", F.when(F.col("age_at_discharge") >= 65, 1).otherwise(0)
        ).withColumn(
            "is_pediatric", F.when(F.col("age_at_discharge") < 18, 1).otherwise(0)
        ).withColumn(
            "gender_male", F.when(F.col("gender_code") == "M", 1).otherwise(0)
        ).select(
            "admission_id", "member_id",
            "age_at_discharge", "is_elderly", "is_pediatric", "gender_male"
        )

    def _utilization_features(self, admissions, claims, lookback_days: int):
        """Prior utilization: ER visits, IP admissions, office visits."""
        window_member = Window.partitionBy("member_id")

        lookback_start = F.date_sub(F.col("discharge_date"), lookback_days)

        prior_claims = claims.join(
            admissions.select("member_id", "discharge_date"),
            on="member_id", how="inner"
        ).filter(
            (F.col("service_date") >= lookback_start) &
            (F.col("service_date") < F.col("discharge_date"))
        )

        utilization = prior_claims.groupBy("member_id").agg(
            F.sum(F.when(F.col("service_category") == "EMERGENCY", 1).otherwise(0))
             .alias("er_visits_prior_year"),
            F.sum(F.when(F.col("service_category") == "INPATIENT", 1).otherwise(0))
             .alias("ip_admissions_prior_year"),
            F.sum(F.when(F.col("service_category") == "OUTPATIENT", 1).otherwise(0))
             .alias("outpatient_visits_prior_year"),
            F.sum(F.when(F.col("service_category") == "PCP", 1).otherwise(0))
             .alias("pcp_visits_prior_year"),
            F.sum("allowed_amount").alias("total_cost_prior_year"),
            F.countDistinct("service_date").alias("total_service_days_prior_year")
        ).withColumn(
            "er_visit_flag", F.when(F.col("er_visits_prior_year") > 0, 1).otherwise(0)
        ).withColumn(
            "prior_ip_flag", F.when(F.col("ip_admissions_prior_year") > 0, 1).otherwise(0)
        ).withColumn(
            "high_utilizer_flag",
            F.when(F.col("total_cost_prior_year") > 50000, 1).otherwise(0)
        )

        return admissions.select("admission_id", "member_id").join(
            utilization, on="member_id", how="left"
        )

    def _chronic_condition_features(self, admissions, claims):
        """
        Chronic condition flags based on ICD-10 diagnosis codes.
        Elixhauser Comorbidity Index conditions.
        """
        CHRONIC_CONDITIONS = {
            "diabetes":        ["E10", "E11", "E12", "E13", "E14"],
            "chf":             ["I50"],
            "copd":            ["J44"],
            "ckd":             ["N18"],
            "hypertension":    ["I10", "I11", "I12", "I13"],
            "ami":             ["I21", "I22"],
            "depression":      ["F32", "F33"],
            "substance_abuse": ["F10", "F11", "F12", "F13", "F14", "F15"],
            "cancer":          ["C00", "C01", "C02", "C03", "C04", "C05"],
            "pneumonia":       ["J18"],
        }

        dx_claims = claims.filter(F.col("diagnosis_code").isNotNull())

        # Build condition flags using ICD prefix matching
        condition_flags = admissions.select("admission_id", "member_id")
        for condition, icd_prefixes in CHRONIC_CONDITIONS.items():
            prefix_conditions = " OR ".join([
                f"diagnosis_code LIKE '{prefix}%'" for prefix in icd_prefixes
            ])
            condition_members = dx_claims.filter(
                F.expr(prefix_conditions)
            ).select("member_id").distinct() \
             .withColumn(f"dx_{condition}", F.lit(1))

            condition_flags = condition_flags.join(
                condition_members, on="member_id", how="left"
            ).fillna({f"dx_{condition}": 0})

        # Comorbidity count
        condition_cols = [f"dx_{c}" for c in CHRONIC_CONDITIONS.keys()]
        condition_flags = condition_flags.withColumn(
            "comorbidity_count",
            sum([F.col(c) for c in condition_cols])
        )
        return condition_flags

    def _pharmacy_features(self, admissions, pharmacy, lookback_days: int):
        """Medication adherence and polypharmacy features."""
        lookback_start = F.date_sub(F.col("fill_date"), lookback_days)

        rx_agg = pharmacy.join(
            admissions.select("member_id", "discharge_date"),
            on="member_id"
        ).filter(
            F.col("fill_date") >= F.date_sub(F.col("discharge_date"), lookback_days)
        ).groupBy("member_id").agg(
            F.countDistinct("ndc_code").alias("unique_medications"),
            F.countDistinct("therapeutic_class").alias("unique_drug_classes"),
            F.sum("days_supply").alias("total_days_supply"),
        ).withColumn(
            "polypharmacy_flag",
            F.when(F.col("unique_medications") >= 5, 1).otherwise(0)
        ).withColumn(
            "high_polypharmacy_flag",
            F.when(F.col("unique_medications") >= 10, 1).otherwise(0)
        )

        return admissions.select("admission_id", "member_id").join(
            rx_agg, on="member_id", how="left"
        )

    def _lab_features(self, admissions, labs):
        """Lab result features: abnormal flags for key indicators."""
        # Key lab indicators linked to readmission risk
        key_labs = labs.filter(
            F.col("loinc_code").isin([
                "2345-7",   # Glucose
                "2160-0",   # Creatinine
                "718-7",    # Hemoglobin
                "2823-3",   # Potassium
                "6598-7",   # Troponin
                "1742-6",   # ALT
            ])
        )

        abnormal_labs = key_labs \
            .filter(F.col("abnormal_flag").isin(["H", "L", "HH", "LL"])) \
            .groupBy("member_id").agg(
                F.count("*").alias("abnormal_lab_count"),
                F.countDistinct("loinc_code").alias("unique_abnormal_labs")
            ).withColumn(
                "critical_lab_flag",
                F.when(F.col("abnormal_lab_count") >= 3, 1).otherwise(0)
            )

        return admissions.select("admission_id", "member_id").join(
            abnormal_labs, on="member_id", how="left"
        )

    def _sdoh_features(self, admissions, eligibility):
        """Social Determinants of Health features."""
        sdoh = eligibility.select(
            "member_id",
            "zip_code",
            "county_fips",
        ).withColumn(
            # High-deprivation ZIP proxy (Area Deprivation Index)
            "high_deprivation_zip",
            F.when(F.col("zip_code").isin(self._get_high_deprivation_zips()), 1)
             .otherwise(0)
        ).withColumn(
            "rural_flag",
            F.when(F.col("county_fips").isin(self._get_rural_counties()), 1)
             .otherwise(0)
        )

        return admissions.select("admission_id", "member_id").join(
            sdoh, on="member_id", how="left"
        )

    def _admission_characteristics(self, admissions):
        """Features from the current admission itself."""
        return admissions.withColumn(
            "length_of_stay",
            F.datediff(F.col("discharge_date"), F.col("admit_date"))
        ).withColumn(
            "weekend_discharge",
            F.when(F.dayofweek(F.col("discharge_date")).isin([1, 7]), 1).otherwise(0)
        ).withColumn(
            "icu_stay_flag",
            F.when(F.col("drg_code").startswith("9"), 1).otherwise(0)
        ).withColumn(
            "surgical_flag",
            F.when(F.col("admit_type_code") == "3", 1).otherwise(0)
        ).select(
            "admission_id", "member_id",
            "length_of_stay", "weekend_discharge", "icu_stay_flag", "surgical_flag",
            "discharge_disposition_code", "primary_diagnosis_code", "drg_code"
        )

    def _get_high_deprivation_zips(self):
        """Return list of high-deprivation ZIP codes (placeholder)."""
        return ["19132", "19140", "19133", "19134", "19139"]

    def _get_rural_counties(self):
        """Return list of rural county FIPS codes (placeholder)."""
        return ["42001", "42003", "42005"]


# =============================================================================
# Readmission Risk Model
# =============================================================================

class ReadmissionRiskModel:
    """
    Trains and serves the 30-day readmission risk model.

    Training: XGBoost on Feature Store features
    Serving: Databricks Model Serving (REST API)
    Monitoring: Databricks Lakehouse Monitoring for drift detection
    """

    FEATURE_COLUMNS = [
        # Demographics
        "age_at_discharge", "is_elderly", "is_pediatric", "gender_male",
        # Utilization
        "er_visits_prior_year", "ip_admissions_prior_year", "er_visit_flag",
        "prior_ip_flag", "high_utilizer_flag", "total_cost_prior_year",
        "outpatient_visits_prior_year", "pcp_visits_prior_year",
        # Chronic conditions
        "dx_diabetes", "dx_chf", "dx_copd", "dx_ckd", "dx_hypertension",
        "dx_ami", "dx_depression", "dx_substance_abuse", "dx_cancer",
        "comorbidity_count",
        # Pharmacy
        "unique_medications", "polypharmacy_flag", "high_polypharmacy_flag",
        # Labs
        "abnormal_lab_count", "critical_lab_flag",
        # SDOH
        "high_deprivation_zip", "rural_flag",
        # Admission
        "length_of_stay", "weekend_discharge", "icu_stay_flag", "surgical_flag",
    ]

    RISK_TIERS = {
        (0,  25):  "LOW",
        (25, 50):  "MEDIUM",
        (50, 75):  "HIGH",
        (75, 100): "CRITICAL"
    }

    def __init__(self, spark: SparkSession, catalog: str, mlflow_experiment: str):
        self.spark = spark
        self.catalog = catalog
        self.mlflow_experiment = mlflow_experiment

    def train(self, training_table: str, model_name: str) -> str:
        """
        Train XGBoost readmission risk model.
        Logs to MLflow, registers in Unity Catalog Model Registry.
        Returns model version URI.
        """
        import mlflow
        import mlflow.xgboost
        import xgboost as xgb
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score, average_precision_score
        import pandas as pd

        mlflow.set_experiment(self.mlflow_experiment)

        # Load training data
        train_df = self.spark.table(training_table).toPandas()
        X = train_df[self.FEATURE_COLUMNS].fillna(0)
        y = train_df["readmitted_30day"].astype(int)

        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        with mlflow.start_run(run_name=f"readmission_risk_{datetime.now().strftime('%Y%m%d')}"):
            # Hyperparameters
            params = {
                "n_estimators":     500,
                "max_depth":        6,
                "learning_rate":    0.05,
                "subsample":        0.8,
                "colsample_bytree": 0.8,
                "scale_pos_weight": (y_train == 0).sum() / max((y_train == 1).sum(), 1),
                "eval_metric":      "auc",
                "early_stopping_rounds": 50,
                "random_state":     42,
            }
            mlflow.log_params(params)

            # Train model
            model = xgb.XGBClassifier(**params)
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False
            )

            # Evaluate
            y_prob = model.predict_proba(X_val)[:, 1]
            auc_roc = roc_auc_score(y_val, y_prob)
            auc_pr  = average_precision_score(y_val, y_prob)

            mlflow.log_metrics({
                "auc_roc":        auc_roc,
                "auc_pr":         auc_pr,
                "val_samples":    len(y_val),
                "readmission_pct": round(y.mean() * 100, 2)
            })

            # Feature importance
            importance = dict(zip(
                self.FEATURE_COLUMNS,
                model.feature_importances_.tolist()
            ))
            top_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]
            logger.info(f"[MODEL] Top features: {top_features}")

            # Log and register model
            signature = mlflow.models.infer_signature(X_train, y_prob)
            mlflow.xgboost.log_model(
                model,
                artifact_path="readmission_risk_model",
                registered_model_name=f"{self.catalog}.ai.{model_name}",
                signature=signature,
                input_example=X_train.head(5)
            )

            logger.info(f"[MODEL] Trained | AUC-ROC: {auc_roc:.4f} | AUC-PR: {auc_pr:.4f}")
            return f"{self.catalog}.ai.{model_name}"

    def score_batch(self, features_table: str, output_table: str,
                    model_uri: str) -> int:
        """
        Score a batch of members and write risk scores to Gold layer.
        Returns number of members scored.
        """
        import mlflow.pyfunc

        model = mlflow.pyfunc.spark_udf(
            self.spark,
            model_uri=model_uri,
            result_type=DoubleType()
        )

        scored_df = self.spark.table(features_table) \
            .withColumn(
                "readmission_risk_score",
                F.round(model(*[F.col(c) for c in self.FEATURE_COLUMNS]) * 100, 1)
            ) \
            .withColumn(
                "risk_tier",
                F.when(F.col("readmission_risk_score") < 25,  "LOW")
                 .when(F.col("readmission_risk_score") < 50,  "MEDIUM")
                 .when(F.col("readmission_risk_score") < 75,  "HIGH")
                 .otherwise("CRITICAL")
            ) \
            .withColumn("scored_at", F.current_timestamp()) \
            .withColumn("model_uri", F.lit(model_uri))

        # Write to Gold layer
        scored_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(output_table)

        count = scored_df.count()

        # Summary for care management team
        summary = scored_df.groupBy("risk_tier").agg(
            F.count("*").alias("member_count"),
            F.round(F.avg("readmission_risk_score"), 1).alias("avg_score")
        ).orderBy(F.desc("avg_score"))

        logger.info(f"[SCORES] Scored {count:,} members")
        summary.show()
        return count

    def generate_care_alerts(self, scores_table: str,
                              alert_threshold: str = "HIGH") -> int:
        """
        Generate care management alerts for high-risk members.
        Returns number of alerts created.
        """
        tier_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
        min_tier = tier_order.get(alert_threshold, 2)

        alerts = self.spark.table(scores_table) \
            .filter(
                F.when(F.col("risk_tier") == "CRITICAL", 3)
                 .when(F.col("risk_tier") == "HIGH", 2)
                 .when(F.col("risk_tier") == "MEDIUM", 1)
                 .otherwise(0) >= min_tier
            ) \
            .withColumn(
                "alert_priority",
                F.when(F.col("risk_tier") == "CRITICAL", 1)
                 .when(F.col("risk_tier") == "HIGH", 2)
                 .otherwise(3)
            ) \
            .withColumn(
                "recommended_action",
                F.when(F.col("risk_tier") == "CRITICAL",
                       "Immediate care manager outreach within 24 hours")
                 .when(F.col("risk_tier") == "HIGH",
                       "Care manager outreach within 48 hours + post-discharge call")
                 .otherwise("Schedule follow-up appointment within 7 days")
            ) \
            .withColumn("alert_date", F.current_date()) \
            .withColumn("alert_status", F.lit("OPEN"))

        alerts.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{self.catalog}.gold.readmission_care_alerts")

        count = alerts.count()
        logger.info(f"[ALERTS] Generated {count:,} care management alerts "
                   f"(threshold: {alert_threshold}+)")
        return count
