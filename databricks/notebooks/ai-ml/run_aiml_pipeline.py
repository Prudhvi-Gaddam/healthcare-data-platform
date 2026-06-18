# Databricks notebook source
# MAGIC %md
# MAGIC # AI/ML Pipeline Orchestrator
# MAGIC Runs all four AI products sequentially after Gold layer is ready:
# MAGIC 1. Readmission Risk Scoring
# MAGIC 2. Claims Anomaly Detection
# MAGIC 3. NLP Clinical Notes Processing
# MAGIC 4. RAG Vector Store Refresh

# COMMAND ----------
from pyspark.sql import functions as F
from datetime import datetime
import sys
sys.path.insert(0, "/HealthcarePlatform")

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("run_date",    "")
ENV      = dbutils.widgets.get("environment")
RUN_DATE = dbutils.widgets.get("run_date") or str(datetime.today().date())
CATALOG  = f"healthcare_{ENV}"

print(f"[AI/ML] Starting AI pipeline | Env: {ENV} | Date: {RUN_DATE}")

# COMMAND ----------
# MAGIC %md ## 1. Readmission Risk Scoring

from ai-ml.readmission-risk.risk_model import ReadmissionRiskModel, ReadmissionFeatureEngineering

feature_eng = ReadmissionFeatureEngineering(spark, CATALOG)

# Get recent discharges (last 7 days)
recent_discharges = spark.table(f"{CATALOG}.gold.claims_fact") \
    .filter(F.col("service_category") == "INPATIENT") \
    .filter(F.col("discharge_date") >= F.date_sub(F.current_date(), 7)) \
    .filter(F.col("claim_status").isin(["PAID", "ADJUSTED"])) \
    .select("claim_id", "member_id", "admit_date",
            "discharge_date", "drg_code", "primary_diagnosis_code",
            "admit_type_code", "discharge_disposition_code")

admission_count = recent_discharges.count()
print(f"[READMISSION] Scoring {admission_count:,} recent discharges")

if admission_count > 0:
    features = feature_eng.compute_all_features(recent_discharges)
    model = ReadmissionRiskModel(spark, CATALOG, f"/HealthcarePlatform/mlflow/readmission")
    scored = model.score_batch(
        features_table=f"{CATALOG}.gold.readmission_features",
        output_table=f"{CATALOG}.gold.readmission_risk_scores",
        model_uri=f"models:/{CATALOG}.ai.readmission_risk_model/Production"
    )
    alerts = model.generate_care_alerts(
        scores_table=f"{CATALOG}.gold.readmission_risk_scores",
        alert_threshold="HIGH"
    )
    print(f"[READMISSION] ✅ Scored: {scored:,} | Care alerts: {alerts:,}")

# COMMAND ----------
# MAGIC %md ## 2. Claims Anomaly Detection

from ai-ml.anomaly-detection.anomaly_model import IsolationForestAnomalyScorer, ClinicalImpossibilityDetector

print("[ANOMALY] Running claims anomaly detection...")

detector = ClinicalImpossibilityDetector()
scorer = IsolationForestAnomalyScorer()

todays_claims = spark.table(f"{CATALOG}.gold.claims_fact") \
    .filter(F.col("_batch_date") == RUN_DATE)

if todays_claims.count() > 0:
    flagged = detector.detect(todays_claims)
    scored_count = scorer.score_batch(
        spark=spark,
        features_table=f"{CATALOG}.gold.claims_anomaly_features",
        model_uri=f"models:/{CATALOG}.ai.isolation_forest/Production",
        output_table=f"{CATALOG}.gold.claims_anomaly_scores"
    )
    critical = spark.table(f"{CATALOG}.gold.claims_anomaly_scores") \
        .filter(F.col("investigation_priority") == "CRITICAL").count()
    print(f"[ANOMALY] ✅ Scored: {scored_count:,} | Critical flags: {critical:,}")

# COMMAND ----------
# MAGIC %md ## 3. NLP Clinical Notes Processing

from ai-ml.nlp-clinical-notes.nlp_clinical_notes import ClinicalNLPPipeline

print("[NLP] Processing clinical notes...")

nlp = ClinicalNLPPipeline(spark, CATALOG)
notes_count = nlp.process_notes_batch(
    notes_table=f"{CATALOG}.bronze.clinical_notes",
    output_table=f"{CATALOG}.gold.clinical_notes_processed"
)
print(f"[NLP] ✅ Processed {notes_count:,} clinical notes")

# COMMAND ----------
# MAGIC %md ## 4. Pipeline Summary

summary = {
    "run_date":          RUN_DATE,
    "environment":       ENV,
    "readmission_scored": admission_count,
    "care_alerts":       alerts if admission_count > 0 else 0,
    "anomaly_scored":    scored_count if todays_claims.count() > 0 else 0,
    "notes_processed":   notes_count,
    "completed_at":      str(datetime.now())
}

print("\n[AI/ML SUMMARY]")
for k, v in summary.items():
    print(f"  {k}: {v}")

spark.createDataFrame([summary]) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{CATALOG}.audit.aiml_pipeline_runs")
