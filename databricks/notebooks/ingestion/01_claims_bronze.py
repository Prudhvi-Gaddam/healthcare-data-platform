# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Claims Bronze Ingestion
# MAGIC Converts raw Parquet landing files (dropped by ADF Copy Activity)
# MAGIC into validated, PHI-masked Delta tables in the Bronze layer.
# MAGIC Called by ADF pipeline: pl_bronze_claims

# COMMAND ----------
from pyspark.sql import functions as F
import sys
sys.path.insert(0, "/HealthcarePlatform/framework")
from ingestion.healthcare_ingestion import HealthcareIngestionFramework, SourceConfig, DQRule, PHIMaskingEngine, DataQualityFramework

dbutils.widgets.text("environment",    "dev")
dbutils.widgets.text("run_date",       "")
dbutils.widgets.text("source_path",    "")
dbutils.widgets.text("records_copied", "0")
dbutils.widgets.text("watermark_from", "")

ENV           = dbutils.widgets.get("environment")
RUN_DATE      = dbutils.widgets.get("run_date")
SOURCE_PATH   = dbutils.widgets.get("source_path")
RECORDS_IN    = int(dbutils.widgets.get("records_copied"))
WATERMARK     = dbutils.widgets.get("watermark_from")
CATALOG       = f"healthcare_{ENV}"
ADLS_BASE     = f"abfss://bronze@adlshealthcare{ENV}.dfs.core.windows.net"

print(f"[CLAIMS BRONZE] Run: {RUN_DATE} | Records from ADF: {RECORDS_IN:,} | Watermark: {WATERMARK}")

# COMMAND ----------
# MAGIC %md ## 1. Read raw Parquet from ADF landing

df_raw = spark.read.parquet(f"{ADLS_BASE}/{SOURCE_PATH}")
print(f"[READ] {df_raw.count():,} rows from {SOURCE_PATH}")

# COMMAND ----------
# MAGIC %md ## 2. Add ingestion metadata

df_stamped = df_raw \
    .withColumn("_source_id",      F.lit("claims_professional")) \
    .withColumn("_schema_version", F.lit("v3")) \
    .withColumn("_ingested_at",    F.current_timestamp()) \
    .withColumn("_batch_date",     F.lit(RUN_DATE).cast("date")) \
    .withColumn("_environment",    F.lit(ENV)) \
    .withColumn("_is_deleted",     F.lit(False)) \
    .withColumn("_watermark_from", F.lit(WATERMARK))

# COMMAND ----------
# MAGIC %md ## 3. Data Quality Validation

dq = DataQualityFramework("claims_professional", RUN_DATE)
rules = [
    DQRule("not_null",    ["claim_id", "member_id", "service_date", "claim_status"], severity="ERROR"),
    DQRule("valid_values", ["claim_status"],
           params={"values": ["PAID","DENIED","PENDING","ADJUSTED","VOID","SUSPENDED"]},
           severity="ERROR"),
    DQRule("date_range",  ["service_date"],
           params={"min": "2015-01-01", "max": "2030-12-31"}, severity="ERROR"),
    DQRule("not_null",    ["allowed_amount", "plan_paid_amount"], severity="WARN"),
    DQRule("row_count",   [], params={"min": 100}, severity="WARN"),
]
dq_result = dq.run_rules(df_stamped, rules)
print(f"[DQ] Score: {dq_result['dq_score']} | Passed: {dq_result['rules_passed']}/{dq_result['rules_run']}")
if not dq_result["passed"]:
    raise Exception(f"DQ FAILED: {dq_result['critical_failures']} critical rules failed")

# COMMAND ----------
# MAGIC %md ## 4. PHI Masking

masker = PHIMaskingEngine()
phi_cols = masker.detect_phi_columns(df_stamped, ["member_id", "subscriber_id", "rendering_provider_npi", "billing_provider_npi"])
df_masked = masker.mask_phi(df_stamped, phi_cols, mode="hash")
print(f"[PHI] Masked {len(phi_cols)} columns: {phi_cols}")

# COMMAND ----------
# MAGIC %md ## 5. Write to Bronze Delta

target = f"{CATALOG}.bronze.claims_professional"
df_masked.write.format("delta") \
    .mode("append") \
    .partitionBy("_batch_date") \
    .saveAsTable(target)

final_count = df_masked.count()
print(f"[BRONZE] Written {final_count:,} rows → {target}")
print(f"[COMPLETE] DQ Score: {dq_result['dq_score']} | PHI masked: {len(phi_cols)} columns")

dbutils.notebook.exit(str(final_count))
