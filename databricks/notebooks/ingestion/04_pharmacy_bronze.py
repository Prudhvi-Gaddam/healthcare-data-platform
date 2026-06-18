# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Pharmacy (Rx) Bronze Ingestion

# COMMAND ----------
from pyspark.sql import functions as F
import sys
sys.path.insert(0, "/HealthcarePlatform/framework")
from ingestion.healthcare_ingestion import PHIMaskingEngine, DataQualityFramework, DQRule

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("run_date",    "")
ENV      = dbutils.widgets.get("environment")
RUN_DATE = dbutils.widgets.get("run_date")
CATALOG  = f"healthcare_{ENV}"
ADLS_BASE = f"abfss://bronze@adlshealthcare{ENV}.dfs.core.windows.net"

# COMMAND ----------
df_raw = spark.read.parquet(f"{ADLS_BASE}/pharmacy/claims/")

df_stamped = df_raw \
    .withColumn("_source_id",   F.lit("pharmacy_claims")) \
    .withColumn("_ingested_at", F.current_timestamp()) \
    .withColumn("_batch_date",  F.lit(RUN_DATE).cast("date")) \
    .withColumn("_environment", F.lit(ENV)) \
    .withColumn("days_supply",  F.col("days_supply").cast("int")) \
    .withColumn("quantity",     F.col("quantity").cast("double")) \
    .withColumn("fill_date",    F.col("fill_date").cast("date"))

dq = DataQualityFramework("pharmacy_claims", RUN_DATE)
rules = [
    DQRule("not_null",   ["rx_claim_id","member_id","ndc_code","fill_date"], severity="ERROR"),
    DQRule("row_count",  [], params={"min": 10}, severity="WARN"),
]
dq_result = dq.run_rules(df_stamped, rules)
if not dq_result["passed"]:
    raise Exception("DQ FAILED on pharmacy data")

masker = PHIMaskingEngine()
phi_cols = masker.detect_phi_columns(df_stamped, ["member_id", "prescriber_npi"])
df_masked = masker.mask_phi(df_stamped, phi_cols, mode="hash")

df_masked.write.format("delta").mode("append") \
    .partitionBy("_batch_date") \
    .saveAsTable(f"{CATALOG}.bronze.pharmacy_claims")

print(f"[BRONZE] Pharmacy written: {df_masked.count():,} rows | DQ: {dq_result['dq_score']}")
dbutils.notebook.exit(str(df_masked.count()))
