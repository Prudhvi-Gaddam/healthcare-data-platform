# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Provider Bronze Ingestion

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
df_raw = spark.read.parquet(f"{ADLS_BASE}/provider/master/")

df_stamped = df_raw \
    .withColumn("_source_id",   F.lit("provider_master")) \
    .withColumn("_ingested_at", F.current_timestamp()) \
    .withColumn("_batch_date",  F.lit(RUN_DATE).cast("date")) \
    .withColumn("_environment", F.lit(ENV))

dq = DataQualityFramework("provider_master", RUN_DATE)
rules = [
    DQRule("not_null",    ["provider_npi", "provider_name"], severity="ERROR"),
    DQRule("valid_values", ["network_status"],
           params={"values": ["IN_NETWORK","OUT_OF_NETWORK","PENDING","TERMINATED"]},
           severity="WARN"),
]
dq_result = dq.run_rules(df_stamped, rules)
if not dq_result["passed"]:
    raise Exception("DQ FAILED on provider data")

masker = PHIMaskingEngine()
phi_cols = masker.detect_phi_columns(df_stamped, ["provider_name", "tax_id"])
df_masked = masker.mask_phi(df_stamped, phi_cols, mode="hash")

df_masked.write.format("delta").mode("append") \
    .saveAsTable(f"{CATALOG}.bronze.provider_master")

print(f"[BRONZE] Provider written: {df_masked.count():,} rows | DQ: {dq_result['dq_score']}")
dbutils.notebook.exit(str(df_masked.count()))
