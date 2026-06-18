# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Eligibility Bronze Ingestion
# MAGIC Ingests member eligibility and enrollment data into Bronze Delta layer.
# MAGIC Handles SCD Type 2 changes for member plan enrollments.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
sys.path.insert(0, "/HealthcarePlatform/framework")
from ingestion.healthcare_ingestion import PHIMaskingEngine, DataQualityFramework, DQRule

dbutils.widgets.text("environment",    "dev")
dbutils.widgets.text("run_date",       "")
dbutils.widgets.text("records_copied", "0")

ENV        = dbutils.widgets.get("environment")
RUN_DATE   = dbutils.widgets.get("run_date")
CATALOG    = f"healthcare_{ENV}"
ADLS_BASE  = f"abfss://bronze@adlshealthcare{ENV}.dfs.core.windows.net"

# COMMAND ----------
# MAGIC %md ## 1. Read from ADF landing zone

df_raw = spark.read.parquet(f"{ADLS_BASE}/eligibility/member_enrollment/")
print(f"[ELIGIBILITY] Records: {df_raw.count():,}")

# COMMAND ----------
# MAGIC %md ## 2. Add metadata + SCD Type 2 columns

df_stamped = df_raw \
    .withColumn("_source_id",    F.lit("member_eligibility")) \
    .withColumn("_ingested_at",  F.current_timestamp()) \
    .withColumn("_batch_date",   F.lit(RUN_DATE).cast("date")) \
    .withColumn("_environment",  F.lit(ENV)) \
    .withColumn("scd_effective_date", F.lit(RUN_DATE).cast("date")) \
    .withColumn("scd_expiry_date",    F.lit("9999-12-31").cast("date")) \
    .withColumn("scd_is_current",     F.lit(True)) \
    .withColumn("scd_version",        F.lit(1)) \
    .withColumn("plan_year",
        F.year(F.coalesce(F.col("effective_date"), F.current_date()))
    ) \
    .withColumn("age_as_of_dec31",
        F.floor(F.datediff(
            F.make_date(F.year(F.current_date()), F.lit(12), F.lit(31)),
            F.col("birth_date").cast("date")
        ) / 365.25)
    )

# COMMAND ----------
# MAGIC %md ## 3. DQ Validation

dq = DataQualityFramework("member_eligibility", RUN_DATE)
rules = [
    DQRule("not_null",    ["member_id", "plan_id", "effective_date"], severity="ERROR"),
    DQRule("valid_values", ["metal_tier"],
           params={"values": ["BRONZE","SILVER","GOLD","PLATINUM","CATASTROPHIC",""]},
           severity="WARN"),
    DQRule("row_count",   [], params={"min": 10}, severity="WARN"),
]
dq_result = dq.run_rules(df_stamped, rules)
print(f"[DQ] Score: {dq_result['dq_score']}")
if not dq_result["passed"]:
    raise Exception("DQ FAILED on eligibility data")

# COMMAND ----------
# MAGIC %md ## 4. PHI Masking

masker = PHIMaskingEngine()
phi_cols = masker.detect_phi_columns(df_stamped, ["member_id","subscriber_id","birth_date"])
df_masked = masker.mask_phi(df_stamped, phi_cols, mode="hash")

# COMMAND ----------
# MAGIC %md ## 5. Write Bronze Delta

df_masked.write.format("delta") \
    .mode("append") \
    .partitionBy("plan_year", "_batch_date") \
    .saveAsTable(f"{CATALOG}.bronze.member_eligibility")

print(f"[BRONZE] Eligibility written: {df_masked.count():,} rows → {CATALOG}.bronze.member_eligibility")
dbutils.notebook.exit(str(df_masked.count()))
