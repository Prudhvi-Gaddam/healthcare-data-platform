# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Observability Dashboard
# MAGIC Real-time pipeline health, SLA monitoring, DQ scorecards.
# MAGIC Runs as the final step of pl_master_ingestion to update operational dashboards.

# COMMAND ----------
from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("run_date",    "")
dbutils.widgets.text("status",      "SUCCESS")

ENV      = dbutils.widgets.get("environment")
RUN_DATE = dbutils.widgets.get("run_date") or str(datetime.today().date())
STATUS   = dbutils.widgets.get("status")
CATALOG  = f"healthcare_{ENV}"

# COMMAND ----------
# MAGIC %md ## Pipeline Health Summary

pipeline_health = spark.table(f"{CATALOG}.audit.pipeline_runs") \
    .filter(F.col("run_date") == RUN_DATE) \
    .filter(F.col("environment") == ENV) \
    .groupBy("run_date", "environment") \
    .agg(
        F.count("*").alias("pipelines_run"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful"),
        F.sum(F.when(F.col("status") == "FAILED",  1).otherwise(0)).alias("failed"),
        F.sum(F.when(F.col("status") == "NO_DATA", 1).otherwise(0)).alias("no_data"),
        F.round(F.avg("dq_score"), 1).alias("avg_dq_score"),
        F.sum("records_written").alias("total_records"),
        F.sum(F.when(F.col("sla_met") == True, 1).otherwise(0)).alias("sla_met_count")
    )

print(f"\n[OBSERVABILITY] Daily Health Summary — {RUN_DATE}")
pipeline_health.show(truncate=False)

# COMMAND ----------
# MAGIC %md ## SLA Status Check

EXPECTED_PIPELINES = [
    "pl_bronze_claims", "pl_bronze_eligibility",
    "pl_bronze_provider", "pl_bronze_pharmacy", "pl_master_ingestion"
]

ran_today = [
    r["pipeline_name"] for r in
    spark.table(f"{CATALOG}.audit.pipeline_runs")
    .filter(F.col("run_date") == RUN_DATE)
    .filter(F.col("status") == "SUCCESS")
    .select("pipeline_name").collect()
]

missed = [p for p in EXPECTED_PIPELINES if p not in ran_today]
if missed:
    print(f"[SLA ALERT] ❌ Missed pipelines: {missed}")
    # Write SLA breach record
    spark.createDataFrame([{
        "breach_date":       RUN_DATE,
        "missed_pipelines":  str(missed),
        "environment":       ENV,
        "detected_at":       str(datetime.now())
    }]).write.format("delta").mode("append") \
      .saveAsTable(f"{CATALOG}.audit.sla_breaches")
else:
    print(f"[SLA] ✅ All {len(EXPECTED_PIPELINES)} pipelines completed on schedule")

# COMMAND ----------
# MAGIC %md ## DQ Score Trend (7-day rolling)

dq_trend = spark.table(f"{CATALOG}.audit.pipeline_runs") \
    .filter(F.col("environment") == ENV) \
    .filter(F.col("run_date") >= F.date_sub(F.current_date(), 7)) \
    .groupBy("run_date") \
    .agg(F.round(F.avg("dq_score"), 1).alias("avg_dq_score")) \
    .orderBy("run_date")

print("\n[DQ TREND] 7-Day Rolling Average:")
dq_trend.show()

# COMMAND ----------
# MAGIC %md ## Write Dashboard Snapshot

snapshot = spark.table(f"{CATALOG}.audit.pipeline_runs") \
    .filter(F.col("run_date") == RUN_DATE) \
    .withColumn("snapshot_at", F.current_timestamp())

snapshot.write.format("delta").mode("overwrite") \
    .option("replaceWhere", f"run_date = '{RUN_DATE}' AND environment = '{ENV}'") \
    .saveAsTable(f"{CATALOG}.audit.daily_health_snapshot")

print(f"\n[DASHBOARD] Snapshot written → {CATALOG}.audit.daily_health_snapshot")
print(f"[COMPLETE] Observability pipeline done | Status: {STATUS}")
