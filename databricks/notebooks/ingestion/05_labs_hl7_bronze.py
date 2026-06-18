# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Labs & HL7 ADT Bronze Ingestion
# MAGIC Processes HL7 v2 ADT messages and structured lab results.
# MAGIC Triggered by ADF event-based trigger on file arrival in ADLS landing zone.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys
sys.path.insert(0, "/HealthcarePlatform/framework")
from ingestion.healthcare_ingestion import PHIMaskingEngine, DataQualityFramework, DQRule

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("run_date",    "")
dbutils.widgets.text("file_path",   "")
dbutils.widgets.text("file_name",   "")

ENV       = dbutils.widgets.get("environment")
RUN_DATE  = dbutils.widgets.get("run_date")
FILE_PATH = dbutils.widgets.get("file_path")
FILE_NAME = dbutils.widgets.get("file_name")
CATALOG   = f"healthcare_{ENV}"
ADLS_BASE = f"abfss://landing@adlshealthcare{ENV}.dfs.core.windows.net"

print(f"[HL7] Processing: {FILE_NAME}")

# COMMAND ----------
# MAGIC %md ## Parse HL7 v2 Messages

hl7_raw = spark.read.text(f"{ADLS_BASE}/{FILE_PATH}/{FILE_NAME}")

@F.udf(returnType=StructType([
    StructField("message_type",    StringType()),
    StructField("event_type",      StringType()),   # A01=Admit A03=Discharge A08=Update
    StructField("patient_id",      StringType()),
    StructField("visit_id",        StringType()),
    StructField("event_timestamp", StringType()),
    StructField("facility_id",     StringType()),
    StructField("admit_reason",    StringType()),
]))
def parse_hl7_message(message):
    """Parse key fields from HL7 v2 ADT message segments."""
    import re
    if not message:
        return ("", "", "", "", "", "", "")
    try:
        msg_type = re.search(r"MSH\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|(\w+\^?\w*)", message)
        patient  = re.search(r"PID\|[^|]*\|[^|]*\|([^\|^]+)", message)
        visit    = re.search(r"PV1\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|([^\|]+)", message)
        ts       = re.search(r"MSH\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|(\d{8,14})", message)
        facility = re.search(r"MSH\|[^|]*\|[^|]*\|([^\|]+)", message)
        event    = re.search(r"MSH\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|[^A-Z]*([A-Z]\d+)", message)
        reason   = re.search(r"DG1\|[^|]*\|[^|]*\|([^\|^]+)", message)
        return (
            msg_type.group(1) if msg_type else "",
            event.group(1)    if event    else "",
            patient.group(1)  if patient  else "",
            visit.group(1)    if visit    else "",
            ts.group(1)       if ts       else "",
            facility.group(1) if facility else "",
            reason.group(1)   if reason   else ""
        )
    except Exception:
        return ("", "", "", "", "", "", "")

df_parsed = hl7_raw \
    .withColumn("parsed", parse_hl7_message(F.col("value"))) \
    .withColumn("message_type",    F.col("parsed.message_type")) \
    .withColumn("event_type",      F.col("parsed.event_type")) \
    .withColumn("patient_id",      F.col("parsed.patient_id")) \
    .withColumn("visit_id",        F.col("parsed.visit_id")) \
    .withColumn("event_timestamp", F.col("parsed.event_timestamp")) \
    .withColumn("facility_id",     F.col("parsed.facility_id")) \
    .withColumn("admit_reason",    F.col("parsed.admit_reason")) \
    .withColumn("raw_message",     F.col("value")) \
    .withColumn("source_file",     F.lit(FILE_NAME)) \
    .withColumn("_ingested_at",    F.current_timestamp()) \
    .withColumn("_batch_date",     F.lit(RUN_DATE).cast("date")) \
    .withColumn("_environment",    F.lit(ENV)) \
    .drop("parsed", "value")

# COMMAND ----------
# MAGIC %md ## PHI Masking + Write Bronze

masker = PHIMaskingEngine()
phi_cols = masker.detect_phi_columns(df_parsed, ["patient_id"])
df_masked = masker.mask_phi(df_parsed, phi_cols, mode="hash")

# Remove raw HL7 message from Delta table (PHI risk)
df_final = df_masked.drop("raw_message")

df_final.write.format("delta").mode("append") \
    .partitionBy("_batch_date", "event_type") \
    .saveAsTable(f"{CATALOG}.bronze.adt_events")

count = df_final.count()
print(f"[HL7] Parsed {count:,} ADT events → {CATALOG}.bronze.adt_events")
dbutils.notebook.exit(str(count))
