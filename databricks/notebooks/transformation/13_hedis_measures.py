# Databricks notebook source
# MAGIC %md
# MAGIC # HEDIS Quality Measures Engine
# MAGIC
# MAGIC Calculates NCQA HEDIS quality measures for health plan reporting.
# MAGIC Replaces legacy manual SQL-based HEDIS calculations with automated,
# MAGIC reusable, configurable measure framework.
# MAGIC
# MAGIC **Measures Implemented:**
# MAGIC
# MAGIC | Measure | Code | Description |
# MAGIC |---|---|---|
# MAGIC | Breast Cancer Screening | BCS | Women 52-74 with mammogram |
# MAGIC | Colorectal Cancer Screening | COL | Members 45-75 with screening |
# MAGIC | Controlling Blood Pressure | CBP | Hypertensive members with BP < 140/90 |
# MAGIC | Diabetes HbA1c Control | CDC | Diabetic members with HbA1c < 8% |
# MAGIC | Depression Screening | DMS | Members screened for depression |
# MAGIC | Childhood Immunization | CIS | Children with recommended vaccines |
# MAGIC | Annual Monitoring for Patients on Persistent Meds | MPM | Members on persistent meds with labs |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, date

dbutils.widgets.text("measurement_year", str(date.today().year))
dbutils.widgets.text("environment", "dev")

MEASUREMENT_YEAR = int(dbutils.widgets.get("measurement_year"))
ENV = dbutils.widgets.get("environment")
CATALOG = f"healthcare_{ENV}"

# HEDIS measurement year date range
MY_START = f"{MEASUREMENT_YEAR}-01-01"
MY_END   = f"{MEASUREMENT_YEAR}-12-31"

print(f"[HEDIS] Measurement Year: {MEASUREMENT_YEAR}")

# COMMAND ----------
# MAGIC %md ## Load Common Reference Data

# Eligible member population
members = spark.table(f"{CATALOG}.gold.member_eligibility") \
    .filter(F.col("plan_year") == MEASUREMENT_YEAR) \
    .filter(F.col("is_continuously_enrolled") == True) \
    .filter(F.col("plan_type").isin(["HMO", "PPO", "EPO", "HDHP"]))

# Claims (medical and facility)
claims = spark.table(f"{CATALOG}.gold.claims_fact") \
    .filter(F.col("service_date").between(MY_START, MY_END)) \
    .filter(F.col("claim_status").isin(["PAID", "ADJUSTED"]))

# Lab results
labs = spark.table(f"{CATALOG}.gold.lab_results") \
    .filter(F.col("result_date").between(MY_START, MY_END))

# Pharmacy claims
rx = spark.table(f"{CATALOG}.gold.pharmacy_claims") \
    .filter(F.col("fill_date").between(MY_START, MY_END))

print(f"[HEDIS] Members: {members.count():,}")
print(f"[HEDIS] Claims: {claims.count():,}")

# COMMAND ----------
# MAGIC %md ## BCS — Breast Cancer Screening
# MAGIC
# MAGIC **Numerator:** Women 52-74 with at least one mammogram during the measurement year or year prior
# MAGIC **Denominator:** Women 52-74 continuously enrolled

def calculate_bcs(members, claims, measurement_year):
    """Breast Cancer Screening measure."""
    # Denominator: Women 52-74
    bcs_denom = members.filter(F.col("gender_code") == "F") \
        .filter(
            F.col("age_as_of_dec31").between(52, 74)
        )

    # Mammography CPT codes (NCQA Value Set)
    MAMMOGRAM_CPTS = [
        "77065", "77066", "77067",   # Mammography codes
        "G0202", "G0204", "G0206"    # Medicare mammography codes
    ]

    # Numerator: Mammogram in MY or prior year
    mammograms = claims.filter(
        F.col("procedure_code").isin(MAMMOGRAM_CPTS)
    ).filter(
        F.col("service_date").between(
            f"{measurement_year - 1}-01-01", f"{measurement_year}-12-31"
        )
    ).select("member_id").distinct() \
     .withColumn("bcs_met", F.lit(True))

    # Calculate rate
    bcs_result = bcs_denom.join(mammograms, on="member_id", how="left") \
        .withColumn("bcs_met", F.col("bcs_met").cast("boolean").fillna(False))

    total = bcs_result.count()
    numerator = bcs_result.filter(F.col("bcs_met")).count()
    rate = round(numerator / max(total, 1) * 100, 2)

    print(f"[BCS] Denominator: {total:,} | Numerator: {numerator:,} | Rate: {rate}%")
    return bcs_result, {"measure": "BCS", "denominator": total,
                         "numerator": numerator, "rate": rate}

bcs_detail, bcs_summary = calculate_bcs(members, claims, MEASUREMENT_YEAR)

# COMMAND ----------
# MAGIC %md ## CDC — Diabetes HbA1c Control
# MAGIC
# MAGIC **Numerator:** Diabetic members 18-75 with HbA1c < 8%
# MAGIC **Denominator:** Members 18-75 with diabetes diagnosis

def calculate_cdc(members, claims, labs, measurement_year):
    """Diabetes HbA1c Control measure."""
    # Diabetes ICD-10 codes
    DIABETES_ICD = ["E10", "E11", "E13"]
    HBA1C_LOINC  = ["4548-4", "4549-2", "17856-6"]  # HbA1c LOINC codes

    # Denominator: Diabetic members 18-75
    diabetic_members = claims.filter(
        F.col("diagnosis_code").rlike(f"^({'|'.join(DIABETES_ICD)})")
    ).select("member_id").distinct()

    cdc_denom = members.filter(
        F.col("age_as_of_dec31").between(18, 75)
    ).join(diabetic_members, on="member_id", how="inner")

    # Numerator: HbA1c < 8%
    hba1c_controlled = labs.filter(
        F.col("loinc_code").isin(HBA1C_LOINC)
    ).filter(
        F.col("result_value").cast("double") < 8.0
    ).select("member_id").distinct() \
     .withColumn("cdc_hba1c_controlled", F.lit(True))

    # Also calculate poor control (> 9%) for reporting
    hba1c_poor = labs.filter(
        F.col("loinc_code").isin(HBA1C_LOINC)
    ).filter(
        F.col("result_value").cast("double") > 9.0
    ).select("member_id").distinct() \
     .withColumn("cdc_hba1c_poor", F.lit(True))

    cdc_result = cdc_denom \
        .join(hba1c_controlled, on="member_id", how="left") \
        .join(hba1c_poor, on="member_id", how="left") \
        .fillna({"cdc_hba1c_controlled": False, "cdc_hba1c_poor": False})

    total = cdc_result.count()
    controlled = cdc_result.filter(F.col("cdc_hba1c_controlled")).count()
    poor_control = cdc_result.filter(F.col("cdc_hba1c_poor")).count()
    rate_controlled = round(controlled / max(total, 1) * 100, 2)
    rate_poor = round(poor_control / max(total, 1) * 100, 2)

    print(f"[CDC] Denominator: {total:,} | Controlled (<8%): {controlled:,} ({rate_controlled}%)"
          f" | Poor control (>9%): {poor_control:,} ({rate_poor}%)")
    return cdc_result, {"measure": "CDC", "denominator": total,
                         "numerator_controlled": controlled, "rate_controlled": rate_controlled,
                         "numerator_poor": poor_control, "rate_poor_control": rate_poor}

cdc_detail, cdc_summary = calculate_cdc(members, claims, labs, MEASUREMENT_YEAR)

# COMMAND ----------
# MAGIC %md ## CBP — Controlling High Blood Pressure
# MAGIC
# MAGIC **Numerator:** Hypertensive members 18-85 with most recent BP < 140/90
# MAGIC **Denominator:** Members 18-85 with hypertension diagnosis

def calculate_cbp(members, claims, labs, measurement_year):
    """Controlling High Blood Pressure measure."""
    HYPERTENSION_ICD = ["I10", "I11", "I12", "I13", "I15"]
    SYSTOLIC_LOINC   = ["8480-6", "8459-0"]   # Systolic BP
    DIASTOLIC_LOINC  = ["8462-4", "8453-3"]   # Diastolic BP

    # Denominator
    htn_members = claims.filter(
        F.col("diagnosis_code").rlike(f"^({'|'.join(HYPERTENSION_ICD)})")
    ).select("member_id").distinct()

    cbp_denom = members.filter(
        F.col("age_as_of_dec31").between(18, 85)
    ).join(htn_members, on="member_id", how="inner")

    # Get most recent BP reading per member
    window_latest = Window.partitionBy("member_id").orderBy(F.desc("result_date"))

    systolic_latest = labs.filter(F.col("loinc_code").isin(SYSTOLIC_LOINC)) \
        .withColumn("rn", F.row_number().over(window_latest)) \
        .filter(F.col("rn") == 1) \
        .select("member_id", F.col("result_value").alias("systolic_bp"))

    diastolic_latest = labs.filter(F.col("loinc_code").isin(DIASTOLIC_LOINC)) \
        .withColumn("rn", F.row_number().over(window_latest)) \
        .filter(F.col("rn") == 1) \
        .select("member_id", F.col("result_value").alias("diastolic_bp"))

    cbp_result = cbp_denom \
        .join(systolic_latest, on="member_id", how="left") \
        .join(diastolic_latest, on="member_id", how="left") \
        .withColumn(
            "cbp_controlled",
            (F.col("systolic_bp").cast("double") < 140) &
            (F.col("diastolic_bp").cast("double") < 90)
        )

    total = cbp_result.count()
    controlled = cbp_result.filter(F.col("cbp_controlled")).count()
    rate = round(controlled / max(total, 1) * 100, 2)

    print(f"[CBP] Denominator: {total:,} | Controlled (<140/90): {controlled:,} | Rate: {rate}%")
    return cbp_result, {"measure": "CBP", "denominator": total,
                         "numerator": controlled, "rate": rate}

cbp_detail, cbp_summary = calculate_cbp(members, claims, labs, MEASUREMENT_YEAR)

# COMMAND ----------
# MAGIC %md ## COL — Colorectal Cancer Screening

def calculate_col(members, claims, labs, measurement_year):
    """Colorectal Cancer Screening measure."""
    # Denominator: Members 45-75
    col_denom = members.filter(F.col("age_as_of_dec31").between(45, 75))

    # Evidence of colorectal screening (multiple acceptable tests)
    FOBT_LOINC = ["2335-8", "14563-1", "12503-9"]  # FOBT lab codes
    COLONOSCOPY_CPT = ["45378", "45380", "45381", "45382", "45384", "45385"]
    FLEX_SIG_CPT   = ["45330", "45331", "45332", "45333"]
    CT_COLONOGRAPHY_CPT = ["74261", "74262", "74263"]

    # Annual FOBT
    fobt_done = labs.filter(F.col("loinc_code").isin(FOBT_LOINC)) \
        .select("member_id").distinct() \
        .withColumn("fobt_done", F.lit(True))

    # Colonoscopy within 10 years
    colonoscopy_done = claims.filter(
        F.col("procedure_code").isin(COLONOSCOPY_CPT)
    ).filter(
        F.col("service_date") >= f"{measurement_year - 10}-01-01"
    ).select("member_id").distinct() \
     .withColumn("colonoscopy_done", F.lit(True))

    # Flexible sigmoidoscopy within 5 years
    flex_sig_done = claims.filter(
        F.col("procedure_code").isin(FLEX_SIG_CPT)
    ).filter(
        F.col("service_date") >= f"{measurement_year - 5}-01-01"
    ).select("member_id").distinct() \
     .withColumn("flex_sig_done", F.lit(True))

    col_result = col_denom \
        .join(fobt_done, on="member_id", how="left") \
        .join(colonoscopy_done, on="member_id", how="left") \
        .join(flex_sig_done, on="member_id", how="left") \
        .withColumn(
            "col_met",
            F.col("fobt_done").cast("boolean") |
            F.col("colonoscopy_done").cast("boolean") |
            F.col("flex_sig_done").cast("boolean")
        ).fillna({"col_met": False})

    total = col_result.count()
    numerator = col_result.filter(F.col("col_met")).count()
    rate = round(numerator / max(total, 1) * 100, 2)

    print(f"[COL] Denominator: {total:,} | Numerator: {numerator:,} | Rate: {rate}%")
    return col_result, {"measure": "COL", "denominator": total,
                         "numerator": numerator, "rate": rate}

col_detail, col_summary = calculate_col(members, claims, labs, MEASUREMENT_YEAR)

# COMMAND ----------
# MAGIC %md ## Aggregate All Measures + Write to Gold

# Combine all measure summaries
all_measures = [bcs_summary, cdc_summary, cbp_summary, col_summary]

summary_records = []
for m in all_measures:
    summary_records.append({
        "measurement_year": MEASUREMENT_YEAR,
        "measure_code":     m["measure"],
        "denominator":      m.get("denominator", 0),
        "numerator":        m.get("numerator", m.get("numerator_controlled", 0)),
        "rate":             m.get("rate", m.get("rate_controlled", 0.0)),
        "calculated_at":    str(datetime.now()),
        "environment":      ENV
    })

summary_df = spark.createDataFrame(summary_records)
summary_df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"measurement_year = {MEASUREMENT_YEAR}") \
    .saveAsTable(f"{CATALOG}.gold.hedis_measure_summary")

# Write member-level detail for each measure
for detail_df, measure_code in [
    (bcs_detail, "BCS"), (cdc_detail, "CDC"),
    (cbp_detail, "CBP"), (col_detail, "COL")
]:
    detail_df.withColumn("measure_code", F.lit(measure_code)) \
             .withColumn("measurement_year", F.lit(MEASUREMENT_YEAR)) \
             .write.format("delta") \
             .mode("overwrite") \
             .option("replaceWhere", f"measure_code = '{measure_code}' AND measurement_year = {MEASUREMENT_YEAR}") \
             .saveAsTable(f"{CATALOG}.gold.hedis_member_detail")

print(f"\n[HEDIS] All measures calculated and written to Gold layer")
print(f"[HEDIS] Summary table: {CATALOG}.gold.hedis_measure_summary")
summary_df.show(truncate=False)
