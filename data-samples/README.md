# Data Samples — Healthcare Data Platform

## ⚠️ Important Notice

**All data in this folder is 100% synthetic and artificially generated.**
- No real patient, member, or provider information is included
- Member IDs are SHA-256 hashes of randomly generated strings (not real PHI)
- Claim amounts, dates, and codes are fictional examples
- Provider NPIs are fictitious 10-digit numbers
- This data is for testing and demonstration purposes only

---

## Files

| File | Description | Rows |
|---|---|---|
| `sample_claims.csv` | Synthetic professional claims data | 10 |
| `sample_eligibility.csv` | Synthetic member eligibility data | 5 |

---

## Using Sample Data for Testing

```python
# Load sample claims for local testing
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

claims = spark.read.option("header", True).option("inferSchema", True) \
    .csv("data-samples/sample_claims.csv")
claims.show(5)
```

---

## Data Schema

### sample_claims.csv
Matches the `bronze.claims_professional` schema with fields:
`claim_id, claim_line_id, claim_type_code, member_id (hashed), plan_id,
rendering_provider_npi (fictitious), service_date, procedure_code,
primary_diagnosis_code, claim_status, billed_amount, allowed_amount,
plan_paid_amount, member_deductible_amt, member_copay_amt, member_coinsurance_amt`

### sample_eligibility.csv
Matches the `bronze.member_eligibility` schema with fields:
`member_id (hashed), subscriber_id (hashed), plan_id, plan_name,
metal_tier, product_type, effective_date, termination_date,
gender_code, zip_code, state_code, is_active, plan_year`
