# Data Dictionary — Healthcare Data Platform

## Table of Contents
- [Bronze Layer](#bronze-layer)
- [Silver Layer](#silver-layer)
- [Gold Layer](#gold-layer)
- [AI/ML Tables](#aiml-tables)
- [Audit Tables](#audit-tables)
- [Reference Tables](#reference-tables)

---

## Bronze Layer
*Raw ingested data — PHI-hashed, schema-validated, unmodified business logic*

### bronze.claims_professional

| Column | Type | Description | PHI? | Nullable |
|---|---|---|---|---|
| claim_id | STRING | Unique claim identifier | No | No |
| claim_line_id | STRING | Claim line number | No | No |
| claim_type_code | STRING | P=Professional, I=Institutional, D=Dental | No | No |
| member_id | STRING | SHA-256 hashed member ID | Hashed | No |
| subscriber_id | STRING | SHA-256 hashed subscriber ID | Hashed | Yes |
| plan_id | STRING | Health plan identifier | No | No |
| rendering_provider_npi | STRING | SHA-256 hashed rendering provider NPI | Hashed | Yes |
| service_date | DATE | Date of service | No | No |
| procedure_code | STRING | CPT or HCPCS procedure code | No | Yes |
| primary_diagnosis_code | STRING | ICD-10-CM primary diagnosis code | No | Yes |
| claim_status | STRING | PAID, DENIED, PENDING, ADJUSTED, VOID | No | No |
| allowed_amount | DECIMAL(18,2) | Contractual allowed amount | No | Yes |
| plan_paid_amount | DECIMAL(18,2) | Amount paid by plan | No | Yes |
| member_deductible_amt | DECIMAL(18,2) | Member deductible applied | No | Yes |
| member_copay_amt | DECIMAL(18,2) | Member copay applied | No | Yes |
| member_coinsurance_amt | DECIMAL(18,2) | Member coinsurance applied | No | Yes |
| _source_id | STRING | Pipeline source identifier | No | No |
| _ingested_at | TIMESTAMP | Ingestion timestamp | No | No |
| _batch_date | DATE | Processing batch date | No | No |
| _phi_masked | BOOLEAN | True = PHI columns hashed | No | No |

### bronze.member_eligibility

| Column | Type | Description | PHI? | Nullable |
|---|---|---|---|---|
| member_id | STRING | SHA-256 hashed member ID | Hashed | No |
| plan_id | STRING | Health plan identifier | No | No |
| metal_tier | STRING | BRONZE, SILVER, GOLD, PLATINUM | No | Yes |
| effective_date | DATE | Coverage effective date | No | No |
| termination_date | DATE | Coverage termination date | No | Yes |
| birth_date | STRING | SHA-256 hashed date of birth | Hashed | Yes |
| gender_code | STRING | M=Male, F=Female, U=Unknown | No | Yes |
| zip_code | STRING | 5-digit ZIP (non-PHI at ZIP level) | No | Yes |

---

## Gold Layer
*Business-ready aggregated data — serving layer for dashboards and APIs*

### gold.claims_fact

| Column | Type | Description | Source |
|---|---|---|---|
| claim_key | BIGINT | Surrogate key | Generated |
| claim_id | STRING | Natural key from source | bronze.claims_professional |
| service_month | DATE | Truncated to month (for aggregation) | Derived |
| plan_id | STRING | Health plan | bronze.claims_professional |
| service_category | STRING | INPATIENT, OUTPATIENT, EMERGENCY, PCP, SPECIALIST, PHARMACY | Derived from revenue/procedure codes |
| allowed_amount | DECIMAL | Allowed amount | bronze.claims_professional |
| plan_paid_amount | DECIMAL | Plan payment | bronze.claims_professional |
| denial_flag | BOOLEAN | True if claim_status = DENIED | Derived |
| length_of_stay | INT | Days for inpatient claims | bronze.claims_professional |

### gold.hedis_measure_summary

| Column | Type | Description |
|---|---|---|
| measurement_year | INT | HEDIS measurement year |
| measure_code | STRING | BCS, COL, CBP, CDC, DMS, etc. |
| denominator | INT | Eligible member count |
| numerator | INT | Members meeting measure criteria |
| rate | DECIMAL(5,2) | Numerator / Denominator * 100 |
| calculated_at | TIMESTAMP | Calculation timestamp |

### gold.readmission_risk_scores

| Column | Type | Description |
|---|---|---|
| member_id | STRING | Hashed member ID |
| admission_id | STRING | Claim ID for the admission |
| discharge_date | DATE | Hospital discharge date |
| readmission_risk_score | DECIMAL(5,1) | 0-100 risk score |
| risk_tier | STRING | LOW, MEDIUM, HIGH, CRITICAL |
| recommended_action | STRING | Care management recommendation |
| scored_at | TIMESTAMP | Model scoring timestamp |
| model_uri | STRING | MLflow model version used |

### gold.claims_anomaly_scores

| Column | Type | Description |
|---|---|---|
| claim_id | STRING | Claim identifier |
| anomaly_score | DECIMAL(5,1) | 0-100 composite anomaly score |
| investigation_priority | STRING | CRITICAL, HIGH, MEDIUM, LOW |
| anomaly_explanation | STRING | Human-readable explanation |
| flag_duplicate_claim | BOOLEAN | Potential duplicate detected |
| flag_high_cost_outlier | BOOLEAN | Cost > 5x specialty average |
| flag_age_procedure_mismatch | BOOLEAN | Age/gender vs procedure mismatch |
| isolation_forest_score | DECIMAL(5,1) | ML model raw score |

---

## AI/ML Tables

### ai.rag_interaction_log

| Column | Type | Description | Notes |
|---|---|---|---|
| query_id | STRING | Unique query identifier | Never store raw query (PHI risk) |
| session_id | STRING | User session identifier | |
| query_hash | STRING | SHA-256 hash of query text | For deduplication only |
| confidence_score | DECIMAL(5,1) | RAG retrieval confidence 0-100 | |
| docs_retrieved | INT | Number of documents retrieved | |
| latency_ms | DECIMAL(10,1) | Response latency in milliseconds | |
| model_used | STRING | LLM endpoint name | |
| timestamp | TIMESTAMP | Interaction timestamp | |

---

## Audit Tables

### audit.pipeline_runs

| Column | Type | Description |
|---|---|---|
| run_id | BIGINT | Auto-generated run identifier |
| pipeline_name | STRING | Pipeline name (e.g., pl_bronze_claims) |
| source_id | STRING | Source system identifier |
| run_date | DATE | Pipeline execution date |
| environment | STRING | dev, stg, prod |
| status | STRING | RUNNING, SUCCESS, FAILED, NO_DATA |
| records_read | INT | Records extracted from source |
| records_written | INT | Records written to target |
| dq_score | DECIMAL(5,1) | Data quality score 0-100 |
| error_message | STRING | Error type only — never contains PHI |
| started_at | TIMESTAMP | Pipeline start time |
| completed_at | TIMESTAMP | Pipeline completion time |
| sla_met | BOOLEAN | True if completed before SLA hour |

---

## Reference Tables

### reference.icd10_codes

| Column | Type | Description |
|---|---|---|
| icd10_code | STRING | ICD-10-CM code (e.g., E11.9) |
| diagnosis_description | STRING | Long description |
| diagnosis_category | STRING | Clinical category grouping |
| is_chronic | BOOLEAN | Chronic condition flag |
| hcc_code | STRING | CMS-HCC category if applicable |
| raf_weight | DECIMAL(8,4) | RAF score weight for risk adjustment |

### reference.hedis_benchmarks

| Column | Type | Description |
|---|---|---|
| measure_code | STRING | HEDIS measure code |
| benchmark_year | INT | Benchmark publication year |
| benchmark_50th | DECIMAL(5,2) | NCQA 50th percentile rate |
| benchmark_75th | DECIMAL(5,2) | NCQA 75th percentile rate |
| benchmark_90th | DECIMAL(5,2) | NCQA 90th percentile rate |
