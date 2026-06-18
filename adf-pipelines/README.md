# ADF Pipelines — Healthcare Data Platform

## Overview

This folder contains all Azure Data Factory (ADF) pipeline definitions as JSON artifacts. These replace **29 legacy SSIS packages** with parameterized, config-driven, cloud-native pipelines.

---

## Pipeline Architecture

```
tr_master_daily_tumbling (Tumbling Window — 1AM EST daily)
    │
    └── pl_master_ingestion (Master Orchestrator)
        │
        ├── pl_bronze_claims      ──┐
        ├── pl_bronze_eligibility ──┤  (run in PARALLEL)
        ├── pl_bronze_provider    ──┤
        └── pl_bronze_pharmacy    ──┘
                    │
                    ▼ (after all Bronze complete)
            Databricks Silver Transformation
                    │
                    ▼
            Databricks Gold Aggregation
                    │
                    ▼
            Databricks AI/ML Scoring

tr_hl7_file_arrival (Event-based — fires on file drop)
    └── pl_bronze_labs_hl7 (near real-time HL7 processing)
```

---

## SSIS-to-ADF Migration Map

| Legacy SSIS Package | ADF Pipeline | Trigger Type |
|---|---|---|
| ETL_Claims_Daily_Load.dtsx | pl_bronze_claims | Tumbling Window |
| ETL_Claims_Adjustment.dtsx | pl_bronze_claims | Tumbling Window |
| ETL_Eligibility_Daily.dtsx | pl_bronze_eligibility | Tumbling Window |
| ETL_Provider_Master.dtsx | pl_bronze_provider | Tumbling Window |
| ETL_Network_Contracts.dtsx | pl_bronze_provider | Tumbling Window |
| ETL_Pharmacy_Daily.dtsx | pl_bronze_pharmacy | Tumbling Window |
| ETL_HL7_ADT_Loader.dtsx | pl_bronze_labs_hl7 | Event-based (file arrival) |
| ETL_Labs_Results.dtsx | pl_bronze_labs_hl7 | Event-based (file arrival) |

---

## Key ADF Design Patterns Used

### 1. Watermark-based Incremental Loads
Every pipeline retrieves the last successful watermark from the audit table before extracting — ensuring no duplicates and no missed records.

### 2. Self-Hosted Integration Runtime (SHIR)
On-premises SQL Server sources connect via SHIR installed on a jump server — no VPN or firewall changes required.

### 3. Parameterized Pipelines
All pipelines accept `environment` and `incremental_from` parameters — the same pipeline definition runs in dev, staging, and prod.

### 4. Key Vault Integration
No connection strings or credentials in pipeline JSON — all secrets retrieved from Azure Key Vault at runtime via managed identity.

### 5. ADF → Databricks Handoff
Copy Activity lands raw data in ADLS Gen2 Bronze layer, then a Databricks Notebook Activity triggers the transformation — clean separation of concerns.

### 6. Tumbling Window vs Event-based Triggers
- **Tumbling Window:** structured daily batch loads (claims, eligibility, provider)
- **Event-based:** near-real-time file-triggered loads (HL7 ADT, lab results)

---

## Folder Structure

```
adf-pipelines/
├── linked-services/
│   ├── ls_sqlserver_onprem.json    # On-prem SQL Server via Self-Hosted IR
│   ├── ls_adls_gen2.json           # ADLS Gen2 (Bronze/Silver/Gold)
│   ├── ls_databricks.json          # Databricks workspace
│   └── ls_keyvault.json            # Azure Key Vault (secrets)
│
├── pipelines/
│   ├── pl_master_ingestion.json    # Master orchestrator — runs all sources
│   ├── pl_bronze_claims.json       # Claims ingestion (watermark incremental)
│   ├── pl_bronze_eligibility.json  # Eligibility & enrollment ingestion
│   └── pl_bronze_provider.json     # Provider & network ingestion
│
└── triggers/
    ├── tr_master_daily_tumbling.json  # Daily 1AM — all batch sources
    └── tr_hl7_file_arrival.json       # Event-based — HL7 file drop
```

---

## Deploying to ADF

```bash
# Using Azure CLI
az datafactory pipeline create \
  --factory-name "adf-healthcare-dev" \
  --resource-group "rg-healthcare-dev" \
  --name "pl_master_ingestion" \
  --pipeline @adf-pipelines/pipelines/pl_master_ingestion.json

# Or deploy all via GitHub Actions CI/CD
# See .github/workflows/ci-cd.yml
```
