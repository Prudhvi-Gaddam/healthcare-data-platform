# Architecture Guide — Healthcare Data Platform

## Design Principles

### 1. Configuration-Over-Code
Every new data source is onboarded via YAML config. The `HealthcareIngestionFramework` reads source configuration at runtime — no pipeline code changes required for new sources, new PHI columns, new DQ rules, or new SLA targets.

### 2. HIPAA-By-Default
PHI protection is built into the framework core, not bolted on:
- Automatic PHI column detection by pattern matching
- SHA-256 hashing applied before any data leaves Bronze
- No PHI in log messages (exception messages use `type(e).__name__`, never `str(e)`)
- Audit tables for lineage, never logging raw data values

### 3. Medallion Architecture
All data flows through three layers before serving:

| Layer | Purpose | Format | Partitioning |
|---|---|---|---|
| Bronze | Raw landing, schema-on-read | Delta | service_date (or batch_date) |
| Silver | Validated, standardized, PHI-hashed | Delta | service_date, claim_status |
| Gold | Business aggregates, conformed dims, serving | Delta | service_date, plan_id |

### 4. Reusability
The framework is designed as a library, not a script:
```python
# Any team can use the framework in 5 lines
from framework.ingestion.healthcare_ingestion import ConfigDrivenPipelineRunner

runner = ConfigDrivenPipelineRunner(spark, "framework/config", adls_base, catalog)
result = runner.run_source("claims_professional", incremental_from="2026-01-01")
```

---

## Data Flow

```
SQL Server / Files / HL7
         │
         ▼ (Self-Hosted IR / ADF Copy)
    Bronze Delta Table
    [Raw, PHI-hashed, schema-validated]
         │
         ▼ (Databricks Transformation Notebook)
    Silver Delta Table
    [Cleaned, standardized, referential integrity]
         │
         ▼ (Databricks Aggregation Notebook)
    Gold Delta Table
    [Business aggregates, HEDIS measures, risk scores]
         │
         ▼
    ┌─────────────┬──────────────┬─────────────┐
    │  Power BI   │ Databricks   │  REST APIs  │
    │  Dashboards │ SQL Queries  │  (Serving)  │
    └─────────────┴──────────────┴─────────────┘
```

---

## AI/ML Architecture

```
Gold Delta Tables (Features)
         │
         ▼ (Feature Store)
    Databricks Feature Store
    [Point-in-time correct, reusable features]
         │
         ▼ (Training)
    ┌──────────────────────────────────────────────┐
    │  MLflow Experiment Tracking                   │
    │  ├── Readmission Risk (XGBoost)               │
    │  ├── Anomaly Detector (Isolation Forest)      │
    │  └── NLP NER (ClinicalBERT fine-tuned)        │
    └──────────────────────────────────────────────┘
         │
         ▼ (Registry)
    Unity Catalog Model Registry
         │
         ▼ (Serving)
    Databricks Model Serving
    [REST endpoint — real-time or batch scoring]
         │
         ▼
    ┌────────────────────┐   ┌──────────────────┐
    │  Care Management   │   │  Claims Review   │
    │  Alerts Dashboard  │   │  Anomaly Flags   │
    └────────────────────┘   └──────────────────┘
```

---

## SSIS-to-ADF Migration Mapping

| SSIS Component | ADF/Databricks Equivalent |
|---|---|
| Connection Manager (SQL Server) | Linked Service + Self-Hosted IR |
| Execute SQL Task | Azure SQL Stored Procedure Activity |
| Data Flow Task | Copy Activity + Mapping Data Flow |
| Script Task (C#) | Azure Function Activity / Python UDF |
| For Each Loop | ADF ForEach Activity |
| Package Configuration (.dtsConfig) | ADF Parameters + Key Vault secrets |
| SQL Server Agent Schedule | ADF Tumbling Window / Schedule Trigger |
| SSIS Event Handlers | ADF Failure Path + Azure Monitor |
| SSIS Catalog (SSISDB) | ADF Monitor + Databricks Audit Tables |
| Jenkins SSIS CI/CD | GitHub Actions CI/CD |

---

## Security Architecture

```
┌─────────────────────────────────────────────┐
│  Azure Active Directory (Identity)           │
│  ├── Service Principals (ADF, Databricks)    │
│  ├── Managed Identities (no secrets needed) │
│  └── User Groups (RBAC)                     │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│  Azure Key Vault (Secrets Management)        │
│  ├── Database connection strings             │
│  ├── API keys and tokens                    │
│  └── Encryption keys                        │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│  Unity Catalog (Data Governance)             │
│  ├── RBAC: Column-level access controls      │
│  ├── ABAC: Attribute-based (PII columns)    │
│  ├── Data Lineage tracking                  │
│  └── Audit logs                            │
└─────────────────────────────────────────────┘
```

---

## Naming Conventions

| Resource | Convention | Example |
|---|---|---|
| ADLS containers | `{layer}` | `bronze`, `silver`, `gold` |
| Delta tables | `{catalog}.{schema}.{domain}_{entity}` | `healthcare_prod.clinical.claims_fact` |
| ADF pipelines | `pl_{domain}_{action}` | `pl_claims_bronze_load` |
| ADF triggers | `tr_{pipeline}_{frequency}` | `tr_claims_daily_tumbling` |
| Databricks notebooks | `{nn}_{domain}_{layer}` | `01_claims_bronze` |
| Python classes | `PascalCase` | `HealthcareIngestionFramework` |
| Config files | `{source_id}.yaml` | `claims_professional.yaml` |
| Terraform modules | `{resource_type}` | `modules/adls/`, `modules/databricks/` |
