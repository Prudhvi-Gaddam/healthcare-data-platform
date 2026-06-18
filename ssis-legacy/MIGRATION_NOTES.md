# SSIS Legacy Packages — Migration Documentation

This folder documents the **29 legacy SSIS packages** that were migrated to Azure Data Factory + Databricks as part of this platform.

---

## Package Inventory

### Claims Domain (8 packages → 1 ADF pipeline)

| SSIS Package | Size | Complexity | ADF Replacement | Notes |
|---|---|---|---|---|
| `ETL_Claims_Daily_Load.dtsx` | 2.1 MB | High | `pl_bronze_claims` | Main daily claims load, 8 data flow tasks |
| `ETL_Claims_Adjustment.dtsx` | 1.4 MB | High | `pl_bronze_claims` | Claim adjustment/void processing |
| `ETL_Claims_Denials.dtsx` | 890 KB | Medium | `pl_bronze_claims` | Denial reason code enrichment |
| `ETL_Claims_COB.dtsx` | 760 KB | Medium | `pl_bronze_claims` | Coordination of benefits |
| `ETL_Claims_Pharmacy.dtsx` | 1.2 MB | Medium | `pl_bronze_pharmacy` | Rx/pharmacy claims |
| `ETL_Claims_Dental.dtsx` | 650 KB | Low | `pl_bronze_claims` | Dental claims |
| `ETL_Claims_Archive.dtsx` | 480 KB | Low | `pl_bronze_claims` | Historical claims archival |
| `ETL_Claims_Reconcile.dtsx` | 920 KB | High | Databricks notebook | End-of-month reconciliation |

### Eligibility Domain (5 packages → 1 ADF pipeline)

| SSIS Package | Size | Complexity | ADF Replacement | Notes |
|---|---|---|---|---|
| `ETL_Eligibility_Daily.dtsx` | 1.8 MB | High | `pl_bronze_eligibility` | Daily enrollment changes, SCD Type 2 |
| `ETL_Member_Demographics.dtsx` | 760 KB | Medium | `pl_bronze_eligibility` | Member demographic updates |
| `ETL_Group_Enrollment.dtsx` | 540 KB | Medium | `pl_bronze_eligibility` | Group/employer enrollment |
| `ETL_COBRA_Processing.dtsx` | 430 KB | Low | `pl_bronze_eligibility` | COBRA continuation coverage |
| `ETL_Eligibility_834.dtsx` | 1.1 MB | High | `pl_bronze_eligibility` | 834 EDI file processing |

### Provider Domain (6 packages → 1 ADF pipeline)

| SSIS Package | Size | Complexity | ADF Replacement | Notes |
|---|---|---|---|---|
| `ETL_Provider_Master.dtsx` | 1.6 MB | High | `pl_bronze_provider` | Provider master roster |
| `ETL_Network_Contracts.dtsx` | 980 KB | High | `pl_bronze_provider` | Contract rate tables |
| `ETL_Provider_Credentialing.dtsx` | 720 KB | Medium | `pl_bronze_provider` | Credentialing status |
| `ETL_Provider_Taxonomy.dtsx` | 340 KB | Low | `pl_bronze_provider` | Specialty/taxonomy codes |
| `ETL_Fee_Schedule_Load.dtsx` | 1.3 MB | High | `pl_bronze_provider` | Fee schedule rate loading |
| `ETL_Provider_Network_Changes.dtsx` | 560 KB | Medium | `pl_bronze_provider` | Network status changes |

### Pharmacy Domain (4 packages → 1 ADF pipeline)

| SSIS Package | Size | Complexity | ADF Replacement | Notes |
|---|---|---|---|---|
| `ETL_Pharmacy_Daily.dtsx` | 1.4 MB | High | `pl_bronze_pharmacy` | Daily Rx claims |
| `ETL_Formulary_Load.dtsx` | 890 KB | Medium | `pl_bronze_pharmacy` | Drug formulary updates |
| `ETL_Drug_Reference.dtsx` | 650 KB | Low | `pl_bronze_pharmacy` | NDC/drug reference data |
| `ETL_PBM_Reconcile.dtsx` | 780 KB | Medium | `pl_bronze_pharmacy` | PBM reconciliation |

### Labs & ADT Domain (3 packages → 1 ADF pipeline)

| SSIS Package | Size | Complexity | ADF Replacement | Notes |
|---|---|---|---|---|
| `ETL_HL7_ADT_Loader.dtsx` | 1.9 MB | Very High | `pl_bronze_labs_hl7` | HL7 ADT event processing |
| `ETL_Labs_Results.dtsx` | 1.1 MB | High | `pl_bronze_labs_hl7` | Lab result ingestion |
| `ETL_Labs_Reference.dtsx` | 420 KB | Low | `pl_bronze_labs_hl7` | LOINC code reference data |

### Reference Data (3 packages → Databricks notebooks)

| SSIS Package | Size | Complexity | ADF Replacement | Notes |
|---|---|---|---|---|
| `ETL_ICD10_Reference.dtsx` | 580 KB | Low | Databricks notebook | ICD-10 code tables |
| `ETL_CPT_Reference.dtsx` | 490 KB | Low | Databricks notebook | CPT procedure codes |
| `ETL_NPI_Registry.dtsx` | 760 KB | Medium | Databricks notebook | CMS NPI registry load |

---

## Migration Approach

### Phase 1 — Lift and Shift (Completed)
Replicate exact SSIS logic in ADF Copy Activity + basic Databricks notebooks. Validate output matches SSIS exactly (record-for-record reconciliation).

### Phase 2 — Modernize (Completed)
- Replace C# Script Tasks with Python/PySpark
- Replace flat-file staging with Delta Lake
- Add watermark-based incremental loads (replace full table reloads)
- Add automated DQ validation
- Add PHI masking at ingestion

### Phase 3 — Optimize (Completed)
- Parallel pipeline execution (8 sources run simultaneously)
- Databricks Auto Loader for streaming ingestion
- Delta CDF (Change Data Feed) for downstream CDC
- AI/ML scoring added to Gold layer

---

## Performance Comparison

| Metric | SSIS (Legacy) | ADF + Databricks | Improvement |
|---|---|---|---|
| Total daily runtime | 8.5 hours | 2.1 hours | **75% faster** |
| Infrastructure cost | $4,200/month (on-prem servers) | $1,800/month (cloud) | **57% cost reduction** |
| Failed runs per month | 22 | 4 | **82% reduction** |
| New source onboarding | 3-4 weeks | 2-3 days | **85% faster** |
| Deployment time | 4 hours manual | 22 min CI/CD | **91% faster** |
| Data quality coverage | 35% | 98% | **+63 percentage points** |

---

## SSIS Component Mapping Reference

| SSIS Component | ADF/Databricks Equivalent |
|---|---|
| Connection Manager | Linked Service + Key Vault secret |
| Execute SQL Task | Azure SQL Stored Procedure Activity |
| Data Flow Task | Copy Activity + Mapping Data Flow |
| Script Task (C#) | Python in Databricks notebook |
| For Each Loop | ADF ForEach Activity |
| Sequence Container | ADF pipeline with dependencies |
| Package Configuration | ADF parameters + Key Vault |
| Event Handler (OnError) | ADF failure path activity |
| SQL Server Agent Job | ADF Tumbling Window Trigger |
| SSIS Catalog (SSISDB) | ADF Monitor + audit Delta tables |
| Flat File Source | ADLS Gen2 + event-based trigger |
| OLE DB Source | ADF SqlServer source + Self-Hosted IR |
| Slowly Changing Dimension | Delta Lake MERGE (upsert) |
| Lookup Transform | Delta Lake broadcast join |
| Audit Transform | Pipeline metadata columns |
