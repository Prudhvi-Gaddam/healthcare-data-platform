# Operations Runbook — Healthcare Data Platform

## On-Call Responsibilities

**SLA:** All Bronze pipelines must complete by **6:00 AM EST** daily.
**Alert channel:** Teams → `#healthcare-platform-alerts`
**Escalation:** Data Engineering Lead → VP of Technology

---

## Common Incidents & Resolution Steps

---

### INC-001: Pipeline SLA Breach (pipeline did not complete by 6 AM)

**Symptoms:** Teams alert: "TCIM Pipeline SLA Breach"

**Step 1 — Identify which pipeline failed:**
```sql
-- Run in Databricks SQL
SELECT pipeline_name, status, started_at, completed_at, error_message
FROM audit.pipeline_runs
WHERE run_date = CURRENT_DATE()
  AND environment = 'prod'
ORDER BY started_at;
```

**Step 2 — Check ADF Monitor:**
- Go to Azure Portal → Data Factory → Monitor
- Filter by today's date, look for Failed or Running activities
- Click the failed activity to see error details

**Step 3 — Common causes and fixes:**

| Cause | Fix |
|---|---|
| Source SQL Server unavailable | Check SHIR status → Azure Portal → ADF → Integration Runtimes |
| ADLS Gen2 throttling | Check storage metrics, reduce parallel copies in ADF |
| Databricks cluster cold start | Pre-warm cluster or increase min workers |
| DQ validation failure | Check `audit.pipeline_runs.error_message` for which rule failed |
| OOM on Databricks | Increase cluster size or add partitioning |

**Step 4 — Manual re-run:**
```bash
# Re-trigger ADF pipeline manually
az datafactory pipeline create-run \
  --factory-name adf-healthcare-prod \
  --resource-group rg-healthcare-prod \
  --name pl_master_ingestion \
  --parameters '{"environment":"prod","incremental_from":"2026-01-01"}'
```

---

### INC-002: Data Quality Score Below Threshold (DQ score < 80)

**Symptoms:** DQ alert in Teams, `audit.pipeline_runs.dq_score < 80`

**Step 1 — Identify failing rules:**
```sql
-- Check which DQ rules failed
SELECT *
FROM audit.dq_rule_results
WHERE run_date = CURRENT_DATE()
  AND passed = false
ORDER BY severity, failed_rows DESC;
```

**Step 2 — Common DQ failures:**

| DQ Rule | Likely Cause | Fix |
|---|---|---|
| not_null on member_id | Source extract missing records | Check source SQL query, verify watermark |
| valid_values on claim_status | New status code added to source | Add new value to DQ rule YAML config |
| date_range on service_date | Future-dated claims | Flag for source system review |
| row_count below minimum | Partial extract | Check source system load completion |

**Step 3 — Quarantine bad records:**
```python
# In Databricks — quarantine records that failed DQ
bad_records = spark.table("healthcare_prod.bronze.claims_professional") \
    .filter(F.col("_batch_date") == "today") \
    .filter(F.col("member_id").isNull())

bad_records.write.format("delta").mode("append") \
    .saveAsTable("healthcare_prod.quarantine.claims_dq_failures")
```

---

### INC-003: Self-Hosted IR (SHIR) Disconnected

**Symptoms:** ADF pipeline fails with "Integration Runtime is offline"

**Step 1 — Check SHIR status:**
- Azure Portal → Data Factory → Manage → Integration Runtimes
- Look for `shir-onprem-prod` — status should be "Running"

**Step 2 — Restart SHIR service on jump server:**
```powershell
# On the SHIR host server (RDP required)
Restart-Service -Name "DIAHostService"
# Wait 60 seconds
Get-Service -Name "DIAHostService"
```

**Step 3 — Verify connectivity:**
```powershell
# Test SQL Server connectivity from SHIR host
Test-NetConnection -ComputerName sql-server-hostname -Port 1433
```

**Step 4 — If SHIR won't reconnect:**
1. Go to Azure Portal → ADF → Integration Runtimes → `shir-onprem-prod`
2. Click "Regenerate Key"
3. On SHIR host: open Integration Runtime Configuration Manager → re-enter key

---

### INC-004: Databricks Cluster Failed to Start

**Symptoms:** Notebook activity fails with "Cluster failed to start"

**Step 1 — Check cluster status:**
- Databricks workspace → Compute → check `healthcare-ingestion-cluster`

**Step 2 — Common causes:**
| Cause | Fix |
|---|---|
| Azure quota exceeded | Request quota increase in Azure Portal |
| VM size unavailable in region | Change node type in cluster config |
| Init script failure | Check cluster event log for script errors |
| Key Vault secret expired | Rotate secret, update Key Vault |

**Step 3 — Fallback — run on job cluster:**
```json
// In ADF Databricks activity, switch from existing cluster to new job cluster
{
  "newClusterVersion": "14.3.x-scala2.12",
  "newClusterNodeType": "Standard_DS4_v2",
  "newClusterNumOfWorker": "2:8"
}
```

---

### INC-005: AI/ML Scoring Pipeline Failure

**Symptoms:** `aiml_scoring` task failed in Databricks job

**Step 1 — Check which model failed:**
```python
# Check AI/ML pipeline audit
spark.table("healthcare_prod.audit.aiml_pipeline_runs") \
    .filter(F.col("run_date") == "today") \
    .show(truncate=False)
```

**Step 2 — Model registry issue:**
```python
import mlflow
# Check model is in Production stage
client = mlflow.MlflowClient()
versions = client.get_latest_versions(
    "healthcare_prod.ai.readmission_risk_model",
    stages=["Production"]
)
print(versions)
```

**Step 3 — If model missing from Production stage:**
```python
# Promote latest version to Production
client.transition_model_version_stage(
    name="healthcare_prod.ai.readmission_risk_model",
    version="1",
    stage="Production"
)
```

---

## Monitoring Queries

### Daily health check (run every morning):
```sql
-- Paste into Databricks SQL
SELECT
    pipeline_name,
    status,
    records_written,
    dq_score,
    DATEDIFF(MINUTE, started_at, completed_at) AS runtime_min,
    CASE WHEN sla_met THEN '✅ SLA Met' ELSE '❌ SLA Missed' END AS sla_status
FROM audit.pipeline_runs
WHERE run_date = CURRENT_DATE()
  AND environment = 'prod'
ORDER BY started_at;
```

### 7-day DQ trend:
```sql
SELECT
    pipeline_name,
    run_date,
    dq_score,
    AVG(dq_score) OVER (
        PARTITION BY pipeline_name
        ORDER BY run_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS dq_7d_avg
FROM audit.pipeline_runs
WHERE environment = 'prod'
  AND run_date >= DATEADD(DAY, -7, CURRENT_DATE())
ORDER BY run_date DESC, pipeline_name;
```

---

## Escalation Matrix

| Severity | Response Time | Escalate To |
|---|---|---|
| P1 — Production down, SLA missed | 15 minutes | Data Engineering Lead immediately |
| P2 — DQ score < 80, partial data | 1 hour | On-call engineer |
| P3 — Single pipeline delayed | 2 hours | On-call engineer |
| P4 — Non-critical warning | Next business day | Assign in backlog |
