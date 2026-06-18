# Power BI Dashboards — Healthcare Data Platform

## Overview

This folder contains Power BI dashboard specifications, DAX measures, and connection details for the Healthcare Data Platform reporting layer.

---

## Dashboards

### 1. Claims Analytics Dashboard
**File:** `Claims_Analytics.pbix` (connect to Databricks SQL endpoint)

**Pages:**
- Claims Volume Trend (line chart by month/category)
- Denial Rate Analysis (bar chart + KPI cards)
- Top Diagnosis Codes by Cost (table with conditional formatting)
- PMPM Cost Trend (line chart with rolling average)

**Key DAX Measures:**
```dax
-- Denial Rate %
Denial Rate =
DIVIDE(
    CALCULATE(COUNTROWS(claims_fact), claims_fact[claim_status] = "DENIED"),
    COUNTROWS(claims_fact),
    0
) * 100

-- PMPM Cost
PMPM Cost =
DIVIDE(
    SUM(claims_fact[plan_paid_amount]),
    DISTINCTCOUNT(claims_fact[member_id]),
    0
)

-- YoY Cost Change %
YoY Cost Change =
VAR CurrentYear = SUM(claims_fact[allowed_amount])
VAR PriorYear = CALCULATE(
    SUM(claims_fact[allowed_amount]),
    DATEADD(claims_fact[service_date], -1, YEAR)
)
RETURN DIVIDE(CurrentYear - PriorYear, PriorYear, 0) * 100

-- 3-Month Rolling Average PMPM
Rolling 3M PMPM =
AVERAGEX(
    DATESINPERIOD(
        dim_date[date],
        LASTDATE(dim_date[date]),
        -3,
        MONTH
    ),
    [PMPM Cost]
)
```

---

### 2. HEDIS Quality Scorecard
**File:** `HEDIS_Quality.pbix`

**Pages:**
- Measure Scorecard (table with NCQA benchmark comparisons)
- BCS Trend (breast cancer screening rate over time)
- Diabetes Management (CDC HbA1c control rates)
- Care Gap Analysis (member-level gaps)

**Key DAX Measures:**
```dax
-- HEDIS Measure Rate
Measure Rate =
DIVIDE(
    SUM(hedis_measure_summary[numerator]),
    SUM(hedis_measure_summary[denominator]),
    0
) * 100

-- Performance vs NCQA 75th Percentile
vs 75th Percentile =
[Measure Rate] - MAX(hedis_benchmarks[benchmark_75th])

-- Performance Tier
Performance Tier =
SWITCH(
    TRUE(),
    [Measure Rate] >= MAX(hedis_benchmarks[benchmark_90th]), "Excellent",
    [Measure Rate] >= MAX(hedis_benchmarks[benchmark_75th]), "Good",
    [Measure Rate] >= MAX(hedis_benchmarks[benchmark_50th]), "Average",
    "Below Average"
)
```

---

### 3. Population Health Dashboard
**File:** `Population_Health.pbix`

**Pages:**
- Risk Stratification (donut chart — LOW/MEDIUM/HIGH/CRITICAL)
- Chronic Disease Prevalence (heat map by age group)
- Care Gaps Summary (count by gap type and risk tier)
- Readmission Risk Alerts (table with member details masked)

---

### 4. Provider Performance Dashboard
**File:** `Provider_Performance.pbix`

**Pages:**
- Cost Index vs Specialty Peers (scatter plot)
- Network Utilization (in vs out of network)
- Quality Metrics (HEDIS-linked provider measures)
- High Value / High Cost Providers (table)

---

### 5. Pipeline Observability Dashboard
**File:** `Pipeline_Health.pbix`

**Pages:**
- Daily SLA Status (green/red KPI cards per pipeline)
- DQ Score Trend (7-day rolling average line chart)
- Record Volume Anomalies (day-over-day % change)
- AI/ML Pipeline Status (readmission, anomaly, NLP run counts)

---

## Connection Setup

### Connect Power BI to Databricks SQL

1. **Open Power BI Desktop**
2. **Get Data → Azure → Azure Databricks**
3. Enter:
   - Server Hostname: `<your-databricks-workspace-url>`
   - HTTP Path: `/sql/1.0/warehouses/<sql-warehouse-id>`
4. Authentication: **Azure Active Directory** (recommended for prod)
5. Select catalog: `healthcare_prod`
6. Import tables from `gold.*` and `audit.*` schemas

### DirectQuery vs Import Mode

| Dashboard | Recommended Mode | Reason |
|---|---|---|
| Claims Analytics | Import + Scheduled Refresh | Large dataset, daily refresh sufficient |
| HEDIS Quality | Import (monthly refresh) | Calculated measures, monthly data |
| Population Health | Import + Scheduled Refresh | Daily AI scoring results |
| Provider Performance | Import (weekly refresh) | Weekly data cadence |
| Pipeline Observability | **Direct Lake** | Real-time monitoring required |

### Row-Level Security (RLS)

All dashboards implement RLS to restrict data access by:
- `plan_id` — health plan administrators see only their plan data
- `region` — regional managers see only their geography
- `provider_npi` — providers see only their own performance data

```dax
-- RLS filter for plan administrators
[plan_id] = USERPRINCIPALNAME()

-- RLS filter for regional managers (from security table)
[state_code] IN
    CALCULATETABLE(
        VALUES(user_region_access[state_code]),
        user_region_access[user_email] = USERPRINCIPALNAME()
    )
```

---

## Incremental Refresh Policy

For large claims tables (millions of rows), configure Incremental Refresh:

1. Right-click table → **Incremental Refresh**
2. Store rows from last **3 years**
3. Refresh rows from last **3 days**
4. Detect data changes using `_ingested_at` column

This reduces refresh time from hours to minutes for large claims datasets.
