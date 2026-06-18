-- ============================================================================
-- REPORTING VIEWS: Healthcare Data Platform
-- Purpose: Simplified views for Power BI, SSRS, and ad-hoc reporting
-- ============================================================================

-- ============================================================================
-- VIEW 1: Claims Summary by Month and Category
-- Used by: Claims Analytics Dashboard (Power BI)
-- ============================================================================
CREATE OR REPLACE VIEW gold.vw_claims_monthly_summary AS
SELECT
    DATE_TRUNC('month', service_date)   AS service_month,
    YEAR(service_date)                  AS service_year,
    MONTH(service_date)                 AS service_month_num,
    service_category,
    claim_status,
    plan_id,
    COUNT(DISTINCT claim_id)            AS claim_count,
    COUNT(DISTINCT member_id)           AS unique_members,
    SUM(billed_amount)                  AS total_billed,
    SUM(allowed_amount)                 AS total_allowed,
    SUM(plan_paid_amount)               AS total_plan_paid,
    SUM(member_deductible_amt +
        member_copay_amt +
        member_coinsurance_amt)         AS total_member_cost_sharing,
    AVG(allowed_amount)                 AS avg_allowed_per_claim,
    SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) AS denied_count,
    ROUND(
        SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(COUNT(DISTINCT claim_id), 0), 2
    )                                   AS denial_rate_pct
FROM gold.claims_fact
WHERE _environment = 'prod'
GROUP BY 1, 2, 3, 4, 5, 6;

-- ============================================================================
-- VIEW 2: PMPM (Per Member Per Month) Cost Trend
-- Used by: Cost Trend Dashboard (Power BI), Actuarial reporting
-- ============================================================================
CREATE OR REPLACE VIEW gold.vw_pmpm_trend AS
SELECT
    DATE_TRUNC('month', c.service_date) AS service_month,
    e.metal_tier,
    e.product_type,
    COUNT(DISTINCT c.member_id)         AS member_months,
    SUM(c.plan_paid_amount)             AS total_plan_cost,
    SUM(c.allowed_amount)               AS total_allowed_cost,
    -- PMPM calculation
    ROUND(SUM(c.plan_paid_amount) /
          NULLIF(COUNT(DISTINCT c.member_id), 0), 2)  AS plan_pmpm,
    ROUND(SUM(c.allowed_amount) /
          NULLIF(COUNT(DISTINCT c.member_id), 0), 2)  AS allowed_pmpm,
    -- Service category breakdown
    ROUND(SUM(CASE WHEN c.service_category = 'INPATIENT'  THEN c.plan_paid_amount ELSE 0 END) /
          NULLIF(COUNT(DISTINCT c.member_id), 0), 2)  AS ip_pmpm,
    ROUND(SUM(CASE WHEN c.service_category = 'OUTPATIENT' THEN c.plan_paid_amount ELSE 0 END) /
          NULLIF(COUNT(DISTINCT c.member_id), 0), 2)  AS op_pmpm,
    ROUND(SUM(CASE WHEN c.service_category = 'EMERGENCY'  THEN c.plan_paid_amount ELSE 0 END) /
          NULLIF(COUNT(DISTINCT c.member_id), 0), 2)  AS er_pmpm,
    ROUND(SUM(CASE WHEN c.service_category = 'PCP'        THEN c.plan_paid_amount ELSE 0 END) /
          NULLIF(COUNT(DISTINCT c.member_id), 0), 2)  AS pcp_pmpm
FROM gold.claims_fact c
JOIN gold.member_eligibility e
    ON  c.member_id = e.member_id
    AND c.plan_id   = e.plan_id
    AND e.scd_is_current = TRUE
WHERE c.claim_status IN ('PAID', 'ADJUSTED')
  AND c._environment = 'prod'
GROUP BY 1, 2, 3;

-- ============================================================================
-- VIEW 3: Provider Performance Scorecard
-- Used by: Provider Performance Dashboard (Power BI)
-- ============================================================================
CREATE OR REPLACE VIEW gold.vw_provider_performance AS
SELECT
    c.rendering_provider_npi            AS provider_npi,
    p.provider_name,
    p.specialty_description,
    p.network_status,
    p.state_code,
    -- Volume metrics
    COUNT(DISTINCT c.claim_id)          AS total_claims,
    COUNT(DISTINCT c.member_id)         AS unique_patients,
    SUM(c.units_of_service)             AS total_units,
    -- Cost metrics
    ROUND(AVG(c.allowed_amount), 2)     AS avg_cost_per_claim,
    SUM(c.allowed_amount)               AS total_allowed_cost,
    -- Quality metrics
    ROUND(SUM(CASE WHEN c.claim_status = 'DENIED' THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(*), 0), 2)       AS denial_rate_pct,
    -- Efficiency: cost index vs specialty average
    ROUND(AVG(c.allowed_amount) /
          NULLIF(AVG(AVG(c.allowed_amount)) OVER
              (PARTITION BY p.specialty_code), 0) * 100, 1) AS cost_index,
    CASE
        WHEN ROUND(AVG(c.allowed_amount) /
             NULLIF(AVG(AVG(c.allowed_amount)) OVER
                 (PARTITION BY p.specialty_code), 0) * 100, 1) <= 90
             THEN 'High Value'
        WHEN ROUND(AVG(c.allowed_amount) /
             NULLIF(AVG(AVG(c.allowed_amount)) OVER
                 (PARTITION BY p.specialty_code), 0) * 100, 1) >= 120
             THEN 'High Cost'
        ELSE 'Average'
    END                                 AS value_tier
FROM gold.claims_fact c
JOIN gold.provider_master p
    ON c.rendering_provider_npi = p.provider_npi
WHERE c.claim_status IN ('PAID', 'ADJUSTED')
  AND c.service_date >= DATEADD(YEAR, -1, CURRENT_DATE())
  AND c._environment = 'prod'
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(DISTINCT c.claim_id) >= 10;

-- ============================================================================
-- VIEW 4: Pipeline Observability Dashboard
-- Used by: Operations team, SLA monitoring
-- ============================================================================
CREATE OR REPLACE VIEW audit.vw_pipeline_health AS
SELECT
    pipeline_name,
    run_date,
    status,
    records_written,
    ROUND(dq_score, 1)                  AS dq_score,
    DATEDIFF(MINUTE, started_at,
             completed_at)              AS runtime_minutes,
    sla_met,
    CASE
        WHEN dq_score >= 95 AND sla_met = TRUE  THEN 'Healthy'
        WHEN dq_score >= 80 OR  sla_met = FALSE THEN 'Warning'
        ELSE 'Critical'
    END                                 AS health_status,
    -- 7-day rolling average
    ROUND(AVG(dq_score) OVER (
        PARTITION BY pipeline_name
        ORDER BY run_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1)                               AS dq_7d_rolling_avg,
    -- Day-over-day record count change
    LAG(records_written) OVER (
        PARTITION BY pipeline_name
        ORDER BY run_date
    )                                   AS prev_day_records,
    ROUND(
        (records_written - LAG(records_written) OVER (
            PARTITION BY pipeline_name ORDER BY run_date
        )) * 100.0 /
        NULLIF(LAG(records_written) OVER (
            PARTITION BY pipeline_name ORDER BY run_date
        ), 0), 1
    )                                   AS record_count_change_pct
FROM audit.pipeline_runs
WHERE environment = 'prod'
  AND run_date >= DATEADD(DAY, -30, CURRENT_DATE());
