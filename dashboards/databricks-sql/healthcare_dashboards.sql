-- ============================================================================
-- HEALTHCARE PLATFORM — Databricks SQL Dashboard Queries
-- Dashboard: Claims Analytics | Population Health | Provider Performance
-- ============================================================================


-- ============================================================================
-- WIDGET 1: CLAIMS VOLUME TREND (Line Chart)
-- X-axis: Month | Y-axis: Claim Count | Series: Service Category
-- ============================================================================
SELECT
    DATE_TRUNC('month', service_date)   AS service_month,
    service_category,
    COUNT(DISTINCT claim_id)            AS claim_count,
    SUM(allowed_amount)                 AS total_allowed,
    SUM(plan_paid_amount)               AS total_plan_paid,
    SUM(member_deductible_amt +
        member_copay_amt +
        member_coinsurance_amt)         AS total_member_cost_sharing,
    AVG(allowed_amount)                 AS avg_claim_cost,
    COUNT(DISTINCT member_id)           AS unique_members
FROM {{ catalog }}.gold.claims_fact
WHERE service_date >= DATE_ADD(CURRENT_DATE(), -365)
  AND claim_status IN ('PAID', 'ADJUSTED')
GROUP BY 1, 2
ORDER BY 1, 2;


-- ============================================================================
-- WIDGET 2: DENIAL RATE BY SERVICE CATEGORY (Bar + KPI)
-- ============================================================================
SELECT
    service_category,
    COUNT(DISTINCT claim_id)                         AS total_claims,
    SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) AS denied_claims,
    ROUND(
        SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(DISTINCT claim_id), 0), 2
    )                                                AS denial_rate_pct,
    AVG(
        DATEDIFF(paid_date, received_date)
    )                                                AS avg_processing_days
FROM {{ catalog }}.gold.claims_fact
WHERE service_date >= DATE_ADD(CURRENT_DATE(), -365)
GROUP BY 1
ORDER BY denial_rate_pct DESC;


-- ============================================================================
-- WIDGET 3: TOP 20 DIAGNOSIS CODES BY COST (Table)
-- ============================================================================
SELECT
    c.primary_diagnosis_code                    AS icd10_code,
    d.diagnosis_description,
    d.diagnosis_category,
    COUNT(DISTINCT c.claim_id)                  AS claim_count,
    COUNT(DISTINCT c.member_id)                 AS member_count,
    SUM(c.allowed_amount)                       AS total_allowed_cost,
    AVG(c.allowed_amount)                       AS avg_cost_per_claim,
    ROUND(
        SUM(c.allowed_amount) * 100.0 /
        SUM(SUM(c.allowed_amount)) OVER (), 2
    )                                           AS pct_of_total_cost
FROM {{ catalog }}.gold.claims_fact c
LEFT JOIN {{ catalog }}.reference.icd10_codes d
    ON c.primary_diagnosis_code = d.icd10_code
WHERE c.service_date >= DATE_ADD(CURRENT_DATE(), -365)
  AND c.claim_status IN ('PAID', 'ADJUSTED')
GROUP BY 1, 2, 3
ORDER BY total_allowed_cost DESC
LIMIT 20;


-- ============================================================================
-- WIDGET 4: PMPM COST TREND (Per Member Per Month)
-- ============================================================================
SELECT
    DATE_TRUNC('month', c.service_date)         AS service_month,
    metal_tier,
    SUM(c.plan_paid_amount)                     AS total_plan_cost,
    COUNT(DISTINCT c.member_id)                 AS unique_members,
    ROUND(
        SUM(c.plan_paid_amount) /
        NULLIF(COUNT(DISTINCT c.member_id), 0), 2
    )                                           AS pmpm_cost,
    ROUND(AVG(
        SUM(c.plan_paid_amount) /
        NULLIF(COUNT(DISTINCT c.member_id), 0)
    ) OVER (
        PARTITION BY metal_tier
        ORDER BY DATE_TRUNC('month', c.service_date)
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                       AS pmpm_3mo_rolling_avg
FROM {{ catalog }}.gold.claims_fact c
LEFT JOIN {{ catalog }}.gold.member_eligibility m USING (member_id, plan_year)
WHERE c.service_date >= DATE_ADD(CURRENT_DATE(), -365)
  AND c.claim_status IN ('PAID', 'ADJUSTED')
GROUP BY 1, 2
ORDER BY 1, 2;


-- ============================================================================
-- WIDGET 5: HEDIS QUALITY SCORECARD (Table + Conditional Formatting)
-- ============================================================================
SELECT
    h.measure_code,
    CASE h.measure_code
        WHEN 'BCS' THEN 'Breast Cancer Screening'
        WHEN 'COL' THEN 'Colorectal Cancer Screening'
        WHEN 'CBP' THEN 'Controlling Blood Pressure'
        WHEN 'CDC' THEN 'Diabetes HbA1c Control'
        WHEN 'DMS' THEN 'Depression Screening'
        WHEN 'MPM' THEN 'Monitoring for Persistent Meds'
        ELSE h.measure_code
    END                                         AS measure_name,
    h.denominator,
    h.numerator,
    h.rate                                      AS current_rate_pct,
    b.benchmark_50th                            AS ncqa_50th_percentile,
    b.benchmark_75th                            AS ncqa_75th_percentile,
    b.benchmark_90th                            AS ncqa_90th_percentile,
    CASE
        WHEN h.rate >= b.benchmark_90th THEN '🟢 Excellent'
        WHEN h.rate >= b.benchmark_75th THEN '🟡 Good'
        WHEN h.rate >= b.benchmark_50th THEN '🟠 Average'
        ELSE '🔴 Below Average'
    END                                         AS performance_tier,
    h.rate - LAG(h.rate) OVER (
        PARTITION BY h.measure_code
        ORDER BY h.measurement_year
    )                                           AS yoy_change_pct
FROM {{ catalog }}.gold.hedis_measure_summary h
LEFT JOIN {{ catalog }}.reference.hedis_benchmarks b
    ON h.measure_code = b.measure_code
    AND h.measurement_year = b.benchmark_year
WHERE h.measurement_year = YEAR(CURRENT_DATE())
ORDER BY performance_tier, measure_code;


-- ============================================================================
-- WIDGET 6: POPULATION HEALTH — RISK STRATIFICATION (Donut/Pie)
-- ============================================================================
SELECT
    risk_tier,
    COUNT(DISTINCT member_id)                       AS member_count,
    ROUND(COUNT(DISTINCT member_id) * 100.0 /
          SUM(COUNT(DISTINCT member_id)) OVER (), 1) AS pct_of_population,
    ROUND(AVG(readmission_risk_score), 1)           AS avg_risk_score,
    SUM(projected_annual_cost)                      AS projected_cost
FROM {{ catalog }}.gold.readmission_care_alerts r
LEFT JOIN {{ catalog }}.gold.member_cost_projections p USING (member_id)
WHERE alert_date = CURRENT_DATE()
GROUP BY risk_tier
ORDER BY CASE risk_tier
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH'     THEN 2
    WHEN 'MEDIUM'   THEN 3
    WHEN 'LOW'      THEN 4
END;


-- ============================================================================
-- WIDGET 7: PROVIDER PERFORMANCE SCORECARD
-- ============================================================================
SELECT
    p.provider_npi,
    p.provider_name,
    p.specialty_description,
    p.network_status,
    COUNT(DISTINCT c.claim_id)                  AS total_claims,
    COUNT(DISTINCT c.member_id)                 AS unique_patients,
    ROUND(AVG(c.allowed_amount), 2)             AS avg_cost_per_claim,
    ROUND(
        SUM(CASE WHEN c.claim_status = 'DENIED' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 2
    )                                           AS denial_rate_pct,
    -- Quality: HEDIS-linked provider measures
    COUNT(DISTINCT CASE WHEN hd.cdc_hba1c_controlled THEN hd.member_id END)
        AS diabetic_patients_controlled,
    COUNT(DISTINCT CASE WHEN hd.cbp_controlled THEN hd.member_id END)
        AS hypertensive_patients_controlled,
    -- Efficiency
    ROUND(
        AVG(c.allowed_amount) /
        NULLIF(AVG(AVG(c.allowed_amount)) OVER (PARTITION BY p.specialty_description), 0)
        * 100, 1
    )                                           AS cost_index_vs_specialty_avg,
    CASE
        WHEN AVG(c.allowed_amount) <=
             PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY c.allowed_amount)
                OVER (PARTITION BY p.specialty_description) THEN '⭐ High Value'
        WHEN AVG(c.allowed_amount) >=
             PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY c.allowed_amount)
                OVER (PARTITION BY p.specialty_description) THEN '⚠️ High Cost'
        ELSE '✅ Average'
    END                                         AS value_tier
FROM {{ catalog }}.gold.claims_fact c
JOIN {{ catalog }}.reference.provider_master p
    ON c.rendering_provider_npi = p.provider_npi
LEFT JOIN {{ catalog }}.gold.hedis_member_detail hd
    ON c.member_id = hd.member_id
WHERE c.service_date >= DATE_ADD(CURRENT_DATE(), -365)
  AND c.claim_status IN ('PAID', 'ADJUSTED')
GROUP BY 1, 2, 3, 4
HAVING total_claims >= 10
ORDER BY total_claims DESC
LIMIT 100;


-- ============================================================================
-- WIDGET 8: PIPELINE OBSERVABILITY — REAL-TIME DATA HEALTH
-- ============================================================================
SELECT
    pipeline_name,
    run_date,
    status,
    records_processed,
    dq_score,
    DATEDIFF(MINUTE, started_at, completed_at) AS runtime_minutes,
    CASE
        WHEN dq_score >= 95 THEN '🟢 Healthy'
        WHEN dq_score >= 80 THEN '🟡 Warning'
        ELSE '🔴 Critical'
    END                                         AS health_status,
    CASE
        WHEN completed_at <= DATE_ADD(run_date, MAKE_INTERVAL(HOURS => sla_hour)) THEN '✅ SLA Met'
        ELSE '❌ SLA Missed'
    END                                         AS sla_status
FROM {{ catalog }}.audit.pipeline_runs
WHERE run_date >= DATE_ADD(CURRENT_DATE(), -7)
ORDER BY run_date DESC, pipeline_name;


-- ============================================================================
-- WIDGET 9: CHRONIC DISEASE PREVALENCE (Heat Map by Age Group + Condition)
-- ============================================================================
SELECT
    CASE
        WHEN age_as_of_dec31 < 30 THEN '18-29'
        WHEN age_as_of_dec31 < 45 THEN '30-44'
        WHEN age_as_of_dec31 < 60 THEN '45-59'
        WHEN age_as_of_dec31 < 75 THEN '60-74'
        ELSE '75+'
    END                                         AS age_group,
    SUM(dx_diabetes)                            AS diabetes_count,
    SUM(dx_chf)                                 AS chf_count,
    SUM(dx_copd)                                AS copd_count,
    SUM(dx_ckd)                                 AS ckd_count,
    SUM(dx_hypertension)                        AS hypertension_count,
    SUM(dx_depression)                          AS depression_count,
    COUNT(DISTINCT member_id)                   AS total_members,
    ROUND(SUM(dx_diabetes) * 100.0 / COUNT(*), 1)      AS diabetes_prevalence_pct,
    ROUND(SUM(dx_hypertension) * 100.0 / COUNT(*), 1)  AS htn_prevalence_pct,
    ROUND(SUM(dx_depression) * 100.0 / COUNT(*), 1)    AS depression_prevalence_pct
FROM {{ catalog }}.gold.readmission_risk_features
JOIN {{ catalog }}.gold.member_eligibility USING (member_id)
WHERE plan_year = YEAR(CURRENT_DATE())
GROUP BY 1
ORDER BY age_group;


-- ============================================================================
-- WIDGET 10: CARE GAPS — MEMBERS MISSING PREVENTIVE CARE
-- ============================================================================
SELECT
    m.member_id,
    m.plan_id,
    m.metal_tier,
    m.age_as_of_dec31                           AS age,
    m.gender_code,
    -- Care gaps per HEDIS
    CASE WHEN bcs.member_id IS NULL
         AND m.gender_code = 'F'
         AND m.age_as_of_dec31 BETWEEN 52 AND 74
         THEN 'Mammogram Due' END                AS bcs_gap,
    CASE WHEN col.member_id IS NULL
         AND m.age_as_of_dec31 BETWEEN 45 AND 75
         THEN 'Colorectal Screening Due' END     AS col_gap,
    CASE WHEN cdc.cdc_hba1c_controlled = FALSE
         AND cdc.member_id IS NOT NULL
         THEN 'HbA1c Not Controlled' END         AS cdc_gap,
    CASE WHEN cbp.cbp_controlled = FALSE
         AND cbp.member_id IS NOT NULL
         THEN 'BP Not Controlled' END            AS cbp_gap,
    -- Total gap count
    (CASE WHEN bcs.member_id IS NULL AND m.gender_code = 'F' THEN 1 ELSE 0 END +
     CASE WHEN col.member_id IS NULL THEN 1 ELSE 0 END +
     CASE WHEN cdc.cdc_hba1c_controlled = FALSE THEN 1 ELSE 0 END +
     CASE WHEN cbp.cbp_controlled = FALSE THEN 1 ELSE 0 END
    )                                            AS total_care_gaps,
    r.risk_tier                                  AS readmission_risk_tier
FROM {{ catalog }}.gold.member_eligibility m
LEFT JOIN {{ catalog }}.gold.hedis_member_detail bcs
    ON m.member_id = bcs.member_id AND bcs.measure_code = 'BCS' AND bcs.bcs_met = TRUE
LEFT JOIN {{ catalog }}.gold.hedis_member_detail col
    ON m.member_id = col.member_id AND col.measure_code = 'COL' AND col.col_met = TRUE
LEFT JOIN {{ catalog }}.gold.hedis_member_detail cdc
    ON m.member_id = cdc.member_id AND cdc.measure_code = 'CDC'
LEFT JOIN {{ catalog }}.gold.hedis_member_detail cbp
    ON m.member_id = cbp.member_id AND cbp.measure_code = 'CBP'
LEFT JOIN {{ catalog }}.gold.readmission_care_alerts r
    ON m.member_id = r.member_id AND r.alert_date = CURRENT_DATE()
WHERE m.plan_year = YEAR(CURRENT_DATE())
  AND m.is_continuously_enrolled = TRUE
HAVING total_care_gaps > 0
ORDER BY total_care_gaps DESC, readmission_risk_tier
LIMIT 10000;
