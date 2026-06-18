-- ============================================================================
-- STORED PROCEDURES: Healthcare Data Platform
-- Purpose: Watermark management, audit logging, data quality reporting
-- ============================================================================

-- ============================================================================
-- SP 1: Update Pipeline Watermark
-- Called by ADF after each successful pipeline run
-- ============================================================================
CREATE OR ALTER PROCEDURE [audit].[usp_UpdateWatermark]
    @PipelineName   VARCHAR(100),
    @WatermarkValue VARCHAR(100),
    @Environment    VARCHAR(20)  = 'dev',
    @RecordsLoaded  INT          = 0,
    @Status         VARCHAR(20)  = 'SUCCESS'
AS
BEGIN
    SET NOCOUNT ON;

    MERGE [audit].[pipeline_watermarks] AS target
    USING (
        SELECT
            @PipelineName   AS pipeline_name,
            @Environment    AS environment,
            @WatermarkValue AS watermark_value,
            @RecordsLoaded  AS records_loaded,
            SYSDATETIME()   AS updated_at
    ) AS source
    ON (
        target.pipeline_name = source.pipeline_name AND
        target.environment   = source.environment
    )
    WHEN MATCHED THEN
        UPDATE SET
            watermark_value = source.watermark_value,
            records_loaded  = source.records_loaded,
            updated_at      = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT (pipeline_name, environment, watermark_value, records_loaded, updated_at)
        VALUES (source.pipeline_name, source.environment, source.watermark_value,
                source.records_loaded, source.updated_at);

    PRINT '[WATERMARK] Updated: ' + @PipelineName + ' → ' + @WatermarkValue;
END;
GO

-- ============================================================================
-- SP 2: Get Pipeline Watermark
-- Called by ADF at pipeline start to get last successful load point
-- ============================================================================
CREATE OR ALTER PROCEDURE [audit].[usp_GetWatermark]
    @PipelineName   VARCHAR(100),
    @Environment    VARCHAR(20)  = 'dev',
    @DefaultDate    VARCHAR(20)  = '2020-01-01'
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        ISNULL(watermark_value, @DefaultDate) AS watermark_value,
        ISNULL(records_loaded, 0)             AS last_records_loaded,
        updated_at                             AS last_run_at
    FROM [audit].[pipeline_watermarks]
    WHERE pipeline_name = @PipelineName
      AND environment   = @Environment;

    -- Return default if no watermark exists yet
    IF @@ROWCOUNT = 0
        SELECT
            @DefaultDate AS watermark_value,
            0            AS last_records_loaded,
            NULL         AS last_run_at;
END;
GO

-- ============================================================================
-- SP 3: Log Pipeline Run
-- Records pipeline execution details for SLA monitoring and audit
-- ============================================================================
CREATE OR ALTER PROCEDURE [audit].[usp_LogPipelineRun]
    @PipelineName   VARCHAR(100),
    @SourceId       VARCHAR(100) = NULL,
    @RunDate        DATE,
    @Environment    VARCHAR(20)  = 'dev',
    @Status         VARCHAR(20)  = 'RUNNING',
    @RecordsRead    INT          = 0,
    @RecordsWritten INT          = 0,
    @DQScore        DECIMAL(5,1) = 0,
    @ErrorMessage   VARCHAR(2000) = NULL,  -- Never include PHI in error messages
    @SLAHour        INT          = 6
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SLAMet BIT = 0;

    -- Check if pipeline completed within SLA
    IF DATEPART(HOUR, SYSDATETIME()) <= @SLAHour
        SET @SLAMet = 1;

    INSERT INTO [audit].[pipeline_runs] (
        pipeline_name, source_id, run_date, environment,
        status, records_read, records_written,
        dq_score, error_message, sla_hour, sla_met,
        started_at, completed_at
    )
    VALUES (
        @PipelineName, @SourceId, @RunDate, @Environment,
        @Status, @RecordsRead, @RecordsWritten,
        @DQScore, @ErrorMessage, @SLAHour, @SLAMet,
        SYSDATETIME(), CASE WHEN @Status IN ('SUCCESS','FAILED') THEN SYSDATETIME() ELSE NULL END
    );

    SELECT SCOPE_IDENTITY() AS run_id;
END;
GO

-- ============================================================================
-- SP 4: Daily Data Quality Summary Report
-- Returns DQ score trends for operational dashboard
-- ============================================================================
CREATE OR ALTER PROCEDURE [audit].[usp_DQSummaryReport]
    @Environment    VARCHAR(20) = 'prod',
    @Days           INT         = 30
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        pipeline_name,
        run_date,
        status,
        records_written,
        dq_score,
        sla_met,
        -- 7-day rolling average DQ score
        AVG(dq_score) OVER (
            PARTITION BY pipeline_name
            ORDER BY run_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS dq_score_7d_avg,
        -- Flag deteriorating quality
        CASE
            WHEN dq_score < 80 THEN 'CRITICAL'
            WHEN dq_score < 90 THEN 'WARNING'
            ELSE 'HEALTHY'
        END AS health_status
    FROM [audit].[pipeline_runs]
    WHERE environment = @Environment
      AND run_date    >= DATEADD(DAY, -@Days, CAST(GETDATE() AS DATE))
    ORDER BY run_date DESC, pipeline_name;
END;
GO

-- ============================================================================
-- SP 5: SLA Breach Alert Check
-- Called by Azure Monitor / Azure Function for alerting
-- ============================================================================
CREATE OR ALTER PROCEDURE [audit].[usp_CheckSLABreaches]
    @Environment    VARCHAR(20) = 'prod',
    @RunDate        DATE        = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @RunDate IS NULL SET @RunDate = CAST(GETDATE() AS DATE);

    -- Expected daily pipelines
    DECLARE @ExpectedPipelines TABLE (pipeline_name VARCHAR(100), sla_hour INT);
    INSERT INTO @ExpectedPipelines VALUES
        ('pl_bronze_claims',      5),
        ('pl_bronze_eligibility', 5),
        ('pl_bronze_provider',    5),
        ('pl_bronze_pharmacy',    5),
        ('pl_master_ingestion',   6);

    -- Find missed or failed pipelines
    SELECT
        ep.pipeline_name,
        ep.sla_hour,
        ISNULL(pr.status, 'NOT_RUN')   AS status,
        pr.dq_score,
        pr.completed_at,
        CASE
            WHEN pr.run_id IS NULL            THEN 'SLA_MISSED_NOT_RUN'
            WHEN pr.status = 'FAILED'         THEN 'SLA_MISSED_FAILED'
            WHEN pr.sla_met = 0               THEN 'SLA_MISSED_LATE'
            ELSE 'OK'
        END                             AS breach_type
    FROM @ExpectedPipelines ep
    LEFT JOIN [audit].[pipeline_runs] pr
        ON  ep.pipeline_name = pr.pipeline_name
        AND pr.run_date      = @RunDate
        AND pr.environment   = @Environment
    WHERE
        pr.run_id IS NULL OR
        pr.status = 'FAILED' OR
        pr.sla_met = 0
    ORDER BY ep.sla_hour;
END;
GO
