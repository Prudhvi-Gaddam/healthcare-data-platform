"""
audit_manager.py
================
REUSABLE FRAMEWORK — Pipeline Audit & Data Lineage Manager

Provides:
  - Pipeline run tracking (start, progress, completion)
  - Data lineage recording (source → target transformations)
  - SLA monitoring and breach detection
  - HIPAA-compliant audit trails (no PHI in logs)
  - Watermark management for incremental loads
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from datetime import datetime
import logging

logger = logging.getLogger("HealthcarePlatform.Audit")


@dataclass
class PipelineRunContext:
    """Context object for a single pipeline execution."""
    pipeline_name: str
    source_id: str
    run_date: str
    environment: str
    run_id: Optional[int] = None
    started_at: str = field(default_factory=lambda: str(datetime.now()))
    status: str = "RUNNING"
    records_read: int = 0
    records_written: int = 0
    dq_score: float = 0.0
    error_message: Optional[str] = None


class AuditManager:
    """
    Full-featured pipeline audit manager.
    Writes to Delta audit tables for lineage tracking and SLA monitoring.
    All log entries are HIPAA-safe — no PHI in any audit record.
    """

    AUDIT_SCHEMA = StructType([
        StructField("pipeline_name",   StringType()),
        StructField("source_id",       StringType()),
        StructField("run_date",        StringType()),
        StructField("environment",     StringType()),
        StructField("status",          StringType()),
        StructField("records_read",    IntegerType()),
        StructField("records_written", IntegerType()),
        StructField("dq_score",        DoubleType()),
        StructField("error_message",   StringType()),
        StructField("started_at",      StringType()),
        StructField("completed_at",    StringType()),
    ])

    LINEAGE_SCHEMA = StructType([
        StructField("source_table",   StringType()),
        StructField("target_table",   StringType()),
        StructField("transformation", StringType()),
        StructField("records_count",  IntegerType()),
        StructField("pipeline_name",  StringType()),
        StructField("run_date",       StringType()),
        StructField("recorded_at",    StringType()),
    ])

    def __init__(self, spark: SparkSession, catalog: str,
                 audit_schema: str = "audit"):
        self.spark = spark
        self.catalog = catalog
        self.audit_table   = f"{catalog}.{audit_schema}.pipeline_runs"
        self.lineage_table = f"{catalog}.{audit_schema}.data_lineage"
        self.watermark_table = f"{catalog}.{audit_schema}.pipeline_watermarks"

    def start_run(self, ctx: PipelineRunContext) -> PipelineRunContext:
        """Register pipeline run start. Returns context with run_id."""
        record = self.spark.createDataFrame([{
            "pipeline_name":   ctx.pipeline_name,
            "source_id":       ctx.source_id,
            "run_date":        ctx.run_date,
            "environment":     ctx.environment,
            "status":          "RUNNING",
            "records_read":    0,
            "records_written": 0,
            "dq_score":        0.0,
            "error_message":   None,
            "started_at":      ctx.started_at,
            "completed_at":    None,
        }])
        record.write.format("delta").mode("append").saveAsTable(self.audit_table)

        # Get assigned run_id
        run_id = self.spark.table(self.audit_table) \
            .filter(F.col("pipeline_name") == ctx.pipeline_name) \
            .filter(F.col("run_date") == ctx.run_date) \
            .filter(F.col("environment") == ctx.environment) \
            .agg(F.max("run_id")).first()[0]

        ctx.run_id = run_id
        logger.info(f"[AUDIT] Started run {run_id}: {ctx.pipeline_name} | {ctx.run_date}")
        return ctx

    def complete_run(self, ctx: PipelineRunContext,
                     status: str = "SUCCESS") -> None:
        """Update audit record on pipeline completion."""
        ctx.status = status
        try:
            self.spark.sql(f"""
                UPDATE {self.audit_table}
                SET status          = '{status}',
                    records_read    = {ctx.records_read},
                    records_written = {ctx.records_written},
                    dq_score        = {ctx.dq_score},
                    error_message   = {'NULL' if not ctx.error_message else f"'{ctx.error_message[:500]}'"},
                    completed_at    = '{datetime.now()}'
                WHERE pipeline_name = '{ctx.pipeline_name}'
                  AND run_date      = '{ctx.run_date}'
                  AND environment   = '{ctx.environment}'
                  AND status        = 'RUNNING'
            """)
            logger.info(
                f"[AUDIT] Completed: {ctx.pipeline_name} | {status} | "
                f"Records: {ctx.records_written:,} | DQ: {ctx.dq_score}"
            )
        except Exception as e:
            # Audit failures should never crash the pipeline
            logger.error(f"[AUDIT] Failed to update run record: {type(e).__name__}")

    def log_lineage(self, source_table: str, target_table: str,
                    transformation: str, records_count: int,
                    pipeline_name: str, run_date: str) -> None:
        """Record data lineage for HIPAA audit trail and debugging."""
        record = self.spark.createDataFrame([{
            "source_table":   source_table,
            "target_table":   target_table,
            "transformation": transformation,
            "records_count":  records_count,
            "pipeline_name":  pipeline_name,
            "run_date":       run_date,
            "recorded_at":    str(datetime.now()),
        }])
        record.write.format("delta").mode("append").saveAsTable(self.lineage_table)

    def get_watermark(self, pipeline_name: str,
                      environment: str,
                      default_date: str = "2020-01-01") -> str:
        """Get last successful load watermark for incremental processing."""
        try:
            result = self.spark.table(self.watermark_table) \
                .filter(F.col("pipeline_name") == pipeline_name) \
                .filter(F.col("environment") == environment) \
                .agg(F.max("watermark_value")).first()[0]
            watermark = result if result else default_date
            logger.info(f"[WATERMARK] {pipeline_name}: {watermark}")
            return watermark
        except Exception:
            logger.info(f"[WATERMARK] No watermark found, using default: {default_date}")
            return default_date

    def update_watermark(self, pipeline_name: str, environment: str,
                         watermark_value: str, records_loaded: int) -> None:
        """Persist new watermark after successful pipeline completion."""
        try:
            # Delete existing watermark for this pipeline+env
            self.spark.sql(f"""
                DELETE FROM {self.watermark_table}
                WHERE pipeline_name = '{pipeline_name}'
                  AND environment   = '{environment}'
            """)
            # Insert new watermark
            record = self.spark.createDataFrame([{
                "pipeline_name":  pipeline_name,
                "environment":    environment,
                "watermark_value": watermark_value,
                "records_loaded": records_loaded,
                "updated_at":     str(datetime.now()),
            }])
            record.write.format("delta").mode("append") \
                .saveAsTable(self.watermark_table)
            logger.info(f"[WATERMARK] Updated: {pipeline_name} → {watermark_value}")
        except Exception as e:
            logger.error(f"[WATERMARK] Update failed: {type(e).__name__}")

    def check_sla_breaches(self, environment: str,
                            expected_pipelines: List[str],
                            sla_hour: int = 6) -> List[str]:
        """
        Check which expected pipelines missed their SLA.
        Returns list of pipeline names that missed SLA.
        """
        from datetime import date
        today = str(date.today())

        completed = [
            row["pipeline_name"] for row in
            self.spark.table(self.audit_table)
            .filter(F.col("run_date") == today)
            .filter(F.col("environment") == environment)
            .filter(F.col("status") == "SUCCESS")
            .select("pipeline_name").collect()
        ]

        missed = [p for p in expected_pipelines if p not in completed]
        if missed:
            logger.warning(f"[SLA] Missed pipelines: {missed}")
        return missed

    def get_health_summary(self, environment: str,
                            days: int = 7) -> DataFrame:
        """Return 7-day pipeline health summary DataFrame."""
        return self.spark.table(self.audit_table) \
            .filter(F.col("environment") == environment) \
            .filter(F.col("run_date") >= F.date_sub(F.current_date(), days)) \
            .groupBy("pipeline_name") \
            .agg(
                F.count("*").alias("total_runs"),
                F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
                F.sum(F.when(F.col("status") == "FAILED",  1).otherwise(0)).alias("failed_runs"),
                F.round(F.avg("dq_score"), 1).alias("avg_dq_score"),
                F.round(F.avg("records_written"), 0).alias("avg_records"),
                F.max("run_date").alias("last_run_date"),
                F.sum(F.when(F.col("sla_met") == True, 1).otherwise(0)).alias("sla_met_count")
            ) \
            .withColumn(
                "success_rate_pct",
                F.round(F.col("successful_runs") / F.col("total_runs") * 100, 1)
            ) \
            .withColumn(
                "health_status",
                F.when(
                    (F.col("avg_dq_score") >= 95) & (F.col("success_rate_pct") >= 95),
                    "Healthy"
                ).when(
                    (F.col("avg_dq_score") >= 80) | (F.col("success_rate_pct") >= 80),
                    "Warning"
                ).otherwise("Critical")
            ) \
            .orderBy("pipeline_name")
