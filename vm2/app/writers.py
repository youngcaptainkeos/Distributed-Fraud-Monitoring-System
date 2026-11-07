"""Iceberg and HDFS write handlers for PySpark Structured Streaming.

Provides idempotent write operations for micro-batches:
1. Table initialization (CREATE TABLE IF NOT EXISTS) for decisions and alerts
2. Idempotent upsert / append using Spark SQL MERGE INTO or append mode
3. Checkpoint management pointing to HDFS
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from config.config import config

logger = logging.getLogger("FraudDetectionWriter")


def initialize_iceberg_tables(spark: SparkSession, decisions_table: str, alerts_table: str):
    """Ensures Iceberg tables are created in the catalog with proper schemas."""
    logger.info("Initializing Iceberg tables: %s and %s", decisions_table, alerts_table)

    # DDL for transaction_decisions
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {decisions_table} (
            transaction_id STRING,
            source_account BIGINT,
            destination_account BIGINT,
            event_timestamp TIMESTAMP,
            evaluated_at TIMESTAMP,
            amount DOUBLE,
            currency STRING,
            r1_triggered BOOLEAN,
            r2_triggered BOOLEAN,
            r3_triggered BOOLEAN,
            r4_triggered BOOLEAN,
            r5_risk_score INT,
            r5_block_triggered BOOLEAN,
            decision STRING,
            related_alert_ids STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_timestamp))
    """)

    # DDL for account_risk_alerts
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {alerts_table} (
            alert_id STRING,
            rule_id STRING,
            source_account BIGINT,
            destination_account BIGINT,
            trigger_transaction_id STRING,
            finding_scope STRING,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            observed_count BIGINT,
            observed_amount_inr DOUBLE,
            count_threshold BIGINT,
            amount_threshold_inr DOUBLE,
            supporting_transaction_ids STRING,
            explanation STRING,
            triggered_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (rule_id)
    """)
    logger.info("Iceberg tables verified/initialized successfully.")


def write_micro_batch_to_iceberg(
    spark: SparkSession,
    decisions_df: DataFrame,
    alerts_df: DataFrame,
    batch_id: int,
    decisions_table: str,
    alerts_table: str,
    use_merge: bool = True
):
    """Writes a micro-batch of decisions and alerts to Iceberg tables.
    
    Uses MERGE INTO by primary key to guarantee idempotency on retries/restarts.
    """
    if decisions_df is not None and not decisions_df.rdd.isEmpty():
        logger.info("[Batch %d] Writing %d decisions to %s", batch_id, decisions_df.count(), decisions_table)
        if use_merge:
            decisions_df.createOrReplaceTempView("batch_decisions")
            spark.sql(f"""
                MERGE INTO {decisions_table} AS target
                USING batch_decisions AS source
                ON target.transaction_id = source.transaction_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            decisions_df.write.format("iceberg").mode("append").save(decisions_table)

    if alerts_df is not None and not alerts_df.rdd.isEmpty():
        logger.info("[Batch %d] Writing %d alerts to %s", batch_id, alerts_df.count(), alerts_table)
        if use_merge:
            alerts_df.createOrReplaceTempView("batch_alerts")
            spark.sql(f"""
                MERGE INTO {alerts_table} AS target
                USING batch_alerts AS source
                ON target.alert_id = source.alert_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            alerts_df.write.format("iceberg").mode("append").save(alerts_table)
