"""Main PySpark Structured Streaming application for Real-Time Fraud Monitoring.

Runs exclusively on VM2 (Spark Master/Driver).
- Ingests payment events from Apache Kafka (VM1)
- Parses JSON with explicit StringType schema and casts to target types
- Applies 1-minute event-time watermark and deduplication on transaction_id
- Evaluates fraud rules R1–R5 over rolling 2-min windows (30s slide) across cumulative history
- Writes outputs to Apache Iceberg tables backed by HDFS (VM3) with MERGE INTO idempotency
- Checkpoints state strictly to HDFS
"""

import logging
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp

from config.config import config
from app.schema import (
    ACCOUNT_RISK_ALERTS_SCHEMA,
    KAFKA_TRANSACTION_SCHEMA,
    TRANSACTION_DECISIONS_SCHEMA,
)
from app.rules import evaluate_batch
from app.writers import initialize_iceberg_tables, write_micro_batch_to_iceberg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("FraudMonitoringJob")

EVENT_COLS = [
    "transaction_id",
    "source_account",
    "destination_account",
    "event_timestamp",
    "amount",
    "currency",
]


def build_spark_session() -> SparkSession:
    """Builds and configures the SparkSession with Iceberg catalog and extensions."""
    logger.info("Initializing SparkSession (configured master: '%s')", config.spark_master_url or "from spark-submit")
    builder = SparkSession.builder.appName(config.spark_app_name)
    if config.spark_master_url:
        builder = builder.master(config.spark_master_url)

    builder = (
        builder
        # Iceberg SQL Extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        # Iceberg Hadoop Catalog configuration on HDFS
        .config(
            f"spark.sql.catalog.{config.iceberg_catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.type",
            "hadoop"
        )
        .config(
            f"spark.sql.catalog.{config.iceberg_catalog_name}.warehouse",
            config.iceberg_warehouse
        )
        # Adaptive execution & time zones
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )
    return builder.getOrCreate()


def process_micro_batch(batch_df, batch_id: int):
    """Processes each streaming micro-batch.
    
    Evaluates cumulative window state by combining previously stored events
    with newly arrived records, ensuring sliding windows spanning micro-batches
    and late arrivals (< 1 min) are accurately resolved and updated via MERGE.
    """
    if batch_df.rdd.isEmpty():
        logger.info("[Batch %d] Empty batch received; skipping.", batch_id)
        return

    logger.info("[Batch %d] Processing micro-batch with %d records.", batch_id, batch_df.count())
    spark = batch_df.sparkSession

    # Select event columns from the current micro-batch
    new_pdf = batch_df.select(*EVENT_COLS).toPandas()
    if new_pdf.empty:
        return

    # Read previously stored events from the Iceberg decisions table to maintain cross-batch window state
    try:
        stored = spark.table(config.decisions_table).select(*EVENT_COLS).toPandas()
    except Exception as e:
        logger.info("[Batch %d] No existing decisions table found (%s); starting with current batch.", batch_id, e)
        stored = pd.DataFrame(columns=EVENT_COLS)

    # Combine historical events with new micro-batch events
    all_pdf = (
        pd.concat([stored, new_pdf], ignore_index=True)
        .drop_duplicates(subset=["transaction_id"])
        .reset_index(drop=True)
    )

    # Re-evaluate R1–R5 rules, sliding windows, and alert linkages across cumulative stream
    df_decisions, df_alerts = evaluate_batch(all_pdf)

    # Convert evaluated results back to PySpark DataFrames with strict schemas
    spark_decisions = spark.createDataFrame(df_decisions, schema=TRANSACTION_DECISIONS_SCHEMA)
    if not df_alerts.empty:
        records_alerts = df_alerts.to_dict(orient="records")
        for r in records_alerts:
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None
                elif isinstance(v, pd.Timestamp):
                    r[k] = v.to_pydatetime()
                elif k in ("source_account", "destination_account", "observed_count", "count_threshold") and v is not None:
                    r[k] = int(v)
                elif k in ("observed_amount_inr", "amount_threshold_inr") and v is not None:
                    r[k] = float(v)
        spark_alerts = spark.createDataFrame(records_alerts, schema=ACCOUNT_RISK_ALERTS_SCHEMA)
    else:
        spark_alerts = None

    # Idempotent write to Iceberg tables on HDFS (MERGE INTO by primary keys)
    write_micro_batch_to_iceberg(
        spark=spark,
        decisions_df=spark_decisions,
        alerts_df=spark_alerts,
        batch_id=batch_id,
        decisions_table=config.decisions_table,
        alerts_table=config.alerts_table,
        use_merge=True
    )
    logger.info("[Batch %d] Successfully updated %d decisions and %d alerts in Iceberg.",
                batch_id, len(df_decisions), len(df_alerts))


def main():
    logger.info("=== Starting Fraud Monitoring Streaming Pipeline (Run: %s) ===", config.run_id)
    logger.info("Kafka Bootstrap: %s | Topic: %s", config.kafka_bootstrap_servers, config.kafka_topic)
    logger.info("HDFS Checkpoint: %s", config.checkpoint_location)
    logger.info("Iceberg Tables: %s, %s", config.decisions_table, config.alerts_table)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Initialize Iceberg tables
    initialize_iceberg_tables(spark, config.decisions_table, config.alerts_table)

    # 1. Read Kafka streaming source
    logger.info("Subscribing to Kafka topic: %s", config.kafka_topic)
    kafka_raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", config.kafka_starting_offsets)
        .load()
    )

    # 2. Parse JSON payload with StringType schema and explicitly cast columns
    parsed_stream = (
        kafka_raw_stream
        .selectExpr("CAST(value AS STRING) as json_val")
        .select(from_json(col("json_val"), KAFKA_TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("source_account", col("source_account").cast("long"))
        .withColumn("destination_account", col("destination_account").cast("long"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("account_age_days", col("account_age_days").cast("int"))
    )

    # 3. Apply 1-minute event-time watermark and deduplication on transaction_id
    watermarked_stream = (
        parsed_stream
        .withWatermark("event_timestamp", config.watermark_delay)
        .dropDuplicates(["transaction_id", "event_timestamp"])
    )

    # 4. Write stream using foreachBatch to evaluate rules cumulatively and sink to Iceberg
    query = (
        watermarked_stream
        .writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", config.checkpoint_location)
        .trigger(processingTime=config.trigger_interval)
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received; stopping query.")
        query.stop()
    except Exception as e:
        logger.error("Streaming query failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
