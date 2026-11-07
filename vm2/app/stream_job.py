"""Main PySpark Structured Streaming application for Real-Time Fraud Monitoring.

Runs exclusively on VM2 (Spark Master/Driver).
- Ingests payment events from Apache Kafka (VM1)
- Parses JSON using explicit schema
- Applies 1-minute event-time watermark and deduplication on transaction_id
- Evaluates fraud rules R1–R5 over rolling 2-min windows (30s slide)
- Writes outputs to Apache Iceberg tables backed by HDFS (VM3)
- Checkpoints state strictly to HDFS
"""

import logging
import sys
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


def build_spark_session() -> SparkSession:
    """Builds and configures the SparkSession with Iceberg catalog and extensions."""
    logger.info("Initializing SparkSession on Master: %s", config.spark_master_url)
    builder = (
        SparkSession.builder
        .appName(config.spark_app_name)
        .master(config.spark_master_url)
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
    """Processes each streaming micro-batch with deduplication, R1–R5 rules, and Iceberg write."""
    if batch_df.rdd.isEmpty():
        logger.info("[Batch %d] Empty batch received; skipping.", batch_id)
        return

    logger.info("[Batch %d] Processing micro-batch with %d records.", batch_id, batch_df.count())
    spark = batch_df.sparkSession

    # Convert Spark DataFrame to pandas for deterministic sliding-window fraud rule evaluation
    pdf = batch_df.toPandas()

    # Drop any in-batch duplicates on transaction_id
    pdf_dedup = pdf.drop_duplicates(subset=["transaction_id"]).reset_index(drop=True)

    # Evaluate R1–R5 rules, sliding windows, and alert linkages
    df_decisions, df_alerts = evaluate_batch(pdf_dedup)

    # Convert back to PySpark DataFrames with strict schemas
    spark_decisions = spark.createDataFrame(df_decisions, schema=TRANSACTION_DECISIONS_SCHEMA)
    spark_alerts = spark.createDataFrame(df_alerts, schema=ACCOUNT_RISK_ALERTS_SCHEMA)

    # Idempotent write to Iceberg tables on HDFS
    write_micro_batch_to_iceberg(
        spark=spark,
        decisions_df=spark_decisions,
        alerts_df=spark_alerts,
        batch_id=batch_id,
        decisions_table=config.decisions_table,
        alerts_table=config.alerts_table,
        use_merge=True
    )
    logger.info("[Batch %d] Successfully wrote %d decisions and %d alerts to Iceberg.",
                batch_id, len(df_decisions), len(df_alerts))


def main():
    logger.info("=== Starting Fraud Monitoring Streaming Pipeline (Run: %s) ===", config.run_id)
    logger.info("Kafka Bootstrap: %s | Topic: %s", config.kafka_bootstrap_servers, config.kafka_topic)
    logger.info("HDFS Checkpoint: %s", config.checkpoint_location)
    logger.info("Iceberg Tables: %s, %s", config.decisions_table, config.alerts_table)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Initialize Iceberg tables if needed
    try:
        initialize_iceberg_tables(spark, config.decisions_table, config.alerts_table)
    except Exception as e:
        logger.warning("Table initialization warning (tables may already exist): %s", e)

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

    # 2. Parse JSON payload and enforce explicit schema
    parsed_stream = (
        kafka_raw_stream
        .selectExpr("CAST(value AS STRING) as json_val")
        .select(from_json(col("json_val"), KAFKA_TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    )

    # 3. Apply 1-minute event-time watermark and deduplication
    watermarked_stream = (
        parsed_stream
        .withWatermark("event_timestamp", config.watermark_delay)
        .dropDuplicates(["transaction_id", "event_timestamp"])
    )

    # 4. Write stream using foreachBatch to evaluate rules and sink to Iceberg
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
