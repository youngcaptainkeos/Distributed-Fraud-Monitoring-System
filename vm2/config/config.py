"""Configuration settings for PySpark Fraud Detection Engine on VM2.

Loads environment variables with sensible defaults for local development
and cluster execution across VM1 (Kafka), VM2 (Spark Master/Driver),
VM3 (HDFS/Iceberg), and VM4 (Worker/Dashboard).
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    # Run identifier (e.g. run1, run2)
    run_id: str = os.getenv("RUN_ID", "run1")

    # Kafka configuration (VM1)
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", f"transactions-{os.getenv('RUN_ID', 'run1')}")
    kafka_starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

    # Spark cluster configuration (VM2 Master)
    spark_master_url: str = os.getenv("SPARK_MASTER_URL", "local[*]")
    spark_app_name: str = os.getenv("SPARK_APP_NAME", f"FraudDetectionEngine-{os.getenv('RUN_ID', 'run1')}")

    # Storage & Iceberg catalog configuration (VM3 HDFS)
    hdfs_namenode: str = os.getenv("HDFS_NAMENODE", "hdfs://localhost:9000")
    iceberg_catalog_name: str = os.getenv("ICEBERG_CATALOG_NAME", "iceberg")
    iceberg_warehouse: str = os.getenv(
        "ICEBERG_WAREHOUSE",
        f"{os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')}/warehouse"
    )
    checkpoint_location: str = os.getenv(
        "CHECKPOINT_LOCATION",
        f"{os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')}/checkpoints/{os.getenv('RUN_ID', 'run1')}"
    )

    # Iceberg table targets
    decisions_table: str = os.getenv(
        "DECISIONS_TABLE",
        f"{os.getenv('ICEBERG_CATALOG_NAME', 'iceberg')}.default.transaction_decisions_{os.getenv('RUN_ID', 'run1')}"
    )
    alerts_table: str = os.getenv(
        "ALERTS_TABLE",
        f"{os.getenv('ICEBERG_CATALOG_NAME', 'iceberg')}.default.account_risk_alerts_{os.getenv('RUN_ID', 'run1')}"
    )

    # Fraud window & watermarking semantics
    watermark_delay: str = "1 minute"
    window_duration_seconds: int = 120
    window_slide_seconds: int = 30
    trigger_interval: str = os.getenv("TRIGGER_INTERVAL", "10 seconds")

    # Thresholds
    r1_amount_threshold: float = 50000.0
    r2_count_threshold: int = 5
    r3_count_threshold: int = 2
    r3_amount_threshold: float = 600000.0
    r4_count_threshold: int = 3


# Global config singleton
config = AppConfig()
