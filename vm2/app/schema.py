"""Schema definitions for PySpark Structured Streaming fraud detection pipeline.

Contains explicit StructType schemas for:
1. Inbound Kafka JSON transaction events (all StringType to handle quoted/unquoted JSON)
2. Apache Iceberg target table: transaction_decisions
3. Apache Iceberg target table: account_risk_alerts
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Inbound Kafka transaction payload schema
# Every field is declared as StringType because JSON serializers may quote numeric values.
# Type casting is performed explicitly in the streaming pipeline.
KAFKA_TRANSACTION_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("source_account", StringType(), True),
    StructField("destination_account", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("event_timestamp", StringType(), False),
    StructField("amount", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("account_age_days", StringType(), True),
])

# Iceberg Table 1: transaction_decisions (one row per transaction)
TRANSACTION_DECISIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("source_account", LongType(), True),
    StructField("destination_account", LongType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("evaluated_at", TimestampType(), True),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("r1_triggered", BooleanType(), False),
    StructField("r2_triggered", BooleanType(), False),
    StructField("r3_triggered", BooleanType(), False),
    StructField("r4_triggered", BooleanType(), False),
    StructField("r5_risk_score", IntegerType(), False),
    StructField("r5_block_triggered", BooleanType(), False),
    StructField("decision", StringType(), False),
    StructField("related_alert_ids", StringType(), False),
])

# Iceberg Table 2: account_risk_alerts (one row per account, window and rule: R2, R3, R4)
ACCOUNT_RISK_ALERTS_SCHEMA = StructType([
    StructField("alert_id", StringType(), False),
    StructField("rule_id", StringType(), False),
    StructField("source_account", LongType(), True),
    StructField("destination_account", LongType(), True),
    StructField("trigger_transaction_id", StringType(), False),
    StructField("finding_scope", StringType(), False),
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("observed_count", LongType(), False),
    StructField("observed_amount_inr", DoubleType(), True),
    StructField("count_threshold", LongType(), False),
    StructField("amount_threshold_inr", DoubleType(), True),
    StructField("supporting_transaction_ids", StringType(), False),
    StructField("explanation", StringType(), False),
    StructField("triggered_at", TimestampType(), True),
])
