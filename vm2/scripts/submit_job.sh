#!/usr/bin/env bash
# Submit the PySpark Structured Streaming fraud monitoring job from VM2
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STREAMING_DIR="$(dirname "${SCRIPT_DIR}")"

# Load environment configuration if available
if [ -f "${STREAMING_DIR}/config/.env" ]; then
    echo "Loading environment variables from config/.env"
    set -a
    source "${STREAMING_DIR}/config/.env"
    set +a
fi

SPARK_HOME=${SPARK_HOME:-/home/shashank/opt/spark}
ZT_IP=$(ip -4 addr show ztr2qxeeov 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' || true)
DRIVER_HOST=${SPARK_DRIVER_HOST:-${ZT_IP:-172.23.124.232}}
SPARK_MASTER=${SPARK_MASTER_URL:-spark://${DRIVER_HOST}:7077}
RUN_ID=${RUN_ID:-run1}

# Package coordinates for Spark 3.3.4 (Scala 2.12)
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4"
ICEBERG_PKG="org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.1"
PACKAGES="${KAFKA_PKG},${ICEBERG_PKG}"

echo "=========================================================="
echo " Submitting Fraud Detection Engine (VM2 Driver)"
echo " Run ID: ${RUN_ID}"
echo " Spark Master: ${SPARK_MASTER}"
echo " Driver Host (ZeroTier): ${DRIVER_HOST}"
echo " Packages: ${PACKAGES}"
echo "=========================================================="

cd "${STREAMING_DIR}"

export PYTHONPATH="${STREAMING_DIR}:${PYTHONPATH}"

"${SPARK_HOME}/bin/spark-submit" \
    --master "${SPARK_MASTER}" \
    --name "FraudDetectionEngine-${RUN_ID}" \
    --packages "${PACKAGES}" \
    --conf "spark.driver.host=${DRIVER_HOST}" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.iceberg.type=hadoop" \
    --conf "spark.sql.catalog.iceberg.warehouse=${ICEBERG_WAREHOUSE:-hdfs://172.23.222.173:9000/warehouse}" \
    --conf "spark.sql.session.timeZone=UTC" \
    app/stream_job.py
