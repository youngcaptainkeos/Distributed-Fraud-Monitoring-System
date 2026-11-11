#!/usr/bin/env bash

set +e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET="${1:-baseline}"

case "$TARGET" in
    baseline|experiment_1_executor_failure|experiment_2_master_driver_failure|experiment_3_one_minute_lateness|experiment_4_beyond_watermark)
        ;;
    *)
        echo "Invalid target: $TARGET"
        exit 1
        ;;
esac

TIMESTAMP=$(date -u +"%Y%m%d_%H%M%S")
OUTPUT="$PROJECT_DIR/evidence/$TARGET/metrics/system_snapshot_${TIMESTAMP}.txt"

export SPARK_HOME="${SPARK_HOME:-$HOME/spark-3.3.4-bin-hadoop3}"

mkdir -p "$(dirname "$OUTPUT")"

{
    echo "DISTRIBUTED FRAUD MONITORING — SYSTEM SNAPSHOT"
    echo "Timestamp (UTC): $(date -u)"
    echo "Hostname: $(hostname)"
    echo

    echo "========== OPERATING SYSTEM =========="
    uname -a
    cat /etc/os-release | head -n 5
    echo

    echo "========== JAVA =========="
    java -version 2>&1
    echo

    echo "========== SPARK =========="
    "$SPARK_HOME/bin/spark-submit" --version 2>&1
    echo

    echo "========== HADOOP =========="
    hadoop version 2>&1 | head -n 5
    echo

    echo "========== JAVA PROCESSES =========="
    jps -l
    echo

    echo "========== LISTENING PORTS =========="
    ss -ltnp | grep -E ':(9000|9870|9866|9864|7077|8080|8081|10000|8501)\b'
    echo

    echo "========== HDFS STATUS =========="
    hdfs dfsadmin -report 2>&1
    echo

    echo "========== HDFS DIRECTORIES =========="
    hdfs dfs -ls / 2>&1
    echo

    echo "========== ZEROTIER STATUS =========="
    sudo zerotier-cli status 2>&1
    sudo zerotier-cli listnetworks 2>&1
    echo

    echo "========== SNAPSHOT COMPLETE =========="
} > "$OUTPUT"

echo "Snapshot saved: $OUTPUT"
echo "File size: $(du -h "$OUTPUT" | cut -f1)"
