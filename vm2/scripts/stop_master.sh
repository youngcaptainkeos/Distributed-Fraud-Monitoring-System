#!/usr/bin/env bash
# Stop Spark Master on VM2
set -e

SPARK_HOME=${SPARK_HOME:-/home/shashank/opt/spark}

echo "=== Stopping Spark Master on VM2 ==="
"${SPARK_HOME}/sbin/stop-master.sh"
echo "Spark Master stopped."
