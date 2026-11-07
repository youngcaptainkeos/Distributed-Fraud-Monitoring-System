#!/usr/bin/env bash
# Start Spark Master on VM2
set -e

SPARK_HOME=${SPARK_HOME:-/home/shashank/opt/spark}
MASTER_HOST=${1:-$(hostname -I | awk '{print $1}')}
MASTER_PORT=7077
WEBUI_PORT=8080

echo "=== Starting Spark Master on VM2 ==="
echo "Master Host: ${MASTER_HOST}"
echo "Master Port: ${MASTER_PORT}"
echo "Web UI Port: ${WEBUI_PORT}"

# Export to spark-env if not set
export SPARK_MASTER_HOST="${MASTER_HOST}"
export SPARK_MASTER_PORT="${MASTER_PORT}"
export SPARK_MASTER_WEBUI_PORT="${WEBUI_PORT}"

"${SPARK_HOME}/sbin/start-master.sh"

echo ""
echo "Spark Master started successfully!"
echo "Master URL: spark://${MASTER_HOST}:${MASTER_PORT}"
echo "Spark Web UI: http://${MASTER_HOST}:${WEBUI_PORT}"
echo ""
echo "Run this command on VM3 and VM4 to join the cluster as workers:"
echo "  \${SPARK_HOME}/sbin/start-worker.sh spark://${MASTER_HOST}:${MASTER_PORT}"
