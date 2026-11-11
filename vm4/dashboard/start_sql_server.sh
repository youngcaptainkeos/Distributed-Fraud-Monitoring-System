#!/usr/bin/env bash
set -euo pipefail

export SPARK_HOME="${SPARK_HOME:-$HOME/spark-3.3.4-bin-hadoop3}"
export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"

HDFS_DEFAULT_FS="${HDFS_DEFAULT_FS:-hdfs://localhost:9000}"
HDFS_WAREHOUSE="${HDFS_WAREHOUSE:-$HDFS_DEFAULT_FS/warehouse}"
SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-$(hostname -I | awk '{print $1}')}"

export SPARK_LOCAL_IP

if [[ ! -x "$SPARK_HOME/sbin/start-thriftserver.sh" ]]; then
    echo "Spark Thrift Server script not found under $SPARK_HOME" >&2
    exit 1
fi

"$SPARK_HOME/sbin/start-thriftserver.sh" \\
  --master "local[1]" \\
  --driver-memory 1g \\
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.1 \\
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \\
  --conf spark.sql.catalog.iceberg.type=hadoop \\
  --conf "spark.sql.catalog.iceberg.warehouse=$HDFS_WAREHOUSE" \\
  --conf spark.sql.session.timeZone=UTC \\
  --conf "spark.driver.host=$SPARK_LOCAL_IP" \\
  --hiveconf hive.server2.thrift.port=10000 \\
  --hiveconf hive.server2.thrift.bind.host=127.0.0.1 \\
  --hiveconf hive.server2.enable.doAs=false
