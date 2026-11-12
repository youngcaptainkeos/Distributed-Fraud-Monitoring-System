# Sumukh demo sheet

Your VM is `172.23.138.248`. You handle both former VM3 and VM4 roles:
HDFS, Iceberg, independent verification, two Spark workers, Thrift and Streamlit.
Use your existing repository checkout and Python environment.

## Keep storage and workers ready

HDFS and two worker processes were already running at the handoff. Keep them
running after the network switch. If HDFS has stopped, start it once:

```bash
"$HADOOP_HOME/sbin/start-dfs.sh"
```

The master is `spark://172.23.124.232:7077`. If both workers are stopped, this
starts two worker instances, each with two cores and 2 GB for executors:

```bash
SPARK_WORKER_INSTANCES=2 SPARK_WORKER_CORES=2 SPARK_WORKER_MEMORY=2g \
  "$SPARK_HOME/sbin/start-worker.sh" spark://172.23.124.232:7077
```

Use the existing worker configuration if your established startup differs.
Do not start duplicate workers on top of running workers. The VM must also
have memory for HDFS, Thrift, the OS and the dashboard.

## Show the live dashboard

If Thrift is stopped, from your repository root:

```bash
HDFS_DEFAULT_FS=hdfs://172.23.138.248:9000 \
  bash vm4/dashboard/start_sql_server.sh
```

That script resolves its Iceberg package online. If this was already installed
and the working server is running, leave it running.

```bash
streamlit run vm4/dashboard/app.py --server.port 8501
```

Open `http://localhost:8501` in the VM browser. Select **LIVE**, experiment run ID
**run6** (or **test** for the recovered run), and refresh datasets after
Shashank's batch succeeds. Show decision
counts, rule filters and a flagged transaction with its related alert and
supporting transactions. DEMO mode reads the supplied ground-truth CSVs.

Expected full baseline: 1,990 decisions, 129 alerts; 1,659 APPROVE, 231 REVIEW,
100 BLOCK. Wait for processing to finish before comparing final totals.

## Compare actual outputs once

From the repository root, after processing completes:

```bash
mkdir -p evidence/run6
PYTHONPATH=vm4/dashboard python3 - <<'PY'
from iceberg_source import load_iceberg_run
decisions, alerts = load_iceberg_run(6)
decisions.to_csv('evidence/run6/actual_decisions.csv', index=False)
alerts.to_csv('evidence/run6/actual_alerts.csv', index=False)
print('Exported', len(decisions), 'decisions and', len(alerts), 'alerts')
PY
python3 vm3/verifier/run_verification.py \
  --actual-decisions evidence/run6/actual_decisions.csv \
  --actual-alerts evidence/run6/actual_alerts.csv
cp vm3/verifier/reports/verification_report.json evidence/run6/
```

The comparator accepts equivalent boolean capitalization, numeric formatting,
UTC timestamp representations and JSON-array spacing. It checks duplicate
decision IDs and related alert references as well as decisions and alert
signatures. Retain both exports and the report. Do not rerun the same comparison
without a relevant change.

## Say and show

“I host the durable transaction decisions, alerts and streaming checkpoints,
and query the Iceberg tables for investigation. The verifier compares actual
streaming outputs with the supplied ground truth.” Show LIVE mode and the
actual comparison result. Only claim a pass if that comparison passed.

Two workers share this VM. The required first experiment kills and restarts an
application executor, not just the worker daemon. Identify the current app's
CoarseGrainedExecutorBackend process using the Spark UI and process list, then
terminate only that executor at the agreed time. Keep workers, NameNode and
DataNode running. Preserve the failure/replacement evidence and verify the
completed output. Do not power off the whole VM.
