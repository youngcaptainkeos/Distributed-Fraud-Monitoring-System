# Shashank demo sheet

Your machine is `172.23.124.232`. You run the Spark master and streaming driver.
Sumukh supplies both workers on `172.23.138.248`. The next baseline is `run6`.

## Start the application

The master is already running at the handoff. Start it only if it has stopped:

```bash
cd '/media/shashank/Data1/PDocuments/Link to sem 5/BD/Project fraund detection/repo_clone/vm2'
./scripts/start_master.sh 172.23.124.232
```

Use the same directory for submission. Wait until Saatwik has created the topic:

```bash
mkdir -p ../evidence/run6
set -o pipefail
./scripts/submit_job.sh run6 2>&1 | tee ../evidence/run6/stream.log
```

The optional `run6` argument selects `transactions-run6`, checkpoint
`/checkpoints/run6`, and both Iceberg tables ending in `_run6`, overriding the
old `.env` run selection. The rest of `.env`, including addresses, Python paths
and `USE_PACKAGES=0`, stays in use. Preinstalled packages must already exist on
each Spark machine.

New runs use four shuffle partitions for the four available executor cores.
Set `SPARK_SQL_SHUFFLE_PARTITIONS` before submission only when deliberately
choosing another value for a new run. Preserve the same state partitioning
when recovering an existing run.

Tell Saatwik to send when the application reports that the streaming query has
started. Keep the terminal open. The useful success signal is a completed batch
write, followed by Sumukh seeing the actual tables. Spark starting alone is not
the end of the demo.

## Say and show

Open `http://172.23.124.232:8080` to show the master and two worker processes.
Show the streaming application and the successful batch log.

“R1 checks individual high-value payments. R2 to R4 detect patterns in two-minute
windows sliding every thirty seconds. R5 combines the rule flags into APPROVE,
REVIEW or BLOCK. Late arrivals can revise earlier decisions.”

The current fraud-rule calculation runs in Pandas on this driver. Describe the
Spark workers' actual ingestion and storage tasks without claiming the Pandas
calculation is distributed.

## If a batch fails

Keep the first exception and its cause from `stream.log`. Resolve that failure
before retrying. For recovery, Ctrl+C, then rerun the same submission command.
Keep the checkpoint and tables. Saatwik must not send the dataset again.

For a new demonstration from empty outputs, agree on a fresh run ID with both
teammates. Do not mix a fresh topic with an old checkpoint.
