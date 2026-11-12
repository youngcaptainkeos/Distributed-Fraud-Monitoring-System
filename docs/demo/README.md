# Demo team running order

Working draft for the merged three-machine deployment. Final rehearsal and
live baseline output comparison are still pending. These sheets describe the
next run; they do not certify that it has succeeded.

The [evaluation requirements and remaining work](EVALUATOR_REQUIREMENTS.md)
take precedence over the original baseline-only rehearsal sequence below.
Retain proof of a baseline plus all four experiments before evaluation.

| Person | ZeroTier IP | Work |
|---|---|---|
| Saatwik | 172.23.211.163 | Kafka and producer |
| Shashank | 172.23.124.232 | Spark master and streaming driver |
| Sumukh | 172.23.138.248 | HDFS, Iceberg, verification, two workers, Thrift and dashboard |

## Resume point

On 12 November, the shared handoff ended before a planned hotspot switch.
The local master was subsequently observed alive, with two registered workers
on Sumukh's IP and no active streaming application. The interrupted run was
`test`. Substantial packet loss and relayed peer connections were recorded
before the switch. The network cause remains a working diagnosis.

A subsequent recovery attempt reached both executors, but Sumukh's executor
timed out fetching Kafka offset 63 after 120 seconds. After the team confirmed
the hotspot switch, another attempt found missing checkpoint state files.
Kafka also reported the `test` topic empty. The old run was preserved and
abandoned; the current fresh baseline is `run6`. Its topic has already been
created and Shashank's application started. Its inspected log later ended at
13:57:40 IST with No Route to Host to Sumukh's HDFS endpoint. Completion is
unproven; establish the current source/checkpoint state before deciding whether
to resume. Do not resend input into a nonempty topic. See
`evidence/test/STATUS.md` for the old run's details.

## One baseline run

1. All three use the agreed network. Confirm ZeroTier reconnects once.
2. Sumukh leaves existing HDFS and workers running. Start only missing services.
3. Saatwik creates a fresh, one-partition topic for the chosen run.
4. Shashank starts the application with the same run ID.
5. Saatwik sends the CSV once. Shashank waits for a successful batch write.
6. Sumukh opens LIVE mode for that run and shows the decisions and alerts.
7. Compare the two actual output exports with ground truth once. Investigate
   a failure directly; do not repeatedly run the offline verifier.

Use `run6` for the next fresh baseline across all three sheets. If it already
exists, agree on another unused numeric run such as `run7` and replace every
matching topic, table, checkpoint and dashboard run selection. Do not delete
existing runs to make a demonstration look clean.

Expected complete in-order baseline: 1,990 unique decisions, 129 alerts,
1,659 APPROVE, 231 REVIEW and 100 BLOCK. Counts alone are not a row-level comparison.

## Explain the design accurately

The producer replays a fixed payment dataset. Spark handles streaming ingestion,
deduplication and Iceberg writes. The current rule implementation collects
cumulative transaction history into Pandas on the driver for each batch.
The system can revise earlier decisions when new transactions trigger a window.
BLOCK is a recorded classification; no real payment gateway is connected.

Two worker processes share Sumukh's machine. Killing one demonstrates worker
process recovery while the other remains available; it does not demonstrate
survival of Sumukh's entire machine failing. Kafka and HDFS are single-node
services. Present failure/lateness results only after executing those runs.

## Stop and recovery

Stop Shashank's foreground application with Ctrl+C. Preserve the run's Kafka
topic, HDFS checkpoint and Iceberg tables. Restart the same run ID to resume;
do not resend the producer. Start a new run ID for a new baseline.

See [Saatwik](SAATWIK.md), [Shashank](SHASHANK.md) and [Sumukh](SUMUKH.md).
