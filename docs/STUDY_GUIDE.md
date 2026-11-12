# Distributed Fraud Monitoring System: Study Guide for the Viva

A guide to the concepts behind our project, how we implemented them, what we
measured, and how to answer questions about it. It is written for all three of
us, so anyone can explain any part. Section numbers are used in the quick index
in section 15.

> Companion documents: [`../README.md`](../README.md) (setup and layout),
> [`../evidence/README.md`](../evidence/README.md) (results table),
> [`../evidence/Fraud_Monitoring_Experiment_Evidence.pdf`](../evidence/Fraud_Monitoring_Experiment_Evidence.pdf)
> (81 captioned screenshots), and [`VM2_STUDY_GUIDE.md`](VM2_STUDY_GUIDE.md)
> (line-by-line walk through the Spark side).

---

## 1. The 60-second pitch

We built a real-time fraud monitoring pipeline. A producer replays 2,000
payments into **Kafka**. **Spark Structured Streaming** reads them in
micro-batches, deduplicates them, applies five fraud rules over rolling
2-minute windows on *event time*, and writes two tables, `transaction_decisions`
and `account_risk_alerts`, into **Apache Iceberg** on **HDFS**. A **Streamlit**
dashboard reads those tables through the Spark Thrift Server so an analyst can
see decisions and investigate an alert. A separate **verifier** compares the
actual tables with the supplied ground truth.

We ran one baseline and four experiments (executor failure, master and driver
failure, late data under one minute, late data beyond the watermark). Four
runs reproduced the ground truth exactly (1,990 decisions, 129 alerts). The
fifth lost 7 late rows, which is the documented, expected behaviour of a
1-minute watermark.

**Say this in the first minute of the viva, then let them ask.**

---

## 2. The problem and the brief

- **Input:** `data/transactions.csv`, 2,000 rows, 1,990 unique `transaction_id`s
  (10 exact duplicate rows). Fields include `transaction_id`, `source_account`,
  `destination_account`, `event_timestamp`, `amount`, `currency`.
- **Output 1, `transaction_decisions`:** one row per unique transaction, with
  flags `r1..r4`, a risk score and a final decision `APPROVE`, `REVIEW` or
  `BLOCK`.
- **Output 2, `account_risk_alerts`:** one row per account, window and rule
  (R2, R3, R4), with an explanation and the supporting transaction ids.
- **Ground truth supplied:** 1,990 decisions (1,659 APPROVE, 231 REVIEW,
  100 BLOCK) and 129 alerts.
- **Required experiments:** baseline plus four fault or disorder runs, each with
  its own Kafka topic, checkpoint and tables, with screenshots and verification.

### The fraud rules (R1 to R5)

| Rule | Meaning | Condition |
|---|---|---|
| R1 | One large payment | currency INR and amount >= 50,000 (stateless) |
| R2 | Burst of activity | >= 5 payments from one source account in a 2-minute window |
| R3 | Large aggregate | >= 2 INR payments totalling > 600,000 in a 2-minute window |
| R4 | Repeated transfers to one payee | >= 3 payments from one account to the same destination in a 2-minute window |
| R5 | Composite decision | score = number of R1..R4 triggered. >= 2 BLOCK, 1 REVIEW, 0 APPROVE |

Ground-truth trigger counts: R1 194, R2 203, R3 66, R4 98 transactions flagged.
BLOCK is only a recorded classification. No payment gateway is connected.

---

## 3. Architecture

```
 VM1 (Saatwik)            VM2 (Shashank)               VM3 + VM4 (Sumukh)
┌──────────────┐      ┌──────────────────────┐     ┌──────────────────────────┐
│ producer.py  │      │ Spark master         │     │ 2 Spark workers          │
│      │       │      │ Spark driver         │◄───►│  (executors run here)    │
│      ▼       │      │  stream_job.py       │     │                          │
│ Kafka 3.9.2  │─────►│  rules.py            │────►│ HDFS (NameNode+DataNode) │
│ topic per run│ read │  writers.py          │MERGE│  /warehouse  (Iceberg)   │
│ 1 partition  │      │                      │     │  /checkpoints/runN       │
└──────────────┘      └──────────────────────┘     │ Thrift Server (SQL)      │
                                                   │ Streamlit dashboard      │
                                                   │ verifier                 │
                                                   └──────────────────────────┘
```

All three machines are joined by a ZeroTier virtual network, so they talk over
private addresses as if on one LAN.

### Why each component exists

| Component | Role | Why this and not something else |
|---|---|---|
| Kafka | Durable, replayable, ordered log of payments | Decouples producer from processor. If Spark dies, data waits in Kafka, which is what makes recovery possible. |
| Spark Structured Streaming | Continuous processing with event-time, watermark, checkpoints | Gives fault tolerance and event-time semantics without writing them ourselves. |
| HDFS | Shared, replicated-capable file system | Checkpoints and tables must outlive any single compute process. |
| Iceberg | Table format on top of files | ACID commits, `MERGE INTO`, schema, snapshots. Plain Parquet files cannot upsert safely. |
| Thrift Server + beeline | SQL endpoint for Iceberg | Lets the dashboard query tables with plain SQL. |
| Streamlit | Dashboard | Quick to build, Python-native. |

### Deployment deviation (be ready to explain it)

The brief shows four machines. We used **three**: VM3 (storage and workers) and
VM4 (dashboard) share Sumukh's machine, and both Spark workers run there.
Consequence: killing a worker demonstrates **process-level** recovery. It does
not demonstrate surviving a whole machine failing. We did not claim otherwise.

---

## 4. Concepts you must be able to explain

### 4.1 Kafka

- **Topic:** a named log. **Partition:** an ordered slice of it. We used **one
  partition per topic**, so Kafka preserves the order the producer sent.
- **Offset:** a message's position in the partition. Spark records the offsets
  it has processed, which is how it knows where to resume.
- **Key:** we set key = `transaction_id`.
- **Producer reliability:** `acks='all'`, `retries=10`,
  `max_in_flight_requests_per_connection=1`. The producer finished with
  `broker_acks == messages_sent == 2000` and `n_errors == 0` in every run.
- **KRaft:** Kafka 3.9.2 runs without ZooKeeper (metadata is in Kafka itself).
  Single broker, replication factor 1. That is a single point of failure, and we
  say so.
- **Why one topic per run:** a topic with leftover messages would contaminate the
  next experiment. `producer.py` refuses to write to a non-empty topic unless
  `--allow-nonempty` is given.

### 4.2 Event time vs processing time

- **Event time:** the `event_timestamp` inside the payment. The rules use only
  this.
- **Processing time:** when Spark happens to see the row.
- **Arrival time:** when the producer sent it. Delays here create out-of-order
  data, and that is what experiments 3 and 4 inject.

### 4.3 Micro-batches and the trigger

Structured Streaming runs a series of small batch jobs. We set
`trigger(processingTime="10 seconds")`, so a batch is attempted every 10 s.
Each micro-batch has a number (`batch_id`). In our runs the dataset flowed
through in roughly 6 to 25 non-empty batches, depending on the producer speed.

### 4.4 Watermark

A watermark says how late data may be. `withWatermark("event_timestamp",
"1 minute")` means: Spark tracks the maximum event time seen, subtracts one
minute, and treats anything older than that as too late. The watermark moves
forward at the end of each batch and applies to the next one.

- Late by **less than 1 minute**: accepted and processed (experiment 3).
- Late by **more than 1 minute**: may be dropped (experiment 4).

### 4.5 Deduplication

Duplicates are removed in three places, deliberately:
1. `dropDuplicates(["transaction_id", "event_timestamp"])` in the stream
   (state is cleaned up using the watermark).
2. `drop_duplicates(subset=["transaction_id"])` when combining stored and new
   rows in the batch.
3. `MERGE INTO ... ON transaction_id` (and `alert_id`), the primary-key upsert.

### 4.6 `foreachBatch`

`foreachBatch(process_micro_batch)` hands each micro-batch to our own Python
function as a normal DataFrame. We use it because the rules need **all history
plus the new rows**, not just the new rows. See section 6 for what this costs.

### 4.7 Checkpoints and exactly-once

The checkpoint on HDFS (`/checkpoints/runN`) stores, per batch, the Kafka offsets
it planned (`offsets/N`) and a marker that the batch finished (`commits/N`).

- **After a crash,** Spark restarts from the checkpoint. If batch N has an offsets
  file but no commit, Spark **runs batch N again**.
- Re-running a batch must not create duplicates. Our write is
  `MERGE INTO ... WHEN MATCHED UPDATE ... WHEN NOT MATCHED INSERT` keyed by
  `transaction_id` and `alert_id`, so a replay overwrites the same rows with the
  same values. Replay-safe writes plus offset checkpointing give **effectively
  exactly-once output**.
- Precisely: Spark gives *at-least-once* execution of the batch function, and our
  *idempotent sink* turns that into exactly-once results.

### 4.8 Spark standalone roles

| Role | What it is | Where |
|---|---|---|
| Master | Cluster manager. Tracks workers and applications | VM2 |
| Worker | A daemon that offers CPU and memory | Sumukh's machine (2 of them) |
| Driver | Our `stream_job.py` process. Plans batches, runs the Python rule engine | VM2 |
| Executor | JVM process on a worker that runs tasks | 2 executors, 2 cores and 512 MB each |

**What each failure kills:**
- Executor dies: the driver notices, asks the master for a new one, reschedules the
  tasks. Stream keeps running. **No human action.**
- Master and driver die: the application is gone. Workers survive. A human restarts
  the master and **resubmits the same job with the same run id** so it picks up the
  same checkpoint.

### 4.9 HDFS vs Iceberg (a common confusion)

- **HDFS** stores files and directories (`/warehouse/...`, `/checkpoints/...`).
- **Iceberg** is a table format that organises Parquet data files and metadata
  (snapshots, manifests) *inside* HDFS. It adds atomic commits and `MERGE INTO`.
- We use the Iceberg **Hadoop catalog**, which keeps table metadata directly in the
  warehouse directory, so no Hive metastore is needed.
- `transaction_decisions` is partitioned by `days(event_timestamp)`;
  `account_risk_alerts` by `rule_id`.

---

## 5. Walk through the code, file by file

| File | What it does |
|---|---|
| `vm1/producer.py` | Reads the CSV, sends one JSON message per row, never sorts the file. Modes `baseline`, `late_lt60`, `late_gt60`. Writes `*_arrival.csv` and `*_summary.json`. |
| `vm1/verify_topic.py` | Reads the topic back and checks content (and order with `--expect-order`). |
| `vm2/config/config.py` | All settings come from environment variables (run id, topic, tables, checkpoint, watermark 1 minute, trigger 10 s). |
| `vm2/scripts/submit_job.sh` | `spark-submit` with Kafka and Iceberg packages. Optional `./submit_job.sh runN` argument derives the topic, checkpoint and table names from the run id. |
| `vm2/app/schema.py` | Explicit schemas. Kafka JSON fields are read as strings and cast afterwards, so quoted numbers do not break parsing. |
| `vm2/app/stream_job.py` | The streaming query (read Kafka, parse, watermark, dedup, `foreachBatch`). |
| `vm2/app/rules.py` | Pure-Python rule engine `evaluate_batch(df)` that returns decisions and alerts. Easy to unit test. |
| `vm2/app/writers.py` | Creates the Iceberg tables and does the `MERGE INTO` writes. |
| `vm3/verifier/run_verification.py` | Compares actual CSV exports with the ground truth. |
| `vm4/dashboard/app.py`, `iceberg_source.py` | Streamlit UI. DEMO mode reads ground-truth CSVs, LIVE mode queries Iceberg through beeline. |

### 5.1 What happens in one micro-batch

1. Spark pulls the new Kafka rows (after watermark and dedup) into the driver as
   `new_pdf`.
2. The driver reads **all previously stored decisions** from Iceberg
   (`spark.table(decisions_table)`) as `stored`.
3. It concatenates stored and new, drops duplicate ids.
4. `evaluate_batch(all_pdf)` recomputes every window and every decision.
5. The results go back to Spark with strict schemas and are `MERGE`d into both
   tables.
6. Spark commits the batch to the checkpoint.

**Why recompute everything?** A new row can change the decisions of *older* rows.
For example, the 5th payment in a window triggers R2 for the previous four. A
batch-local calculation would miss that. Recomputing from history is the simplest
way to get that right.

**The cost:** work per batch grows with total history. Our batches took **20 to 48 s
against a 10 s trigger**. Spark logged warnings that it was falling behind. It
still produced correct output because Kafka buffers the backlog.

### 5.2 Window mechanics (worked example)

A window is 120 s long and slides every 30 s, so each payment falls into **4
overlapping windows**. Windows are half-open: `[start, end)`.

For a payment at 14:05:10:

| Window start | Window end |
|---|---|
| 14:05:00 | 14:07:00 |
| 14:04:30 | 14:06:30 |
| 14:04:00 | 14:06:00 |
| 14:03:30 | 14:05:30 |

The code expands every payment into those four rows, groups by
`(source_account, window_start, window_end)`, and tests R2 (count >= 5) and R3
(INR count >= 2 and sum > 600,000). R4 groups additionally by destination.
Real alert from our baseline: `R2-5137069718-20261027140430`, window
14:04:30 to 14:06:30.

- **Alert id** = rule + account + compact window start, so the same alert always has
  the same id. This is what lets `MERGE` treat a replay as the same row.
- **R4** may find several destinations in one window. We keep the one with the
  highest count so there is exactly one R4 alert per account and window.
- A transaction is linked to every alert of its account whose window contains its
  timestamp (`related_alert_ids`).

### 5.3 The decision

```
score = R1 + R2 + R3 + R4           (each 0 or 1)
score >= 2  -> BLOCK      score == 1 -> REVIEW      score == 0 -> APPROVE
```

---

## 6. Honest design trade-offs (and why they are fine here)

These are the questions an examiner is most likely to press. Know the answer and
the next step.

| Observation | Why it is that way | What we would do at scale |
|---|---|---|
| Whole history is read into the driver's Pandas each batch | The rules need cross-batch window state and revisions of earlier decisions. Simple and verifiably correct for 2,000 rows | Use Spark window aggregations or `applyInPandasWithState` / `flatMapGroupsWithState` with the watermark, so only open windows are kept |
| Batches take longer than the 10 s trigger | Cost per batch grows with history (see 5.1) | Same as above. Raise the trigger interval as a stopgap |
| Late data beyond 1 minute is lost | That is what a watermark does: it trades completeness for bounded state | Side-output late events to a "late" topic or table, or use a longer watermark |
| Master and driver failure is not automatic | Standalone Spark has no supervisor for a client-mode driver | Standalone HA with ZooKeeper, `--deploy-mode cluster --supervise`, or YARN/Kubernetes |
| Kafka and HDFS are single node | Course environment with three machines | Replication factor 3, multiple brokers, HDFS replication and HA NameNode |
| Two workers share one machine | VM3 and VM4 were merged | Put workers on separate hosts |

---

## 7. The experiments

Every run used its own topic `transactions-runN`, checkpoint
`/checkpoints/runN` and tables `..._runN`, so nothing was reused across
experiments. Input was always the same CSV.

| Experiment | Run | What we did | Result |
|---|---|---|---|
| Baseline | run1 | Clean, in-order stream | **PASS**: 1,990 decisions, 129 alerts, 0 mismatches |
| Exp 1: executor failure | run2 | `kill -9` one executor at 15:14:07 UTC | **PASS** |
| Exp 2: master and driver failure | run3 | `stop_master.sh` at 15:43:40 UTC; restart master; resubmit with the same run id | **PASS** |
| Exp 3: late < 1 minute | run4 | 7 rows delayed 30 to 48 s in event time | **PASS** |
| Exp 4: late > 1 minute | run5 | 7 rows delayed 93 to 129 s | **FAIL as expected**: 1,983 decisions, 7 missing |

### 7.1 Baseline (run1)

Spark started before the producer. The producer took 104.1 s. Decisions grew
156, 490, 850, 1,364, 1,989, 1,990 over batches 1 to 6 and alerts grew
0, 0, 39, 83, 129, 129. The final batch finished 77 s after the producer. The
verifier passed every stage.

**Run 6 was the first attempt and failed the alert comparison.** All 129 alerts had
windows exactly **5 h 30 min early**. Cause: pandas timestamps were converted using
the driver machine's local timezone (IST, UTC+05:30) when building the Spark
DataFrame. Fix in `stream_job.py`:
`v.to_pydatetime().replace(tzinfo=timezone.utc)`. This is evidence that the
verifier is effective, because it caught a real defect before the experiments.

### 7.2 Executor failure (run2)

- Before: 2 executors active, 265 tasks completed, 0 failed.
- 20:44:07 IST: executor 0 killed. Driver logged `Lost executor 0`. A new executor
  process was visible two seconds later (executor 2 in the UI).
- Batch 11 completed at 20:44:17 and the stream carried on. Final batch 25 wrote
  1,990 decisions and 129 alerts.
- Live executors completed 1,329 tasks, the dead one 222. **No task failed.** No
  manual step.

### 7.3 Master and driver failure (run3)

- Batches 9 and 10 completed. Batch 11 (66 records) was being written when the
  driver log stops (21:13:16 IST). The master was stopped at 21:13:40 and its UI
  refused connections.
- The producer was unaffected and finished at 21:20:12. Kafka held everything.
- A new driver started at 21:16:27 (about 2 min 48 s after the stop). Workers had
  re-registered by themselves.
- **Batch 11 was run again from the checkpointed offsets** (66 records, 592
  decisions) and committed at 21:17:11. Commits 0 to 18 are continuous across the
  restart. Final: 1,990 and 129. **Manual** restart and resubmit.
- The new application id (`app-...-0000`) restarted from zero because the master
  restarted, which is normal.
- How exactly the driver process ended before the master stop was not recorded;
  the report says so.

### 7.4 Late data under one minute (run4)

Producer mode `late_lt60`, `--fraction 0.005 --min-wall-hold 63`. Seven rows were
held back for 63 s of wall time, producing event-time lags of 30, 33, 33, 46, 36, 45
and 48 s, all below the 60 s watermark. All 7 arrived and every decision and alert
matched. One incident: `config/.env` got mangled by a `sed` edit (`RUN_ID=run4un3`),
a wrongly named application started and was stopped before it processed data; its
stray tables and checkpoint were deleted after listing them. Run 4 was then started
correctly.

### 7.5 Late data beyond one minute (run5)

Producer mode `late_gt60`, same parameters. Seven rows with lags of 105, 129, 118,
120, 93, 94 and 94 s. Result: 1,983 decisions (7 missing), 129 alerts (unchanged).

- Missing ids: TXN0000125, 291, 369, 395, 684, 1023, 1953.
- They are **identical** to the list of rows the producer injected as late
  (`diff` prints IDENTICAL).
- The dashboard returns "not found" for TXN0000125 and TXN0001953 and finds the
  control row TXN0000133.
- No alert changed in this dataset, because none of those rows was needed to
  trigger a window.

**How we explain it:** a watermark of 1 minute plus `dropDuplicates` state on
`(transaction_id, event_timestamp)`. Rows older than the watermark are discarded.
We inferred this mechanism from the configuration and the exact match of ids. The
mechanism was measured in one run. Run 4's rows (lag under 60 s) were all kept
and run 5's (lag over 60 s) were all lost, which is the boundary the watermark
predicts.

**Verifier note:** the verifier is *correct to fail* here. The ground truth describes
in-order, on-time delivery, so run5 is judged against a different truth. The report
labels it "FAIL (expected): documented limitation", not a pass.

### 7.6 Results table (for slides)

| | run1 | run2 | run3 | run4 | run5 |
|---|---|---|---|---|---|
| Decisions | 1,990 | 1,990 | 1,990 | 1,990 | 1,983 |
| Alerts | 129 | 129 | 129 | 129 | 129 |
| Missing | 0 | 0 | 0 | 0 | 7 |
| Duplicates | 0 | 0 | 0 | 0 | 0 |
| Longest batch | 40.2 s | 47.7 s | 20.2 s / 44.0 s after resume | 46.5 s | 35.6 s |
| Manual action | none | none | restart and resubmit | none | none |

---

## 8. Verification: how we know the output is right

`vm3/verifier/run_verification.py` runs in stages:

1. Validate the input CSV (2,000 rows, 1,990 unique, 10 duplicates).
2. Validate the ground truth (1,990 decisions, 129 alerts).
3. Compare actual decisions with ground truth.
4. Compare actual alerts with ground truth.

It exits with code 0 on success and 1 on any failure, and saves a JSON report.
`~/export_verify3.sh runN` exports both Iceberg tables to CSV through the Thrift
Server and then runs the verifier, so the check is of the **stored tables**, not of
log output.

Before the distributed runs, independent verifiers `verify_r1.py` to
`verify_r5.py` recomputed each rule separately from the input and confirmed they
agree with the ground truth. Think of it as two independent implementations that
agree.

---

## 9. The dashboard

Two modes in the sidebar:
- **DEMO:** reads the ground-truth CSVs (works with no cluster).
- **LIVE:** pick a run; queries Iceberg through the Thrift Server using `beeline`
  with `--outputformat=tsv2`.

Sections: KPI cards (total, approved, under review, blocked), rule activity R1 to
R4 and R5 blocks, charts, **transaction volume per minute** by decision, a
transactions table, alert summary and distribution, and an **Investigation** panel.

Investigation, by alert or by transaction id, shows: why it was flagged (the stored
explanation), the trigger transaction, supporting transactions, and the **account's
last 20 transactions**.

**Bug fixed:** values containing commas broke the first parser (`csv2` output).
Switching beeline to `tsv2` fixed it.

**Reading "not found":** for a dropped row and for a row that never arrived the
dashboard says the same thing, so the producer log and the verifier report are the
ground truth about *why*.

---

## 10. Failure scenarios (a quick reference)

| What dies | What survives | Who recovers | Why no data is lost |
|---|---|---|---|
| One executor | Everything else | Spark, automatically | Failed tasks are rescheduled and sinks are idempotent |
| Master + driver | Workers, Kafka, HDFS | A human | Offsets and progress are in the HDFS checkpoint; Kafka retains the data |
| Kafka broker | HDFS, Spark | Not covered (single broker) | n/a |
| HDFS | Kafka | Not covered | n/a |

---

## 11. Bugs and incidents we hit (good stories for a viva)

1. **Timezone shift (run 6).** Alerts 5 h 30 min early. Cause: driver-local timezone
   used for naive timestamps. Fix: attach UTC. Lesson: verify against ground truth,
   not just row counts.
2. **Dashboard comma parsing.** beeline `csv2` output breaks on commas. Fix: `tsv2`.
3. **Mangled `.env` (run 4).** A `sed` edit produced `RUN_ID=run4un3`. Caught
   before data was processed; cleaned up after listing. Lesson: check config with
   `grep` before submitting. Later we also added a run-id argument to
   `submit_job.sh` so names are derived rather than hand-edited.
4. **Network loss during an early `test` run.** Packet loss and a switch to a hotspot
   interrupted a run; we abandoned it and started a fresh run id rather than
   mixing state.

---

## 12. Viva questions and answers

**Why Kafka and not read the file directly?** A log decouples producer and consumer,
lets Spark replay after a crash, and models real payment streams.

**Why one partition?** It preserves order, which the baseline check relies on, and
the dataset is tiny. More partitions would parallelise reads but order is then only
per partition.

**What is event time and why not use arrival time?** Fraud windows are defined by
when the payment happened. Arrival time depends on network delay and would put the
same payments in different windows on different runs.

**What does the watermark do exactly?** It sets the cutoff for lateness: max event
time seen minus 1 minute. Older rows can be dropped and old state cleaned.

**Why did run5 lose exactly 7 rows?** The producer delivered 7 rows more than 60 s
behind the newest event time, i.e. behind the watermark, and the 7 missing ids are
identical to that list.

**Does a delay over 60 s always mean the row is dropped?** No. The watermark moves
per batch and depends on what else arrived. We verified the outcome by comparing the
actual ids, not by assuming it.

**How is the output exactly-once?** Offsets in the checkpoint plus replay-safe
`MERGE INTO` with deterministic keys. A replayed batch rewrites the same rows.

**What happens if the driver dies between the write and the checkpoint commit?**
The batch is replayed after restart. `MERGE` makes the second write a no-op in
effect. This is what run 3 showed with batch 11.

**What is the difference between a checkpoint and the Iceberg table?** The
checkpoint is Spark's progress record (offsets, commits). The table is the output
data. We keep both on HDFS, and a restart needs both intact.

**Why `MERGE INTO` instead of append?** Append would duplicate rows on replay and
could not update earlier decisions when a new payment completes a window.

**Why does each batch re-read all decisions?** New rows can change earlier rows'
decisions; the rules need full history. It is the simplest correct approach for this
size and its cost is why batches exceed the trigger.

**How would you scale it?** Move window state into Spark (stateful aggregation with
watermark), partition Kafka by account, add workers on separate machines, replicate
Kafka and HDFS.

**What happens if a worker machine fails?** In our deployment both workers share one
machine, so we only demonstrated executor and process recovery. With workers on
separate hosts Spark would reschedule on the others, but we did not test that.

**What is the Iceberg catalog and why Hadoop type?** The catalog maps table names to
metadata. The Hadoop catalog keeps metadata in the warehouse directory, so no extra
service is needed.

**Why is the verifier credible?** It compares exported stored tables with a supplied
ground truth, it detected a real timezone defect, and it correctly fails run 5.

**Is run 5 a failure of the system?** It is an expected result of the design choice
(1-minute watermark). The report states that and says whether it is acceptable is a
design decision.

**What does BLOCK do?** It only records a classification. No payment is actually
stopped.

**How do the dashboard and the pipeline stay consistent?** The dashboard reads the
same Iceberg tables through the Thrift Server. It does not recompute anything.

**What did each of you do?** Saatwik: Kafka, producer modes, arrival logs. Shashank:
Spark master and driver, streaming job, failure injection. Sumukh: HDFS and Iceberg,
workers, verifier, dashboard.

---

## 13. Numbers to memorise

| Fact | Value |
|---|---|
| Input rows / unique / duplicates | 2,000 / 1,990 / 10 |
| Decisions / alerts expected | 1,990 / 129 |
| APPROVE / REVIEW / BLOCK | 1,659 / 231 / 100 |
| R1 / R2 / R3 / R4 triggers | 194 / 203 / 66 / 98 |
| Window / slide / watermark / trigger | 120 s / 30 s / 1 minute / 10 s |
| Versions | Kafka 3.9.2 (KRaft), Spark 3.3.4, Iceberg runtime 1.4.1 |
| Cluster | 1 master, 2 workers, 2 executors of 2 cores and 512 MB |
| Run 5 | 7 late rows, 1,983 decisions, 129 alerts |
| Run 6 | alerts 5 h 30 min early (timezone) |
| Run 3 | restart gap 2 min 48 s, first commit after 3 min 31 s |

---

## 14. Demo plan (5 minutes)

1. Show `evidence/README.md` (results table).
2. Open the PDF cover and results page.
3. Dashboard in LIVE mode on run1: KPI cards, then Investigation of one alert.
4. Dashboard on run5: look up TXN0000125 (not found) and TXN0000133 (found).
5. Show `evidence/experiment_4_beyond_watermark/sumukh/verification_run5.txt`
   (decisions FAILED, alerts PASSED) next to `late_ids_run5.txt`.

Do not start a run live unless asked. Each full run takes about 10 to 20 minutes.

---

## 15. Quick index

| Topic | Section |
|---|---|
| Pitch | 1 |
| Rules | 2 |
| Architecture | 3 |
| Kafka, watermark, checkpoint | 4 |
| Code map | 5 |
| Trade-offs | 6 |
| Experiments | 7 |
| Verifier | 8 |
| Dashboard | 9 |
| Failures | 10 |
| Bugs | 11 |
| Q&A | 12 |
