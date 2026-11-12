# Distributed Fraud Monitoring — Experiment Report

## 1. Experiment Details
- Experiment ID: Baseline (Run 1 official; Run 6 first attempt)
- Experiment name: Baseline
- Date and time: 12 Nov 2026. Run 6: Spark 19:14 IST. Run 1: Spark 20:20 IST, producer 20:26:41 IST.
- Person conducting experiment: Saatwik (VM1, Kafka and producer), Shashank (VM2, Spark), Sumukh (VM3/VM4, HDFS, workers, dashboard, verifier)
- Spark version: 3.3.4 (standalone, 1 master, 2 workers, 2 executors of 2 cores / 512 MB)
- Hadoop version: Spark build `spark-3.3.4-bin-hadoop3`, HDFS NameNode on VM3 (exact HDFS version not captured in the evidence)
- Kafka run/topic: transactions-run1 (and transactions-run6) (Kafka 3.9.2, KRaft single node, 1 partition, replication factor 1)

## 2. Objective
Show that Kafka → Spark Structured Streaming → Iceberg on HDFS reproduces the supplied ground truth exactly (1,990 decisions, 129 alerts) when nothing fails, and that the verifier detects defects.

## 3. Initial System State
- Kafka: broker up, topic created empty (end offset 0).
- Spark master: up, 2 workers alive, 4 cores.
- Spark workers: 2 (one executor each, 2 cores, 512 MB).
- Streaming application: FraudDetectionEngine-run1 submitted before the producer started.
- HDFS: NameNode and DataNode up on VM3.
- Iceberg tables: `iceberg.default.transaction_decisions_run1`, `iceberg.default.account_risk_alerts_run1`.
- Checkpoint: `hdfs://<VM3>:9000/checkpoints/run1`.

## 4. Procedure
1. `kafka-topics.sh --create --topic transactions-run1 --partitions 1 --replication-factor 1`
2. Shashank: set `RUN_ID=run1`, `KAFKA_TOPIC=transactions-run1` in `config/.env`, submit the job.
3. Saatwik: `python3 vm1/producer.py --mode baseline --topic transactions-run1 --run-id run1 --input data/transactions.csv --bootstrap $KAFKA_BOOTSTRAP --log-dir ~/fraud/logs/run1`
4. Saatwik: `python3 vm1/verify_topic.py --topic transactions-run1 --input data/transactions.csv --expect-order`
5. Sumukh: `bash ~/export_verify3.sh run1` (exports both tables, runs `vm3/verifier/run_verification.py`).
6. Run 6 (first attempt) followed the same steps on topic transactions-run6.

## 5. Observations
- Run 6: the producer finished before Spark started, so Batch 0 processed all 1,990 unique records at once (19:14:53 to 19:15:25 IST). Decisions passed, alerts FAILED: every alert window was exactly 5 h 30 min earlier than expected (R2-5137069718-20261027140430 expected 14:04:30–14:06:30, got 08:34:30–08:36:30). Cause: pandas timestamps converted with the driver local timezone (IST). Fix: attach `tzinfo=UTC` in `vm2/app/stream_job.py`.
- Run 1: Batches 1 to 6 arrived as the producer sent (104.1 s). Decisions grew 156, 490, 850, 1,364, 1,989, 1,990; alerts 0, 0, 39, 83, 129, 129. The last batch finished 20:29:42 IST, 77 s after the producer stopped. Longest batch 40.2 s against a 10 s trigger.

## 6. Recovery
- Not applicable (no fault injected).
- Checkpoint commits 0 to 7 written once each.

## 7. Data Correctness
- Input transactions: 2,000 rows, 1,990 unique, 10 duplicate rows
- Output decisions: 1,990 (run 1)
- Output alerts: 129 (run 1)
- Missing transactions: 0
- Duplicate transactions: 0
- Mismatches: 0
- Verification result: all six checks PASSED, exit code 0. Run 6: decisions PASSED, alerts FAILED (129 missing / 129 extra, timezone shift).

## 8. Evidence
- Logs: producer arrival log and summary (`saatwik/`), Spark driver logs (`shashank/`), HDFS listings (`sumukh/hdfs_*.txt`). Folders: `evidence/baseline/saatwik/`, `evidence/baseline/shashank/`, `evidence/baseline/sumukh/`
- Screenshots: `evidence/Fraud_Monitoring_Experiment_Evidence.pdf`, Fig 1–19 (captioned, in order).
- Metrics: longest micro-batch per run in `shashank/longest_batch_ms.txt` (run 1 40.2 s, run 2 47.7 s, run 3 20.2 s / 44.0 s after resume, run 4 46.5 s, run 5 35.6 s). Producer duration in the summary JSON.
- Verification report: `sumukh/verification_report_run1.json` and `sumukh/verification_run1.txt`; exported tables `decisions_run1.csv` and `alerts_run1.csv`.

## 9. Result
- PASS (Run 1). Run 6: FAIL, defect found and fixed.
- Explanation: The output equals the ground truth. The first attempt shows the verifier catching a real timezone defect before the experiments.
- Issues encountered: Run 6 timezone defect (fixed). Dashboard LIVE parser failed on values containing commas (beeline csv2 → tsv2, fixed). Batches take 20–48 s against a 10 s trigger.

## 10. Conclusion
The pipeline is correct end to end. The verifier is an effective gate: it found the timezone defect.
