# Distributed Fraud Monitoring — Experiment Report

## 1. Experiment Details
- Experiment ID: Experiment 3
- Experiment name: Late data under one minute
- Date and time: 12 Nov 2026, producer 21:40:46 to 21:49:50 IST (16:10:46 to 16:19:50 UTC)
- Person conducting experiment: Saatwik (producer), Shashank (Spark), Sumukh (verifier)
- Spark version: 3.3.4 (standalone, 1 master, 2 workers, 2 executors of 2 cores / 512 MB)
- Hadoop version: Spark build `spark-3.3.4-bin-hadoop3`, HDFS NameNode on VM3 (exact HDFS version not captured in the evidence)
- Kafka run/topic: transactions-run4 (Kafka 3.9.2, KRaft single node, 1 partition, replication factor 1)

## 2. Objective
Send events that arrive up to a minute after newer events and show they are accepted and every result is correct (watermark = 1 minute).

## 3. Initial System State
- Kafka: topic transactions-run4 created empty.
- Spark master: up, 2 workers alive.
- Spark workers: 2 executors.
- Streaming application: FraudDetectionEngine-run4 started 21:38:19 IST.
- HDFS: up.
- Iceberg tables: `transaction_decisions_run4`, `account_risk_alerts_run4`.
- Checkpoint: `/checkpoints/run4`.

## 4. Procedure
1. Create topic transactions-run4; set RUN_ID=run4 and KAFKA_TOPIC=transactions-run4 in `config/.env` (check with `grep -nE "^(RUN_ID|KAFKA_TOPIC)=" config/.env`); submit the job.
2. Producer: `python3 vm1/producer.py --mode late_lt60 --topic transactions-run4 --run-id run4 --input data/transactions.csv --bootstrap $KAFKA_BOOTSTRAP --fraction 0.005 --min-wall-hold 63 --log-dir ~/fraud/logs/run4`
3. Topic check without `--expect-order` (order is deliberately changed).
4. Late rows listed with `awk -F, "NR==1 || \$9+0>30" run4_late_lt60_arrival.csv`.
5. Sumukh: `bash ~/export_verify3.sh run4`.

## 5. Observations
- Producer summary: 7 rows injected, all 7 actually late, 0 with lag above 60 s. Event-time lags 30, 33, 33, 46, 36, 45 and 48 s; each row held 63 s of wall time. 544.6 s in total.
- Kafka held all 2,000 messages (same content, order differs from the file as intended).
- Spark processed 22 micro-batches (15 non-empty). Final batch 22 at 21:50:38 IST wrote 1,990 decisions and 129 alerts. Longest batch 46.5 s.
- Incident before the run: `config/.env` was mangled by a sed edit (RUN_ID=run4un3) and a wrongly named application started. It was stopped before any data was processed; the stray tables and checkpoint (run4un3) were removed after a listing check. Run 4 was then started with correct values.

## 6. Recovery
- Not applicable (no failure injected).
- Checkpoint commits 0 to 23 written.

## 7. Data Correctness
- Input transactions: 2,000 rows, 1,990 unique
- Output decisions: 1,990
- Output alerts: 129
- Missing transactions: 0
- Duplicate transactions: 0
- Mismatches: 0
- Verification result: PASSED, exit code 0

## 8. Evidence
- Logs: producer arrival log and summary (`saatwik/`), Spark driver logs (`shashank/`), HDFS listings (`sumukh/hdfs_*.txt`). Folders: `evidence/experiment_3_one_minute_lateness/saatwik/`, `evidence/experiment_3_one_minute_lateness/shashank/`, `evidence/experiment_3_one_minute_lateness/sumukh/`
- Screenshots: `evidence/Fraud_Monitoring_Experiment_Evidence.pdf`, Fig 54–62 (captioned, in order).
- Metrics: longest micro-batch per run in `shashank/longest_batch_ms.txt` (run 1 40.2 s, run 2 47.7 s, run 3 20.2 s / 44.0 s after resume, run 4 46.5 s, run 5 35.6 s). Producer duration in the summary JSON.
- Verification report: `sumukh/verification_report_run4.json` and `sumukh/verification_run4.txt`; exported tables `decisions_run4.csv` and `alerts_run4.csv`.

## 9. Result
- PASS
- Explanation: Events up to 48 s late were accepted; no decision or alert changed.
- Issues encountered: The mistyped run id incident (see Observations). Batches slower than the trigger interval.

## 10. Conclusion
Out-of-order arrival inside the 1-minute watermark is handled correctly.
