# Distributed Fraud Monitoring — Experiment Report

## 1. Experiment Details
- Experiment ID: Experiment 4
- Experiment name: Late data beyond the watermark
- Date and time: 12 Nov 2026, producer 22:31:40 to 22:40:43 IST (17:01:40 to 17:10:43 UTC)
- Person conducting experiment: Saatwik (producer), Shashank (Spark), Sumukh (verifier, dashboard)
- Spark version: 3.3.4 (standalone, 1 master, 2 workers, 2 executors of 2 cores / 512 MB)
- Hadoop version: Spark build `spark-3.3.4-bin-hadoop3`, HDFS NameNode on VM3 (exact HDFS version not captured in the evidence)
- Kafka run/topic: transactions-run5 (Kafka 3.9.2, KRaft single node, 1 partition, replication factor 1)

## 2. Objective
Send events that arrive more than a minute after newer events and show exactly what the system does with them.

## 3. Initial System State
- Kafka: topic transactions-run5 created empty.
- Spark master: up, 2 workers alive.
- Spark workers: 2 executors.
- Streaming application: FraudDetectionEngine-run5 started 22:31:02 IST.
- HDFS: up.
- Iceberg tables: `transaction_decisions_run5`, `account_risk_alerts_run5`.
- Checkpoint: `/checkpoints/run5`.

## 4. Procedure
1. Dry run: `python3 vm1/producer.py --mode late_gt60 ... --dry-run` (predicts 7 rows with lag above 60 s).
2. Create topic transactions-run5; set RUN_ID=run5 and KAFKA_TOPIC=transactions-run5; submit the job.
3. Producer: `python3 vm1/producer.py --mode late_gt60 --topic transactions-run5 --run-id run5 --input data/transactions.csv --bootstrap $KAFKA_BOOTSTRAP --fraction 0.005 --min-wall-hold 63 --log-dir ~/fraud/logs/run5`
4. List the late ids from the producer log (`injected_delay == 1`) into `late_ids_run5.txt`.
5. Sumukh: `bash ~/export_verify3.sh run5`; compare the missing ids with the late ids using `comm -13` and `diff`.
6. Dashboard LIVE, run 5: Investigation by Transaction ID for TXN0000125, TXN0000133 (control) and TXN0001953.

## 5. Observations
- Producer summary: 7 rows injected, all 7 with event-time lag above 60 s (105, 129, 118, 120, 93, 94 and 94 s). Kafka held all 2,000 messages.
- Spark: final Batch 21 at 22:41:25 IST wrote 1,983 decisions and 129 alerts.
- Verifier: decisions FAILED (expected 1,990, actual 1,983, missing 7, extra 0, mismatches 0); alerts PASSED (129 / 129).
- The 7 missing ids (TXN0000125, 291, 369, 395, 684, 1023, 1953) are identical to the producer late list (`diff` prints IDENTICAL).
- Dashboard run 5: 1,983 transactions, 1,652 approved (1,659 in run 1), 231 review, 100 blocked; rule counts and alert counts unchanged. TXN0000125 and TXN0001953 are not found; TXN0000133 (not late) is found.

## 6. Recovery
- Not applicable. The system did not fail; it dropped the late rows.
- Checkpoint commits 0 to 22 written.

## 7. Data Correctness
- Input transactions: 2,000 rows, 1,990 unique
- Output decisions: 1,983
- Output alerts: 129
- Missing transactions: 7 (all late rows)
- Duplicate transactions: 0
- Mismatches: 0
- Verification result: decisions FAILED, alerts PASSED, exit code 1. Expected outcome for this experiment.

## 8. Evidence
- Logs: producer arrival log and summary (`saatwik/`), Spark driver logs (`shashank/`), HDFS listings (`sumukh/hdfs_*.txt`). Folders: `evidence/experiment_4_beyond_watermark/saatwik/`, `evidence/experiment_4_beyond_watermark/shashank/`, `evidence/experiment_4_beyond_watermark/sumukh/`
- Screenshots: `evidence/Fraud_Monitoring_Experiment_Evidence.pdf`, Fig 63–76 (captioned, in order).
- Metrics: longest micro-batch per run in `shashank/longest_batch_ms.txt` (run 1 40.2 s, run 2 47.7 s, run 3 20.2 s / 44.0 s after resume, run 4 46.5 s, run 5 35.6 s). Producer duration in the summary JSON.
- Verification report: `sumukh/verification_report_run5.json` and `sumukh/verification_run5.txt`; exported tables `decisions_run5.csv` and `alerts_run5.csv`.

## 9. Result
- FAIL (expected): documented limitation
- Explanation: Rows more than 60 s behind the newest event time were not in the output. This is consistent with the 1-minute watermark plus the dropDuplicates state on (transaction_id, event_timestamp). The mechanism is inferred from the configuration and the exact id match; it was measured in one run.
- Issues encountered: The dashboard says "not found" both for a dropped row and for a row that never arrived, so it must be read together with the producer log and the verifier report. No alert was affected in this data set.

## 10. Conclusion
Late data beyond the watermark is lost (7 of 1,990 decisions, 0.35 %). The verifier detects it exactly. Whether this is acceptable is a design decision.
