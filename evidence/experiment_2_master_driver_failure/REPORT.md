# Distributed Fraud Monitoring — Experiment Report

## 1. Experiment Details
- Experiment ID: Experiment 2
- Experiment name: Master and driver failure
- Date and time: 12 Nov 2026, master stopped 21:13:40 IST (15:43:40 UTC)
- Person conducting experiment: Saatwik (producer), Shashank (Spark), Sumukh (verifier)
- Spark version: 3.3.4 (standalone, 1 master, 2 workers, 2 executors of 2 cores / 512 MB)
- Hadoop version: Spark build `spark-3.3.4-bin-hadoop3`, HDFS NameNode on VM3 (exact HDFS version not captured in the evidence)
- Kafka run/topic: transactions-run3 (Kafka 3.9.2, KRaft single node, 1 partition, replication factor 1)

## 2. Objective
Stop the Spark master and driver mid-batch. After restart, show the job resumes from the HDFS checkpoint with complete, duplicate-free output while Kafka keeps the data.

## 3. Initial System State
- Kafka: topic transactions-run3 created empty.
- Spark master: up, 2 workers alive.
- Spark workers: 2 executors.
- Streaming application: FraudDetectionEngine-run3 (app-20261112210320-0003) started 21:03:20 IST.
- HDFS: up.
- Iceberg tables: `transaction_decisions_run3`, `account_risk_alerts_run3`.
- Checkpoint: `/checkpoints/run3`.

## 4. Procedure
1. Create topic; submit job with RUN_ID=run3.
2. Producer: `python3 vm1/producer.py --mode baseline --topic transactions-run3 --run-id run3 --send-delay 0.3 ...` (604 s).
3. At 21:13:40 IST (15:43:40 UTC): `date -u +%T; bash scripts/stop_master.sh` on VM2. Confirm the master UI refuses connections.
4. Restart the master, confirm 2 workers re-register, resubmit the job with the SAME RUN_ID and topic (same checkpoint path).
5. Watch the driver log and Spark UI until the backlog is processed.
6. Sumukh: `bash ~/export_verify3.sh run3`; `hdfs dfs -ls /checkpoints/run3/commits | sort -V`.

## 5. Observations
- Before: Batch 9 (461 decisions) and Batch 10 (526) completed at 21:12:46 and 21:13:05 IST.
- Batch 11 (66 records) started 21:13:10; the log shows `Writing 592 decisions` at 21:13:16 and then ends. The master was stopped at 21:13:40; the UI at port 8080 refused connections.
- The producer was not affected and finished at 21:20:12 IST; Kafka held the data.
- 21:16:27 IST: new driver started (new application id app-20261112211628-0000, the counter reset because the master restarted). Workers had re-registered on their own.
- Batch 11 was processed again from the checkpointed offsets (66 records, 592 decisions, 5 alerts) and committed at 21:17:11. Batches 12 to 17 followed; Batch 17 at 21:20:56 wrote 1,990 decisions and 129 alerts; Batch 18 empty.

## 6. Recovery
- Was recovery successful? Yes.
- Was manual intervention required? Yes: restart the master and resubmit the job.
- Recovery time: 2 min 48 s from master stop to the job resubmitted; 3 min 31 s to the first committed batch.
- Were checkpoints reused? Yes, the same checkpoint; commits 0 to 18 are continuous across the restart.
- Did processing resume? Yes, from the unfinished batch 11, not from the beginning.

## 7. Data Correctness
- Input transactions: 2,000 rows, 1,990 unique
- Output decisions: 1,990
- Output alerts: 129
- Missing transactions: 0
- Duplicate transactions: 0 (replay of batch 11 created none; MERGE INTO is idempotent)
- Mismatches: 0
- Verification result: PASSED, exit code 0

## 8. Evidence
- Logs: producer arrival log and summary (`saatwik/`), Spark driver logs (`shashank/`), HDFS listings (`sumukh/hdfs_*.txt`). Folders: `evidence/experiment_2_master_driver_failure/saatwik/`, `evidence/experiment_2_master_driver_failure/shashank/`, `evidence/experiment_2_master_driver_failure/sumukh/`
- Screenshots: `evidence/Fraud_Monitoring_Experiment_Evidence.pdf`, Fig 42–53 (captioned, in order).
- Metrics: longest micro-batch per run in `shashank/longest_batch_ms.txt` (run 1 40.2 s, run 2 47.7 s, run 3 20.2 s / 44.0 s after resume, run 4 46.5 s, run 5 35.6 s). Producer duration in the summary JSON.
- Verification report: `sumukh/verification_report_run3.json` and `sumukh/verification_run3.txt`; exported tables `decisions_run3.csv` and `alerts_run3.csv`.

## 9. Result
- PASS
- Explanation: Checkpointing and idempotent writes gave exactly-once results across a full restart.
- Issues encountered: Recovery is manual; there is no standby master or automatic driver restart. The dashboard LIVE parse bug (csv2 vs tsv2) was fixed during the outage; it is not part of the result. The driver log ends at 21:13:16 IST, 24 s before `stop_master.sh` (21:13:40 IST); the exact way the driver process ended was not recorded, so this report does not claim one.

## 10. Conclusion
The pipeline survives loss of master and driver without data loss or duplicates, at the cost of a manual restart of about 3 minutes.
