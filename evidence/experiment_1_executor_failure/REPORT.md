# Distributed Fraud Monitoring — Experiment Report

## 1. Experiment Details
- Experiment ID: Experiment 1
- Experiment name: Executor failure
- Date and time: 12 Nov 2026, kill at 20:44:07 IST (15:14:07 UTC)
- Person conducting experiment: Saatwik (producer), Shashank (Spark), Sumukh (kill, verifier)
- Spark version: 3.3.4 (standalone, 1 master, 2 workers, 2 executors of 2 cores / 512 MB)
- Hadoop version: Spark build `spark-3.3.4-bin-hadoop3`, HDFS NameNode on VM3 (exact HDFS version not captured in the evidence)
- Kafka run/topic: transactions-run2 (Kafka 3.9.2, KRaft single node, 1 partition, replication factor 1)

## 2. Objective
Kill one Spark executor while the stream is running and show that the job completes with correct, complete results and no manual action.

## 3. Initial System State
- Kafka: topic transactions-run2 created empty.
- Spark master: up, 2 workers alive.
- Spark workers: 2, executors 0 and 1 running.
- Streaming application: FraudDetectionEngine-run2 (app-20261112203910-0002) started 20:39:10 IST.
- HDFS: up.
- Iceberg tables: `transaction_decisions_run2`, `account_risk_alerts_run2`.
- Checkpoint: `/checkpoints/run2`.

## 4. Procedure
1. Create topic transactions-run2; set RUN_ID=run2 and KAFKA_TOPIC=transactions-run2; submit the job.
2. Producer: `python3 vm1/producer.py --mode baseline --topic transactions-run2 --run-id run2 --send-delay 0.3 ...` (604 s).
3. Sumukh: `jps -l | grep CoarseGrainedExecutorBackend` (PIDs 30200, 30201).
4. Sumukh: `PID=$(jps -l | grep CoarseGrainedExecutorBackend | head -1 | awk "{print \$1}"); kill -9 $PID` at 15:14:07 UTC.
5. Watch the Spark UI (Executors, Jobs) and the driver log.
6. After the producer finished: `bash ~/export_verify3.sh run2`; `hdfs dfs -ls /checkpoints/run2/commits`.

## 5. Observations
- Before: two active executors, 265 tasks completed, 0 failed.
- 20:44:07 IST: driver logged `Lost executor 0 … Remote RPC client disassociated`. jps showed a new executor process (32119) two seconds after the kill.
- Spark UI: executor 0 Dead, new executor 2 Active; Jobs timeline shows Executor 0 removed and Executor 2 added at about 20:44; jobs continued.
- Batch 11 (50 records) completed at 20:44:17; batches 12 to 25 followed. Final: batch 25 at 20:52:38 IST wrote 1,990 decisions and 129 alerts; batch 26 empty.
- The two live executors completed 1,329 tasks, the dead one 222. No failed tasks. WARN lines show batches slower than the 10 s trigger (up to 47.7 s).

## 6. Recovery
- Was recovery successful? Yes.
- Was manual intervention required? No.
- Recovery time: replacement executor process visible 2 s after the kill; the stream never stopped.
- Were checkpoints reused? Yes, one continuous checkpoint, commits 0 to 26.
- Did processing resume? It never stopped.

## 7. Data Correctness
- Input transactions: 2,000 rows, 1,990 unique
- Output decisions: 1,990
- Output alerts: 129
- Missing transactions: 0
- Duplicate transactions: 0
- Mismatches: 0
- Verification result: PASSED, exit code 0

## 8. Evidence
- Logs: producer arrival log and summary (`saatwik/`), Spark driver logs (`shashank/`), HDFS listings (`sumukh/hdfs_*.txt`). Folders: `evidence/experiment_1_executor_failure/saatwik/`, `evidence/experiment_1_executor_failure/shashank/`, `evidence/experiment_1_executor_failure/sumukh/`
- Screenshots: `evidence/Fraud_Monitoring_Experiment_Evidence.pdf`, Fig 32–41 (captioned, in order).
- Metrics: longest micro-batch per run in `shashank/longest_batch_ms.txt` (run 1 40.2 s, run 2 47.7 s, run 3 20.2 s / 44.0 s after resume, run 4 46.5 s, run 5 35.6 s). Producer duration in the summary JSON.
- Verification report: `sumukh/verification_report_run2.json` and `sumukh/verification_run2.txt`; exported tables `decisions_run2.csv` and `alerts_run2.csv`.

## 9. Result
- PASS
- Explanation: Loss of one executor did not change any decision or alert.
- Issues encountered: None for this run. Batches slower than the trigger interval.

## 10. Conclusion
Spark tolerates executor loss: it replaces the executor, reschedules tasks and the output stays exact.
