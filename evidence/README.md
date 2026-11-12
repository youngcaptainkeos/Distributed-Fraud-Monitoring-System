# Distributed Fraud Monitoring — Evidence Index

All runs were executed on 12 November 2026 on the three-VM cluster (Kafka on VM1, Spark master and driver on VM2, HDFS, two Spark workers, dashboard and verifier on VM3/VM4). Every run used its own Kafka topic, HDFS checkpoint and Iceberg tables named by run number, and was checked by `vm3/verifier/run_verification.py` against the supplied ground truth (1,990 decisions, 129 alerts).

## Results

| Experiment | Run | Result | Report | Evidence |
|---|---|---|---|---|
| Baseline | Run 1 | PASS (Run 1). Run 6: FAIL, defect found and fixed. | [`REPORT.md`](baseline/REPORT.md) | [`baseline/`](baseline/) |
| Executor failure | Run 2 | PASS | [`REPORT.md`](experiment_1_executor_failure/REPORT.md) | [`experiment_1_executor_failure/`](experiment_1_executor_failure/) |
| Master and driver failure | Run 3 | PASS | [`REPORT.md`](experiment_2_master_driver_failure/REPORT.md) | [`experiment_2_master_driver_failure/`](experiment_2_master_driver_failure/) |
| Late data under one minute | Run 4 | PASS | [`REPORT.md`](experiment_3_one_minute_lateness/REPORT.md) | [`experiment_3_one_minute_lateness/`](experiment_3_one_minute_lateness/) |
| Late data beyond the watermark | Run 5 | FAIL (expected): documented limitation | [`REPORT.md`](experiment_4_beyond_watermark/REPORT.md) | [`experiment_4_beyond_watermark/`](experiment_4_beyond_watermark/) |

Run 6 was the first end-to-end attempt. It failed the alert comparison because of a timezone defect (alert windows 5 h 30 min early), which was fixed in `vm2/app/stream_job.py`. It is kept under `baseline/sumukh/run6_first_attempt/`.

## Screenshots
[`Fraud_Monitoring_Experiment_Evidence.pdf`](Fraud_Monitoring_Experiment_Evidence.pdf): 81 captioned figures in order, with a summary page per experiment.

## Layout of each experiment folder
- `REPORT.md`: the experiment report.
- `saatwik/`: producer arrival log and summary, Kafka topic verification (`topic_runN.txt`).
- `shashank/`: Spark driver logs (IP addresses redacted), longest-batch measurements.
- `sumukh/`: exported decisions and alerts, verifier text and JSON report, HDFS checkpoint and warehouse listings.

## Notes
- The input has 2,000 rows and 1,990 unique transaction ids (10 duplicate rows).
- Run 5 is expected to fail the decision comparison: the 7 missing ids are exactly the 7 rows the producer sent more than 60 s late.
- Recovery from a master and driver failure (run 3) needed a manual restart and resubmit.
- Micro-batches took 20 to 48 s against a 10 s trigger because each batch re-evaluates all stored decisions.
- IP addresses in logs and exports are redacted as `<ZT_IP>`.
