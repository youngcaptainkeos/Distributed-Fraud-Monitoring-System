# Baseline — Normal Distributed Execution

## Objective
Establish the normal system behavior and output counts before conducting failure and lateness experiments.

## Part A — Already Completed Local Validation
- [x] Input dataset validation.
- [x] Ground-truth validation.
- [x] Independent R1 verification.
- [x] Independent R2 verification.
- [x] Independent R3 verification.
- [x] Independent R4 verification.
- [x] Independent R5 verification.
- [x] Dashboard DEMO mode validation.
- [x] Python-to-Beeline SQL connectivity.

Reference report:
verifier/reports/master_report_20261110_185030.txt

## Part B — Pending Distributed Baseline
Complete when the team VMs and streaming pipeline are available.

1. Start Kafka, Spark master, Spark workers and the streaming application.
2. Confirm HDFS and Iceberg connectivity.
3. Start the baseline Kafka topic (transactions-run1).
4. Process the baseline transaction dataset.
5. Record Spark batch IDs and processing progress.
6. Confirm transaction decisions and account risk alerts are written to Iceberg.
7. Verify dashboard LIVE mode against the baseline Iceberg tables.
8. Compare output against the supplied ground truth.
9. Record processing time, counts and any mismatches.

## Expected Ground Truth
- Unique transactions: 1990
- Duplicate input rows: 10
- APPROVE: 1659
- REVIEW: 231
- BLOCK: 100
- R1 triggers: 194
- R2 triggers: 203
- R3 triggers: 66
- R4 triggers: 98
- Account risk alerts: 129

## Evidence to Capture
- Kafka producer and consumer logs.
- Spark master and worker UI screenshots.
- Streaming application logs and progress.
- HDFS warehouse and checkpoint listings.
- Iceberg table schemas and row counts.
- Dashboard LIVE mode screenshot.
- Independent verification report.

## Result
Pending distributed execution.
Do not mark PASS until Part B is completed and verified.
