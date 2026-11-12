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

## Part B — Distributed Baseline (completed, run 1)
All steps were executed on 12 November 2026. Evidence is in `saatwik/`, `shashank/` and `sumukh/` and in [`REPORT.md`](REPORT.md).

- [x] 1. Start Kafka, Spark master, Spark workers and the streaming application.
- [x] 2. Confirm HDFS and Iceberg connectivity.
- [x] 3. Start the baseline Kafka topic (`transactions-run1`).
- [x] 4. Process the baseline transaction dataset (2,000 messages, all acknowledged).
- [x] 5. Record Spark batch IDs and processing progress (batches 1 to 6, commits 0 to 7).
- [x] 6. Confirm transaction decisions and account risk alerts are written to Iceberg.
- [x] 7. Verify dashboard LIVE mode against the baseline Iceberg tables.
- [x] 8. Compare output against the supplied ground truth (0 mismatches).
- [x] 9. Record processing time, counts and mismatches (last batch finished 77 s after the producer; longest batch 40.2 s).

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
PASS (run 1): 1,990 decisions and 129 alerts, identical to the ground truth.
Run 6, the first attempt, failed the alert comparison because of a timezone
defect (windows 5 h 30 min early); it was fixed and is kept as evidence under
`sumukh/run6_first_attempt/`.
