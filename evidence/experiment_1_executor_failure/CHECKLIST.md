# Experiment 1 — Spark Executor Failure

## Objective
Verify that Spark recovers from executor failure without losing or duplicating transaction results.

## Procedure
1. Start Kafka, Spark master, workers and the streaming application.
2. Confirm transactions are being processed.
3. Record the current batch ID and processed transaction count.
4. Identify and terminate one executor process.
5. Observe Spark detecting the lost executor.
6. Wait for Spark to allocate a replacement executor.
7. Confirm streaming processing resumes.
8. Run independent verification against the output tables.

## Capture
- Executor list before and after failure.
- Executor termination logs.
- Spark application recovery logs.
- Batch processing progress.
- Final decision and alert counts.
- Verification report.

## Expected Result
The application continues processing after executor replacement, with no unexplained data loss or duplicate output.

## Status
Completed on 12 November 2026.

## Result
PASS (run 2). One executor was killed with `kill -9`; Spark replaced it within about 2 s, no task failed, no manual step, output exact (1,990 decisions, 129 alerts). See [`REPORT.md`](REPORT.md).
