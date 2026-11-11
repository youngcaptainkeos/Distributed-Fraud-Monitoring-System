# Distributed Fraud Monitoring — Evidence Index

## Baseline
- Local input and ground-truth validation: COMPLETE
- Independent R1-R5 verification: COMPLETE
- Dashboard DEMO validation: COMPLETE
- Python-to-Beeline connectivity: COMPLETE
- Local infrastructure snapshot: COMPLETE
- Distributed Kafka-Spark-Iceberg baseline: PENDING

## Experiments

| Experiment | Preparation | Actual execution |
|---|---|---|
| Executor failure | Checklist and snapshot ready | Pending |
| Master/driver failure | Checklist ready | Pending |
| One-minute lateness | Checklist ready | Pending |
| Beyond-watermark lateness | Checklist ready | Pending |

## Evidence Structure
Each experiment contains:
- logs/ — application and service logs
- screenshots/ — UI and terminal evidence
- metrics/ — snapshots and verification results

## Important Verification Notes
- The input has 2000 rows and 1990 unique transaction IDs.
- There are 10 duplicate input rows.
- All six baseline verifier checks passed.
- R4 decision flags are reproduced using the account-level scope of supplied R4 alert windows. The underlying alert patterns remain destination-specific.
- Distributed recovery and end-to-end integration have not yet been demonstrated.

## Snapshot Utility
Run:
./scripts/capture_system_snapshot.sh baseline

Or specify an experiment folder:
./scripts/capture_system_snapshot.sh experiment_1_executor_failure
