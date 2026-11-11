# Experiment 2 — Spark Master and Driver Failure

## Objective
Test recovery after Spark master and streaming driver failure.

## Procedure
1. Start the complete distributed pipeline.
2. Confirm transactions are being processed.
3. Record the latest completed batch and checkpoint location.
4. Stop the Spark master and streaming driver as specified by the experiment plan.
5. Observe worker disconnection and application interruption.
6. Restart the Spark master.
7. Restart or resubmit the streaming application using the existing checkpoint.
8. Confirm processing resumes.
9. Run independent verification against the output tables.

## Capture
- Spark master UI before and after failure.
- Master and driver termination logs.
- Worker reconnection logs.
- Streaming application restart logs.
- Checkpoint reuse evidence.
- Final decision and alert counts.
- Verification report.

## Expected Result
The cluster and streaming application recover, and previously committed output is not duplicated.

Note: Record whether driver recovery was automatic or required manual resubmission.
