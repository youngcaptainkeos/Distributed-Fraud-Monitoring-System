# Experiment 3 — Out-of-Order Events Within Watermark

## Objective
Verify that events arriving out of order but within the one-minute watermark are handled correctly.

## Procedure
1. Start the complete streaming pipeline.
2. Send a controlled transaction sequence that creates a qualifying fraud pattern.
3. Delay selected events so they arrive out of event-time order.
4. Ensure the delayed events are no more than one minute behind the current event-time watermark.
5. Allow the relevant windows to complete.
6. Inspect transaction decisions and risk alerts.
7. Compare results against independently calculated expected results.

## Capture
- Original event timestamps.
- Actual arrival order and timestamps.
- Watermark progress.
- Spark streaming progress.
- Resulting decisions and alerts.
- Verification report.

## Expected Result
Events arriving within the configured lateness allowance are processed correctly.

## Status
Completed on 12 November 2026.

## Result
PASS (run 4). Seven rows arrived 30 to 48 s late in event time; all were processed and the output equals the ground truth. See [`REPORT.md`](REPORT.md).
