# Experiment 4 — Events Beyond Watermark

## Objective
Verify the handling of events arriving more than one minute behind the event-time watermark.

## Procedure
1. Start the complete streaming pipeline.
2. Send a controlled transaction sequence that creates a qualifying fraud pattern.
3. Advance event time until the watermark passes the relevant window.
4. Send selected events more than one minute late.
5. Inspect streaming progress and resulting decisions and alerts.
6. Compare actual results with the expected late-event handling.

## Capture
- Original event timestamps.
- Actual arrival order and timestamps.
- Watermark progression.
- Late-event logs or metrics, if available.
- Resulting decisions and alerts.
- Verification report.

## Expected Result
Events beyond the configured watermark are not expected to update already-closed stateful windows. Record actual behavior and any sink-specific effects.

## Status
Completed on 12 November 2026.

## Result
Expected difference (run 5). Seven rows arrived 93 to 129 s late; exactly those 7 ids are absent from the decisions (1,983 of 1,990), alerts unchanged (129). Consistent with the 1-minute watermark plus deduplication state; observed in one run. See [`REPORT.md`](REPORT.md).
