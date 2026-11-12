# Evaluation requirements and remaining demo work

> **Status update, 13 November 2026.** This working draft was written before the runs.
> All five runs have since been completed and verified; the statements below about
> pending work, run 6 and report templates are historical. Current results are in
> [`../../evidence/README.md`](../../evidence/README.md).


Source: the evaluator's message supplied in this chat and the
[official project brief](https://big-data-2026-supersite.super.site/projects-1/distributed-fraud-monitoring-system).
Assessment date: 12 November 2026. This records inspected source and saved
evidence; it does not establish the current state of teammates' machines.

## What must be ready before evaluation

The evaluator expects an implemented pipeline with proof of previous runs,
ideally retained Iceberg tables. Be ready to execute the stream, recover using
a checkpoint, demonstrate late-arrival behavior, run the verifier and explain
the experiments. Preparing to execute every experiment for the first time
during evaluation is insufficient.

The project brief calls for a baseline followed by four experiments, with
screenshots and actual-output verification after each. Each scenario should
finish within approximately 20 minutes.

## Five completed runs to retain

| Run purpose | What to demonstrate | Evidence to keep |
|---|---|---|
| Clean baseline | In-order input produces correct decisions and alerts | Both populated Iceberg tables, actual exports, verifier report, producer summary, screenshots |
| Executor failure | Kill and restart an application executor during ingestion; work resumes | Executor IDs/process evidence before and after, failure/recovery log, final tables and verifier report |
| Master and driver failure | Restart from the same intact checkpoint without replaying input | Same topic/checkpoint/table names, before/after batch progress, manual resubmission record, final verifier report |
| Less-than-one-minute disorder | Controlled delayed payments still contribute correctly | Arrival log, event times, watermark progress, actual outputs and verifier report |
| More-than-one-minute disorder | Explain observed late-event exclusion and resulting output changes | Arrival log, actual watermark advancement, drop metrics where available, output differences and conclusion |

Use a new topic, checkpoint and table pair for each independent run. Reuse
them only within a deliberate recovery run. Preserve the completed baseline
and experiments; do not recycle old tables into new demonstrations.

The supplied ground truth describes in-order, on-time delivery. For the
beyond-watermark experiment, differences can be expected: identify which
events were actually excluded and explain the consequences. A producer delay
over 60 seconds alone does not prove Spark dropped that record. Do not force
an expected-difference experiment into a misleading baseline PASS.

## What the saved evidence currently proves

- Local rule-engine tests matched supplied ground truth.
- Saved VM1 preload evidence records 2,000 messages delivered and verified.
- The old test run's exported output contained 63 decisions and zero alerts;
  its 63 decisions matched the checked decision fields but the run was incomplete.
- A later test recovery failed with missing checkpoint state; the topic was
  subsequently observed empty. Cause/timing of external resets is unknown.
- The inspected run6 log ended at 13:57:40 IST with No Route to Host to
  172.23.138.248:9000. A successful complete run6 is not established by that log.
- The four experiment report files are still templates in the inspected checkout.

Offline tests, DEMO dashboard screenshots and Kafka acknowledgements are useful
component evidence, but do not establish a completed distributed baseline.

## Concrete implementation gaps identified from the brief

The local dashboard currently has decision/rule filters, transaction/account
search, rule/decision charts and an alert table. The following required views
are not implemented in the inspected code:

- A time-range filter applied consistently with account and rule selection.
- Transaction, alert and flagged-account summary KPIs together.
- Both transaction and alert volume per minute; the current transaction chart
  groups into 15-minute bins and has no alert-volume-per-minute counterpart.
- A dedicated flagged-accounts table.
- Selecting an alert to show its explanation, trigger transaction and the
  source account's last 20 transactions in a dedicated investigation view.

Actual-output comparison exists via run_verification.py with both actual CSV
arguments. Strengthen coverage for original transaction fields and alert
threshold/explanation fields before describing it as a complete schema-field
comparison. Preserve a small separate invalid-output example to show the
verifier detects a known error; never modify the real run tables for that test.

The brief lists VM layout among fixed choices and shows four roles/machines.
The team currently combines VM3 and VM4 on Sumukh's machine. Explain the
deployment deviation and confirm whether that arrangement is acceptable to
the evaluator. Two worker processes on one VM do not demonstrate two-machine
worker redundancy.

## Team responsibilities

- Saatwik: producer modes, controlled disorder, acknowledgements and arrival logs.
- Shashank: streaming application, executor/master/driver failure procedure,
  checkpoint reuse, query progress and watermark evidence.
- Sumukh: HDFS/Iceberg persistence, retained older tables, actual-output verifier,
  completed dashboard features and LIVE investigation demonstration.

## Completion order

1. Restore stable connectivity and complete one actual baseline.
2. Export both actual tables and compare with ground truth once; fix concrete
   mismatches if present. Retain the successful Iceberg tables.
3. Complete the missing dashboard views and verifier coverage.
4. Execute the four required experiments, preserving results separately.
5. Finish each run report with observations and an honest PASS/FAIL/INCONCLUSIVE
   conclusion. Finalize the individual demo sheets using rehearsed commands.
