import csv
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

INPUT = ROOT / "data/transactions.csv"
GROUND_TRUTH = (
    ROOT / "data/"
    "ground_truth_transaction_decisions.csv"
)
ALERTS = (
    ROOT / "data/"
    "ground_truth_account_risk_alerts.csv"
)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_timestamp(value):
    dt = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


transactions = read_csv(INPUT)
decisions = read_csv(GROUND_TRUTH)
alerts = read_csv(ALERTS)

# Deduplicate transactions.
unique_transactions = {}

for row in transactions:
    unique_transactions.setdefault(
        row["transaction_id"], row
    )

# Ground-truth R4 flags.
expected_r4 = {
    row["transaction_id"]
    for row in decisions
    if row["r4_triggered"] == "true"
}

# Use R4 alert windows to independently reconstruct
# account-level transaction flags.
r4_alerts = [
    row for row in alerts
    if row["rule_id"] == "R4"
]

calculated_r4 = set()

for alert in r4_alerts:

    source_account = alert["source_account"]

    start = parse_timestamp(alert["window_start"])
    end = parse_timestamp(alert["window_end"])

    for txn_id, row in unique_transactions.items():

        if row["source_account"] != source_account:
            continue

        event_time = parse_timestamp(
            row["event_timestamp"]
        )

        if start <= event_time < end:
            calculated_r4.add(txn_id)


matched = calculated_r4 & expected_r4
false_positives = calculated_r4 - expected_r4
false_negatives = expected_r4 - calculated_r4

print("\n========== R4 INDEPENDENT VERIFICATION ==========")

print("Unique input transactions:", len(unique_transactions))
print("R4 alerts processed:", len(r4_alerts))

print("Expected R4 transactions:", len(expected_r4))
print("Calculated R4 transactions:", len(calculated_r4))

print("Matched:", len(matched))
print("False positives:", len(false_positives))
print("False negatives:", len(false_negatives))

print("\n========== MISMATCH EXAMPLES ==========")

print("\nFalse positives:")
for txn_id in sorted(false_positives)[:15]:
    print(txn_id)

print("\nFalse negatives:")
for txn_id in sorted(false_negatives)[:15]:
    print(txn_id)

print("\n========== RESULT ==========")

if not false_positives and not false_negatives:
    print("R4 PASSED")
else:
    print("R4 FAILED")
