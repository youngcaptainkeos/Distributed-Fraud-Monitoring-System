from pathlib import Path
import csv
import json
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2] / "data"

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


transactions = read_csv(ROOT / "transactions.csv")
decisions = read_csv(
    ROOT / "ground_truth_transaction_decisions.csv"
)
alerts = read_csv(
    ROOT / "ground_truth_account_risk_alerts.csv"
)

# Deduplicate transactions.
unique = {}
for row in transactions:
    unique.setdefault(row["transaction_id"], row)

expected = {
    row["transaction_id"]
    for row in decisions
    if row["r4_triggered"] == "true"
}

r4_alerts = [
    row for row in alerts
    if row["rule_id"] == "R4"
]

def parse_time(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).replace(tzinfo=timezone.utc)


# Existing alert supporting IDs.
supporting = set()

# All transactions from the source account within each alert window.
account_window_ids = set()

# All transactions to the alert's repeated destination.
destination_window_ids = set()

for alert in r4_alerts:
    source = alert["source_account"]
    destination = alert["destination_account"]

    start = parse_time(alert["window_start"])
    end = parse_time(alert["window_end"])

    supporting.update(
        json.loads(alert["supporting_transaction_ids"])
    )

    for txn_id, row in unique.items():
        if row["source_account"] != source:
            continue

        timestamp = parse_time(row["event_timestamp"])

        if start <= timestamp < end:
            account_window_ids.add(txn_id)

            if row["destination_account"] == destination:
                destination_window_ids.add(txn_id)


def report(label, calculated):
    matched = calculated & expected
    fp = calculated - expected
    fn = expected - calculated

    print(f"\n========== {label} ==========")
    print("Calculated:", len(calculated))
    print("Expected:", len(expected))
    print("Matched:", len(matched))
    print("False positives:", len(fp))
    print("False negatives:", len(fn))
    print("False-positive examples:", sorted(fp)[:15])
    print("False-negative examples:", sorted(fn)[:15])


print("========== R4 SCOPE INVESTIGATION ==========")
print("R4 alerts:", len(r4_alerts))
print("Expected R4 flags:", len(expected))
print("Alert supporting IDs:", len(supporting))

report("DESTINATION-LEVEL ALERT WINDOWS", destination_window_ids)
report("ACCOUNT-LEVEL ALERT WINDOWS", account_window_ids)

print("\n========== MISSING FLAGS IN ACCOUNT WINDOWS ==========")
print(sorted(expected - account_window_ids)[:30])

print("\n========== EXTRA ACCOUNT-WINDOW TRANSACTIONS ==========")
print(sorted(account_window_ids - expected)[:30])
