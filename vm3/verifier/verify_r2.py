import csv
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

INPUT = ROOT / "data/transactions.csv"
GROUND_TRUTH = (
    ROOT / "data/"
    "ground_truth_transaction_decisions.csv"
)

WINDOW_SECONDS = 120
SLIDE_SECONDS = 30
THRESHOLD = 5


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_timestamp(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


transactions = read_csv(INPUT)
decisions = read_csv(GROUND_TRUTH)

# Deduplicate transactions using transaction_id.
unique_transactions = {}

for row in transactions:
    unique_transactions.setdefault(
        row["transaction_id"], row
    )

# Group transactions by source account.
accounts = defaultdict(list)

for txn_id, row in unique_transactions.items():
    timestamp = parse_timestamp(row["event_timestamp"])

    accounts[row["source_account"]].append({
        "transaction_id": txn_id,
        "timestamp": timestamp,
    })

# Independently calculate qualifying R2 windows.
calculated_r2 = set()
qualifying_windows = []

for account, events in accounts.items():

    events.sort(key=lambda x: x["timestamp"])

    # Sliding windows are aligned to Unix epoch multiples
    # of 30 seconds, as in Spark's sliding-window convention.
    possible_starts = set()

    for event in events:
        timestamp = event["timestamp"]
        epoch_seconds = int(timestamp.timestamp())

        latest_start = (
            epoch_seconds // SLIDE_SECONDS
        ) * SLIDE_SECONDS

        # An event may belong to four overlapping
        # 120-second windows.
        for i in range(WINDOW_SECONDS // SLIDE_SECONDS):
            start = latest_start - i * SLIDE_SECONDS
            possible_starts.add(start)

    for start_epoch in possible_starts:

        start = datetime.fromtimestamp(
            start_epoch, tz=timezone.utc
        )

        end = start + timedelta(seconds=WINDOW_SECONDS)

        members = [
            event for event in events
            if start <= event["timestamp"] < end
        ]

        if len(members) >= THRESHOLD:

            member_ids = {
                event["transaction_id"]
                for event in members
            }

            calculated_r2.update(member_ids)

            qualifying_windows.append({
                "account": account,
                "start": start,
                "end": end,
                "count": len(members),
                "transaction_ids": member_ids,
            })


# Compare against ground truth.
expected_r2 = {
    row["transaction_id"]
    for row in decisions
    if row["r2_triggered"] == "true"
}

matched = calculated_r2 & expected_r2
false_positives = calculated_r2 - expected_r2
false_negatives = expected_r2 - calculated_r2

print("\n========== R2 INDEPENDENT VERIFICATION ==========")

print("Unique input transactions:", len(unique_transactions))
print("Accounts processed:", len(accounts))
print("Qualifying 120-second windows:", len(qualifying_windows))

print("Expected R2 transactions:", len(expected_r2))
print("Calculated R2 transactions:", len(calculated_r2))

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
    print("R2 PASSED")
else:
    print("R2 FAILED — investigate window semantics")
