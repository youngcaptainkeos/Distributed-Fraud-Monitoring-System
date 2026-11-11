import csv
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

INPUT = ROOT / "data/transactions.csv"
GROUND_TRUTH = (
    ROOT / "data/"
    "ground_truth_transaction_decisions.csv"
)

WINDOW_SECONDS = 120
SLIDE_SECONDS = 30
MIN_COUNT = 2
AMOUNT_THRESHOLD = Decimal("600000")


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_timestamp(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


transactions = read_csv(INPUT)
decisions = read_csv(GROUND_TRUTH)

# Deduplicate using transaction_id.
unique_transactions = {}

for row in transactions:
    unique_transactions.setdefault(row["transaction_id"], row)

# Group eligible INR transactions by source account.
accounts = defaultdict(list)

for txn_id, row in unique_transactions.items():
    if row["currency"].strip().upper() != "INR":
        continue

    accounts[row["source_account"]].append({
        "transaction_id": txn_id,
        "timestamp": parse_timestamp(row["event_timestamp"]),
        "amount": Decimal(row["amount"]),
    })

calculated_r3 = set()
qualifying_windows = []

for account, events in accounts.items():
    events.sort(key=lambda x: x["timestamp"])

    possible_starts = set()

    for event in events:
        epoch_seconds = int(event["timestamp"].timestamp())

        latest_start = (
            epoch_seconds // SLIDE_SECONDS
        ) * SLIDE_SECONDS

        for i in range(WINDOW_SECONDS // SLIDE_SECONDS):
            possible_starts.add(
                latest_start - i * SLIDE_SECONDS
            )

    for start_epoch in possible_starts:
        start = datetime.fromtimestamp(
            start_epoch, tz=timezone.utc
        )
        end = start + timedelta(seconds=WINDOW_SECONDS)

        members = [
            event for event in events
            if start <= event["timestamp"] < end
        ]

        total_amount = sum(
            (event["amount"] for event in members),
            Decimal("0")
        )

        if (
            len(members) >= MIN_COUNT
            and total_amount > AMOUNT_THRESHOLD
        ):
            member_ids = {
                event["transaction_id"]
                for event in members
            }

            calculated_r3.update(member_ids)

            qualifying_windows.append({
                "account": account,
                "start": start,
                "end": end,
                "count": len(members),
                "amount": total_amount,
            })


expected_r3 = {
    row["transaction_id"]
    for row in decisions
    if row["r3_triggered"] == "true"
}

matched = calculated_r3 & expected_r3
false_positives = calculated_r3 - expected_r3
false_negatives = expected_r3 - calculated_r3

print("\n========== R3 INDEPENDENT VERIFICATION ==========")

print("Unique input transactions:", len(unique_transactions))
print("INR accounts processed:", len(accounts))
print("Qualifying 120-second windows:", len(qualifying_windows))

print("Expected R3 transactions:", len(expected_r3))
print("Calculated R3 transactions:", len(calculated_r3))

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
    print("R3 PASSED")
else:
    print("R3 FAILED — investigate window semantics")
