from pathlib import Path
import csv
from collections import defaultdict
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[2] / "data"

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


transactions = read_csv(
    ROOT / "transactions.csv"
)

decisions = read_csv(
    ROOT / "ground_truth_transaction_decisions.csv"
)

# Deduplicate by transaction_id.
unique = {}

for row in transactions:
    unique.setdefault(row["transaction_id"], row)

expected = {
    row["transaction_id"]
    for row in decisions
    if row["r4_triggered"] == "true"
}


def parse_timestamp(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


def calculate_r4(payment_only=False):
    groups = defaultdict(list)

    for txn_id, row in unique.items():

        if payment_only and row["transaction_type"] != "PAYMENT":
            continue

        key = (
            row["source_account"],
            row["destination_account"]
        )

        groups[key].append({
            "transaction_id": txn_id,
            "timestamp": parse_timestamp(row["event_timestamp"])
        })

    calculated = set()
    qualifying_windows = 0

    for key, events in groups.items():

        events.sort(key=lambda x: x["timestamp"])
        possible_starts = set()

        for event in events:
            epoch = int(event["timestamp"].timestamp())
            latest_start = (epoch // 30) * 30

            for i in range(4):
                possible_starts.add(latest_start - i * 30)

        for start_epoch in possible_starts:
            start = datetime.fromtimestamp(
                start_epoch, tz=timezone.utc
            )
            end = start + timedelta(seconds=120)

            members = [
                event for event in events
                if start <= event["timestamp"] < end
            ]

            if len(members) >= 3:
                qualifying_windows += 1

                calculated.update(
                    event["transaction_id"]
                    for event in members
                )

    matched = calculated & expected
    fp = calculated - expected
    fn = expected - calculated

    print("\nPayment only:" if payment_only else "\nAll transaction types:")

    print("Qualifying windows:", qualifying_windows)
    print("Calculated transactions:", len(calculated))
    print("Expected transactions:", len(expected))
    print("Matched:", len(matched))
    print("False positives:", len(fp))
    print("False negatives:", len(fn))
    print("False-positive examples:", sorted(fp)[:10])
    print("False-negative examples:", sorted(fn)[:10])


print("========== R4 DIAGNOSTIC ==========")
print("Unique input transactions:", len(unique))
print("Expected R4 transactions:", len(expected))

calculate_r4(payment_only=False)
calculate_r4(payment_only=True)
