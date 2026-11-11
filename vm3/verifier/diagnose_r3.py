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
MIN_AMOUNT = Decimal("600000")


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_timestamp(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


raw = read_csv(INPUT)
decisions = read_csv(GROUND_TRUTH)

unique = {}
for row in raw:
    unique.setdefault(row["transaction_id"], row)

expected = {
    row["transaction_id"]
    for row in decisions
    if row["r3_triggered"] == "true"
}

print("Expected R3 transactions:", len(expected))


def calculate_r3(payment_only):
    accounts = defaultdict(list)

    for txn_id, row in unique.items():

        if row["currency"] != "INR":
            continue

        if payment_only and row["transaction_type"] != "PAYMENT":
            continue

        accounts[row["source_account"]].append({
            "id": txn_id,
            "timestamp": parse_timestamp(row["event_timestamp"]),
            "amount": Decimal(row["amount"]),
        })

    triggered = set()
    qualifying_windows = 0

    for account, events in accounts.items():

        possible_starts = set()

        for event in events:
            epoch = int(event["timestamp"].timestamp())
            latest = (epoch // SLIDE_SECONDS) * SLIDE_SECONDS

            for i in range(WINDOW_SECONDS // SLIDE_SECONDS):
                possible_starts.add(
                    latest - i * SLIDE_SECONDS
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

            count = len(members)
            total = sum(
                (event["amount"] for event in members),
                Decimal("0")
            )

            if count >= MIN_COUNT and total > MIN_AMOUNT:
                qualifying_windows += 1
                triggered.update(
                    event["id"] for event in members
                )

    matched = triggered & expected
    fp = triggered - expected
    fn = expected - triggered

    return {
        "triggered": triggered,
        "windows": qualifying_windows,
        "matched": len(matched),
        "fp": len(fp),
        "fn": len(fn),
        "fp_examples": sorted(fp)[:10],
        "fn_examples": sorted(fn)[:10],
    }


for payment_only in (False, True):

    label = (
        "PAYMENT transactions only"
        if payment_only
        else "All INR transaction types"
    )

    result = calculate_r3(payment_only)

    print("\n==========", label, "==========")
    print("Qualifying windows:", result["windows"])
    print("Calculated transactions:", len(result["triggered"]))
    print("Matched:", result["matched"])
    print("False positives:", result["fp"])
    print("False negatives:", result["fn"])
    print("False-positive examples:", result["fp_examples"])
    print("False-negative examples:", result["fn_examples"])
