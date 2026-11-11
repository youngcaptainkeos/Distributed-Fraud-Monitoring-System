import csv
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

INPUT = ROOT / "data/transactions.csv"
GROUND_TRUTH = (
    ROOT / "data/"
    "ground_truth_transaction_decisions.csv"
)

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


transactions = read_csv(INPUT)
decisions = read_csv(GROUND_TRUTH)

# Deduplicate using transaction_id
unique_transactions = {}

for row in transactions:
    unique_transactions.setdefault(
        row["transaction_id"], row
    )

# Independently calculate R1
calculated = {}

for txn_id, row in unique_transactions.items():
    amount = Decimal(row["amount"])
    currency = row["currency"]

    calculated[txn_id] = (
        currency == "INR" and amount >= Decimal("50000")
    )

# Compare against ground truth
expected = {
    row["transaction_id"]: row["r1_triggered"] == "true"
    for row in decisions
}

matched = 0
mismatches = []

for txn_id, expected_value in expected.items():
    actual_value = calculated.get(txn_id)

    if actual_value == expected_value:
        matched += 1
    else:
        mismatches.append({
            "transaction_id": txn_id,
            "expected": expected_value,
            "actual": actual_value
        })

print("\n========== R1 VERIFICATION ==========")
print("Unique transactions:", len(calculated))
print("Ground-truth records:", len(expected))
print("Expected R1 triggers:", sum(expected.values()))
print("Calculated R1 triggers:", sum(calculated.values()))
print("Matched:", matched)
print("Mismatches:", len(mismatches))

if mismatches:
    print("\nFirst 20 mismatches:")
    for item in mismatches[:20]:
        print(item)

print("\nRESULT:", "PASSED" if not mismatches else "FAILED")
