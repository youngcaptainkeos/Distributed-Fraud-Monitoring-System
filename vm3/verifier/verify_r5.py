import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

GROUND_TRUTH = (
    ROOT / "data/"
    "ground_truth_transaction_decisions.csv"
)

RULE_COLUMNS = [
    "r1_triggered",
    "r2_triggered",
    "r3_triggered",
    "r4_triggered",
]


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


decisions = read_csv(GROUND_TRUTH)

checked = 0
matched = 0
mismatches = []

decision_counts = {
    "APPROVE": 0,
    "REVIEW": 0,
    "BLOCK": 0,
}

for row in decisions:
    checked += 1

    # Independently calculate the number of active rules.
    active_rules = sum(
        row[column].strip().lower() == "true"
        for column in RULE_COLUMNS
    )

    # R5 decision policy.
    if active_rules >= 2:
        calculated_decision = "BLOCK"
    elif active_rules == 1:
        calculated_decision = "REVIEW"
    else:
        calculated_decision = "APPROVE"

    calculated_block = active_rules >= 2

    expected_decision = row["decision"].strip().upper()
    expected_score = int(row["r5_risk_score"])
    expected_block = (
        row["r5_block_triggered"].strip().lower() == "true"
    )

    errors = []

    if active_rules != expected_score:
        errors.append(
            f"risk score: expected {expected_score}, "
            f"calculated {active_rules}"
        )

    if calculated_block != expected_block:
        errors.append(
            f"block flag: expected {expected_block}, "
            f"calculated {calculated_block}"
        )

    if calculated_decision != expected_decision:
        errors.append(
            f"decision: expected {expected_decision}, "
            f"calculated {calculated_decision}"
        )

    decision_counts[calculated_decision] += 1

    if errors:
        mismatches.append({
            "transaction_id": row["transaction_id"],
            "errors": errors,
        })
    else:
        matched += 1


print("\n========== R5 INDEPENDENT VERIFICATION ==========")

print("Transactions checked:", checked)
print("Matched:", matched)
print("Mismatches:", len(mismatches))

print("\nCalculated decision distribution:")

for decision, count in decision_counts.items():
    print(f"{decision}: {count}")

print("\n========== MISMATCH EXAMPLES ==========")

for mismatch in mismatches[:15]:
    print(mismatch["transaction_id"])
    for error in mismatch["errors"]:
        print("  -", error)

print("\n========== RESULT ==========")

if not mismatches:
    print("R5 PASSED")
else:
    print("R5 FAILED")
