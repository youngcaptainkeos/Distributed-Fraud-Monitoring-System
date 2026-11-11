import argparse
import csv
import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = ROOT / "data/transactions.csv"

GT_DECISIONS = (
    ROOT / "data/"
    "ground_truth_transaction_decisions.csv"
)

GT_ALERTS = (
    ROOT / "data/"
    "ground_truth_account_risk_alerts.csv"
)

REPORT_DIR = ROOT / "vm3/verifier/reports"


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_json_report(report, path):
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)

    print(f"\nReport saved to: {path}")


def check_required_columns(rows, required, filename):
    if not rows:
        return []

    available = set(rows[0].keys())
    missing = set(required) - available

    if missing:
        return [
            f"{filename}: missing columns {sorted(missing)}"
        ]

    return []


def validate_input(transactions):
    errors = []
    warnings = []

    required = [
        "event_id",
        "transaction_id",
        "source_account",
        "destination_account",
        "event_timestamp",
        "amount",
        "currency",
        "transaction_type",
    ]

    errors.extend(
        check_required_columns(
            transactions, required, "transactions.csv"
        )
    )

    if errors:
        return {
            "status": "FAILED",
            "errors": errors,
            "warnings": warnings,
        }

    groups = defaultdict(list)

    for row in transactions:
        groups[row["transaction_id"]].append(row)

    duplicate_ids = {
        txn_id: rows
        for txn_id, rows in groups.items()
        if len(rows) > 1
    }

    duplicate_conflicts = []

    for txn_id, rows in duplicate_ids.items():
        normalized = [
            {
                k: v
                for k, v in row.items()
                if k != "event_id"
            }
            for row in rows
        ]

        if not all(
            row == normalized[0]
            for row in normalized[1:]
        ):
            duplicate_conflicts.append(txn_id)

    unique_transactions = {}

    for txn_id, rows in groups.items():
        unique_transactions[txn_id] = rows[0]

    invalid_amounts = []

    for row in unique_transactions.values():
        try:
            amount = float(row["amount"])

            if amount < 0:
                invalid_amounts.append(
                    row["transaction_id"]
                )

        except (ValueError, TypeError):
            invalid_amounts.append(
                row["transaction_id"]
            )

    invalid_timestamps = []

    for row in unique_transactions.values():
        try:
            datetime.fromisoformat(
                row["event_timestamp"].replace("Z", "+00:00")
            )
        except ValueError:
            invalid_timestamps.append(
                row["transaction_id"]
            )

    if duplicate_conflicts:
        errors.append(
            f"Conflicting duplicate IDs: {duplicate_conflicts}"
        )

    if invalid_amounts:
        errors.append(
            f"Invalid transaction amounts: {invalid_amounts}"
        )

    if invalid_timestamps:
        errors.append(
            f"Invalid timestamps: {invalid_timestamps}"
        )

    currency_counts = Counter(
        row["currency"]
        for row in unique_transactions.values()
    )

    return {
        "status": "FAILED" if errors else "PASSED",
        "total_input_rows": len(transactions),
        "unique_transactions": len(unique_transactions),
        "duplicate_rows": (
            len(transactions) - len(unique_transactions)
        ),
        "duplicate_transaction_ids": list(duplicate_ids),
        "duplicate_conflicts": duplicate_conflicts,
        "invalid_amounts": invalid_amounts,
        "invalid_timestamps": invalid_timestamps,
        "currency_distribution": dict(currency_counts),
        "errors": errors,
        "warnings": warnings,
    }


def validate_ground_truth(transactions, decisions, alerts):
    errors = []

    input_ids = {
        row["transaction_id"]
        for row in transactions
    }

    decision_ids = [
        row["transaction_id"]
        for row in decisions
    ]

    decision_id_set = set(decision_ids)

    missing_ids = input_ids - decision_id_set
    extra_ids = decision_id_set - input_ids

    duplicate_decisions = [
        txn_id
        for txn_id, count in Counter(decision_ids).items()
        if count > 1
    ]

    if missing_ids:
        errors.append(
            f"Missing decisions: {sorted(missing_ids)}"
        )

    if extra_ids:
        errors.append(
            f"Unexpected decision IDs: {sorted(extra_ids)}"
        )

    if duplicate_decisions:
        errors.append(
            f"Duplicate decision IDs: {duplicate_decisions}"
        )

    decision_distribution = Counter(
        row["decision"]
        for row in decisions
    )

    rule_distribution = {}

    for rule in ["r1", "r2", "r3", "r4"]:
        rule_distribution[rule] = sum(
            row[f"{rule}_triggered"] == "true"
            for row in decisions
        )

    # Validate R5 from the four rule flags.
    r5_errors = []

    for row in decisions:
        score = sum(
            row[f"r{i}_triggered"] == "true"
            for i in range(1, 5)
        )

        expected_decision = (
            "BLOCK" if score >= 2
            else "REVIEW" if score == 1
            else "APPROVE"
        )

        expected_block = score >= 2

        if int(row["r5_risk_score"]) != score:
            r5_errors.append(
                f"{row['transaction_id']}: incorrect risk score"
            )

        if row["decision"] != expected_decision:
            r5_errors.append(
                f"{row['transaction_id']}: incorrect decision"
            )

        if (row["r5_block_triggered"] == "true") != expected_block:
            r5_errors.append(
                f"{row['transaction_id']}: incorrect block flag"
            )

    errors.extend(r5_errors)

    # Validate alert references.
    missing_supporting_ids = []

    for alert in alerts:
        try:
            supporting_ids = json.loads(
                alert["supporting_transaction_ids"]
            )

            for txn_id in supporting_ids:
                if txn_id not in decision_id_set:
                    missing_supporting_ids.append(txn_id)

        except (json.JSONDecodeError, TypeError):
            errors.append(
                f"Invalid supporting_transaction_ids: "
                f"{alert['alert_id']}"
            )

    if missing_supporting_ids:
        errors.append(
            "Alert references missing decisions: "
            f"{missing_supporting_ids}"
        )

    alert_distribution = Counter(
        row["rule_id"] for row in alerts
    )

    alert_scope_distribution = Counter(
        row["finding_scope"] for row in alerts
    )

    return {
        "status": "FAILED" if errors else "PASSED",
        "decision_count": len(decisions),
        "alert_count": len(alerts),
        "decision_distribution": dict(decision_distribution),
        "rule_trigger_distribution": rule_distribution,
        "alert_rule_distribution": dict(alert_distribution),
        "alert_scope_distribution": dict(
            alert_scope_distribution
        ),
        "missing_decisions": sorted(missing_ids),
        "extra_decisions": sorted(extra_ids),
        "duplicate_decisions": duplicate_decisions,
        "r5_consistency_errors": r5_errors,
        "missing_alert_supporting_ids": missing_supporting_ids,
        "errors": errors,
    }


def compare_decisions(actual_path, ground_truth):
    actual = read_csv(actual_path)

    actual_map = {
        row["transaction_id"]: row
        for row in actual
    }

    expected_map = {
        row["transaction_id"]: row
        for row in ground_truth
    }

    expected_ids = set(expected_map)
    actual_ids = set(actual_map)

    missing = expected_ids - actual_ids
    extra = actual_ids - expected_ids

    fields = [
        "r1_triggered",
        "r2_triggered",
        "r3_triggered",
        "r4_triggered",
        "r5_risk_score",
        "r5_block_triggered",
        "decision",
    ]

    mismatches = []

    for txn_id in expected_ids & actual_ids:
        for field in fields:
            expected_value = expected_map[txn_id].get(field)
            actual_value = actual_map[txn_id].get(field)

            if actual_value != expected_value:
                mismatches.append({
                    "transaction_id": txn_id,
                    "field": field,
                    "expected": expected_value,
                    "actual": actual_value,
                })

    return {
        "expected_count": len(expected_ids),
        "actual_count": len(actual_ids),
        "missing_count": len(missing),
        "extra_count": len(extra),
        "missing_ids": sorted(missing),
        "extra_ids": sorted(extra),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
        "status": (
            "PASSED"
            if not missing and not extra and not mismatches
            else "FAILED"
        ),
    }


def compare_alerts(actual_path, ground_truth):
    actual = read_csv(actual_path)

    # Compare alerts by their complete identifying signature.
    # Alert IDs may differ between independent implementations.
    fields = [
        "rule_id",
        "source_account",
        "destination_account",
        "trigger_transaction_id",
        "finding_scope",
        "window_start",
        "window_end",
        "observed_count",
        "observed_amount_inr",
        "supporting_transaction_ids",
    ]

    def signature(row):
        return tuple(row.get(field, "") for field in fields)

    expected_counts = Counter(
        signature(row) for row in ground_truth
    )

    actual_counts = Counter(
        signature(row) for row in actual
    )

    missing = expected_counts - actual_counts
    extra = actual_counts - expected_counts

    return {
        "expected_count": len(ground_truth),
        "actual_count": len(actual),
        "missing_alerts": sum(missing.values()),
        "extra_alerts": sum(extra.values()),
        "status": (
            "PASSED" if not missing and not extra else "FAILED"
        ),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Distributed Fraud Monitoring independent verifier"
    )

    parser.add_argument(
        "--actual-decisions",
        help="CSV exported from the actual Spark/Iceberg decision table"
    )

    parser.add_argument(
        "--actual-alerts",
        help="CSV exported from the actual Spark/Iceberg alert table"
    )

    args = parser.parse_args()

    print("\n========================================")
    print(" DISTRIBUTED FRAUD MONITORING VERIFIER")
    print("========================================")

    transactions = read_csv(INPUT_FILE)
    decisions = read_csv(GT_DECISIONS)
    alerts = read_csv(GT_ALERTS)

    print("\n[1/3] Validating transaction input...")

    input_report = validate_input(transactions)

    print("Input validation:", input_report["status"])
    print(
        "Raw rows:",
        input_report.get("total_input_rows")
    )
    print(
        "Unique transactions:",
        input_report.get("unique_transactions")
    )
    print(
        "Duplicate rows:",
        input_report.get("duplicate_rows")
    )

    print("\n[2/3] Validating supplied ground truth...")

    ground_truth_report = validate_ground_truth(
        transactions,
        decisions,
        alerts
    )

    print(
        "Ground-truth validation:",
        ground_truth_report["status"]
    )

    print(
        "Expected decisions:",
        ground_truth_report["decision_count"]
    )

    print(
        "Expected alerts:",
        ground_truth_report["alert_count"]
    )

    report = {
        "input_validation": input_report,
        "ground_truth_validation": ground_truth_report,
    }

    if args.actual_decisions:
        print("\n[3/3] Comparing actual transaction decisions...")

        decision_comparison = compare_decisions(
            args.actual_decisions,
            decisions
        )

        report["decision_comparison"] = decision_comparison

        print(
            "Decision comparison:",
            decision_comparison["status"]
        )

        print(
            "Mismatches:",
            decision_comparison["mismatch_count"]
        )

    if args.actual_alerts:
        print("\nComparing actual account-risk alerts...")

        alert_comparison = compare_alerts(
            args.actual_alerts,
            alerts
        )

        report["alert_comparison"] = alert_comparison

        print(
            "Alert comparison:",
            alert_comparison["status"]
        )

    report_path = REPORT_DIR / "verification_report.json"

    write_json_report(report, report_path)

    print("\n========================================")
    print(" VERIFICATION FINISHED")
    print("========================================")


if __name__ == "__main__":
    main()
