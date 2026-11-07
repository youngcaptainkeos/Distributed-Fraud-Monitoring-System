"""Unit tests verifying rule engine implementation against ground truth data.

Validates that:
1. All 1,990 transaction decisions match ground truth (100% accuracy).
2. All 129 account risk alerts match ground truth (100% accuracy).
3. R1, R2, R3, R4, and R5 composite scores match perfectly.
"""

import json
import os
import unittest
import pandas as pd

from app.rules import evaluate_batch


class TestFraudRules(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Locate project data with robust fallback paths
        possible_dirs = [
            os.getenv("DATA_DIR", ""),
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data")),
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data")),
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")),
        ]
        data_dir = next((d for d in possible_dirs if d and os.path.isdir(d)), "")
        if not data_dir:
            raise FileNotFoundError("Could not locate data directory containing transactions.csv")
        
        tx_path = os.path.join(data_dir, "transactions.csv")
        gt_alerts_path = os.path.join(data_dir, "ground_truth_account_risk_alerts.csv")
        gt_dec_path = os.path.join(data_dir, "ground_truth_transaction_decisions.csv")

        cls.df_tx = pd.read_csv(tx_path).drop_duplicates(subset=["transaction_id"])
        cls.df_gt_alerts = pd.read_csv(gt_alerts_path)
        cls.df_gt_dec = pd.read_csv(gt_dec_path)

        # Run evaluation
        cls.df_dec, cls.df_alerts = evaluate_batch(cls.df_tx)

    def test_transaction_count_and_uniqueness(self):
        """Verifies deduplicated transactions match 1,990 unique records."""
        self.assertEqual(len(self.df_dec), 1990)
        self.assertEqual(self.df_dec["transaction_id"].nunique(), 1990)

    def test_alert_count(self):
        """Verifies alert count matches exact 129 ground truth alerts."""
        self.assertEqual(len(self.df_alerts), 129)
        self.assertEqual(len(self.df_alerts[self.df_alerts["rule_id"] == "R2"]), 90)
        self.assertEqual(len(self.df_alerts[self.df_alerts["rule_id"] == "R3"]), 14)
        self.assertEqual(len(self.df_alerts[self.df_alerts["rule_id"] == "R4"]), 25)

    def test_alert_ids_match_ground_truth(self):
        """Verifies all alert IDs match ground truth exactly."""
        gen_ids = set(self.df_alerts["alert_id"])
        gt_ids = set(self.df_gt_alerts["alert_id"])
        self.assertEqual(gen_ids, gt_ids)

    def test_decisions_distribution(self):
        """Verifies decision distribution matches 1659 APPROVE, 231 REVIEW, 100 BLOCK."""
        counts = self.df_dec["decision"].value_counts().to_dict()
        self.assertEqual(counts.get("APPROVE", 0), 1659)
        self.assertEqual(counts.get("REVIEW", 0), 231)
        self.assertEqual(counts.get("BLOCK", 0), 100)

    def test_individual_transaction_decisions_and_scores(self):
        """Verifies every single transaction decision, score, and rule flags match GT."""
        gt_map = {row["transaction_id"]: row for _, row in self.df_gt_dec.iterrows()}

        for _, row in self.df_dec.iterrows():
            tid = row["transaction_id"]
            gt = gt_map[tid]

            self.assertEqual(row["r1_triggered"], bool(gt["r1_triggered"]), f"R1 mismatch on {tid}")
            self.assertEqual(row["r2_triggered"], bool(gt["r2_triggered"]), f"R2 mismatch on {tid}")
            self.assertEqual(row["r3_triggered"], bool(gt["r3_triggered"]), f"R3 mismatch on {tid}")
            self.assertEqual(row["r4_triggered"], bool(gt["r4_triggered"]), f"R4 mismatch on {tid}")
            self.assertEqual(row["r5_risk_score"], int(gt["r5_risk_score"]), f"R5 score mismatch on {tid}")
            self.assertEqual(row["r5_block_triggered"], bool(gt["r5_block_triggered"]), f"R5 block mismatch on {tid}")
            self.assertEqual(row["decision"], gt["decision"], f"Decision mismatch on {tid}")

            gen_related = sorted(json.loads(row["related_alert_ids"]))
            gt_related = sorted(json.loads(gt["related_alert_ids"]))
            self.assertEqual(gen_related, gt_related, f"Related alerts mismatch on {tid}")

    def test_alert_triggers_and_supporting_txns(self):
        """Verifies alert trigger transaction IDs and supporting transactions match GT."""
        gt_alert_map = {row["alert_id"]: row for _, row in self.df_gt_alerts.iterrows()}

        for _, row in self.df_alerts.iterrows():
            aid = row["alert_id"]
            gt = gt_alert_map[aid]

            self.assertEqual(row["trigger_transaction_id"], gt["trigger_transaction_id"])
            self.assertEqual(row["finding_scope"], gt["finding_scope"])
            self.assertEqual(row["observed_count"], int(gt["observed_count"]))
            self.assertEqual(row["explanation"], gt["explanation"])

            gen_supp = json.loads(row["supporting_transaction_ids"])
            gt_supp = json.loads(gt["supporting_transaction_ids"])
            self.assertEqual(gen_supp, gt_supp, f"Supporting txns mismatch on {aid}")


if __name__ == "__main__":
    unittest.main()
