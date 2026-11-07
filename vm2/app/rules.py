"""Fraud detection rule engine (R1–R5).

Implements the exact business logic and mathematical criteria for:
- R1: Single INR payment of >= 50,000 (Stateless)
- R2: >= 5 payments from one account in rolling 2-min window (sliding every 30s)
- R3: >= 2 INR payments from one account totalling > 600,000 in rolling 2-min window
- R4: >= 3 payments from one account to the same destination in rolling 2-min window
- R5: Composite decision (score >= 2: BLOCK, score == 1: REVIEW, 0: APPROVE)

Window Interval Semantics:
- Windows are half-open: [window_start, window_end).
- An account alert active in [window_start, window_end) is linked to all
  transactions of that source account whose event_timestamp falls in that window.
"""

from datetime import datetime, timedelta, timezone
import json
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd


def compute_windows_for_timestamp(ts: pd.Timestamp) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Computes all 2-minute rolling windows (30s slide) containing event timestamp ts.
    
    In Spark SQL, sliding windows are aligned to 00s and 30s.
    A 120s window sliding every 30s covers 4 overlapping windows for any given instant.
    Interval is half-open: [w_start, w_end).
    """
    # Align to 30-second boundary
    base_seconds = (ts.minute * 60 + ts.second) % 30
    floor_ts = ts - timedelta(seconds=base_seconds, microseconds=ts.microsecond)

    windows = []
    for offset_seconds in (0, 30, 60, 90):
        w_start = floor_ts - timedelta(seconds=offset_seconds)
        w_end = w_start + timedelta(seconds=120)
        windows.append((w_start, w_end))
    return windows


def evaluate_batch(
    df_transactions: pd.DataFrame,
    evaluated_at: Optional[datetime] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluates a batch of transactions against fraud rules R1–R5.

    Args:
        df_transactions: pandas DataFrame containing unique transactions with columns:
            transaction_id, source_account, destination_account,
            event_timestamp, amount, currency
        evaluated_at: Optional timestamp of evaluation (defaults to utcnow)

    Returns:
        Tuple of (df_decisions, df_alerts):
            df_decisions: 1 row per unique transaction_id
            df_alerts: 1 row per triggered account risk alert (R2, R3, R4)
    """
    if df_transactions.empty:
        empty_decisions = pd.DataFrame(columns=[
            "transaction_id", "source_account", "destination_account",
            "event_timestamp", "evaluated_at", "amount", "currency",
            "r1_triggered", "r2_triggered", "r3_triggered", "r4_triggered",
            "r5_risk_score", "r5_block_triggered", "decision", "related_alert_ids"
        ])
        empty_alerts = pd.DataFrame(columns=[
            "alert_id", "rule_id", "source_account", "destination_account",
            "trigger_transaction_id", "finding_scope", "window_start",
            "window_end", "observed_count", "observed_amount_inr",
            "count_threshold", "amount_threshold_inr",
            "supporting_transaction_ids", "explanation", "triggered_at"
        ])
        return empty_decisions, empty_alerts

    eval_time = evaluated_at or datetime.now(timezone.utc).replace(tzinfo=None)
    df = df_transactions.copy()
    df["ts"] = pd.to_datetime(df["event_timestamp"])
    if df["ts"].dt.tz is not None:
        df["ts"] = df["ts"].dt.tz_localize(None)

    # 1. Expand transactions into participating 2-minute rolling windows (30s slide)
    window_records = []
    for _, row in df.iterrows():
        ts = row["ts"]
        windows = compute_windows_for_timestamp(ts)
        for w_start, w_end in windows:
            window_records.append({
                "transaction_id": row["transaction_id"],
                "source_account": row["source_account"],
                "destination_account": row["destination_account"],
                "amount": float(row["amount"]),
                "currency": str(row["currency"]).strip().upper(),
                "ts": ts,
                "w_start": w_start,
                "w_end": w_end,
            })

    df_win = pd.DataFrame(window_records)

    # 2. Evaluate Rule R2 & R3 by grouping on (source_account, w_start, w_end)
    alerts_list = []
    r4_candidates = []

    for (acc, w_start, w_end), group in df_win.groupby(["source_account", "w_start", "w_end"]):
        sorted_group = group.sort_values(by=["ts", "transaction_id"])
        w_start_str = w_start.strftime("%Y-%m-%d %H:%M:%S")
        w_end_str = w_end.strftime("%Y-%m-%d %H:%M:%S")
        w_compact = w_start.strftime("%Y%m%d%H%M%S")

        # R2: >= 5 payments from one account
        obs_count_r2 = len(sorted_group)
        if obs_count_r2 >= 5:
            supp_r2 = sorted_group["transaction_id"].tolist()
            alert_id_r2 = f"R2-{acc}-{w_compact}"
            alerts_list.append({
                "alert_id": alert_id_r2,
                "rule_id": "R2",
                "source_account": acc,
                "destination_account": None,
                "trigger_transaction_id": supp_r2[-1],
                "finding_scope": "WINDOW",
                "window_start": w_start,
                "window_end": w_end,
                "observed_count": obs_count_r2,
                "observed_amount_inr": None,
                "count_threshold": 5,
                "amount_threshold_inr": None,
                "supporting_transaction_ids": json.dumps(supp_r2),
                "explanation": f"Account made {obs_count_r2} transactions in a 120s window.",
                "triggered_at": eval_time
            })

        # R3: >= 2 INR payments totalling > 600,000 INR
        inr_group = sorted_group[sorted_group["currency"] == "INR"]
        obs_count_r3 = len(inr_group)
        obs_amount_r3 = inr_group["amount"].sum()
        if obs_count_r3 >= 2 and obs_amount_r3 > 600000.0:
            supp_r3 = inr_group["transaction_id"].tolist()
            alert_id_r3 = f"R3-{acc}-{w_compact}"
            alerts_list.append({
                "alert_id": alert_id_r3,
                "rule_id": "R3",
                "source_account": acc,
                "destination_account": None,
                "trigger_transaction_id": supp_r3[-1],
                "finding_scope": "WINDOW",
                "window_start": w_start,
                "window_end": w_end,
                "observed_count": obs_count_r3,
                "observed_amount_inr": float(obs_amount_r3),
                "count_threshold": 2,
                "amount_threshold_inr": 600000.0,
                "supporting_transaction_ids": json.dumps(supp_r3),
                "explanation": f"Account made {obs_count_r3} INR transactions totalling INR {obs_amount_r3:.2f} in a 120s window.",
                "triggered_at": eval_time
            })

    # 3. Evaluate Rule R4 by grouping on (source_account, destination_account, w_start, w_end)
    for (acc, dest, w_start, w_end), group in df_win.groupby(["source_account", "destination_account", "w_start", "w_end"]):
        obs_count_r4 = len(group)
        if obs_count_r4 >= 3:
            sorted_group = group.sort_values(by=["ts", "transaction_id"])
            supp_r4 = sorted_group["transaction_id"].tolist()
            w_compact = w_start.strftime("%Y%m%d%H%M%S")
            alert_id_r4 = f"R4-{acc}-{w_compact}"
            r4_candidates.append({
                "alert_id": alert_id_r4,
                "rule_id": "R4",
                "source_account": acc,
                "destination_account": dest,
                "trigger_transaction_id": supp_r4[-1],
                "finding_scope": "PATTERN",
                "window_start": w_start,
                "window_end": w_end,
                "observed_count": obs_count_r4,
                "observed_amount_inr": None,
                "count_threshold": 3,
                "amount_threshold_inr": None,
                "supporting_transaction_ids": json.dumps(supp_r4),
                "explanation": f"Account sent {obs_count_r4} transactions to one destination in a 120s window.",
                "triggered_at": eval_time
            })

    # Deduplicate R4 candidates: exactly 1 alert row per (account, window, rule)
    # If multiple destinations exceed threshold in same window, retain highest observed_count
    if r4_candidates:
        df_r4 = pd.DataFrame(r4_candidates)
        df_r4 = df_r4.sort_values(by=["observed_count"], ascending=False).drop_duplicates(subset=["alert_id"])
        alerts_list.extend(df_r4.to_dict(orient="records"))

    # Convert alerts to DataFrame
    df_alerts = pd.DataFrame(alerts_list)
    if not df_alerts.empty:
        df_alerts = df_alerts.sort_values(by=["window_start", "alert_id"]).reset_index(drop=True)

    # 4. Correlate transactions to triggered alerts and compute R1 & R5 decisions
    decisions = []
    for _, tx in df.iterrows():
        tid = tx["transaction_id"]
        acc = tx["source_account"]
        ts = tx["ts"]
        amt = float(tx["amount"])
        curr = str(tx["currency"]).strip().upper()

        # Find all active alerts for this source account where w_start <= ts < w_end
        if not df_alerts.empty:
            matched_alerts = df_alerts[
                (df_alerts["source_account"] == acc) &
                (df_alerts["window_start"] <= ts) &
                (ts < df_alerts["window_end"])
            ]
            matched_alert_ids = sorted(list(set(matched_alerts["alert_id"].tolist())))
            active_rules = set(matched_alerts["rule_id"].tolist())
        else:
            matched_alert_ids = []
            active_rules = set()

        # R1: Single INR payment of >= 50,000
        r1_trig = bool((curr == "INR") and (amt >= 50000.0))
        r2_trig = bool("R2" in active_rules)
        r3_trig = bool("R3" in active_rules)
        r4_trig = bool("R4" in active_rules)

        # R5: Composite risk score
        risk_score = int(r1_trig) + int(r2_trig) + int(r3_trig) + int(r4_trig)
        block_trig = bool(risk_score >= 2)

        if risk_score >= 2:
            decision = "BLOCK"
        elif risk_score == 1:
            decision = "REVIEW"
        else:
            decision = "APPROVE"

        decisions.append({
            "transaction_id": tid,
            "source_account": acc,
            "destination_account": tx["destination_account"],
            "event_timestamp": ts,
            "evaluated_at": eval_time,
            "amount": amt,
            "currency": curr,
            "r1_triggered": r1_trig,
            "r2_triggered": r2_trig,
            "r3_triggered": r3_trig,
            "r4_triggered": r4_trig,
            "r5_risk_score": risk_score,
            "r5_block_triggered": block_trig,
            "decision": decision,
            "related_alert_ids": json.dumps(matched_alert_ids),
        })

    df_decisions = pd.DataFrame(decisions).reset_index(drop=True)
    return df_decisions, df_alerts
