"""Simulate the stream as micro-batches and compare with the ground truth.
Run from vm2/:   python tests/sim_batches.py
Design A = current job (each batch evaluated alone). Design B = stored events + new batch, then MERGE.
late=3 means the last 3 rows of every batch (~40 s of event time) arrive one batch later (< 1 minute late)."""
import json, os, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from app.rules import evaluate_batch

D = os.getenv("DATA_DIR", os.path.join(HERE, "..", "..", "data"))
rd = lambda f: pd.read_csv(os.path.join(D, f), dtype=str, keep_default_na=False)
tx = rd("transactions.csv").drop_duplicates("transaction_id")
tx = tx.astype({"source_account": "int64", "destination_account": "int64", "amount": "float64"})
tx = tx[["transaction_id", "source_account", "destination_account", "event_timestamp", "amount", "currency"]].reset_index(drop=True)
gt_dec = rd("ground_truth_transaction_decisions.csv").set_index("transaction_id")
gt_alerts = set(rd("ground_truth_account_risk_alerts.csv").alert_id)
COLS = ["r2_triggered", "r3_triggered", "r4_triggered", "r5_risk_score", "decision"]


def make_batches(n, late=0):
    chunks, held = [], tx.iloc[0:0]
    for i in range(0, len(tx), n):
        c = tx.iloc[i:i + n]
        if late:
            chunks.append(pd.concat([held, c.iloc[:-late]]))
            held = c.iloc[-late:]
        else:
            chunks.append(c)
    if len(held):
        chunks.append(held)
    return chunks


def design_a(chunks):
    dec, al = {}, {}
    for b in chunks:
        d, a = evaluate_batch(b)
        dec.update({r["transaction_id"]: r for r in d.to_dict("records")})
        al.update({r["alert_id"]: r for r in a.to_dict("records")})
    return dec, al


def design_b(chunks):
    store, dec, al = tx.iloc[0:0], {}, {}
    for b in chunks:
        store = pd.concat([store, b]).drop_duplicates("transaction_id")
        d, a = evaluate_batch(store)
        dec.update({r["transaction_id"]: r for r in d.to_dict("records")})
        al.update({r["alert_id"]: r for r in a.to_dict("records")})
    return dec, al


def score(dec, al):
    wrong = 0
    for k, g in gt_dec.iterrows():
        r = dec.get(k)
        ok = r is not None and all(str(r[c]).lower() == str(g[c]).lower() for c in COLS) \
            and sorted(json.loads(r["related_alert_ids"])) == sorted(json.loads(g["related_alert_ids"]))
        wrong += not ok
    return wrong, len(gt_alerts - set(al)), len(set(al) - gt_alerts)


if __name__ == "__main__":
    print("rows per batch | late | design | wrong decisions (of 1990) | alerts missing / extra (of 129)")
    for n in (100, 200, 400):
        for late in (0, 3):
            for name, fn in (("A current", design_a), ("B stored+new", design_b)):
                w, m, x = score(*fn(make_batches(n, late)))
                print(f"{n:14d} | {late:4d} | {name:12s} | {w:25d} | {m} / {x}")
