#!/usr/bin/env python3
"""Fraud-monitor producer (VM1). One Kafka message = one CSV row as JSON, key = transaction_id.
Modes: baseline (file order) | late_lt60 | late_gt60 (event-time-lag disorder injection).
The file is NEVER sorted. event_timestamp is passed through untouched."""
import argparse, csv, hashlib, json, os, random, socket, sys, time
from datetime import datetime, timezone
import pandas as pd

def utcnow(): return datetime.now(timezone.utc)
def iso(t): return t.isoformat(timespec='milliseconds') if t is not None and not pd.isna(t) else ''

def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''): h.update(b)
    return h.hexdigest()

def main():
    a = argparse.ArgumentParser()
    a.add_argument('--input', default=os.getenv('INPUT_CSV'))
    a.add_argument('--mode', choices=['baseline', 'late_lt60', 'late_gt60'], default='baseline')
    a.add_argument('--run-id', default=os.getenv('RUN_ID', 'run1'))
    a.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP'))
    a.add_argument('--topic', default=os.getenv('KAFKA_TOPIC'))
    a.add_argument('--send-delay', type=float, default=float(os.getenv('SEND_DELAY_SECONDS', '0.05')))
    a.add_argument('--late-seconds', type=float, default=None, help='event-time lag to aim for (default 30 for lt60, 90 for gt60)')
    a.add_argument('--fraction', type=float, default=0.02, help='share of rows selected for delay')
    a.add_argument('--seed', type=int, default=42)
    a.add_argument('--max-buffer', type=int, default=2000)
    a.add_argument('--min-wall-hold', type=float, default=0.0, help='held rows wait at least this many wall seconds (stalls the stream)')
    a.add_argument('--limit', type=int, default=0)
    a.add_argument('--log-dir', default=os.path.expanduser('~/fraud/logs'))
    a.add_argument('--dry-run', action='store_true', help='no Kafka, no sleeping; only write logs')
    a.add_argument('--allow-nonempty', action='store_true')
    o = a.parse_args()
    if not o.input: sys.exit('--input (or INPUT_CSV) required')

    df = pd.read_csv(o.input, dtype=str, keep_default_na=False)
    if o.limit: df = df.head(o.limit)
    ts = pd.to_datetime(df['event_timestamp'], utc=True, errors='coerce')   # used for logging/disorder only
    if o.mode != 'baseline' and ts.isna().any():
        sys.exit(f'{int(ts.isna().sum())} unparseable event_timestamp values; disorder modes need all parseable')
    target, cap = None, None
    if o.mode == 'late_lt60':
        target, cap = (o.late_seconds or 30.0), 59.0
        if not 0 < target < 60: sys.exit('late_lt60 needs 0 < --late-seconds < 60')
    elif o.mode == 'late_gt60':
        target = o.late_seconds or 90.0
        if target <= 60: sys.exit('late_gt60 needs --late-seconds > 60')

    producer = None
    if not o.dry_run:
        if not (o.bootstrap and o.topic): sys.exit('bootstrap and topic required')
        from kafka import KafkaProducer, KafkaConsumer, TopicPartition
        c = KafkaConsumer(bootstrap_servers=o.bootstrap)
        parts = c.partitions_for_topic(o.topic)
        if not parts: sys.exit(f'topic {o.topic} does not exist (create it first)')
        end = c.end_offsets([TopicPartition(o.topic, p) for p in parts]); c.close()
        if sum(end.values()) > 0 and not o.allow_nonempty:
            sys.exit(f'topic {o.topic} already has {sum(end.values())} messages; use a fresh topic')
        producer = KafkaProducer(bootstrap_servers=o.bootstrap,
                                 key_serializer=lambda x: x.encode(),
                                 value_serializer=lambda x: json.dumps(x).encode(),
                                 acks='all', retries=10, max_in_flight_requests_per_connection=1)
    ok, errs = [], []
    rng = random.Random(o.seed)
    rows = df.to_dict(orient='records')
    os.makedirs(o.log_dir, exist_ok=True)
    stem = os.path.join(o.log_dir, f'{o.run_id}_{o.mode}')
    start = utcnow()
    held, state, log = [], {'max': None}, []
    stats = dict(behind=0, behind_gt60=0, inj=0, inj_behind=0, inj_gt60=0)

    def emit(i, injected, scanned):
        r = rows[i]; t = ts.iloc[i]; now = utcnow()
        mx = state['max']
        lag = (mx - t).total_seconds() if (mx is not None and not pd.isna(t)) else None
        if lag is not None and lag > 0:
            stats['behind'] += 1; stats['behind_gt60'] += lag > 60
            if injected: stats['inj_behind'] += 1; stats['inj_gt60'] += lag > 60
        if injected: stats['inj'] += 1
        log.append([len(log), i, r['transaction_id'], r.get('event_id', ''), r['event_timestamp'], int(injected),
                    iso(now), iso(scanned), round((now - scanned).total_seconds(), 3), iso(mx),
                    '' if lag is None else round(lag, 3)])
        if producer:
            f = producer.send(o.topic, key=r['transaction_id'], value=r)
            f.add_callback(lambda m: ok.append(1)); f.add_errback(lambda e: errs.append(repr(e)))
        if not pd.isna(t) and (mx is None or t > mx): state['max'] = t
        if len(log) % 500 == 0: print(f'sent {len(log)}/{len(rows)}', flush=True)
        if not o.dry_run: time.sleep(o.send_delay)

    for i, r in enumerate(rows):
        t = ts.iloc[i]; scanned = utcnow()
        if target is not None:
            new_max = t if state['max'] is None or t > state['max'] else state['max']
            rel = [h for h in held if (state['max'] is not None and (state['max'] - h[1]).total_seconds() >= target)
                   or (cap and (new_max - h[1]).total_seconds() > cap)]
            while len(held) - len(rel) > o.max_buffer and held:
                rel.append([h for h in held if h not in rel][0])
            for h in rel:
                held.remove(h)
                if o.min_wall_hold: time.sleep(max(0.0, o.min_wall_hold - (utcnow() - h[2]).total_seconds()))
                emit(h[0], True, h[2])
            if rng.random() < o.fraction:
                held.append((i, t, scanned)); continue
        emit(i, False, scanned)
    for h in list(held):
        if o.min_wall_hold: time.sleep(max(0.0, o.min_wall_hold - (utcnow() - h[2]).total_seconds()))
        emit(h[0], True, h[2])          # end of file: flush whatever is still held

    if producer:
        producer.flush(); producer.close()
    end_t = utcnow()
    with open(stem + '_arrival.csv', 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['send_order', 'file_row', 'transaction_id', 'event_id', 'event_timestamp', 'injected_delay',
                    'arrival_utc', 'scanned_utc', 'wall_delay_s', 'max_event_time_sent_before', 'event_lag_s'])
        w.writerows(log)
    summary = dict(run_id=o.run_id, mode=o.mode, topic=o.topic, bootstrap=o.bootstrap, input=o.input,
                   input_sha256=sha256(o.input), host=socket.gethostname(), dry_run=o.dry_run,
                   rows_in_file=len(rows), unique_transaction_ids=int(df.transaction_id.nunique()),
                   messages_sent=len(log), broker_acks=len(ok), send_errors=errs[:5], n_errors=len(errs),
                   seed=o.seed, fraction=o.fraction, target_event_lag_s=target, hard_cap_s=cap,
                   send_delay_s=o.send_delay, min_wall_hold_s=o.min_wall_hold, injected=stats['inj'], injected_actually_late=stats['inj_behind'],
                   injected_lag_gt_60s=stats['inj_gt60'], all_rows_behind_running_max=stats['behind'],
                   all_rows_lag_gt_60s=stats['behind_gt60'], start_utc=iso(start), end_utc=iso(end_t),
                   duration_s=round((end_t - start).total_seconds(), 1))
    json.dump(summary, open(stem + '_summary.json', 'w'), indent=2)
    print(json.dumps(summary, indent=2))
    if errs or (producer and len(ok) != len(log)): sys.exit('SEND ERRORS - do not trust this run')

if __name__ == '__main__': main()
