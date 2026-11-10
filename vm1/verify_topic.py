#!/usr/bin/env python3
"""Compare a Kafka topic with the source CSV.
Usage: python verify_topic.py --topic transactions-run1 [--expect-order]"""
import argparse, json, os, sys
from collections import Counter
import pandas as pd

def compare(expected, got):
    """expected: CSV rows (dicts, file order). got: [(key, value_dict)] in topic order."""
    k = lambda d: json.dumps(d, sort_keys=True)
    ce, cg = Counter(map(k, expected)), Counter(k(v) for _, v in got)
    return {
        'csv_rows': len(expected), 'topic_messages': len(got),
        'missing_from_topic': sum((ce - cg).values()), 'extra_in_topic': sum((cg - ce).values()),
        'same_content_ignoring_order': ce == cg,
        'same_order_as_file': [v for _, v in got] == expected,
        'key_equals_transaction_id': all(key == v.get('transaction_id') for key, v in got),
        'duplicate_ids_kept': (len(expected) - len({e['transaction_id'] for e in expected}))
                              == (len(got) - len({key for key, _ in got})),
        'all_values_are_strings': all(isinstance(x, str) for _, v in got for x in v.values()),
    }

def read_topic(bootstrap, topic):
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=bootstrap, group_id=None, enable_auto_commit=False)
    parts = c.partitions_for_topic(topic)
    if not parts: sys.exit(f'topic {topic} does not exist')
    tps = [TopicPartition(topic, p) for p in sorted(parts)]
    c.assign(tps); c.seek_to_beginning(*tps)
    end = c.end_offsets(tps); got, idle = [], 0
    while any(c.position(tp) < end[tp] for tp in tps) and idle < 20:
        batch = c.poll(timeout_ms=1000); idle = idle + 1 if not batch else 0
        for recs in batch.values():
            for m in recs: got.append((m.key.decode(), json.loads(m.value.decode())))
    c.close(); return got

if __name__ == '__main__':
    a = argparse.ArgumentParser()
    a.add_argument('--topic', default=os.getenv('KAFKA_TOPIC'))
    a.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP'))
    a.add_argument('--input', default=os.getenv('INPUT_CSV'))
    a.add_argument('--expect-order', action='store_true', help='also require the exact file order (baseline)')
    o = a.parse_args()
    expected = pd.read_csv(o.input, dtype=str, keep_default_na=False).to_dict(orient='records')
    r = compare(expected, read_topic(o.bootstrap, o.topic))
    for key, val in r.items(): print(f'{key:32s} {val}')
    ok = (r['csv_rows'] == r['topic_messages'] and r['missing_from_topic'] == 0 and r['extra_in_topic'] == 0
          and r['same_content_ignoring_order'] and r['key_equals_transaction_id']
          and r['duplicate_ids_kept'] and r['all_values_are_strings'])
    if o.expect_order: ok = ok and r['same_order_as_file']
    print('RESULT:', 'PASS' if ok else 'FAIL'); sys.exit(0 if ok else 1)
