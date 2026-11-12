# Saatwik demo sheet

Your machine is `172.23.211.163`. You run Kafka and send payments.
Run these on your machine, from your repository checkout, with your existing
Python environment active. The next fresh baseline is `run6`.

## Prepare the topic

For the current guided `run6` attempt, the topic has already been created:
skip creation and go straight to sending. The creation command below is for
a future fresh demonstration with a newly agreed run ID.

Keep the existing Kafka broker running. If it is stopped, use the broker
installation and configuration from the existing setup; do not format storage.

```bash
source ~/fraud/configs/cluster.env
export KAFKA_BOOTSTRAP=172.23.211.163:9092
export RUN_ID=run6
export KAFKA_TOPIC=transactions-run6
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --topic "$KAFKA_TOPIC" --partitions 1 --replication-factor 1
```

If the topic exists from an earlier attempt, agree with the others whether to
resume that attempt or choose a fresh run ID. Do not use `--allow-nonempty`.

## Send when Shashank has started the application

From the repository root:

```bash
python3 vm1/producer.py --mode baseline \
  --bootstrap 172.23.211.163:9092 --topic transactions-run6 --run-id run6 \
  --input data/transactions.csv --log-dir "$HOME/fraud/logs"
```

Let it finish once. Expected summary: 2,000 messages, 2,000 broker acknowledgements
and zero send errors. Keep the arrival log and summary. Do not replay the producer
when Shashank restarts a job using the same checkpoint.

## Say and show

“I replay payment events into a single Kafka partition to preserve file order.
The input has 2,000 events, including ten repeated transactions. The downstream
engine should produce 1,990 unique decisions.” Show the producer summary.

## Later experiment runs

Only after the baseline works, agree on fresh run IDs with the others.
Use `--mode late_lt60 --fraction 0.01 --min-wall-hold 12` for the within-watermark
experiment, and `--mode late_gt60 --fraction 0.01 --min-wall-hold 20` for the
beyond-watermark experiment. Replace the topic and run ID as well. A producer
lag statistic alone does not prove Spark dropped an event; use the actual output
and stream progress to explain the result.
