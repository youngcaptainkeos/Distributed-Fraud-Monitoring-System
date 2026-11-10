# VM1: Kafka broker and transaction producer (Saatwik)

Kafka 3.9.2, single-node KRaft, Java 11. Producer: Python 3.12 venv with kafka-python and pandas.
One broker and replication factor 1: no broker fault tolerance (lab limitation).
The real VM1 address is shared in the team chat; below it is written as <VM1_IP>.

## Broker
advertised.listeners in the broker config must be the ZeroTier IP, never localhost (see configs/server-fraud.properties.example).
Format storage once only. Never format again on a restart.

    source ~/fraud/configs/cluster.env
    $KAFKA_HOME/bin/kafka-server-start.sh -daemon ~/fraud/configs/server-fraud.properties
    $KAFKA_HOME/bin/kafka-broker-api-versions.sh --bootstrap-server $KAFKA_BOOTSTRAP | head -1

## Topic per run (1 partition so the stream keeps file order)

    $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --topic transactions-run1 --partitions 1 --replication-factor 1

Run mapping: run1 baseline, run2 Exp 1 (worker kill), run3 Exp 2 (master restart), run4 Exp 3 (delay under 1 min), run5 Exp 4 (delay over 1 min).

## Producer
The producer never sorts. It refuses a topic that is missing or not empty. Every run writes an arrival CSV and a summary JSON.

    python3 vm1/producer.py --mode baseline  --topic transactions-run1 --run-id run1 --input data/transactions.csv
    python3 vm1/producer.py --mode late_lt60 --topic transactions-run4 --run-id run4 --input data/transactions.csv --fraction 0.01 --min-wall-hold 12
    python3 vm1/producer.py --mode late_gt60 --topic transactions-run5 --run-id run5 --input data/transactions.csv --fraction 0.01 --min-wall-hold 20

--min-wall-hold makes a delayed row wait that many real seconds so it lands in a later micro-batch (trigger is 10 s).

## Verify a topic against the CSV

    python3 vm1/verify_topic.py --topic transactions-run1 --input data/transactions.csv --expect-order

Messages: one JSON object per CSV row, all values strings, key = transaction_id. Cast numerics in Spark.
Evidence for the pre-load rehearsal is in experiment-log/run1-preload/ (IPs redacted).
