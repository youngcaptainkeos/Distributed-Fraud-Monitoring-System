# VM1: Data Ingestion & Kafka Producer

This directory contains the Kafka broker configurations and transaction producer scripts running on **VM1**.

---

## Responsibilities

1. **Apache Kafka Broker (KRaft mode):**
   - Runs Kafka broker accessible on `<VM1_PRIVATE_IP>:9092`.
   - Manages dynamic topic lifecycle per experiment run (e.g. `transactions-run1`, `transactions-run2`).

2. **Transaction Producer:**
   - Ingests `transactions.csv` and streams messages as JSON objects to Kafka.
   - Message key: `transaction_id`.
   - Continuous streaming rate without sorting data.

3. **Disorder Injection (Controlled Stress Experiments):**
   - **Baseline & Exp 1 & 2:** In-order delivery.
   - **Exp 3 (Disorder < 1 min):** Selected events delayed by $\le 60$ seconds.
   - **Exp 4 (Disorder > 1 min):** Selected events delayed by $> 60$ seconds.
