# VM2: Streaming Fraud Detection Engine

This component hosts the Spark Structured Streaming application running on **VM2**. It acts as the Spark Master and Driver, ingesting payments from Apache Kafka (VM1), applying event-time watermarking, deduplicating incoming events, evaluating fraud rules R1–R5 across rolling 2-minute sliding windows (30s slide), and writing outputs to Apache Iceberg tables backed by HDFS on VM3.

---

## Architecture & Responsibilities

- **Host Node:** VM2 (Spark Master + Spark Driver)
- **Application Scope:** All core stream processing logic resides here and is submitted from here. Workers on VM3 and VM4 execute tasks remotely.
- **Kafka Ingestion:** Consumes directly from `transactions-runN` on VM1.
- **Watermarking & Deduplication:** 1-minute allowable lateness watermark (`event_timestamp`), deduplication on `transaction_id`.
- **Sliding-Window Rule Evaluation:** 120s duration, 30s slide for R2–R4; stateless R1; composite R5 decision.
- **Iceberg Sink:** Writes micro-batches to `transaction_decisions_runN` and `account_risk_alerts_runN` on HDFS with HDFS checkpointing.

---

## Directory Structure

```
vm2/
├── app/
│   ├── __init__.py
│   ├── schema.py                  # StructType schemas for Kafka payload and Iceberg tables
│   ├── rules.py                   # Pure R1–R5 fraud rules engine & window aggregations
│   ├── writers.py                 # Iceberg table initialization & idempotent MERGE logic
│   └── stream_job.py              # Main PySpark Structured Streaming entrypoint
├── config/
│   ├── __init__.py
│   ├── config.py                  # Dataclass configuration loader
│   └── cluster.env.example        # Environment configuration template
├── scripts/
│   ├── start_master.sh            # Starts Spark Master daemon
│   ├── stop_master.sh             # Stops Spark Master daemon
│   └── submit_job.sh              # spark-submit with Iceberg 1.4.1 & Kafka packages
├── tests/
│   ├── __init__.py
│   └── test_rules.py              # Unit test suite verifying against Ground Truth
├── README.md                      # Component guide
└── requirements.txt               # Dependencies
```

---

## Fraud Rules Specification

| Rule | Description | Scope | Condition |
| :--- | :--- | :--- | :--- |
| **R1** | High-value single payment | Stateless | `currency == 'INR' and amount >= 50000.0` |
| **R2** | Account velocity / burst activity | 2-min window, 30s slide | $\ge 5$ payments from one `source_account` |
| **R3** | High aggregate INR amount | 2-min window, 30s slide | $\ge 2$ INR payments totalling $> 600,000.0$ |
| **R4** | Repeated transfers to same destination | 2-min window, 30s slide | $\ge 3$ payments from one account to same destination |
| **R5** | Composite risk decision | Transaction level | $\ge 2$ active $\implies$ **`BLOCK`**; $1$ active $\implies$ **`REVIEW`**; $0 \implies$ **`APPROVE`** |

### Event Linkage
- Sliding windows are evaluated as half-open intervals: $[W_{\text{start}}, W_{\text{end}})$.
- Trigger transaction for an alert is the last transaction in `supporting_transaction_ids`.
- Any transaction from an account occurring within an active window $[W_{\text{start}}, W_{\text{end}})$ of an alert inherits the alert ID in `related_alert_ids` and the active rule flag.

---

## Setup & Execution

### 1. Verification Test Suite
Run local unit tests verifying 100% agreement against ground truth:
```bash
cd vm2
PYTHONPATH=. python3 -m unittest tests/test_rules.py -v
```

### 2. Configure Environment
```bash
cp config/cluster.env.example config/.env
# Set KAFKA_BOOTSTRAP_SERVERS, SPARK_MASTER_URL, HDFS_NAMENODE, RUN_ID
```

### 3. Start Spark Master
```bash
./scripts/start_master.sh <VM2_PRIVATE_IP>
```
Verify workers from VM3 and VM4 are connected at `http://<VM2_PRIVATE_IP>:8080`.

### 4. Submit Streaming Job
```bash
./scripts/submit_job.sh
```
