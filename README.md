# Real-Time Distributed Fraud Monitoring System

A multi-machine, distributed stream processing pipeline for real-time payment fraud detection built with **Apache Kafka**, **Apache Spark Structured Streaming (PySpark)**, **HDFS**, **Apache Iceberg**, and **Streamlit**.

The system scores each transaction as it arrives, applies event-time sliding-window rules, generates human-readable explanations, persists decisions and alerts to ACID Iceberg tables on HDFS, and provides an interactive investigation dashboard.

---

## 1. System Architecture & VM Topology

```
+---------------------------------------------------------------------------------------------------+
|                                      DISTRIBUTED TOPOLOGY                                         |
+---------------------------------------------------------------------------------------------------+
|                                                                                                   |
|    +------------------------+                                                                     |
|    |          VM1           |                                                                     |
|    |  Kafka Broker (KRaft)  |                                                                     |
|    |  Transaction Producer  |                                                                     |
|    +-----------+------------+                                                                     |
|                |                                                                                  |
|                | Ingests raw JSON stream (transactions-runN)                                      |
|                v                                                                                  |
|    +------------------------+             +------------------------+    +------------------------+|
|    |          VM2           |             |          VM3           |    |          VM4           ||
|    |  Spark Master & Driver |  RPC (7077) |  Spark Worker 1        |    |  Spark Worker 2        ||
|    |  Streaming Rules R1–R5 | ==========> |  HDFS (NameNode/Data)  |    |  Streamlit Dashboard   ||
|    |  Dedupe & Watermarking |             |  Iceberg Catalog       |    |  Investigation UI      ||
|    |  (Application Host)    |             |  Automated Verifier    |    |  (Query Layer)         ||
|    +-----------+------------+             +-----------+------------+    +-----------+------------+|
|                |                                      ^                             ^             |
|                | Writes Iceberg tables & checkpoints  |                             |             |
|                +--------------------------------------+                             |             |
|                |                                                                    |             |
|                +---------------- Reads ACID tables from Iceberg / HDFS -------------+             |
|                                                                                                   |
+---------------------------------------------------------------------------------------------------+
```

### VM Component Mapping

| VM | Component | Primary Responsibilities |
| :--- | :--- | :--- |
| **VM1** | **Data Ingestion** | Kafka broker (KRaft mode), transaction producer (`transactions.csv`), dynamic topic management, controlled disorder injection scripts (Exp 3 & 4). |
| **VM2** | **Fraud Engine** | Spark Master and Driver node. **Exclusive host for streaming application code**. Event-time watermarking (`1 minute`), deduplication on `transaction_id`, sliding-window rule evaluation (R1–R5), and writes to Iceberg tables. |
| **VM3** | **Storage & Correctness** | Single-node HDFS storage (NameNode + DataNode), Iceberg catalog, Spark Worker 1, checkpoint storage (`/checkpoints/runN`), and automated ground-truth verifier. |
| **VM4** | **Observability** | Spark Worker 2, interactive Streamlit fraud monitoring dashboard, alert investigation drilldown (showing explanation, trigger txn, and account's last 20 transactions). |

> **Architectural Constraint:** The application code is submitted and managed exclusively on **VM2**. VM3 and VM4 act as headless Spark workers; tasks are distributed dynamically across executors over the network.

---

## 2. Repository Layout

```
.
├── .gitignore
├── README.md                      # System overview and architecture guide
├── vm1/                           # Data Ingestion: Kafka broker setup & producer scripts
│   └── README.md
├── vm2/                           # Fraud Detection Engine: Spark Master, Driver & Rules
│   ├── app/
│   │   ├── __init__.py
│   │   ├── rules.py               # Deterministic R1–R5 fraud logic & window aggregations
│   │   ├── schema.py              # StructType schemas for Kafka & Iceberg tables
│   │   ├── stream_job.py          # PySpark Structured Streaming entrypoint
│   │   └── writers.py             # Iceberg table initialization & idempotent MERGE logic
│   ├── config/
│   │   ├── __init__.py
│   │   ├── cluster.env.example    # Multi-node network configuration template
│   │   └── config.py              # Application settings dataclass
│   ├── scripts/
│   │   ├── start_master.sh        # Starts Spark Master daemon on VM2
│   │   ├── stop_master.sh         # Stops Spark Master daemon
│   │   └── submit_job.sh          # Submits PySpark streaming job with Iceberg/Kafka jars
│   ├── tests/
│   │   ├── __init__.py
│   │   └── test_rules.py          # Ground truth unit test suite (100% precision/recall)
│   ├── requirements.txt
│   └── README.md
├── vm3/                           # Storage & Correctness: HDFS, Iceberg & Automated Verifier
│   └── README.md
└── vm4/                           # Dashboard & Observability: Streamlit investigation app
    └── README.md
```

---

## 3. Fraud Rules & Stream Semantics

### Stream Processing Invariants
* **Event Time:** Logic is keyed strictly to `event_timestamp`, not message arrival time.
* **Rolling Window:** 2-minute observation window with a 30-second slide for aggregate rules (R2–R4). Windows operate as half-open intervals: $[W_{\text{start}}, W_{\text{end}})$.
* **Watermark & Lateness:** `.withWatermark("event_timestamp", "1 minute")`. Events arriving up to 1 minute late are processed into their corresponding windows; events older than the watermark are dropped.
* **Deduplication:** Repeated events with identical `transaction_id` are processed exactly once.

### Detection Rules (R1–R5)

| Rule | Plain Terms | Evaluation Logic | Threshold |
| :--- | :--- | :--- | :--- |
| **R1** | High-value single payment | Stateless | `currency == 'INR' and amount >= 50000.0` |
| **R2** | Account velocity / burst | Rolling 2-min window | $\ge 5$ payments from one `source_account` |
| **R3** | High aggregate INR amount | Rolling 2-min window | $\ge 2$ INR payments totalling $> 600,000.0$ |
| **R4** | Repeated transfers to same destination | Rolling 2-min window | $\ge 3$ payments from one account to the same destination |
| **R5** | Composite risk decision | Transaction level | $\text{Score} = \text{sum}(\text{active rules R1..R4})$<br>$\ge 2 \implies$ **`BLOCK`**<br>$== 1 \implies$ **`REVIEW`**<br>$== 0 \implies$ **`APPROVE`** |

### Alert & Decision Association
When an account breaches R2, R3, or R4 in an active window $[W_{\text{start}}, W_{\text{end}})$, all payments originating from that account within the window interval are tagged with the generated alert in `related_alert_ids` and have their corresponding boolean rule flag set to `True`.

---

## 4. Apache Iceberg Table Schemas

### `transaction_decisions` (One row per unique transaction)
* `transaction_id`: String (Primary key)
* `source_account`: Long
* `destination_account`: Long
* `event_timestamp`: Timestamp
* `evaluated_at`: Timestamp
* `amount`: Double
* `currency`: String
* `r1_triggered`, `r2_triggered`, `r3_triggered`, `r4_triggered`: Boolean
* `r5_risk_score`: Integer (0–4)
* `r5_block_triggered`: Boolean
* `decision`: String (`APPROVE`, `REVIEW`, `BLOCK`)
* `related_alert_ids`: String (JSON array of alert IDs)

### `account_risk_alerts` (One row per account, window, and rule)
* `alert_id`: String (Format: `{rule_id}-{source_account}-{window_start_compact}`)
* `rule_id`: String (`R2`, `R3`, `R4`)
* `source_account`: Long
* `destination_account`: Long (Populated for R4, null for R2/R3)
* `trigger_transaction_id`: String (Latest transaction in supporting set)
* `finding_scope`: String (`WINDOW` for R2/R3, `PATTERN` for R4)
* `window_start`, `window_end`: Timestamp
* `observed_count`: Long
* `observed_amount_inr`: Double (Populated for R3)
* `count_threshold`, `amount_threshold_inr`: Long, Double
* `supporting_transaction_ids`: String (JSON array of transaction IDs)
* `explanation`: String (Plain English summary)
* `triggered_at`: Timestamp

---

## 5. End-to-End Pipeline Execution

### Step 1: Initialize Storage (VM3)
1. Start single-node HDFS:
   ```bash
   $HADOOP_HOME/sbin/start-dfs.sh
   ```
2. Verify NameNode/DataNode and create directories:
   ```bash
   hdfs dfs -mkdir -p /checkpoints /warehouse
   ```

### Step 2: Start Message Broker (VM1)
1. Start Kafka broker:
   ```bash
   kafka-server-start.sh -daemon $KAFKA_HOME/config/kraft/server.properties
   ```
2. Create run topic:
   ```bash
   kafka-topics.sh --bootstrap-server localhost:9092 --create --topic transactions-run1 --partitions 1 --replication-factor 1
   ```

### Step 3: Launch Spark Master & Workers (VM2, VM3, VM4)
1. On **VM2** (Spark Master):
   ```bash
   cd vm2
   ./scripts/start_master.sh <VM2_PRIVATE_IP>
   ```
2. On **VM3** and **VM4** (Workers):
   ```bash
   $SPARK_HOME/sbin/start-worker.sh spark://<VM2_PRIVATE_IP>:7077
   ```
3. Confirm both workers appear in Spark Web UI at `http://<VM2_PRIVATE_IP>:8080`.

### Step 4: Submit Streaming Pipeline (VM2)
```bash
cd vm2
./scripts/submit_job.sh
```

### Step 5: Start Ingestion Producer (VM1)
Stream payments continuously:
```bash
python3 producer.py --topic transactions-run1 --file data/transactions.csv
```

### Step 6: Start Dashboard (VM4)
```bash
cd vm4
streamlit run dashboard.py --server.port 8501
```

### Step 7: Run Automated Verifier (VM3)
Compare generated Iceberg tables against ground truth:
```bash
cd vm3
python3 verifier.py --run run1
```

---

## 6. Stress Experiments Protocol

| Experiment | Fault / Disorder Injected | Verification Expectation |
| :--- | :--- | :--- |
| **Baseline** | Clean in-order stream | Output matches ground truth with **100% precision & recall**. |
| **Exp 1: Worker Kill** | `kill -9` worker process on VM3 or VM4 mid-run | Spark Master redistributes tasks to remaining worker; **zero record loss**, verifier reports PASS. |
| **Exp 2: Master Kill** | Terminate Master & Driver on VM2; restart with **same checkpoint** and **same run ID** | Job resumes from HDFS checkpoint without data loss or duplicate records (enforced via idempotent `MERGE INTO`). |
| **Exp 3: Delay < 1 min** | Producer delays selected events by $< 60$ seconds | Events fall within the 1-minute watermark buffer; all events processed into proper windows matching ground truth. |
| **Exp 4: Delay > 1 min** | Producer delays selected events by $> 60$ seconds | Events older than the watermark threshold are dropped; verifier flags dropped events vs ground truth. |
