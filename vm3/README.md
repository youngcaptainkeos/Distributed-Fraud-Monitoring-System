# VM3: Storage, Catalog & Correctness Verifier

This directory contains storage configurations, table definitions, and the automated verification script running on **VM3**.

---

## Responsibilities

1. **Single-Node HDFS:**
   - Hosts HDFS NameNode and DataNode (`hdfs://<VM3_PRIVATE_IP>:9000`).
   - Maintains durable checkpoint directory (`/checkpoints/runN`) and Iceberg warehouse directory (`/warehouse`).

2. **Apache Iceberg Catalog:**
   - Initializes table schemas for:
     - `transaction_decisions_runN`
     - `account_risk_alerts_runN`
   - Configures catalog metadata and maintenance.

3. **Spark Worker 1:**
   - Runs worker process registered with Spark Master on VM2:
     ```bash
     $SPARK_HOME/sbin/start-worker.sh spark://<VM2_PRIVATE_IP>:7077
     ```

4. **Automated Verifier (`verifier.py`):**
   - Independent verification script executed on VM3 after each run.
   - Compares Iceberg tables against ground truth CSVs (`ground_truth_transaction_decisions.csv` and `ground_truth_account_risk_alerts.csv`).
   - Reports exact metrics: missing, extra, duplicate, and mismatched rows.
