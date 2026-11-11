# VM3: Storage, Catalog & Correctness Verification

VM3 is responsible for HDFS storage, Iceberg warehouse infrastructure, and independent verification of fraud detection results.

## Responsibilities

### 1. HDFS Storage

- Hosts the HDFS NameNode and DataNode in the intended deployment.
- Stores streaming checkpoints under `/checkpoints/runN`.
- Stores the Iceberg warehouse under `/warehouse`.
- Example HDFS URI: `hdfs://<VM3_PRIVATE_IP>:9000`.

Start HDFS:

```bash
$HADOOP_HOME/sbin/start-dfs.sh
```

Create the required directories:

```bash
hdfs dfs -mkdir -p /checkpoints /warehouse
hdfs dfs -ls /
```

### 2. Apache Iceberg Storage

The intended warehouse contains the run-specific tables:

- `transaction_decisions_runN`
- `account_risk_alerts_runN`

The Iceberg catalog uses HadoopCatalog backed by HDFS.

Table creation and population depend on the streaming application running on VM2.

### 3. Spark Worker 1

VM3 can operate as a Spark Worker registered with the Spark Master on VM2:

```bash
$SPARK_HOME/sbin/start-worker.sh spark://<VM2_PRIVATE_IP>:7077
```

Confirm registration through the Spark Master Web UI:

`http://<VM2_PRIVATE_IP>:8080`

### 4. Independent Ground-Truth Verifier

The verifier is located in `vm3/verifier/`.

Run all verification checks from the repository root:

```bash
python3 vm3/verifier/verify_all.py
```

Individual rule verifiers:

```bash
python3 vm3/verifier/verify_r1.py
python3 vm3/verifier/verify_r2.py
python3 vm3/verifier/verify_r3.py
python3 vm3/verifier/verify_r4.py
python3 vm3/verifier/verify_r5.py
```

The verification suite includes:

- Input and ground-truth validation.
- R1: High-value transactions.
- R2: Transaction velocity.
- R3: High cumulative amount.
- R4: Repeated destination pattern.
- R5: Final decision policy.

### 5. Verification Reports

Generated reports are stored under:

`vm3/verifier/reports/`

The verification suite produces a JSON verification report and a human-readable master report.

### 6. Infrastructure Snapshot

Capture a snapshot of the local infrastructure using:

```bash
bash vm3/scripts/capture_system_snapshot.sh
```

## Current Validation Status

- Input and ground-truth validation: PASS.
- Independent R1–R5 verification: PASS.
- Local HDFS and Iceberg infrastructure: prepared and tested.
- Distributed streaming output verification: dependent on successful end-to-end pipeline execution.

## Deployment Note

VM3 describes a logical project role. During development and testing, VM3 storage and verification components may be hosted on the same physical machine as VM4 dashboard components.
