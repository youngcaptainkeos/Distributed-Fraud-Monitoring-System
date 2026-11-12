# Your fraud monitoring project and VM2 study guide

This guide follows the files actually present on Shashank's laptop on
12 November 2026. The numbered source appendix explains every VM2 source file
in order. Line numbers refer to this snapshot; they can change after editing.
This is an explanation of the implementation, not a claim that the full live
demo has passed. No services need to be started to study it.

## 1 Where your files are

The real project directory is:

```text
/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/
```

The path containing `Link to sem 5` reaches the same directory:

```text
/media/shashank/Data1/PDocuments/Link to sem 5/BD/Project fraund detection/
```

`Link to sem 5` is a symbolic link to `sem 5`. These paths do not hold two
copies. The spelling `fraund` is the actual folder name.

```text
Project fraund detection/
├── Distributed Fraud Monitoring System.pdf    Original project reference
├── Fraud_Monitoring_Manuals_Fresh.zip          Archive of team manuals
├── Fraud_Monitoring_Manuals_Fresh/             Extracted manuals
│   ├── Shashank_Manual.docx
│   ├── Saatwik_Manual.docx
│   ├── Sumukh_Manual.docx
│   ├── Vijeth_Manual.docx                     Historical separate role
│   └── Team_Integration_Runbook.docx
├── data/                                     Empty outer folder at inspection
├── streaming/                                Earlier standalone engine copy
└── repo_clone/                               Active GitHub repository checkout
    ├── .git/                                 Git history and remote information
    ├── .gitignore                            Paths Git normally ignores
    ├── README.md                             Whole-system overview
    ├── requirements.txt                      Shared/dashboard Python dependencies
    ├── data/
    │   ├── transactions.csv                  Input payment events
    │   ├── ground_truth_transaction_decisions.csv
    │   └── ground_truth_account_risk_alerts.csv
    ├── vm1/                                  Saatwik's code; not running here by default
    ├── vm2/                                  Your active application code
    ├── vm3/                                  Storage setup and verifier code
    ├── vm4/                                  Dashboard and query-layer code
    ├── evidence/                             Local logs, exports and experiment reports
    └── docs/                                 Study guide and demo sheets
```

Your active engine is `repo_clone/vm2`. `streaming` is a separate earlier copy;
its `stream_job.py` differs from the active version. Editing one does not edit
the other. The current rules files matched at inspection, but do not assume
the entire trees match. Use the repository copy when studying the live job.

The whole repository being on your disk does not mean every component runs
on your laptop. Your checkout contains the other teammates' programs so that
the source can be shared and versioned together.

## 2 Your exact VM2 layout

```text
repo_clone/vm2/
├── README.md                    Component documentation
├── requirements.txt             Your Python dependency declarations
├── app/
│   ├── __init__.py              Package exports
│   ├── schema.py                Input and output column/type definitions
│   ├── stream_job.py            Streaming entrypoint and micro-batch callback
│   ├── rules.py                 Pandas implementation of R1–R5
│   └── writers.py               Iceberg table creation and MERGE writes
├── config/
│   ├── __init__.py              Exposes AppConfig and config
│   ├── config.py                Reads environment into a configuration object
│   ├── .env                     Actual laptop/run settings, ignored by Git
│   └── cluster.env.example      Template, not the loaded live settings
├── scripts/
│   ├── start_master.sh          Start the Spark master daemon
│   ├── stop_master.sh           Stop that daemon
│   └── submit_job.sh            Configure, package and launch the application
└── tests/
    ├── test_rules.py            Compare rule implementation with supplied answers
    └── sim_batches.py           Simulate split batches and delayed arrival
```

`__pycache__` folders contain generated Python bytecode. They are not the
source files you should edit. A `.py` file is source; a `.pyc` file is a cache.
An `__init__.py` file participates in importing a Python package.

## 3 The three machines and your responsibility

| Person | Address | Processes and responsibilities |
|---|---|---|
| Saatwik | 172.23.211.163 | Kafka broker and producer |
| Shashank | 172.23.124.232 | Spark master and application driver |
| Sumukh | 172.23.138.248 | HDFS, Iceberg warehouse, two Spark workers, executors, verifier, Thrift and Streamlit |

VM1–VM4 are logical role labels. Sumukh now handles both VM3 and VM4. Your
role called VM2 runs on your laptop; the label does not itself imply a separate
guest operating system. A Python virtual environment is also different from
a virtual machine: it isolates Python packages, not an entire operating system.

Your master allocates resources to applications. A worker is a daemon that
offers cores and memory. An executor is an application-specific JVM process
started by a worker to execute tasks. The driver runs your application logic
and schedules those tasks. Two workers on Sumukh's VM remain one physical
failure domain. Stopping your master and stopping your driver are different
actions; `stop_master.sh` is not a general stop-everything command.

## 4 The complete transaction flow

```text
Saatwik's transactions.csv
    ↓ producer.py reads rows as strings, creates JSON, key = transaction_id
Kafka topic transactions-run6 on Saatwik's broker
    ↓ Spark Kafka source assigns reads to executors on Sumukh's workers
Spark parses JSON, casts types, watermarks and deduplicates
    ↓ foreachBatch calls your Python driver callback
Current batch collected into Pandas on YOUR laptop
    + historical events read from Iceberg and collected into Pandas
    ↓ remove repeated transaction IDs
rules.evaluate_batch computes all windows, alerts and decisions on YOUR laptop
    ↓ convert results back into Spark DataFrames
writers.py runs Iceberg MERGE statements using distributed Spark tasks
    ↓ files and table metadata stored in HDFS on Sumukh's VM
Sumukh's Thrift Server queries Iceberg → Beeline → Streamlit dashboard
```

The input CSV is a replay dataset, not a live bank feed. The supplied ground
truth files are expected answers for checking results; the streaming engine
does not read them to decide whether a payment is fraudulent.

Kafka is a durable event log. A topic groups events; a partition is an ordered
log inside a topic. An offset identifies a position within one partition.
It is not a transaction ID. The topic uses one partition to preserve producer
order; later Spark shuffles can still distribute work across both workers.

The Kafka bootstrap address is the initial discovery address. The broker also
advertises the address clients must use. `localhost` on Sumukh's executor would
refer to Sumukh's VM, not Saatwik. That is why the broker's advertised address
must be reachable by the executors, not only by your driver.

## 5 Follow one example payment

Suppose a row has transaction ID `T1`, source account A, destination B, event
time `10:00:10`, amount ₹60,000 and currency INR. Saatwik serializes the row;
Spark receives the JSON text and converts amount to a floating-point number,
account IDs to 64-bit integers and time to a timestamp.

R1 is immediately true because the payment is INR and at least ₹50,000.
If no account-window alert applies, score = 1 and the classification is REVIEW.
If enough payments from A arrive in the same two-minute window, R2 also becomes
true. The cumulative evaluation can revisit T1 and change its score to 2 and
its classification to BLOCK. MERGE updates its existing output row.

BLOCK is a classification stored in a table. There is no bank API call here
that cancels the transfer. This is deterministic rule evaluation, not a trained
machine-learning model. The labels express the supplied policy rather than
independently confirmed real-world fraud.

## 6 Three different time concepts

| Concept | Current setting | Meaning |
|---|---|---|
| Micro-batch trigger | 10 seconds | How often Spark attempts to process newly available input |
| Rule window | 120 seconds | Event-time interval over which activity is counted |
| Window slide | 30 seconds | Spacing between adjacent observation-window starts |
| Watermark delay | 1 minute | Event-time lateness allowance used by the streaming stateful operator |

Processing time is when the system handles an event. Event time comes from
`event_timestamp` inside the payment. An October event replayed in November
is not automatically late because the calendar date is old: lateness is
relative to the stream's observed event-time progress.

A ten-second trigger does not guarantee a ten-second result. A batch that
takes longer delays the next batch. The 30-second slide is not a sleep or a
separate timer: `rules.py` constructs windows aligned to :00 and :30.

For event time 10:01:10, the four containing windows are:

```text
[09:59:30, 10:01:30)
[10:00:00, 10:02:00)
[10:00:30, 10:02:30)
[10:01:00, 10:03:00)
```

The left endpoint is included; the right endpoint is excluded. One payment
participates in four windows because 120 / 30 = 4. It still has one transaction
decision. Several overlapping alerts can refer to it.

Conceptually, the watermark follows the maximum event time seen minus the
configured delay, with advancement tied to completed processing. It is not
simply wall-clock delay. Do not promise that every event delayed over 60 seconds
is always dropped: beyond-delay behavior depends on the effective watermark
and batch placement. Observe the actual stream in the lateness experiment.
See the [Spark 3.3.4 streaming guide](https://archive.apache.org/dist/spark/docs/3.3.4/structured-streaming-programming-guide.html).

This implementation uses Spark watermarking with deduplication. The R2–R4
window calculations themselves are Pandas operations over retained history;
they are not Spark `groupBy(window(...))` aggregations.

## 7 Fraud rules and association details

| Rule | Actual condition |
|---|---|
| R1 | INR and amount >= 50,000 |
| R2 | Count >= 5 for a source account in a window |
| R3 | At least 2 INR payments, sum strictly > 600,000 in a source-account window |
| R4 | Count >= 3 for a source/destination pair in a window |
| R5 | Sum the four boolean flags; 0 APPROVE, 1 REVIEW, >= 2 BLOCK |

R2 and R4 count payments without filtering currency. R3 filters INR first.
Exactly ₹600,000 does not satisfy R3. Exactly ₹50,000 satisfies R1.
Four separate R2 window alerts still contribute only one R2 flag to a payment;
the score counts active rule types, not the number of alert rows.

An alert is linked to every transaction from the same source account inside
its interval. R4's pattern detection is destination-specific, but its final
decision linkage is source-account-and-window based. A payment to another
destination can therefore inherit R4 when it falls inside that source's alert
window. This is the supplied project's account-risk scope.

Alert IDs have the form `R2-account-YYYYMMDDHHMMSS`. R4's ID does not contain
the destination. If multiple destinations qualify for the same source/window,
the implementation keeps the candidate with the highest count. A count tie
has no explicitly specified destination tie-breaker in this code.

The supporting transaction list is sorted by event time, then transaction ID.
The last element is saved as `trigger_transaction_id`. Because evaluation is
cumulative, that is the latest supporting payment in the current evaluation;
it does not preserve an immutable record of the very first threshold crossing.

## 8 What lives in memory and what lives on disk

| Item | Location | Why it exists |
|---|---|---|
| Application source | Your `repo_clone/vm2` | Programs submitted from your laptop |
| Dataset and supplied answers | Your `repo_clone/data` | Shared input and checking fixtures |
| Current batch and full Pandas history | Driver memory on your laptop | Rule evaluation |
| Spark task/shuffle data | Executor memory and temporary local disk on Sumukh | Distributed processing |
| Kafka message log | Kafka-configured disk directory on Saatwik | Replayable input |
| Iceberg tables and metadata | HDFS warehouse on Sumukh | Durable output and historical events |
| Spark streaming checkpoint | HDFS `/checkpoints/run6` on Sumukh | Source progress and state recovery |
| Master daemon log | `/home/shashank/opt/spark/logs` | Master/worker registration and application lifecycle |
| Captured application log | Your `repo_clone/evidence/run6/stream.log` | Driver output retained for investigation |
| Exported CSVs | Your `repo_clone/evidence/test` or Sumukh's chosen evidence directory | Copies of actual tables for checking |

A Pandas DataFrame is local Python-managed data. A Spark DataFrame describes
distributed data and operations. `toPandas()` is a collection boundary:
records cross the network and accumulate in your driver memory. The design
re-reads and re-evaluates the entire stored history each batch. The watermark
does not trim that Pandas history or delete historical Iceberg rows.

There are two different kinds of continuing state: Spark's deduplication state
in the checkpoint, and the historical event fields retained in Iceberg decisions.
Neither is a replacement for the other.

## 9 HDFS and Iceberg are different layers

HDFS stores files across its NameNode/DataNode architecture. In this deployment,
Sumukh hosts the single NameNode and DataNode. The NameNode manages paths and
block metadata; the DataNode stores and serves the actual blocks. Port 9000 is
the configured NameNode RPC endpoint, not the only possible storage connection.
Clients also need to reach the DataNode address returned by the NameNode.

Iceberg supplies table schemas, partition information, snapshots and metadata
that describe which data files form a table. Spark reads and writes through
that table format. The Hadoop catalog resolves table names inside the warehouse;
there is no separate always-running Iceberg server in this configuration.

```text
Logical SQL name:
iceberg.default.transaction_decisions_run6
  │       │                └── table name
  │       └── namespace
  └── configured Spark catalog name

Configured warehouse:
hdfs://172.23.138.248:9000/warehouse

Typical table paths in that warehouse:
/warehouse/default/transaction_decisions_run6/
/warehouse/default/account_risk_alerts_run6/
```

An Iceberg table directory contains metadata and data files, rather than a
single editable CSV. The normal data-file format here is Parquet unless table
properties override it. Metadata includes schema/snapshot JSON and manifest
information. The DataNode's underlying physical block-storage directory is
configured on Sumukh's VM; it was not inspected for this guide. Do not confuse
that directory with an HDFS logical path.

`writers.py` partitions decisions by `days(event_timestamp)` and alerts by
`rule_id`. These storage partitions are different from a Kafka topic partition
and different from the four Spark shuffle partitions.

MERGE matches decisions by transaction ID and alerts by alert ID. Existing rows
are updated and new IDs are inserted. That protects logical row uniqueness on
replay when inputs are unique; it is not a declared SQL PRIMARY KEY constraint.
Updates can create replacement data files and new snapshots, rather than
editing bytes in an old Parquet file. See [Iceberg's write documentation](https://iceberg.apache.org/docs/1.4.1/spark-writes/).

The two MERGE statements commit separately. There is no transaction in this
code that atomically commits both tables together. A dashboard can briefly see
new decisions and old alerts while a batch is being written. `evaluated_at`
and `triggered_at` also change during reevaluation, so idempotence here means
the same logical IDs and classifications can be maintained, not byte-identical
output across every retry.

## 10 Checkpoints and recovery

```text
hdfs://172.23.138.248:9000/checkpoints/run6/
├── metadata     Streaming query identity
├── offsets/     Input offset ranges selected for batches
├── commits/     Completed-batch records
├── sources/     Source-specific progress information
└── state/       Deduplication state, partitioned by operator and partition
```

This is a conceptual layout based on the observed checkpoint family. Internal
filenames are owned by Spark. State `.delta` files record changes to a version
of an operator's saved state. Deleting some while keeping offset/commit records
can make the checkpoint unusable, as seen in the old `test` run.

`startingOffsets=earliest` applies to a new query without saved progress. A
restart using a valid checkpoint resumes saved offsets instead of restarting
from zero. Keep the same topic, checkpoint, output tables and compatible query
state for recovery. A new experiment should use a fresh run ID consistently.

The callback receives `batch_id`, but this code does not keep a durable ledger
of committed batch IDs. It uses MERGE keys to make retries avoid extra logical
rows. `foreachBatch` alone does not establish exactly-once output; that depends
on the sink implementation and recovery behavior. See the [Spark foreachBatch documentation](https://archive.apache.org/dist/spark/docs/3.3.4/structured-streaming-programming-guide.html#foreachbatch).

## 11 Software installed outside the repository

```text
/home/shashank/opt/spark/
├── bin/spark-submit       Launcher for Spark applications
├── sbin/                  Master/worker/Thrift daemon scripts
├── jars/                  Java/Scala libraries, including Kafka and Iceberg
├── conf/spark-env.sh      Java, Hadoop and Python environment setup
├── conf/spark-defaults.conf
└── logs/                  Master daemon logs

/home/shashank/opt/python3.10/
└── bin/python3.10         Your configured driver Python interpreter

/opt/hadoop/
├── bin/hdfs               HDFS command-line client
└── etc/hadoop/            Hadoop client configuration

/tmp/fraud_engine_modules.zip
                           Temporary packaged app and config modules
```

Having Hadoop installed on your laptop lets Spark use HDFS client libraries;
it does not mean this project's authoritative HDFS data is stored here.

The JVM runs Spark's Java/Scala execution engine. PySpark connects Python to
that engine using Py4J. A JAR is a Java library archive. The Kafka connector
coordinates reads with Kafka; the Iceberg runtime provides table/catalog/write
support. The `_2.12` coordinate identifies the Scala binary build, not Python.

The packaged Python ZIP is sent through `--py-files` so executors can import
application modules. It does not install Pandas, PySpark, Java or connector
JARs on teammates' computers. Those dependencies must already be available.

The global `spark-defaults.conf` currently contains:

```text
spark.master local[*]
spark.eventLog.enabled false
spark.driver.memory 1g
spark.executor.memory 512m
```

Your submission explicitly selects `spark://172.23.124.232:7077`, overriding
the local-master default. Launching Spark another way without that override
can accidentally run everything locally. Captured text logs are different
from Spark event logs; event logging is disabled in these defaults.

`spark-env.sh` configures Java 11, Hadoop under `/opt/hadoop`, Hadoop's classpath,
and Python under your home directory. The submission script also supplies
explicit Spark Python settings so the worker interpreter can be
`/usr/bin/python3.10` even though the driver interpreter has a different path.
The file paths are local to each machine; executors cannot open your home
directory just because the driver can.

## 12 Configuration precedence and the current run

The practical flow for the current launcher is:

```text
existing shell environment
    → source vm2/config/.env (overwrites variables it defines)
    → optional run argument overrides topic/checkpoint/table run settings
    → spark-submit receives explicit options
    → Python imports config.py and creates config once
```

Spark also loads its own environment/default files. For the master and Python
interpreter, explicit Spark properties supplied by the launcher are important.

The `.env` file at inspection selects `RUN_ID=run6`. Driver Python and worker
Python entries occur twice with identical values; the later assignments win
but currently have the same effect. `config.py` does not itself parse `.env`:
the shell sources it, exports the variables, and Python reads them with
`os.getenv`. Editing `.env` does not reconfigure an already-running process.

`AppConfig` is a frozen dataclass: a collection of named settings that normal
code cannot reassign after construction. `config = AppConfig()` is created
when the module is imported. Some settings such as rule thresholds and window
durations are also hard-coded directly in `rules.py`; changing only the
configuration fields does not currently change that rule implementation.

## 13 Read the entrypoint in execution order

The textual order of function definitions is not the execution order.
Python imports dependencies and defines functions, then the main guard calls
`main()`. Follow this sequence:

```text
submit_job.sh
  → imports config, schema, rules, writers
  → main()
      → build_spark_session()
      → initialize_iceberg_tables()
      → define Kafka source, parsing and deduplication plan
      → writeStream.foreachBatch(process_micro_batch).start()
          → process_micro_batch(...) for each batch
              → evaluate_batch(...)
              → write_micro_batch_to_iceberg(...)
      → awaitTermination()
```

Many DataFrame operations build a lazy plan. Actions such as `count`, collecting
to Pandas, checking an RDD for emptiness, and writes cause work to execute.
This callback uses several actions on the same batch and does not cache it;
repeated computation/state loading can therefore add overhead. That observation
is part of understanding the code, not a claim of a new live failure.

When reading source, `pdf` means a Pandas DataFrame variable, not a PDF document.
`df` is a general DataFrame abbreviation; `spark_decisions` is a Spark frame;
`df_decisions` returned by the rules function is Pandas.

## 14 How to read the evidence and tests

`evidence/baseline` and the four `experiment_*` folders contain checklists and
report locations. A report template being present is not evidence an experiment
ran. `evidence/run6/stream.log` is captured driver output; it is not the output
transaction table. `vm3/verifier/reports` contains verifier reports, which may
be checks of supplied data rather than actual streaming outputs.

`test_rules.py` runs `evaluate_batch` on a deduplicated local CSV. Its six checks
cover counts, alert IDs, final decision distribution, per-payment rule flags and
links, and supporting transactions/explanations. They do not start Kafka,
HDFS, a streaming query or a distributed Spark cluster.

`sim_batches.py` compares independent-batch evaluation against cumulative
history evaluation in Python. Its old label calls design A the current design,
but the actual streaming job now implements the cumulative approach B. The
simulation does not reproduce Kafka delivery, the Spark watermark, Iceberg
commits or checkpoint recovery.

The last saved `run6` log inspected for this guide ends on 12 November at
13:57:40 IST with `No Route to Host` to `172.23.138.248:9000`. It is a historical
failure record, not evidence the app is still running now. This explanation
does not restart the job or change the team's services.

## 15 Useful reading commands

These commands only navigate and read files. The absolute paths deliberately
avoid confusion between the two engine copies.

```bash
cd '/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2'
ls -la
nl -ba app/stream_job.py
nl -ba app/rules.py
nl -ba app/schema.py
nl -ba app/writers.py
cat config/.env
```

`nl -ba` prints line numbers, including blank lines. `ls -la` shows hidden files
such as `.env`. `tail -n 40 ../evidence/run6/stream.log` reads the end of the
captured log. These reading commands do not launch the pipeline.

## 16 Source reading order

Read the numbered appendix in this sequence for a connected explanation:
configuration → launcher → entrypoint → schemas → rules → writers → tests.
The appendix includes the exact source text and line/block explanations for
each VM2 code file, including package initializers and dependency declarations.


# Numbered VM2 source walkthrough
Read each explanation together with the numbered source immediately below it.
Every source line is included once per file; a single explanation sometimes
covers a multiline statement, record definition, comment block or blank-line
separator. Blank lines and comments organize reading but do not perform work.

Python syntax used throughout: indentation defines a block; `def` defines a
function; `return` sends a result back; `if` selects a branch; `for` iterates;
`try/except` catches exceptions; a dictionary maps keys to values; a list keeps
an ordered sequence; a set keeps unique members. `[]` also selects columns or
indexes collections, depending on the object. Function type hints describe
expected values without enforcing them at runtime. In Bash, `$name` expands a
variable, `export` passes it to child processes, and `source` executes another
file in the current shell. In SQL, a table/view is the relation addressed by a
query. These files combine all three languages.

## vm2/config/config.py

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/config/config.py`

**Lines 1–7.** The module docstring describes its purpose and the historical VM role names. Blank lines separate definitions and have no runtime effect.

```text
  1 | """Configuration settings for PySpark Fraud Detection Engine on VM2.
  2 | 
  3 | Loads environment variables with sensible defaults for local development
  4 | and cluster execution across VM1 (Kafka), VM2 (Spark Master/Driver),
  5 | VM3 (HDFS/Iceberg), and VM4 (Worker/Dashboard).
  6 | """
  7 | 
```

**Lines 8–11.** Import os to read process environment variables and dataclass to define a settings container.

```text
  8 | import os
  9 | from dataclasses import dataclass
 10 | 
 11 | 
```

**Lines 12–15.** The decorator generates dataclass methods; frozen=True prevents normal later reassignment. AppConfig defines named, type-annotated fields. RUN_ID comes from the environment, falling back to run1. Defaults here are evaluated when Python imports the module.

```text
 12 | @dataclass(frozen=True)
 13 | class AppConfig:
 14 |     # Run identifier (e.g. run1, run2)
 15 |     run_id: str = os.getenv("RUN_ID", "run1")
```

**Lines 16–20.** Kafka bootstrap is the discovery endpoint. The default topic embeds RUN_ID. earliest means a new query starts at the earliest available Kafka offsets; a valid checkpoint supplies restart progress instead. os.getenv(name, fallback) uses the fallback only when the variable is absent.

```text
 16 | 
 17 |     # Kafka configuration (VM1)
 18 |     kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
 19 |     kafka_topic: str = os.getenv("KAFKA_TOPIC", f"transactions-{os.getenv('RUN_ID', 'run1')}")
 20 |     kafka_starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
```

**Lines 21–25.** An empty master setting leaves spark-submit in control; a nonempty value is applied by the session builder. The app name identifies the job in logs and the UI. An explicitly set SPARK_APP_NAME can retain an older name even when RUN_ID changes.

```text
 21 | 
 22 |     # Spark cluster configuration (VM2 Master)
 23 |     # Default to empty string so spark-submit --master is preserved unless explicitly set
 24 |     spark_master_url: str = os.getenv("SPARK_MASTER_URL", "")
 25 |     spark_app_name: str = os.getenv("SPARK_APP_NAME", f"FraudDetectionEngine-{os.getenv('RUN_ID', 'run1')}")
```

**Lines 26–33.** HDFS_NAMENODE identifies the HDFS endpoint; iceberg is the catalog alias. ICEBERG_WAREHOUSE can explicitly override the default warehouse derived from HDFS_NAMENODE. f-strings insert values into a string.

```text
 26 | 
 27 |     # Storage & Iceberg catalog configuration (VM3 HDFS)
 28 |     hdfs_namenode: str = os.getenv("HDFS_NAMENODE", "hdfs://localhost:9000")
 29 |     iceberg_catalog_name: str = os.getenv("ICEBERG_CATALOG_NAME", "iceberg")
 30 |     iceberg_warehouse: str = os.getenv(
 31 |         "ICEBERG_WAREHOUSE",
 32 |         f"{os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')}/warehouse"
 33 |     )
```

**Lines 34–37.** The checkpoint path is either explicitly configured or derived from the HDFS address and run ID. Keep it stable when recovering a compatible run.

```text
 34 |     checkpoint_location: str = os.getenv(
 35 |         "CHECKPOINT_LOCATION",
 36 |         f"{os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')}/checkpoints/{os.getenv('RUN_ID', 'run1')}"
 37 |     )
```

**Lines 38–47.** Build fully qualified names catalog.namespace.table for the two run-specific output tables, unless explicit environment overrides are present.

```text
 38 | 
 39 |     # Iceberg table targets
 40 |     decisions_table: str = os.getenv(
 41 |         "DECISIONS_TABLE",
 42 |         f"{os.getenv('ICEBERG_CATALOG_NAME', 'iceberg')}.default.transaction_decisions_{os.getenv('RUN_ID', 'run1')}"
 43 |     )
 44 |     alerts_table: str = os.getenv(
 45 |         "ALERTS_TABLE",
 46 |         f"{os.getenv('ICEBERG_CATALOG_NAME', 'iceberg')}.default.account_risk_alerts_{os.getenv('RUN_ID', 'run1')}"
 47 |     )
```

**Lines 48–53.** These fields describe one-minute watermarking, a 120-second window, a 30-second slide and the trigger string. The streaming entrypoint reads watermark_delay and trigger_interval; rules.py separately hard-codes its window sizes.

```text
 48 | 
 49 |     # Fraud window & watermarking semantics
 50 |     watermark_delay: str = "1 minute"
 51 |     window_duration_seconds: int = 120
 52 |     window_slide_seconds: int = 30
 53 |     trigger_interval: str = os.getenv("TRIGGER_INTERVAL", "10 seconds")
```

**Lines 54–60.** Rule thresholds are documented as fields. They are not dynamically consumed by the present rules.py, which repeats the constants. Type annotations describe intended Python types; they are not automatic validation.

```text
 54 | 
 55 |     # Thresholds
 56 |     r1_amount_threshold: float = 50000.0
 57 |     r2_count_threshold: int = 5
 58 |     r3_count_threshold: int = 2
 59 |     r3_amount_threshold: float = 600000.0
 60 |     r4_count_threshold: int = 3
```

**Lines 61–64.** Instantiate the shared immutable settings object. Importing config gives other files access to these settings; it does not reload .env continuously.

```text
 61 | 
 62 | 
 63 | # Global config singleton
 64 | config = AppConfig()
```

## vm2/config/.env

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/config/.env`

**Lines 1–3.** Choose run6 and its Kafka topic on Saatwik. These are shell assignments, not Python. No spaces belong around the equals sign.

```text
  1 | RUN_ID=run6
  2 | KAFKA_BOOTSTRAP_SERVERS=172.23.211.163:9092
  3 | KAFKA_TOPIC=transactions-run6
```

**Lines 4–7.** Choose your Spark master, Sumukh HDFS, its warehouse, and the network address executors use to reach your driver. hdfs://, spark:// and a plain host:port identify different endpoints/protocols.

```text
  4 | SPARK_MASTER_URL=spark://172.23.124.232:7077
  5 | HDFS_NAMENODE=hdfs://172.23.138.248:9000
  6 | ICEBERG_WAREHOUSE=hdfs://172.23.138.248:9000/warehouse
  7 | SPARK_DRIVER_HOST=172.23.124.232
```

**Lines 8–10.** 512m is an executor memory setting; the global Spark defaults also specify 512m. The quoted trigger contains a space, so quotes keep it a single shell value. USE_PACKAGES=0 skips dependency resolution in this launcher and assumes required JARs are already installed.

```text
  8 | SPARK_EXECUTOR_MEMORY=512m
  9 | TRIGGER_INTERVAL="10 seconds"
 10 | USE_PACKAGES=0
```

**Lines 11–12.** Worker Python is an absolute path on each worker machine; driver Python is an absolute path on your laptop. Different directory paths can point to the same Python version.

```text
 11 | PYSPARK_PYTHON=/usr/bin/python3.10
 12 | PYSPARK_DRIVER_PYTHON=/home/shashank/opt/python3.10/bin/python3.10
```

**Lines 13–14.** Repeat the same two assignments. A shell uses the last assignment; because these values are identical, the duplicates currently change nothing.

```text
 13 | PYSPARK_PYTHON=/usr/bin/python3.10
 14 | PYSPARK_DRIVER_PYTHON=/home/shashank/opt/python3.10/bin/python3.10
```

## vm2/config/cluster.env.example

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/config/cluster.env.example`

**Lines 1–11.** Comment-only instructions and proposed historical run mapping. A template is not automatically loaded. Actual run6 does not have to follow the original run1–run5 labels.

```text
  1 | # Cluster Environment Configuration Template
  2 | # Copy this file to `config/.env` and fill in the assigned ZeroTier IP addresses.
  3 | # Note: `config/.env` is ignored by Git to prevent leaking internal IPs.
  4 | 
  5 | # Run Identifier (change per run: run1, run2, run3, run4, run5)
  6 | # Proposed mapping:
  7 | #   run1: Baseline (clean in-order live stream)
  8 | #   run2: Experiment 1 (Executor Failure & recovery)
  9 | #   run3: Experiment 2 (Spark Master/Driver Restart from same checkpoint)
 10 | #   run4: Experiment 3 (Event Disorder < 1 minute)
 11 | #   run5: Experiment 4 (Event Disorder > 1 minute)
```

**Lines 12–18.** Example run and Kafka settings. Angle-bracket IP placeholders must be replaced before a copied file can be sourced as a real shell configuration.

```text
 12 | RUN_ID=run1
 13 | 
 14 | # VM1 (Kafka Broker & Transaction Producer)
 15 | # Assigned ZeroTier IP: 172.23.211.163
 16 | KAFKA_BOOTSTRAP_SERVERS=<VM1_IP>:9092
 17 | KAFKA_TOPIC=transactions-run1
 18 | KAFKA_STARTING_OFFSETS=earliest
```

**Lines 19–23.** Example Spark master and app-name settings. The IP comment records your machine. An app name is a label; it does not select output tables.

```text
 19 | 
 20 | # VM2 (Spark Master & Driver)
 21 | # Assigned ZeroTier IP: 172.23.124.232
 22 | SPARK_MASTER_URL=spark://<VM2_IP>:7077
 23 | SPARK_APP_NAME=FraudDetectionEngine-run1
```

**Lines 24–31.** The updated comment combines Sumukh roles. The example HDFS address, warehouse and checkpoint require replacement with the real endpoint and the selected run.

```text
 24 | 
 25 | # Combined VM3 and VM4 on Sumukh's machine: HDFS, Iceberg, verifier,
 26 | # two Spark worker processes, Thrift Server and Streamlit dashboard.
 27 | # Assigned ZeroTier IP: 172.23.138.248
 28 | HDFS_NAMENODE=hdfs://<VM3_IP>:9000
 29 | ICEBERG_CATALOG_NAME=iceberg
 30 | ICEBERG_WAREHOUSE=hdfs://<VM3_IP>:9000/warehouse
 31 | CHECKPOINT_LOCATION=hdfs://<VM3_IP>:9000/checkpoints/run1
```

**Lines 32–35.** Explicit output table examples. Unlike derived defaults, these remain fixed until edited or overridden by the launcher run argument.

```text
 32 | 
 33 | # Iceberg Target Tables
 34 | DECISIONS_TABLE=iceberg.default.transaction_decisions_run1
 35 | ALERTS_TABLE=iceberg.default.account_risk_alerts_run1
```

**Lines 36–38.** The example trigger has a space without shell quotes. If copying this template into an actual sourced .env, write TRIGGER_INTERVAL="10 seconds", as your current .env does. Otherwise the shell interprets seconds as a command. This guide explains the template without modifying it.

```text
 36 | 
 37 | # Streaming Micro-Batch Trigger (agreed with VM1 producer hold delays)
 38 | TRIGGER_INTERVAL=10 seconds
```

## vm2/scripts/submit_job.sh

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/scripts/submit_job.sh`

**Lines 1–4.** The shebang chooses Bash. The comment describes the script. set -e stops the script after many kinds of unhandled command failure; it is not a guarantee that every possible shell failure is caught.

```text
  1 | #!/usr/bin/env bash
  2 | # Submit the PySpark Structured Streaming fraud monitoring job from VM2
  3 | set -e
  4 | 
```

**Lines 5–7.** BASH_SOURCE identifies this script. dirname finds its directory; cd plus pwd produces an absolute path. STREAMING_DIR is vm2, one directory above scripts. This lets the script locate files even when invoked from elsewhere.

```text
  5 | SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  6 | STREAMING_DIR="$(dirname "${SCRIPT_DIR}")"
  7 | 
```

**Lines 8–14.** If config/.env exists, source it in this shell. set -a automatically exports assignments to child processes; set +a ends that mode. This is why Python can read the settings with os.getenv. Existing shell values are overwritten by assignments in .env.

```text
  8 | # Load environment configuration if available
  9 | if [ -f "${STREAMING_DIR}/config/.env" ]; then
 10 |     echo "Loading environment variables from config/.env"
 11 |     set -a
 12 |     source "${STREAMING_DIR}/config/.env"
 13 |     set +a
 14 | fi
```

**Lines 15–16.** Use an existing SPARK_HOME or the installed Spark path. ${name:-fallback} uses the fallback when a variable is missing or empty.

```text
 15 | 
 16 | SPARK_HOME=${SPARK_HOME:-/home/shashank/opt/spark}
```

**Lines 17–18.** Read IPv4 addresses on the named ZeroTier interface, extract the address with a regular expression, and tolerate an unavailable interface using || true. Prefer an explicit driver host, then the discovered address, then the known fallback.

```text
 17 | ZT_IP=$(ip -4 addr show ztr2qxeeov 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' || true)
 18 | DRIVER_HOST=${SPARK_DRIVER_HOST:-${ZT_IP:-172.23.124.232}}
```

**Lines 19–20.** Select the master URL and default run ID. A variable assignment here is distinct from starting the master daemon.

```text
 19 | SPARK_MASTER=${SPARK_MASTER_URL:-spark://${DRIVER_HOST}:7077}
 20 | RUN_ID=${RUN_ID:-run1}
```

**Lines 21–26.** Allow zero or one positional run argument. $# counts arguments. More than one prints usage to standard error and exits with code 2.

```text
 21 | # Optional run argument overrides the loaded .env and selects isolated outputs.
 22 | # No argument preserves the existing configuration for checkpoint recovery.
 23 | if [ "$#" -gt 1 ]; then
 24 |     echo "Usage: $0 [run-id]" >&2
 25 |     exit 2
 26 | fi
```

**Lines 27–31.** When a run argument exists, validate $1 with a Bash regular expression. Letters, digits and underscores are accepted; punctuation that would break SQL identifiers is rejected.

```text
 27 | if [ "$#" -eq 1 ]; then
 28 |     if [[ ! "$1" =~ ^[A-Za-z0-9_]+$ ]]; then
 29 |         echo "Run ID must contain only letters, numbers, and underscores." >&2
 30 |         exit 2
 31 |     fi
```

**Lines 32–37.** Export the chosen run and matching topic, checkpoint and table identifiers together. The checkpoint is based on HDFS_NAMENODE. With no argument, the loaded .env and Python defaults remain in charge.

```text
 32 |     export RUN_ID="$1"
 33 |     export KAFKA_TOPIC="transactions-${RUN_ID}"
 34 |     export CHECKPOINT_LOCATION="${HDFS_NAMENODE:-hdfs://172.23.138.248:9000}/checkpoints/${RUN_ID}"
 35 |     export DECISIONS_TABLE="${ICEBERG_CATALOG_NAME:-iceberg}.default.transaction_decisions_${RUN_ID}"
 36 |     export ALERTS_TABLE="${ICEBERG_CATALOG_NAME:-iceberg}.default.account_risk_alerts_${RUN_ID}"
 37 | fi
```

**Lines 38–42.** Define Maven dependency coordinates for Spark Kafka and Iceberg. _2.12 is the Scala binary version. 3.3 identifies the Iceberg build for the Spark family; 1.4.1 is the Iceberg version. PACKAGES joins coordinates with a comma.

```text
 38 | 
 39 | # Package coordinates for Spark 3.3.4 (Scala 2.12)
 40 | KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4"
 41 | ICEBERG_PKG="org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.1"
 42 | PACKAGES="${KAFKA_PKG},${ICEBERG_PKG}"
```

**Lines 43–45.** A Bash array contains optional --packages arguments. USE_PACKAGES=1 adds dependency resolution; zero leaves an empty array. Expanding the quoted array later preserves argument boundaries.

```text
 43 | # USE_PACKAGES=0 means the jars are pre-installed in $SPARK_HOME/jars on every machine (nothing is downloaded over the network)
 44 | PKG_FLAGS=()
 45 | if [ "${USE_PACKAGES:-1}" = "1" ]; then PKG_FLAGS=(--packages "${PACKAGES}"); fi
```

**Lines 46–49.** Export the master URL and Python interpreters for the launched process. Explicit spark.pyspark properties later also pass these choices to Spark.

```text
 46 | 
 47 | export SPARK_MASTER_URL="${SPARK_MASTER}"
 48 | export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-/home/shashank/opt/python3.10/bin/python3.10}"
 49 | export PYSPARK_PYTHON="${PYSPARK_PYTHON:-/usr/bin/python3.10}"
```

**Lines 50–59.** Print the selected configuration for the operator. These echo lines do not create a connection, execute tasks or establish that the job works.

```text
 50 | 
 51 | echo "=========================================================="
 52 | echo " Submitting Fraud Detection Engine (VM2 Driver)"
 53 | echo " Run ID: ${RUN_ID}"
 54 | echo " Spark Master: ${SPARK_MASTER}"
 55 | echo " Driver Host (ZeroTier): ${DRIVER_HOST}"
 56 | echo " Driver Python: ${PYSPARK_DRIVER_PYTHON}"
 57 | echo " Worker Python: ${PYSPARK_PYTHON}"
 58 | echo " Packages: ${PACKAGES}"
 59 | echo "=========================================================="
```

**Lines 60–63.** Change to vm2 and prepend it to Python import paths so app and config are importable. PYTHONPATH contains module search directories, not storage paths for transaction data.

```text
 60 | 
 61 | cd "${STREAMING_DIR}"
 62 | 
 63 | export PYTHONPATH="${STREAMING_DIR}:${PYTHONPATH}"
```

**Lines 64–66.** Zip app and config into a temporary package. -q is quiet and -r includes subdirectories. The current zip command includes everything beneath those folders, including .env and any caches; it is not a filtered source-only package. Do not add credentials there expecting this ZIP to exclude them.

```text
 64 | 
 65 | # Package application modules so remote executors on VM4 can import them cleanly
 66 | zip -q -r /tmp/fraud_engine_modules.zip app config
```

**Lines 67–70.** Launch Spark using the installed spark-submit, select the standalone cluster and set the display name. A trailing backslash continues the shell command on the next line.

```text
 67 | 
 68 | "${SPARK_HOME}/bin/spark-submit" \
 69 |     --master "${SPARK_MASTER}" \
 70 |     --name "FraudDetectionEngine-${RUN_ID}" \
```

**Lines 71–72.** Expand optional dependency flags and attach the Python module ZIP. --py-files distributes Python importable code, not Java JARs or complete Python environments.

```text
 71 |     "${PKG_FLAGS[@]}" \
 72 |     --py-files /tmp/fraud_engine_modules.zip \
```

**Lines 73–75.** Advertise the reachable driver address and explicitly select driver and executor Python paths. The driver address is not the Kafka or HDFS endpoint.

```text
 73 |     --conf "spark.driver.host=${DRIVER_HOST}" \
 74 |     --conf "spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON}" \
 75 |     --conf "spark.pyspark.python=${PYSPARK_PYTHON}" \
```

**Line 76.** Set shuffle partitions, default four, for distributed redistribution/state. This number is unrelated to Kafka topic partitions and Iceberg table partitions. Keep compatible state partitioning for checkpoint recovery.

```text
 76 |     --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
```

**Lines 77–80.** Enable Iceberg SQL commands, register catalog iceberg using SparkCatalog, select its Hadoop implementation and set the HDFS warehouse. --conf supplies Spark properties.

```text
 77 |     --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
 78 |     --conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
 79 |     --conf "spark.sql.catalog.iceberg.type=hadoop" \
 80 |     --conf "spark.sql.catalog.iceberg.warehouse=${ICEBERG_WAREHOUSE:-hdfs://172.23.138.248:9000/warehouse}" \
```

**Lines 81–83.** Use UTC for Spark SQL timestamps and select app/stream_job.py as the Python entrypoint. The final filename tells spark-submit what application to execute.

```text
 81 |     --conf "spark.sql.session.timeZone=UTC" \
 82 |     app/stream_job.py
 83 | 
```

## vm2/app/stream_job.py

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/app/stream_job.py`

**Lines 1–11.** Module documentation and spacing. This explains the intended application but does not execute it. The actual deduplication key below includes both transaction ID and event time.

```text
  1 | """Main PySpark Structured Streaming application for Real-Time Fraud Monitoring.
  2 | 
  3 | Runs exclusively on VM2 (Spark Master/Driver).
  4 | - Ingests payment events from Apache Kafka (VM1)
  5 | - Parses JSON with explicit StringType schema and casts to target types
  6 | - Applies 1-minute event-time watermark and deduplication on transaction_id
  7 | - Evaluates fraud rules R1–R5 over rolling 2-min windows (30s slide) across cumulative history
  8 | - Writes outputs to Apache Iceberg tables backed by HDFS (VM3) with MERGE INTO idempotency
  9 | - Checkpoints state strictly to HDFS
 10 | """
 11 | 
```

**Lines 12–16.** Import logging, process exit utilities, Pandas, SparkSession and Spark column/JSON/timestamp expression constructors. pd is the conventional short alias for pandas.

```text
 12 | import logging
 13 | import sys
 14 | import pandas as pd
 15 | from pyspark.sql import SparkSession
 16 | from pyspark.sql.functions import col, from_json, to_timestamp
```

**Lines 17–25.** Import the instantiated settings object, three schema constants, the rule evaluator and the table writer functions. These imports link the modules in your application.

```text
 17 | 
 18 | from config.config import config
 19 | from app.schema import (
 20 |     ACCOUNT_RISK_ALERTS_SCHEMA,
 21 |     KAFKA_TRANSACTION_SCHEMA,
 22 |     TRANSACTION_DECISIONS_SCHEMA,
 23 | )
 24 | from app.rules import evaluate_batch
 25 | from app.writers import initialize_iceberg_tables, write_micro_batch_to_iceberg
```

**Lines 26–32.** Configure INFO-level Python messages with time, severity and logger name. This is Python logging; sparkContext.setLogLevel below controls Spark logging separately.

```text
 26 | 
 27 | # Configure logging
 28 | logging.basicConfig(
 29 |     level=logging.INFO,
 30 |     format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
 31 | )
 32 | logger = logging.getLogger("FraudMonitoringJob")
```

**Lines 33–41.** List the six event fields retained for cumulative evaluation. Fields such as location and device_id exist in incoming data but are not used by these fraud rules.

```text
 33 | 
 34 | EVENT_COLS = [
 35 |     "transaction_id",
 36 |     "source_account",
 37 |     "destination_account",
 38 |     "event_timestamp",
 39 |     "amount",
 40 |     "currency",
 41 | ]
```

**Lines 42–49.** Define a session-building function. -> SparkSession is a type hint. Give the app its configured name and set the master only if one is explicitly configured.

```text
 42 | 
 43 | 
 44 | def build_spark_session() -> SparkSession:
 45 |     """Builds and configures the SparkSession with Iceberg catalog and extensions."""
 46 |     logger.info("Initializing SparkSession (configured master: '%s')", config.spark_master_url or "from spark-submit")
 47 |     builder = SparkSession.builder.appName(config.spark_app_name)
 48 |     if config.spark_master_url:
 49 |         builder = builder.master(config.spark_master_url)
```

**Lines 50–57.** Begin a chained builder expression and enable Iceberg SQL extensions, needed for operations such as MERGE. Parentheses let the expression span lines.

```text
 50 | 
 51 |     builder = (
 52 |         builder
 53 |         # Iceberg SQL Extensions
 54 |         .config(
 55 |             "spark.sql.extensions",
 56 |             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
 57 |         )
```

**Lines 58–66.** Register the configured catalog name as a SparkCatalog backed by the Hadoop catalog implementation. This is configuration, not launching a separate Iceberg server.

```text
 58 |         # Iceberg Hadoop Catalog configuration on HDFS
 59 |         .config(
 60 |             f"spark.sql.catalog.{config.iceberg_catalog_name}",
 61 |             "org.apache.iceberg.spark.SparkCatalog"
 62 |         )
 63 |         .config(
 64 |             f"spark.sql.catalog.{config.iceberg_catalog_name}.type",
 65 |             "hadoop"
 66 |         )
```

**Lines 67–70.** Point that catalog to the configured warehouse. Spark resolves table metadata and data through this storage location.

```text
 67 |         .config(
 68 |             f"spark.sql.catalog.{config.iceberg_catalog_name}.warehouse",
 69 |             config.iceberg_warehouse
 70 |         )
```

**Lines 71–75.** Set SQL time zone to UTC and enable deletion of temporary checkpoint locations when applicable. The latter does not mean Spark deletes your explicitly supplied persistent HDFS checkpoint. getOrCreate obtains the session.

```text
 71 |         # Adaptive execution & time zones
 72 |         .config("spark.sql.session.timeZone", "UTC")
 73 |         .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
 74 |     )
 75 |     return builder.getOrCreate()
```

**Lines 76–84.** Define the callback invoked by foreachBatch. batch_df is a Spark DataFrame for that batch; batch_id identifies the batch. Function documentation explains cumulative rather than isolated-batch evaluation.

```text
 76 | 
 77 | 
 78 | def process_micro_batch(batch_df, batch_id: int):
 79 |     """Processes each streaming micro-batch.
 80 |     
 81 |     Evaluates cumulative window state by combining previously stored events
 82 |     with newly arrived records, ensuring sliding windows spanning micro-batches
 83 |     and late arrivals (< 1 min) are accurately resolved and updated via MERGE.
 84 |     """
```

**Lines 85–87.** Check whether the batch RDD has any rows. This is an action that can execute Spark work. If empty, log and return without evaluating or writing anything.

```text
 85 |     if batch_df.rdd.isEmpty():
 86 |         logger.info("[Batch %d] Empty batch received; skipping.", batch_id)
 87 |         return
```

**Lines 88–90.** Count input rows for the log, then obtain the SparkSession associated with the batch. Counting is another action.

```text
 88 | 
 89 |     logger.info("[Batch %d] Processing micro-batch with %d records.", batch_id, batch_df.count())
 90 |     spark = batch_df.sparkSession
```

**Lines 91–95.** Select the six event columns and collect them into a local Pandas frame named new_pdf. If it is empty, return. The collection moves data into driver memory.

```text
 91 | 
 92 |     # Select event columns from the current micro-batch
 93 |     new_pdf = batch_df.select(*EVENT_COLS).toPandas()
 94 |     if new_pdf.empty:
 95 |         return
```

**Lines 96–99.** Read all stored decisions, select the original event fields, and collect them to Pandas too. This reconstructs historical events from the output table rather than a separate in-memory state service.

```text
 96 | 
 97 |     # Read previously stored events from the Iceberg decisions table to maintain cross-batch window state
 98 |     try:
 99 |         stored = spark.table(config.decisions_table).select(*EVENT_COLS).toPandas()
```

**Lines 100–103.** Any exception while reading history is caught and treated as no stored history. This also catches network/read failures, even though the log describes a missing table. Understand this limitation: the handler does not distinguish first-run absence from other failures.

```text
100 |     except Exception as e:
101 |         logger.info("[Batch %d] No existing decisions table found (%s); starting with current batch.", batch_id, e)
102 |         stored = pd.DataFrame(columns=EVENT_COLS)
103 | 
```

**Lines 104–109.** Concatenate history before new events; remove repeated transaction IDs, retaining the first occurrence by default; reset row numbering. A repeated ID with changed fields would keep the older stored row. The supplied duplicates have matching content.

```text
104 |     # Combine historical events with new micro-batch events
105 |     all_pdf = (
106 |         pd.concat([stored, new_pdf], ignore_index=True)
107 |         .drop_duplicates(subset=["transaction_id"])
108 |         .reset_index(drop=True)
109 |     )
```

**Lines 110–112.** Call the Pandas evaluator on all retained unique events. It returns two Pandas frames: transaction decisions and account/window alerts.

```text
110 | 
111 |     # Re-evaluate R1–R5 rules, sliding windows, and alert linkages across cumulative stream
112 |     df_decisions, df_alerts = evaluate_batch(all_pdf)
```

**Lines 113–115.** Convert decisions back to a Spark DataFrame with explicit column names and types. This is the boundary from local rule results back to Spark processing.

```text
113 | 
114 |     # Convert evaluated results back to PySpark DataFrames with strict schemas
115 |     spark_decisions = spark.createDataFrame(df_decisions, schema=TRANSACTION_DECISIONS_SCHEMA)
```

**Lines 116–119.** If alerts exist, convert them to a list of row dictionaries. Iterate over every row and field to normalize Pandas values before strict Spark conversion.

```text
116 |     if not df_alerts.empty:
117 |         records_alerts = df_alerts.to_dict(orient="records")
118 |         for r in records_alerts:
119 |             for k, v in r.items():
```

**Lines 120–123.** Convert Pandas missing values such as NaN/NaT to Python None. Convert a Pandas Timestamp to a standard Python datetime. These conversions avoid incompatible nullable/timestamp objects reaching Spark.

```text
120 |                 if pd.isna(v):
121 |                     r[k] = None
122 |                 elif isinstance(v, pd.Timestamp):
123 |                     r[k] = v.to_pydatetime()
```

**Lines 124–127.** Coerce account/count fields to Python ints and amount fields to floats, preserving missing values as None. This matters when Pandas nullable columns otherwise promote integer values to floating point.

```text
124 |                 elif k in ("source_account", "destination_account", "observed_count", "count_threshold") and v is not None:
125 |                     r[k] = int(v)
126 |                 elif k in ("observed_amount_inr", "amount_threshold_inr") and v is not None:
127 |                     r[k] = float(v)
```

**Lines 128–130.** Build the alerts Spark frame with its schema. If no alerts were produced, use None so the writer skips that table for this callback.

```text
128 |         spark_alerts = spark.createDataFrame(records_alerts, schema=ACCOUNT_RISK_ALERTS_SCHEMA)
129 |     else:
130 |         spark_alerts = None
```

**Lines 131–141.** Call the sink with the session, both frames, batch ID and table names. Named arguments make the mapping explicit. use_merge=True chooses upsert behavior instead of raw append.

```text
131 | 
132 |     # Idempotent write to Iceberg tables on HDFS (MERGE INTO by primary keys)
133 |     write_micro_batch_to_iceberg(
134 |         spark=spark,
135 |         decisions_df=spark_decisions,
136 |         alerts_df=spark_alerts,
137 |         batch_id=batch_id,
138 |         decisions_table=config.decisions_table,
139 |         alerts_table=config.alerts_table,
140 |         use_merge=True
141 |     )
```

**Lines 142–145.** Log the total evaluated decision and alert counts. Because evaluation is cumulative, these numbers are not necessarily counts of newly arriving rows. Blank lines separate the next function.

```text
142 |     logger.info("[Batch %d] Successfully updated %d decisions and %d alerts in Iceberg.",
143 |                 batch_id, len(df_decisions), len(df_alerts))
144 | 
145 | 
```

**Lines 146–150.** Define main and log the run, source, checkpoint and target names. Read these lines in a captured log to establish which run actually launched.

```text
146 | def main():
147 |     logger.info("=== Starting Fraud Monitoring Streaming Pipeline (Run: %s) ===", config.run_id)
148 |     logger.info("Kafka Bootstrap: %s | Topic: %s", config.kafka_bootstrap_servers, config.kafka_topic)
149 |     logger.info("HDFS Checkpoint: %s", config.checkpoint_location)
150 |     logger.info("Iceberg Tables: %s, %s", config.decisions_table, config.alerts_table)
```

**Lines 151–156.** Create the Spark session, reduce Spark logs to WARN, and ensure both Iceberg tables exist before starting input processing.

```text
151 | 
152 |     spark = build_spark_session()
153 |     spark.sparkContext.setLogLevel("WARN")
154 | 
155 |     # Initialize Iceberg tables
156 |     initialize_iceberg_tables(spark, config.decisions_table, config.alerts_table)
```

**Lines 157–167.** Define the Kafka streaming source. format("kafka") chooses the connector; bootstrap selects discovery; subscribe selects the topic; startingOffsets sets the new-query start policy; load builds the source DataFrame.

```text
157 | 
158 |     # 1. Read Kafka streaming source
159 |     logger.info("Subscribing to Kafka topic: %s", config.kafka_topic)
160 |     kafka_raw_stream = (
161 |         spark.readStream
162 |         .format("kafka")
163 |         .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
164 |         .option("subscribe", config.kafka_topic)
165 |         .option("startingOffsets", config.kafka_starting_offsets)
166 |         .load()
167 |     )
```

**Lines 168–174.** Cast Kafka value bytes to text, parse JSON using the explicit schema into a struct called data, and expand that struct into columns. Kafka keys are not used by this parser; transaction_id is taken from the payload.

```text
168 | 
169 |     # 2. Parse JSON payload with StringType schema and explicitly cast columns
170 |     parsed_stream = (
171 |         kafka_raw_stream
172 |         .selectExpr("CAST(value AS STRING) as json_val")
173 |         .select(from_json(col("json_val"), KAFKA_TRANSACTION_SCHEMA).alias("data"))
174 |         .select("data.*")
```

**Lines 175–180.** Convert the event-time string to a timestamp; account IDs to long integers; amount to double; age to integer. These are Spark expression transformations, not Python row-by-row casts. Invalid input handling is not an explicit quarantine pipeline here.

```text
175 |         .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
176 |         .withColumn("source_account", col("source_account").cast("long"))
177 |         .withColumn("destination_account", col("destination_account").cast("long"))
178 |         .withColumn("amount", col("amount").cast("double"))
179 |         .withColumn("account_age_days", col("account_age_days").cast("int"))
180 |     )
```

**Lines 181–187.** Attach a one-minute event-time watermark and deduplicate on the pair transaction_id,event_timestamp. Identical IDs with different timestamps are distinct at this operator; the later Pandas deduplication uses ID alone.

```text
181 | 
182 |     # 3. Apply 1-minute event-time watermark and deduplication on transaction_id
183 |     watermarked_stream = (
184 |         parsed_stream
185 |         .withWatermark("event_timestamp", config.watermark_delay)
186 |         .dropDuplicates(["transaction_id", "event_timestamp"])
187 |     )
```

**Lines 188–196.** Use a streaming writer with the callback, explicit persistent checkpoint and configured processing-time trigger. start launches the query. Merely defining the previous DataFrames would not launch it.

```text
188 | 
189 |     # 4. Write stream using foreachBatch to evaluate rules cumulatively and sink to Iceberg
190 |     query = (
191 |         watermarked_stream
192 |         .writeStream
193 |         .foreachBatch(process_micro_batch)
194 |         .option("checkpointLocation", config.checkpoint_location)
195 |         .trigger(processingTime=config.trigger_interval)
196 |         .start()
```

**Lines 197–201.** Keep the driver alive while the streaming query runs. A streaming application normally waits indefinitely for new input rather than exiting just because a producer finished.

```text
197 |     )
198 | 
199 |     logger.info("Streaming query started. Awaiting termination...")
200 |     try:
201 |         query.awaitTermination()
```

**Lines 202–204.** Handle an interrupt, such as Ctrl+C delivered to Python, by logging and requesting a graceful query stop.

```text
202 |     except KeyboardInterrupt:
203 |         logger.info("Keyboard interrupt received; stopping query.")
204 |         query.stop()
```

**Lines 205–207.** Other query failures are logged and lead to process exit code 1. A nonzero code signals failure to the launcher/operator.

```text
205 |     except Exception as e:
206 |         logger.error("Streaming query failed: %s", e)
207 |         sys.exit(1)
```

**Lines 208–211.** The main guard runs main only when this file is executed as the application, not merely imported by another Python module.

```text
208 | 
209 | 
210 | if __name__ == "__main__":
211 |     main()
```

## vm2/app/schema.py

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/app/schema.py`

**Lines 1–8.** Documentation names the three schema constants. These define the structure of records, rather than loading any data.

```text
  1 | """Schema definitions for PySpark Structured Streaming fraud detection pipeline.
  2 | 
  3 | Contains explicit StructType schemas for:
  4 | 1. Inbound Kafka JSON transaction events (all StringType to handle quoted/unquoted JSON)
  5 | 2. Apache Iceberg target table: transaction_decisions
  6 | 3. Apache Iceberg target table: account_risk_alerts
  7 | """
  8 | 
```

**Lines 9–18.** Import Spark type objects. String is text; long is a 64-bit integer; integer is 32-bit; double is a floating-point number; boolean is true/false; timestamp represents time; StructType is a record schema and StructField is a column definition.

```text
  9 | from pyspark.sql.types import (
 10 |     BooleanType,
 11 |     DoubleType,
 12 |     IntegerType,
 13 |     LongType,
 14 |     StringType,
 15 |     StructField,
 16 |     StructType,
 17 |     TimestampType,
 18 | )
```

**Lines 19–23.** Declare the inbound record schema. All payload fields initially use text because the producer reads the input as strings. StructField(name, type, nullable) describes a column. nullable=False expresses a schema constraint, not a complete input-validation or quarantine implementation.

```text
 19 | 
 20 | # Inbound Kafka transaction payload schema
 21 | # Every field is declared as StringType because JSON serializers may quote numeric values.
 22 | # Type casting is performed explicitly in the streaming pipeline.
 23 | KAFKA_TRANSACTION_SCHEMA = StructType([
```

**Line 24.** event_id identifies an input event, including a duplicate delivery row; it differs from the logical payment transaction ID.

```text
 24 |     StructField("event_id", StringType(), True),
```

**Line 25.** transaction_id identifies the payment and is used for downstream deduplication and MERGE matching.

```text
 25 |     StructField("transaction_id", StringType(), False),
```

**Lines 26–27.** user_id and customer_id identify people/entities in the input. The current six-column rule path does not use them.

```text
 26 |     StructField("user_id", StringType(), True),
 27 |     StructField("customer_id", StringType(), True),
```

**Lines 28–29.** source_account is the sending account; destination_account is the receiving account. Both are initially text, then cast to longs.

```text
 28 |     StructField("source_account", StringType(), True),
 29 |     StructField("destination_account", StringType(), True),
```

**Lines 30–31.** merchant_id and device_id are available input attributes but are not used by R1–R5.

```text
 30 |     StructField("merchant_id", StringType(), True),
 31 |     StructField("device_id", StringType(), True),
```

**Lines 32–34.** event_timestamp is the payment occurrence time; amount is the payment value; currency determines whether INR-only rules apply. They are parsed/cast later.

```text
 32 |     StructField("event_timestamp", StringType(), False),
 33 |     StructField("amount", StringType(), False),
 34 |     StructField("currency", StringType(), False),
```

**Lines 35–38.** transaction_type, location, channel and account_age_days are additional input attributes. Age is cast but not used by the fraud evaluator.

```text
 35 |     StructField("transaction_type", StringType(), True),
 36 |     StructField("location", StringType(), True),
 37 |     StructField("channel", StringType(), True),
 38 |     StructField("account_age_days", StringType(), True),
```

**Lines 39–42.** Close the input schema and begin the transaction-decision schema.

```text
 39 | ])
 40 | 
 41 | # Iceberg Table 1: transaction_decisions (one row per transaction)
 42 | TRANSACTION_DECISIONS_SCHEMA = StructType([
```

**Lines 43–49.** Store the original payment ID, sending/receiving accounts, event time, evaluation time, amount and currency with their final Spark types. Nullable evaluation time can be missing.

```text
 43 |     StructField("transaction_id", StringType(), False),
 44 |     StructField("source_account", LongType(), True),
 45 |     StructField("destination_account", LongType(), True),
 46 |     StructField("event_timestamp", TimestampType(), False),
 47 |     StructField("evaluated_at", TimestampType(), True),
 48 |     StructField("amount", DoubleType(), False),
 49 |     StructField("currency", StringType(), False),
```

**Lines 50–53.** Store one boolean per active rule R1–R4. An overlapping alert does not add a second copy of the same rule flag.

```text
 50 |     StructField("r1_triggered", BooleanType(), False),
 51 |     StructField("r2_triggered", BooleanType(), False),
 52 |     StructField("r3_triggered", BooleanType(), False),
 53 |     StructField("r4_triggered", BooleanType(), False),
```

**Lines 54–57.** Store integer risk score, block boolean, human-readable decision and related-alert IDs encoded as JSON text. related_alert_ids is StringType, not a native Spark array.

```text
 54 |     StructField("r5_risk_score", IntegerType(), False),
 55 |     StructField("r5_block_triggered", BooleanType(), False),
 56 |     StructField("decision", StringType(), False),
 57 |     StructField("related_alert_ids", StringType(), False),
```

**Lines 58–61.** Close decisions and begin the account/window alert schema.

```text
 58 | ])
 59 | 
 60 | # Iceberg Table 2: account_risk_alerts (one row per account, window and rule: R2, R3, R4)
 61 | ACCOUNT_RISK_ALERTS_SCHEMA = StructType([
```

**Lines 62–65.** Store alert ID, rule ID, source account and optional destination. R2/R3 have no single destination; R4 carries the selected pattern destination.

```text
 62 |     StructField("alert_id", StringType(), False),
 63 |     StructField("rule_id", StringType(), False),
 64 |     StructField("source_account", LongType(), True),
 65 |     StructField("destination_account", LongType(), True),
```

**Lines 66–69.** Store the latest supporting payment ID, finding scope (WINDOW/PATTERN), and half-open observation-window endpoints.

```text
 66 |     StructField("trigger_transaction_id", StringType(), False),
 67 |     StructField("finding_scope", StringType(), False),
 68 |     StructField("window_start", TimestampType(), False),
 69 |     StructField("window_end", TimestampType(), False),
```

**Lines 70–73.** Store observed count/INR sum and the thresholds used. Amount-related columns can be null for count-only rules.

```text
 70 |     StructField("observed_count", LongType(), False),
 71 |     StructField("observed_amount_inr", DoubleType(), True),
 72 |     StructField("count_threshold", LongType(), False),
 73 |     StructField("amount_threshold_inr", DoubleType(), True),
```

**Lines 74–77.** Store supporting transaction IDs as JSON text, the explanation string and an evaluation timestamp, then close the schema.

```text
 74 |     StructField("supporting_transaction_ids", StringType(), False),
 75 |     StructField("explanation", StringType(), False),
 76 |     StructField("triggered_at", TimestampType(), True),
 77 | ])
```

## vm2/app/rules.py

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/app/rules.py`

**Lines 1–15.** The docstring specifies deterministic rules and half-open account-window association. The module does not train a model or call a banking API.

```text
  1 | """Fraud detection rule engine (R1–R5).
  2 | 
  3 | Implements the exact business logic and mathematical criteria for:
  4 | - R1: Single INR payment of >= 50,000 (Stateless)
  5 | - R2: >= 5 payments from one account in rolling 2-min window (sliding every 30s)
  6 | - R3: >= 2 INR payments from one account totalling > 600,000 in rolling 2-min window
  7 | - R4: >= 3 payments from one account to the same destination in rolling 2-min window
  8 | - R5: Composite decision (score >= 2: BLOCK, score == 1: REVIEW, 0: APPROVE)
  9 | 
 10 | Window Interval Semantics:
 11 | - Windows are half-open: [window_start, window_end).
 12 | - An account alert active in [window_start, window_end) is linked to all
 13 |   transactions of that source account whose event_timestamp falls in that window.
 14 | """
 15 | 
```

**Lines 16–19.** Import time utilities, JSON encoding, typing annotations and Pandas. Some typing imports are unused; type annotations do not run distributed computations.

```text
 16 | from datetime import datetime, timedelta, timezone
 17 | import json
 18 | from typing import Dict, List, Optional, Set, Tuple
 19 | import pandas as pd
```

**Lines 20–28.** Define the timestamp-to-windows helper. It returns a list of (start,end) timestamp pairs. The docstring explains alignment and overlap.

```text
 20 | 
 21 | 
 22 | def compute_windows_for_timestamp(ts: pd.Timestamp) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
 23 |     """Computes all 2-minute rolling windows (30s slide) containing event timestamp ts.
 24 |     
 25 |     In Spark SQL, sliding windows are aligned to 00s and 30s.
 26 |     A 120s window sliding every 30s covers 4 overlapping windows for any given instant.
 27 |     Interval is half-open: [w_start, w_end).
 28 |     """
```

**Lines 29–31.** Compute how far the timestamp lies past the previous 30-second boundary and subtract that plus microseconds. For 10:01:10, floor_ts becomes 10:01:00.

```text
 29 |     # Align to 30-second boundary
 30 |     base_seconds = (ts.minute * 60 + ts.second) % 30
 31 |     floor_ts = ts - timedelta(seconds=base_seconds, microseconds=ts.microsecond)
```

**Lines 32–38.** Start an empty list; subtract 0,30,60,90 seconds from floor_ts for four starts; add 120 seconds for each end; append each pair and return the list.

```text
 32 | 
 33 |     windows = []
 34 |     for offset_seconds in (0, 30, 60, 90):
 35 |         w_start = floor_ts - timedelta(seconds=offset_seconds)
 36 |         w_end = w_start + timedelta(seconds=120)
 37 |         windows.append((w_start, w_end))
 38 |     return windows
```

**Lines 39–44.** Define evaluate_batch. It accepts a Pandas transaction frame and an optional evaluation timestamp, returning a pair of Pandas frames.

```text
 39 | 
 40 | 
 41 | def evaluate_batch(
 42 |     df_transactions: pd.DataFrame,
 43 |     evaluated_at: Optional[datetime] = None
 44 | ) -> Tuple[pd.DataFrame, pd.DataFrame]:
```

**Lines 45–57.** Document expected columns and returned decisions/alerts. The caller is responsible for providing unique valid transactions; this function does not deduplicate its own input.

```text
 45 |     """Evaluates a batch of transactions against fraud rules R1–R5.
 46 | 
 47 |     Args:
 48 |         df_transactions: pandas DataFrame containing unique transactions with columns:
 49 |             transaction_id, source_account, destination_account,
 50 |             event_timestamp, amount, currency
 51 |         evaluated_at: Optional timestamp of evaluation (defaults to utcnow)
 52 | 
 53 |     Returns:
 54 |         Tuple of (df_decisions, df_alerts):
 55 |             df_decisions: 1 row per unique transaction_id
 56 |             df_alerts: 1 row per triggered account risk alert (R2, R3, R4)
 57 |     """
```

**Lines 58–64.** If there are no input rows, construct an empty decisions frame with the expected column names. These are column labels, not records.

```text
 58 |     if df_transactions.empty:
 59 |         empty_decisions = pd.DataFrame(columns=[
 60 |             "transaction_id", "source_account", "destination_account",
 61 |             "event_timestamp", "evaluated_at", "amount", "currency",
 62 |             "r1_triggered", "r2_triggered", "r3_triggered", "r4_triggered",
 63 |             "r5_risk_score", "r5_block_triggered", "decision", "related_alert_ids"
 64 |         ])
```

**Lines 65–72.** Construct the equivalent empty alerts frame and return both immediately.

```text
 65 |         empty_alerts = pd.DataFrame(columns=[
 66 |             "alert_id", "rule_id", "source_account", "destination_account",
 67 |             "trigger_transaction_id", "finding_scope", "window_start",
 68 |             "window_end", "observed_count", "observed_amount_inr",
 69 |             "count_threshold", "amount_threshold_inr",
 70 |             "supporting_transaction_ids", "explanation", "triggered_at"
 71 |         ])
 72 |         return empty_decisions, empty_alerts
```

**Lines 73–78.** Use the provided evaluation time or current UTC stripped of timezone metadata. Copy the input, parse event times into helper column ts, and remove timezone metadata if present. tz_localize(None) removes the zone without converting the clock value; input times are expected to follow the UTC convention.

```text
 73 | 
 74 |     eval_time = evaluated_at or datetime.now(timezone.utc).replace(tzinfo=None)
 75 |     df = df_transactions.copy()
 76 |     df["ts"] = pd.to_datetime(df["event_timestamp"])
 77 |     if df["ts"].dt.tz is not None:
 78 |         df["ts"] = df["ts"].dt.tz_localize(None)
```

**Lines 79–85.** Build expanded window records. iterrows visits local Pandas rows; underscore ignores the index. Each input payment is associated with every containing window.

```text
 79 | 
 80 |     # 1. Expand transactions into participating 2-minute rolling windows (30s slide)
 81 |     window_records = []
 82 |     for _, row in df.iterrows():
 83 |         ts = row["ts"]
 84 |         windows = compute_windows_for_timestamp(ts)
 85 |         for w_start, w_end in windows:
```

**Lines 86–95.** For each payment/window pair, save payment ID, both accounts, numeric amount, normalized currency, event time and window endpoints. strip removes surrounding currency spaces; upper makes INR comparisons case-normalized.

```text
 86 |             window_records.append({
 87 |                 "transaction_id": row["transaction_id"],
 88 |                 "source_account": row["source_account"],
 89 |                 "destination_account": row["destination_account"],
 90 |                 "amount": float(row["amount"]),
 91 |                 "currency": str(row["currency"]).strip().upper(),
 92 |                 "ts": ts,
 93 |                 "w_start": w_start,
 94 |                 "w_end": w_end,
 95 |             })
```

**Lines 96–101.** Convert expanded rows to a frame, and initialize lists for final alerts and R4 candidates.

```text
 96 | 
 97 |     df_win = pd.DataFrame(window_records)
 98 | 
 99 |     # 2. Evaluate Rule R2 & R3 by grouping on (source_account, w_start, w_end)
100 |     alerts_list = []
101 |     r4_candidates = []
```

**Lines 102–107.** Group by source and window to evaluate R2/R3. Sort support by timestamp then ID. Build a compact start-time string for deterministic alert IDs. w_start_str and w_end_str are assigned but not used later.

```text
102 | 
103 |     for (acc, w_start, w_end), group in df_win.groupby(["source_account", "w_start", "w_end"]):
104 |         sorted_group = group.sort_values(by=["ts", "transaction_id"])
105 |         w_start_str = w_start.strftime("%Y-%m-%d %H:%M:%S")
106 |         w_end_str = w_end.strftime("%Y-%m-%d %H:%M:%S")
107 |         w_compact = w_start.strftime("%Y%m%d%H%M%S")
```

**Lines 108–113.** Count all payments in the group. If there are at least five, extract their ordered IDs and build the R2 alert ID.

```text
108 | 
109 |         # R2: >= 5 payments from one account
110 |         obs_count_r2 = len(sorted_group)
111 |         if obs_count_r2 >= 5:
112 |             supp_r2 = sorted_group["transaction_id"].tolist()
113 |             alert_id_r2 = f"R2-{acc}-{w_compact}"
```

**Lines 114–123.** Begin the R2 alert record: identify rule/source, no destination, latest supporting payment, WINDOW scope, endpoints and observed count.

```text
114 |             alerts_list.append({
115 |                 "alert_id": alert_id_r2,
116 |                 "rule_id": "R2",
117 |                 "source_account": acc,
118 |                 "destination_account": None,
119 |                 "trigger_transaction_id": supp_r2[-1],
120 |                 "finding_scope": "WINDOW",
121 |                 "window_start": w_start,
122 |                 "window_end": w_end,
123 |                 "observed_count": obs_count_r2,
```

**Lines 124–130.** Complete R2 with no amount metric, count threshold five, JSON-encoded support IDs, readable explanation and evaluation time, then append it.

```text
124 |                 "observed_amount_inr": None,
125 |                 "count_threshold": 5,
126 |                 "amount_threshold_inr": None,
127 |                 "supporting_transaction_ids": json.dumps(supp_r2),
128 |                 "explanation": f"Account made {obs_count_r2} transactions in a 120s window.",
129 |                 "triggered_at": eval_time
130 |             })
```

**Lines 131–138.** For R3, select only INR payments, count them, sum amounts, and require at least two with total strictly greater than 600000. Extract ordered support IDs and build the R3 ID.

```text
131 | 
132 |         # R3: >= 2 INR payments totalling > 600,000 INR
133 |         inr_group = sorted_group[sorted_group["currency"] == "INR"]
134 |         obs_count_r3 = len(inr_group)
135 |         obs_amount_r3 = inr_group["amount"].sum()
136 |         if obs_count_r3 >= 2 and obs_amount_r3 > 600000.0:
137 |             supp_r3 = inr_group["transaction_id"].tolist()
138 |             alert_id_r3 = f"R3-{acc}-{w_compact}"
```

**Lines 139–148.** Begin the R3 record with source/window identity, latest INR support, WINDOW scope, endpoints and INR payment count.

```text
139 |             alerts_list.append({
140 |                 "alert_id": alert_id_r3,
141 |                 "rule_id": "R3",
142 |                 "source_account": acc,
143 |                 "destination_account": None,
144 |                 "trigger_transaction_id": supp_r3[-1],
145 |                 "finding_scope": "WINDOW",
146 |                 "window_start": w_start,
147 |                 "window_end": w_end,
148 |                 "observed_count": obs_count_r3,
```

**Lines 149–155.** Save the numeric INR total, count and amount thresholds, support JSON, a two-decimal-place explanation and evaluation time.

```text
149 |                 "observed_amount_inr": float(obs_amount_r3),
150 |                 "count_threshold": 2,
151 |                 "amount_threshold_inr": 600000.0,
152 |                 "supporting_transaction_ids": json.dumps(supp_r3),
153 |                 "explanation": f"Account made {obs_count_r3} INR transactions totalling INR {obs_amount_r3:.2f} in a 120s window.",
154 |                 "triggered_at": eval_time
155 |             })
```

**Lines 156–164.** For R4 group by source,destination,window. Require at least three; sort support and build the ID from rule/source/start. Destination is deliberately not part of the ID.

```text
156 | 
157 |     # 3. Evaluate Rule R4 by grouping on (source_account, destination_account, w_start, w_end)
158 |     for (acc, dest, w_start, w_end), group in df_win.groupby(["source_account", "destination_account", "w_start", "w_end"]):
159 |         obs_count_r4 = len(group)
160 |         if obs_count_r4 >= 3:
161 |             sorted_group = group.sort_values(by=["ts", "transaction_id"])
162 |             supp_r4 = sorted_group["transaction_id"].tolist()
163 |             w_compact = w_start.strftime("%Y%m%d%H%M%S")
164 |             alert_id_r4 = f"R4-{acc}-{w_compact}"
```

**Lines 165–174.** Build an R4 candidate including the destination and PATTERN finding scope. Save the window and observed count.

```text
165 |             r4_candidates.append({
166 |                 "alert_id": alert_id_r4,
167 |                 "rule_id": "R4",
168 |                 "source_account": acc,
169 |                 "destination_account": dest,
170 |                 "trigger_transaction_id": supp_r4[-1],
171 |                 "finding_scope": "PATTERN",
172 |                 "window_start": w_start,
173 |                 "window_end": w_end,
174 |                 "observed_count": obs_count_r4,
```

**Lines 175–181.** R4 has no amount test, count threshold three, support JSON, explanation and evaluation timestamp.

```text
175 |                 "observed_amount_inr": None,
176 |                 "count_threshold": 3,
177 |                 "amount_threshold_inr": None,
178 |                 "supporting_transaction_ids": json.dumps(supp_r4),
179 |                 "explanation": f"Account sent {obs_count_r4} transactions to one destination in a 120s window.",
180 |                 "triggered_at": eval_time
181 |             })
```

**Lines 182–188.** If multiple destinations qualify for one R4 ID, sort descending by observed_count and retain one candidate per ID. Append selected records to the final alerts. Equal-count candidates have no explicit secondary tie-breaker.

```text
182 | 
183 |     # Deduplicate R4 candidates: exactly 1 alert row per (account, window, rule)
184 |     # If multiple destinations exceed threshold in same window, retain highest observed_count
185 |     if r4_candidates:
186 |         df_r4 = pd.DataFrame(r4_candidates)
187 |         df_r4 = df_r4.sort_values(by=["observed_count"], ascending=False).drop_duplicates(subset=["alert_id"])
188 |         alerts_list.extend(df_r4.to_dict(orient="records"))
```

**Lines 189–193.** Build the alerts frame. If nonempty, sort by window start and alert ID and reset the integer row index.

```text
189 | 
190 |     # Convert alerts to DataFrame
191 |     df_alerts = pd.DataFrame(alerts_list)
192 |     if not df_alerts.empty:
193 |         df_alerts = df_alerts.sort_values(by=["window_start", "alert_id"]).reset_index(drop=True)
```

**Lines 194–202.** Visit each original payment, extracting its identity/source/time and normalizing amount and currency. This loop also runs locally on the driver.

```text
194 | 
195 |     # 4. Correlate transactions to triggered alerts and compute R1 & R5 decisions
196 |     decisions = []
197 |     for _, tx in df.iterrows():
198 |         tid = tx["transaction_id"]
199 |         acc = tx["source_account"]
200 |         ts = tx["ts"]
201 |         amt = float(tx["amount"])
202 |         curr = str(tx["currency"]).strip().upper()
```

**Lines 203–210.** Find matching alerts using source account and half-open time inclusion only. The three boolean conditions are combined element-wise with &. There is no destination filter here, including for R4.

```text
203 | 
204 |         # Find all active alerts for this source account where w_start <= ts < w_end
205 |         if not df_alerts.empty:
206 |             matched_alerts = df_alerts[
207 |                 (df_alerts["source_account"] == acc) &
208 |                 (df_alerts["window_start"] <= ts) &
209 |                 (ts < df_alerts["window_end"])
210 |             ]
```

**Lines 211–215.** Produce a sorted unique list of alert IDs and a set of active rule IDs. If no alerts exist, both collections are empty.

```text
211 |             matched_alert_ids = sorted(list(set(matched_alerts["alert_id"].tolist())))
212 |             active_rules = set(matched_alerts["rule_id"].tolist())
213 |         else:
214 |             matched_alert_ids = []
215 |             active_rules = set()
```

**Lines 216–221.** Compute R1 directly from INR/amount. Compute R2,R3,R4 by checking membership in the active rule-ID set, converting each result to a Python bool.

```text
216 | 
217 |         # R1: Single INR payment of >= 50,000
218 |         r1_trig = bool((curr == "INR") and (amt >= 50000.0))
219 |         r2_trig = bool("R2" in active_rules)
220 |         r3_trig = bool("R3" in active_rules)
221 |         r4_trig = bool("R4" in active_rules)
```

**Lines 222–225.** Convert booleans to 1/0 and sum four flags. Score >=2 also sets the block flag. Many alerts of the same type still count only once.

```text
222 | 
223 |         # R5: Composite risk score
224 |         risk_score = int(r1_trig) + int(r2_trig) + int(r3_trig) + int(r4_trig)
225 |         block_trig = bool(risk_score >= 2)
```

**Lines 226–232.** Choose BLOCK for scores two or greater, REVIEW for one, APPROVE for zero. This assigns a string; it does not contact a payment gateway.

```text
226 | 
227 |         if risk_score >= 2:
228 |             decision = "BLOCK"
229 |         elif risk_score == 1:
230 |             decision = "REVIEW"
231 |         else:
232 |             decision = "APPROVE"
```

**Lines 233–241.** Begin the decision record with the original payment information, event time and current evaluation timestamp.

```text
233 | 
234 |         decisions.append({
235 |             "transaction_id": tid,
236 |             "source_account": acc,
237 |             "destination_account": tx["destination_account"],
238 |             "event_timestamp": ts,
239 |             "evaluated_at": eval_time,
240 |             "amount": amt,
241 |             "currency": curr,
```

**Lines 242–250.** Attach four flags, risk score, block boolean, decision and JSON text for related alert IDs; append the record.

```text
242 |             "r1_triggered": r1_trig,
243 |             "r2_triggered": r2_trig,
244 |             "r3_triggered": r3_trig,
245 |             "r4_triggered": r4_trig,
246 |             "r5_risk_score": risk_score,
247 |             "r5_block_triggered": block_trig,
248 |             "decision": decision,
249 |             "related_alert_ids": json.dumps(matched_alert_ids),
250 |         })
```

**Lines 251–253.** Build the decisions frame, reset row indices and return decisions and alerts to the streaming callback.

```text
251 | 
252 |     df_decisions = pd.DataFrame(decisions).reset_index(drop=True)
253 |     return df_decisions, df_alerts
```

## vm2/app/writers.py

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/app/writers.py`

**Lines 1–8.** Documentation describes table creation and writes. Checkpoint configuration is actually attached in stream_job.py, rather than being implemented as a separate checkpoint manager here.

```text
  1 | """Iceberg and HDFS write handlers for PySpark Structured Streaming.
  2 | 
  3 | Provides idempotent write operations for micro-batches:
  4 | 1. Table initialization (CREATE TABLE IF NOT EXISTS) for decisions and alerts
  5 | 2. Idempotent upsert / append using Spark SQL MERGE INTO or append mode
  6 | 3. Checkpoint management pointing to HDFS
  7 | """
  8 | 
```

**Lines 9–16.** Import logging and Spark objects and create a writer logger. Optional, col and config are imported but not used by this module in the current code.

```text
  9 | import logging
 10 | from typing import Optional
 11 | from pyspark.sql import DataFrame, SparkSession
 12 | from pyspark.sql.functions import col
 13 | 
 14 | from config.config import config
 15 | 
 16 | logger = logging.getLogger("FraudDetectionWriter")
```

**Lines 17–21.** Define the table initializer and log its two target table names.

```text
 17 | 
 18 | 
 19 | def initialize_iceberg_tables(spark: SparkSession, decisions_table: str, alerts_table: str):
 20 |     """Ensures Iceberg tables are created in the catalog with proper schemas."""
 21 |     logger.info("Initializing Iceberg tables: %s and %s", decisions_table, alerts_table)
```

**Lines 22–27.** Split the qualified decisions name at dots. Rejoin everything before the final table name to obtain the catalog/namespace prefix, such as iceberg.default.

```text
 22 | 
 23 |     # Ensure namespace exists (e.g. iceberg.default)
 24 |     parts = decisions_table.split(".")
 25 |     if len(parts) > 1:
 26 |         namespace = ".".join(parts[:-1])
 27 |         logger.info("Ensuring Iceberg namespace exists: %s", namespace)
```

**Lines 28–31.** Create that namespace if needed. A namespace error is logged and swallowed, but later table creation can still fail; it is not proof the namespace exists.

```text
 28 |         try:
 29 |             spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
 30 |         except Exception as ns_err:
 31 |             logger.info("Namespace creation note: %s", ns_err)
```

**Lines 32–35.** Begin a multiline formatted SQL statement that creates the decisions table only if it is absent.

```text
 32 | 
 33 |     # DDL for transaction_decisions
 34 |     spark.sql(f"""
 35 |         CREATE TABLE IF NOT EXISTS {decisions_table} (
```

**Lines 36–42.** Declare transaction ID, source/destination accounts, event/evaluation timestamps, amount and currency using SQL types corresponding to schema.py.

```text
 36 |             transaction_id STRING,
 37 |             source_account BIGINT,
 38 |             destination_account BIGINT,
 39 |             event_timestamp TIMESTAMP,
 40 |             evaluated_at TIMESTAMP,
 41 |             amount DOUBLE,
 42 |             currency STRING,
```

**Lines 43–47.** Declare four rule booleans and the integer risk score.

```text
 43 |             r1_triggered BOOLEAN,
 44 |             r2_triggered BOOLEAN,
 45 |             r3_triggered BOOLEAN,
 46 |             r4_triggered BOOLEAN,
 47 |             r5_risk_score INT,
```

**Lines 48–54.** Declare block flag, decision and JSON-text links; choose Iceberg as the provider; partition storage by event date. This DDL does not declare a primary-key constraint.

```text
 48 |             r5_block_triggered BOOLEAN,
 49 |             decision STRING,
 50 |             related_alert_ids STRING
 51 |         )
 52 |         USING iceberg
 53 |         PARTITIONED BY (days(event_timestamp))
 54 |     """)
```

**Lines 55–58.** Begin the second CREATE TABLE statement for alerts.

```text
 55 | 
 56 |     # DDL for account_risk_alerts
 57 |     spark.sql(f"""
 58 |         CREATE TABLE IF NOT EXISTS {alerts_table} (
```

**Lines 59–65.** Declare alert/rule/source/destination IDs, trigger payment, finding scope and window start.

```text
 59 |             alert_id STRING,
 60 |             rule_id STRING,
 61 |             source_account BIGINT,
 62 |             destination_account BIGINT,
 63 |             trigger_transaction_id STRING,
 64 |             finding_scope STRING,
 65 |             window_start TIMESTAMP,
```

**Lines 66–73.** Declare window end, observed measures, thresholds, support JSON, explanation and trigger/evaluation timestamp.

```text
 66 |             window_end TIMESTAMP,
 67 |             observed_count BIGINT,
 68 |             observed_amount_inr DOUBLE,
 69 |             count_threshold BIGINT,
 70 |             amount_threshold_inr DOUBLE,
 71 |             supporting_transaction_ids STRING,
 72 |             explanation STRING,
 73 |             triggered_at TIMESTAMP
```

**Lines 74–78.** Close the table definition; select Iceberg and partition by rule ID. Log completion only after both CREATE statements returned.

```text
 74 |         )
 75 |         USING iceberg
 76 |         PARTITIONED BY (rule_id)
 77 |     """)
 78 |     logger.info("Iceberg tables verified/initialized successfully.")
```

**Lines 79–89.** Define the write function with Spark session, optional frames, logging batch ID, target names and a default MERGE-mode switch.

```text
 79 | 
 80 | 
 81 | def write_micro_batch_to_iceberg(
 82 |     spark: SparkSession,
 83 |     decisions_df: DataFrame,
 84 |     alerts_df: DataFrame,
 85 |     batch_id: int,
 86 |     decisions_table: str,
 87 |     alerts_table: str,
 88 |     use_merge: bool = True
 89 | ):
```

**Lines 90–95.** Document intended retry behavior. If decisions exist and contain rows, log their count. isEmpty and count are Spark actions.

```text
 90 |     """Writes a micro-batch of decisions and alerts to Iceberg tables.
 91 |     
 92 |     Uses MERGE INTO by primary key to guarantee idempotency on retries/restarts.
 93 |     """
 94 |     if decisions_df is not None and not decisions_df.rdd.isEmpty():
 95 |         logger.info("[Batch %d] Writing %d decisions to %s", batch_id, decisions_df.count(), decisions_table)
```

**Lines 96–98.** In MERGE mode, expose the decisions frame as a temporary SQL view. A temporary view is a session-level name for the frame, not another durable HDFS table.

```text
 96 |         if use_merge:
 97 |             decisions_df.createOrReplaceTempView("batch_decisions")
 98 |             spark.sql(f"""
```

**Lines 99–104.** MERGE target table using the view. Match by transaction_id; update every field on a match; insert new rows otherwise. An identical logical payment therefore need not create an additional row on replay.

```text
 99 |                 MERGE INTO {decisions_table} AS target
100 |                 USING batch_decisions AS source
101 |                 ON target.transaction_id = source.transaction_id
102 |                 WHEN MATCHED THEN UPDATE SET *
103 |                 WHEN NOT MATCHED THEN INSERT *
104 |             """)
```

**Lines 105–106.** The optional alternative appends all decision rows. The streaming callback currently passes use_merge=True, so this branch is not its normal path.

```text
105 |         else:
106 |             decisions_df.write.format("iceberg").mode("append").save(decisions_table)
```

**Lines 107–112.** If alerts exist and are nonempty, log their count and create a temporary alert view for MERGE.

```text
107 | 
108 |     if alerts_df is not None and not alerts_df.rdd.isEmpty():
109 |         logger.info("[Batch %d] Writing %d alerts to %s", batch_id, alerts_df.count(), alerts_table)
110 |         if use_merge:
111 |             alerts_df.createOrReplaceTempView("batch_alerts")
112 |             spark.sql(f"""
```

**Lines 113–118.** MERGE alerts by alert_id, updating matches and inserting new IDs. This statement commits separately from the decisions MERGE.

```text
113 |                 MERGE INTO {alerts_table} AS target
114 |                 USING batch_alerts AS source
115 |                 ON target.alert_id = source.alert_id
116 |                 WHEN MATCHED THEN UPDATE SET *
117 |                 WHEN NOT MATCHED THEN INSERT *
118 |             """)
```

**Lines 119–120.** The optional alternative appends alert rows directly. batch_id is used for logging, not as a persisted exactly-once commit ledger.

```text
119 |         else:
120 |             alerts_df.write.format("iceberg").mode("append").save(alerts_table)
```

## vm2/scripts/start_master.sh

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/scripts/start_master.sh`

**Lines 1–4.** Choose Bash, describe the script and stop on unhandled failures.

```text
  1 | #!/usr/bin/env bash
  2 | # Start Spark Master on VM2
  3 | set -e
  4 | 
```

**Lines 5–8.** Select Spark installation, discover the ZeroTier IPv4 address with a fallback, and choose the master host. A supplied first argument wins over SPARK_MASTER_HOST, which wins over the discovered/default host.

```text
  5 | SPARK_HOME=${SPARK_HOME:-/home/shashank/opt/spark}
  6 | ZT_IP=$(ip -4 addr show ztr2qxeeov 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' || true)
  7 | DEFAULT_HOST=${ZT_IP:-172.23.124.232}
  8 | MASTER_HOST=${1:-${SPARK_MASTER_HOST:-${DEFAULT_HOST}}}
```

**Lines 9–10.** 7077 is the master cluster-registration/RPC port; 8080 is the master web interface. These are different services.

```text
  9 | MASTER_PORT=7077
 10 | WEBUI_PORT=8080
```

**Lines 11–15.** Print selected host and ports for the operator.

```text
 11 | 
 12 | echo "=== Starting Spark Master on VM2 ==="
 13 | echo "Master Host: ${MASTER_HOST}"
 14 | echo "Master Port: ${MASTER_PORT}"
 15 | echo "Web UI Port: ${WEBUI_PORT}"
```

**Lines 16–20.** Export the bind/advertise host and master/UI ports for Spark daemon scripts.

```text
 16 | 
 17 | export SPARK_LOCAL_IP="${MASTER_HOST}"
 18 | export SPARK_MASTER_HOST="${MASTER_HOST}"
 19 | export SPARK_MASTER_PORT="${MASTER_PORT}"
 20 | export SPARK_MASTER_WEBUI_PORT="${WEBUI_PORT}"
```

**Lines 21–22.** Invoke the installed Spark master startup script. This starts the resource-manager daemon; it does not start your fraud streaming application.

```text
 21 | 
 22 | "${SPARK_HOME}/sbin/start-master.sh"
```

**Lines 23–28.** Print the selected master URL and web UI link. A success message means the startup script returned, not that every worker/application has been checked.

```text
 23 | 
 24 | echo ""
 25 | echo "Spark Master started successfully!"
 26 | echo "Master URL: spark://${MASTER_HOST}:${MASTER_PORT}"
 27 | echo "Spark Web UI: http://${MASTER_HOST}:${WEBUI_PORT}"
 28 | echo ""
```

**Lines 29–30.** Print a suggested worker registration command. The text still uses separate historical VM labels; currently both workers belong to Sumukh.

```text
 29 | echo "Run this command on VM3 and VM4 to join the cluster as workers:"
 30 | echo "  \${SPARK_HOME}/sbin/start-worker.sh spark://${MASTER_HOST}:${MASTER_PORT}"
```

## vm2/scripts/stop_master.sh

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/scripts/stop_master.sh`

**Lines 1–5.** Choose Bash, set failure behavior and resolve the Spark installation.

```text
  1 | #!/usr/bin/env bash
  2 | # Stop Spark Master on VM2
  3 | set -e
  4 | 
  5 | SPARK_HOME=${SPARK_HOME:-/home/shashank/opt/spark}
```

**Lines 6–9.** Print status, invoke Spark stop-master.sh, then print completion. This targets the master daemon, not every application driver or remote worker.

```text
  6 | 
  7 | echo "=== Stopping Spark Master on VM2 ==="
  8 | "${SPARK_HOME}/sbin/stop-master.sh"
  9 | echo "Spark Master stopped."
```

## vm2/config/__init__.py

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/config/__init__.py`

**Lines 1–3.** Describe the package and re-export the AppConfig class and instantiated config object using a relative import.

```text
  1 | """Config package initialization."""
  2 | 
  3 | from .config import AppConfig, config
```

**Lines 4–5.** Declare the public export names for wildcard imports; no configuration reload occurs here.

```text
  4 | 
  5 | __all__ = ["AppConfig", "config"]
```

## vm2/app/__init__.py

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/app/__init__.py`

**Lines 1–3.** Describe the package and expose the rule evaluator through a relative import.

```text
  1 | """Fraud detection engine app package."""
  2 | 
  3 | from .rules import evaluate_batch
```

**Lines 4–8.** Expose the three schema constants from the schema module.

```text
  4 | from .schema import (
  5 |     ACCOUNT_RISK_ALERTS_SCHEMA,
  6 |     KAFKA_TRANSACTION_SCHEMA,
  7 |     TRANSACTION_DECISIONS_SCHEMA,
  8 | )
```

**Lines 9–15.** List public names for wildcard imports. Importing app also imports its schema dependencies; it does not start the stream.

```text
  9 | 
 10 | __all__ = [
 11 |     "evaluate_batch",
 12 |     "ACCOUNT_RISK_ALERTS_SCHEMA",
 13 |     "KAFKA_TRANSACTION_SCHEMA",
 14 |     "TRANSACTION_DECISIONS_SCHEMA",
 15 | ]
```

## vm2/requirements.txt

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/requirements.txt`

**Line 1.** Pin the Python Spark package to version 3.3.4. The selected runtime and connector coordinates must correspond to the actual Spark installation.

```text
  1 | pyspark==3.3.4
```

**Line 2.** Require Pandas 2.0.0 or newer for local data manipulation. >= is a minimum version rather than a fixed lock.

```text
  2 | pandas>=2.0.0
```

**Line 3.** Require PyArrow for Arrow/columnar data interoperability. Arrow acceleration is not explicitly enabled by this requirement alone.

```text
  3 | pyarrow>=14.0.0
```

**Line 4.** Declare a Python Kafka client for producer/support tooling. The Spark Kafka source itself uses its JVM connector JAR, not kafka-python.

```text
  4 | kafka-python>=2.0.2
```

**Line 5.** Declare date parsing utilities. Listing a dependency is not the same as installing it in the selected driver/worker interpreter.

```text
  5 | python-dateutil>=2.8.2
```

## vm2/tests/test_rules.py

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/tests/test_rules.py`

**Lines 1–8.** Describe fixture-based rule tests and expected counts. The word accuracy refers to agreement with this supplied fixture, not measured real-world fraud detection quality.

```text
  1 | """Unit tests verifying rule engine implementation against ground truth data.
  2 | 
  3 | Validates that:
  4 | 1. All 1,990 transaction decisions match ground truth (100% accuracy).
  5 | 2. All 129 account risk alerts match ground truth (100% accuracy).
  6 | 3. R1, R2, R3, R4, and R5 composite scores match perfectly.
  7 | """
  8 | 
```

**Lines 9–14.** Import JSON, filesystem utilities, unittest, Pandas and the evaluator under test.

```text
  9 | import json
 10 | import os
 11 | import unittest
 12 | import pandas as pd
 13 | 
 14 | from app.rules import evaluate_batch
```

**Lines 15–19.** Define a unittest class. setUpClass is a class method run once before tests so data/evaluation are shared across methods.

```text
 15 | 
 16 | 
 17 | class TestFraudRules(unittest.TestCase):
 18 |     @classmethod
 19 |     def setUpClass(cls):
```

**Lines 20–29.** Try an explicit DATA_DIR and several paths relative to this test file. Choose the first existing directory, otherwise raise FileNotFoundError.

```text
 20 |         # Locate project data with robust fallback paths
 21 |         possible_dirs = [
 22 |             os.getenv("DATA_DIR", ""),
 23 |             os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data")),
 24 |             os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data")),
 25 |             os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")),
 26 |         ]
 27 |         data_dir = next((d for d in possible_dirs if d and os.path.isdir(d)), "")
 28 |         if not data_dir:
 29 |             raise FileNotFoundError("Could not locate data directory containing transactions.csv")
```

**Lines 30–37.** Construct the three CSV paths; load input and remove duplicate payment IDs; load supplied expected alerts and decisions.

```text
 30 |         
 31 |         tx_path = os.path.join(data_dir, "transactions.csv")
 32 |         gt_alerts_path = os.path.join(data_dir, "ground_truth_account_risk_alerts.csv")
 33 |         gt_dec_path = os.path.join(data_dir, "ground_truth_transaction_decisions.csv")
 34 | 
 35 |         cls.df_tx = pd.read_csv(tx_path).drop_duplicates(subset=["transaction_id"])
 36 |         cls.df_gt_alerts = pd.read_csv(gt_alerts_path)
 37 |         cls.df_gt_dec = pd.read_csv(gt_dec_path)
```

**Lines 38–40.** Evaluate the deduplicated input locally once. No Kafka, HDFS or streaming query is launched by this test.

```text
 38 | 
 39 |         # Run evaluation
 40 |         cls.df_dec, cls.df_alerts = evaluate_batch(cls.df_tx)
```

**Lines 41–45.** Check that exactly 1,990 decisions were produced and transaction IDs are unique.

```text
 41 | 
 42 |     def test_transaction_count_and_uniqueness(self):
 43 |         """Verifies deduplicated transactions match 1,990 unique records."""
 44 |         self.assertEqual(len(self.df_dec), 1990)
 45 |         self.assertEqual(self.df_dec["transaction_id"].nunique(), 1990)
```

**Lines 46–52.** Check exactly 129 alerts and the expected per-rule distribution: 90 R2, 14 R3, 25 R4.

```text
 46 | 
 47 |     def test_alert_count(self):
 48 |         """Verifies alert count matches exact 129 ground truth alerts."""
 49 |         self.assertEqual(len(self.df_alerts), 129)
 50 |         self.assertEqual(len(self.df_alerts[self.df_alerts["rule_id"] == "R2"]), 90)
 51 |         self.assertEqual(len(self.df_alerts[self.df_alerts["rule_id"] == "R3"]), 14)
 52 |         self.assertEqual(len(self.df_alerts[self.df_alerts["rule_id"] == "R4"]), 25)
```

**Lines 53–58.** Compare the set of generated alert IDs to the expected set; the separate count check helps catch repeated IDs.

```text
 53 | 
 54 |     def test_alert_ids_match_ground_truth(self):
 55 |         """Verifies all alert IDs match ground truth exactly."""
 56 |         gen_ids = set(self.df_alerts["alert_id"])
 57 |         gt_ids = set(self.df_gt_alerts["alert_id"])
 58 |         self.assertEqual(gen_ids, gt_ids)
```

**Lines 59–65.** Count decision labels and compare them with 1,659 APPROVE, 231 REVIEW and 100 BLOCK.

```text
 59 | 
 60 |     def test_decisions_distribution(self):
 61 |         """Verifies decision distribution matches 1659 APPROVE, 231 REVIEW, 100 BLOCK."""
 62 |         counts = self.df_dec["decision"].value_counts().to_dict()
 63 |         self.assertEqual(counts.get("APPROVE", 0), 1659)
 64 |         self.assertEqual(counts.get("REVIEW", 0), 231)
 65 |         self.assertEqual(counts.get("BLOCK", 0), 100)
```

**Lines 66–73.** Map expected decisions by ID, then visit every actual result and find its expected counterpart.

```text
 66 | 
 67 |     def test_individual_transaction_decisions_and_scores(self):
 68 |         """Verifies every single transaction decision, score, and rule flags match GT."""
 69 |         gt_map = {row["transaction_id"]: row for _, row in self.df_gt_dec.iterrows()}
 70 | 
 71 |         for _, row in self.df_dec.iterrows():
 72 |             tid = row["transaction_id"]
 73 |             gt = gt_map[tid]
```

**Lines 74–81.** Compare each rule flag, risk score, block flag and classification. The fixture loader infers boolean columns; bool("false") would not be a valid generic string-to-boolean parser.

```text
 74 | 
 75 |             self.assertEqual(row["r1_triggered"], bool(gt["r1_triggered"]), f"R1 mismatch on {tid}")
 76 |             self.assertEqual(row["r2_triggered"], bool(gt["r2_triggered"]), f"R2 mismatch on {tid}")
 77 |             self.assertEqual(row["r3_triggered"], bool(gt["r3_triggered"]), f"R3 mismatch on {tid}")
 78 |             self.assertEqual(row["r4_triggered"], bool(gt["r4_triggered"]), f"R4 mismatch on {tid}")
 79 |             self.assertEqual(row["r5_risk_score"], int(gt["r5_risk_score"]), f"R5 score mismatch on {tid}")
 80 |             self.assertEqual(row["r5_block_triggered"], bool(gt["r5_block_triggered"]), f"R5 block mismatch on {tid}")
 81 |             self.assertEqual(row["decision"], gt["decision"], f"Decision mismatch on {tid}")
```

**Lines 82–85.** Parse actual/expected JSON alert arrays and sort before comparison because link order does not change membership.

```text
 82 | 
 83 |             gen_related = sorted(json.loads(row["related_alert_ids"]))
 84 |             gt_related = sorted(json.loads(gt["related_alert_ids"]))
 85 |             self.assertEqual(gen_related, gt_related, f"Related alerts mismatch on {tid}")
```

**Lines 86–93.** Map expected alerts by ID and iterate generated alerts.

```text
 86 | 
 87 |     def test_alert_triggers_and_supporting_txns(self):
 88 |         """Verifies alert trigger transaction IDs and supporting transactions match GT."""
 89 |         gt_alert_map = {row["alert_id"]: row for _, row in self.df_gt_alerts.iterrows()}
 90 | 
 91 |         for _, row in self.df_alerts.iterrows():
 92 |             aid = row["alert_id"]
 93 |             gt = gt_alert_map[aid]
```

**Lines 94–98.** Compare trigger ID, scope, observed count and exact explanation text.

```text
 94 | 
 95 |             self.assertEqual(row["trigger_transaction_id"], gt["trigger_transaction_id"])
 96 |             self.assertEqual(row["finding_scope"], gt["finding_scope"])
 97 |             self.assertEqual(row["observed_count"], int(gt["observed_count"]))
 98 |             self.assertEqual(row["explanation"], gt["explanation"])
```

**Lines 99–102.** Parse and compare supporting transaction lists in order. Unlike related-alert links, chronological support ordering is part of the expected output.

```text
 99 | 
100 |             gen_supp = json.loads(row["supporting_transaction_ids"])
101 |             gt_supp = json.loads(gt["supporting_transaction_ids"])
102 |             self.assertEqual(gen_supp, gt_supp, f"Supporting txns mismatch on {aid}")
```

**Lines 103–106.** When executed directly, hand control to unittest to discover/run the test methods.

```text
103 | 
104 | 
105 | if __name__ == "__main__":
106 |     unittest.main()
```

## vm2/tests/sim_batches.py

Source: `/media/shashank/Data1/PDocuments/sem 5/BD/Project fraund detection/repo_clone/vm2/tests/sim_batches.py`

**Lines 1–4.** Describe a Python simulation. The label design A = current is historical; the live streaming job now uses cumulative history, like design B. Simulated lateness is not a live watermark experiment.

```text
  1 | """Simulate the stream as micro-batches and compare with the ground truth.
  2 | Run from vm2/:   python tests/sim_batches.py
  3 | Design A = current job (each batch evaluated alone). Design B = stored events + new batch, then MERGE.
  4 | late=3 means the last 3 rows of every batch (~40 s of event time) arrive one batch later (< 1 minute late)."""
```

**Lines 5–10.** Import helpers, calculate this file directory, add vm2 to the module search path and import the evaluator.

```text
  5 | import json, os, sys
  6 | import pandas as pd
  7 | 
  8 | HERE = os.path.dirname(os.path.abspath(__file__))
  9 | sys.path.insert(0, os.path.join(HERE, ".."))
 10 | from app.rules import evaluate_batch
```

**Lines 11–16.** Find data, define a CSV reader preserving text/empty strings, deduplicate IDs, cast numeric fields and keep the six event columns.

```text
 11 | 
 12 | D = os.getenv("DATA_DIR", os.path.join(HERE, "..", "..", "data"))
 13 | rd = lambda f: pd.read_csv(os.path.join(D, f), dtype=str, keep_default_na=False)
 14 | tx = rd("transactions.csv").drop_duplicates("transaction_id")
 15 | tx = tx.astype({"source_account": "int64", "destination_account": "int64", "amount": "float64"})
 16 | tx = tx[["transaction_id", "source_account", "destination_account", "event_timestamp", "amount", "currency"]].reset_index(drop=True)
```

**Lines 17–19.** Index expected decisions by payment ID, collect expected alert IDs into a set, and select decision fields checked by scoring.

```text
 17 | gt_dec = rd("ground_truth_transaction_decisions.csv").set_index("transaction_id")
 18 | gt_alerts = set(rd("ground_truth_account_risk_alerts.csv").alert_id)
 19 | COLS = ["r2_triggered", "r3_triggered", "r4_triggered", "r5_risk_score", "decision"]
```

**Lines 20–25.** Define the batch generator; start with no held rows and take chunks of n rows from the input.

```text
 20 | 
 21 | 
 22 | def make_batches(n, late=0):
 23 |     chunks, held = [], tx.iloc[0:0]
 24 |     for i in range(0, len(tx), n):
 25 |         c = tx.iloc[i:i + n]
```

**Lines 26–30.** For simulated lateness, emit previously held rows plus the current chunk excluding its last late rows; hold those trailing rows for the next chunk. Otherwise emit the full chunk.

```text
 26 |         if late:
 27 |             chunks.append(pd.concat([held, c.iloc[:-late]]))
 28 |             held = c.iloc[-late:]
 29 |         else:
 30 |             chunks.append(c)
```

**Lines 31–33.** Flush any held final rows and return the list of batch frames.

```text
 31 |     if len(held):
 32 |         chunks.append(held)
 33 |     return chunks
```

**Lines 34–42.** Design A evaluates each chunk independently and keeps the latest record per ID in dictionaries. It can miss patterns that span chunk boundaries.

```text
 34 | 
 35 | 
 36 | def design_a(chunks):
 37 |     dec, al = {}, {}
 38 |     for b in chunks:
 39 |         d, a = evaluate_batch(b)
 40 |         dec.update({r["transaction_id"]: r for r in d.to_dict("records")})
 41 |         al.update({r["alert_id"]: r for r in a.to_dict("records")})
 42 |     return dec, al
```

**Lines 43–52.** Design B appends each chunk to a deduplicated history frame, re-evaluates all history, and updates the accumulated dictionaries. This models the current callback strategy without real storage.

```text
 43 | 
 44 | 
 45 | def design_b(chunks):
 46 |     store, dec, al = tx.iloc[0:0], {}, {}
 47 |     for b in chunks:
 48 |         store = pd.concat([store, b]).drop_duplicates("transaction_id")
 49 |         d, a = evaluate_batch(store)
 50 |         dec.update({r["transaction_id"]: r for r in d.to_dict("records")})
 51 |         al.update({r["alert_id"]: r for r in a.to_dict("records")})
 52 |     return dec, al
```

**Lines 53–58.** Define scoring and iterate expected payments, counting missing or mismatched results.

```text
 53 | 
 54 | 
 55 | def score(dec, al):
 56 |     wrong = 0
 57 |     for k, g in gt_dec.iterrows():
 58 |         r = dec.get(k)
```

**Lines 59–62.** Compare selected fields using lowercase strings and compare sorted related-alert lists. Return wrong decisions, missing alert IDs and extra alert IDs.

```text
 59 |         ok = r is not None and all(str(r[c]).lower() == str(g[c]).lower() for c in COLS) \
 60 |             and sorted(json.loads(r["related_alert_ids"])) == sorted(json.loads(g["related_alert_ids"]))
 61 |         wrong += not ok
 62 |     return wrong, len(gt_alerts - set(al)), len(set(al) - gt_alerts)
```

**Lines 63–71.** When run directly, print a results table for batch sizes 100/200/400, delayed-tail settings 0/3, and both designs. These loops rerun local evaluations; they do not establish distributed recovery.

```text
 63 | 
 64 | 
 65 | if __name__ == "__main__":
 66 |     print("rows per batch | late | design | wrong decisions (of 1990) | alerts missing / extra (of 129)")
 67 |     for n in (100, 200, 400):
 68 |         for late in (0, 3):
 69 |             for name, fn in (("A current", design_a), ("B stored+new", design_b)):
 70 |                 w, m, x = score(*fn(make_batches(n, late)))
 71 |                 print(f"{n:14d} | {late:4d} | {name:12s} | {w:25d} | {m} / {x}")
```
