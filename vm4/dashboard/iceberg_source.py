import os
import subprocess
import io
import pandas as pd
from pathlib import Path


SPARK_HOME = Path(os.environ.get(
    "SPARK_HOME",
    str(Path.home() / "spark-3.3.4-bin-hadoop3")
))

BEELINE = SPARK_HOME / "bin" / "beeline"
JDBC_URL = "jdbc:hive2://127.0.0.1:10000/default"
USERNAME = os.environ.get("USER") or os.environ.get("LOGNAME") or "spark"


def query_sql(sql):
    if not BEELINE.exists():
        raise RuntimeError(f"Beeline not found: {BEELINE}")

    result = subprocess.run(
        [
            str(BEELINE),
            "-u", JDBC_URL,
            "-n", USERNAME,
            "--silent=true",
            "--showHeader=true",
            "--outputformat=tsv2",
            "-e", sql.rstrip().rstrip(";") + ";"
        ],
        capture_output=True,
        text=True,
        timeout=60
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)

    return pd.read_csv(io.StringIO(result.stdout), sep="\t", quoting=3, dtype=str)


def load_iceberg_run(run_number):
    if not str(run_number).isdigit():
        raise ValueError("Run number must be numeric")

    decisions_table = (
        f"iceberg.default.transaction_decisions_run{run_number}"
    )
    alerts_table = (
        f"iceberg.default.account_risk_alerts_run{run_number}"
    )

    decisions = query_sql(f"SELECT * FROM {decisions_table}")
    alerts = query_sql(f"SELECT * FROM {alerts_table}")

    decisions["event_timestamp"] = pd.to_datetime(
        decisions["event_timestamp"], utc=True, errors="coerce"
    )

    return decisions, alerts
