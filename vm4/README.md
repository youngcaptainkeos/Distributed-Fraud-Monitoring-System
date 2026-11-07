# VM4: Dashboard & Validation

This directory contains the Streamlit dashboard, investigation workflows, and monitoring utilities running on **VM4**.

---

## Responsibilities

1. **Spark Worker 2:**
   - Runs worker process registered with Spark Master on VM2:
     ```bash
     $SPARK_HOME/sbin/start-worker.sh spark://<VM2_PRIVATE_IP>:7077
     ```

2. **Streamlit Monitoring Dashboard:**
   - Real-time UI querying Iceberg tables on HDFS.
   - Global filters: Time range, account ID, fraud rule (R1–R5).
   - Core KPIs: Total transactions processed, total alerts generated, flagged accounts count.
   - Visualizations: Alerts by rule (bar charts), transaction and alert volume per minute (time series).
   - Data Tables: Flagged accounts list and detailed alert table.

3. **Alert Investigation Interface:**
   - Drilldown view for any selected alert showing:
     - Plain-language rule explanation
     - Triggering transaction details
     - Most recent 20 transactions from the source account (ordered descending by event time)
   - Graceful load error states and diagnostic run IDs.
