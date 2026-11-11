import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(
    page_title="FraudWatch | Monitoring",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

BASE = Path(__file__).resolve().parents[2]
DATA = BASE / "data"

# ---------- THEME ----------
st.markdown("""
<style>
.stApp {
    background: #ffffff;
    color: #262730;
}
[data-testid="stSidebar"] {
    background: #f7f7f7;
    border-right: 1px solid #e5e5e5;
}
.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
    max-width: 1500px;
}
h1, h2, h3 {
    color: #262730 !important;
}
p, label, span {
    color: #444444;
}
[data-testid="stMetric"] {
    background: #ffffff;
    border: 1px solid #e5e5e5;
    padding: 18px;
    border-radius: 10px;
}
[data-testid="stMetricValue"] {
    color: #262730;
}
[data-testid="stMetricLabel"] {
    color: #666666;
}
div[data-testid="stDataFrame"] {
    border: 1px solid #e5e5e5;
    border-radius: 8px;
}
hr {
    border-color: #e5e5e5;
}
.status {
    display: inline-block;
    padding: 6px 13px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: bold;
    background: #fff7df;
    color: #946200;
    border: 1px solid #f0d58a;
}
.subtitle {
    color: #777777;
    font-size: 14px;
}
</style>
""", unsafe_allow_html=True)

# ---------- DATA ----------
@st.cache_data
def load_data():
    d = pd.read_csv(DATA / "ground_truth_transaction_decisions.csv")
    a = pd.read_csv(DATA / "ground_truth_account_risk_alerts.csv")

    d["amount"] = pd.to_numeric(d["amount"], errors="coerce")
    d["event_timestamp"] = pd.to_datetime(
        d["event_timestamp"], utc=True, errors="coerce"
    )
    return d, a


@st.cache_data(ttl=10)
def load_live_data(run_number):
    from iceberg_source import load_iceberg_run

    d, a = load_iceberg_run(run_number)
    d["amount"] = pd.to_numeric(d["amount"], errors="coerce")
    d["event_timestamp"] = pd.to_datetime(
        d["event_timestamp"], utc=True, errors="coerce"
    )

    for col in [
        "r1_triggered", "r2_triggered", "r3_triggered",
        "r4_triggered", "r5_block_triggered"
    ]:
        if col in d.columns:
            d[col] = (
                d[col].astype(str).str.lower().map({
                    "true": True, "1": True,
                    "false": False, "0": False
                }).fillna(False).astype(bool)
            )

    return d, a

# ---------- DATA SOURCE ----------
data_mode = st.sidebar.selectbox(
    "Data source",
    ["DEMO", "LIVE"],
    index=0
)

run_number = 1

if data_mode == "LIVE":
    run_number = st.sidebar.number_input(
        "Experiment run",
        min_value=1,
        value=1,
        step=1
    )

try:
    if data_mode == "DEMO":
        df, alerts = load_data()
    else:
        df, alerts = load_live_data(int(run_number))
except Exception as e:
    st.error(f"{data_mode} data loading failed: {e}")
    if data_mode == "LIVE":
        st.warning("Check Spark Thrift Server and Iceberg table availability.")
    st.stop()

# ---------- SIDEBAR ----------
st.sidebar.markdown("## 🛡️ FraudWatch")
st.sidebar.caption("Distributed Fraud Monitoring")
st.sidebar.divider()

st.sidebar.markdown("### Monitoring Filters")

decision_options = ["ALL", "APPROVE", "REVIEW", "BLOCK"]
selected_decision = st.sidebar.selectbox(
    "Transaction decision", decision_options
)

rule_options = ["ALL", "R1", "R2", "R3", "R4"]
selected_rule = st.sidebar.selectbox("Fraud rule", rule_options)

st.sidebar.divider()
st.sidebar.markdown("### Data Source")
st.sidebar.markdown(
    f'<span class="status">● {data_mode} MODE</span>',
    unsafe_allow_html=True
)
st.sidebar.caption(
    "Source: Verified ground-truth CSV datasets"
    if data_mode == "DEMO"
    else f"Source: Iceberg tables | Run {int(run_number)}"
)

if st.sidebar.button("🔄 Refresh datasets", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()
st.sidebar.caption("FraudWatch V2 | Academic Project")

# ---------- HEADER ----------
header_left, header_right = st.columns([5, 1])

with header_left:
    st.markdown("# 🛡️ FraudWatch")
    st.markdown(
        '<p class="subtitle">Distributed Fraud Monitoring & Risk Analytics</p>',
        unsafe_allow_html=True
    )

with header_right:
    st.markdown(f'<span class="status">● {data_mode} MODE</span>',
                unsafe_allow_html=True)

st.divider()

# ---------- FILTER DATA ----------
filtered = df.copy()

if selected_decision != "ALL":
    filtered = filtered[filtered["decision"] == selected_decision]

if selected_rule != "ALL":
    col = f"{selected_rule.lower()}_triggered"
    filtered = filtered[filtered[col].fillna(False).astype(bool)]

# ---------- KPI CARDS ----------
st.markdown("### System Overview")

total = len(filtered)
approved = int((filtered["decision"] == "APPROVE").sum())
reviewed = int((filtered["decision"] == "REVIEW").sum())
blocked = int((filtered["decision"] == "BLOCK").sum())

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Transactions", f"{total:,}")
c2.metric("Approved", f"{approved:,}")
c3.metric("Under Review", f"{reviewed:,}")
c4.metric("Blocked", f"{blocked:,}")

# ---------- RULE METRICS ----------
st.markdown("### Fraud Rule Activity")

rule_cols = st.columns(5)
for i, col in enumerate(rule_cols, start=1):
    if i <= 4:
        count = int(df[f"r{i}_triggered"].fillna(False).sum())
        label = f"R{i} Triggers"
    else:
        count = int(df["r5_block_triggered"].fillna(False).sum())
        label = "R5 Blocks"

    col.metric(label, f"{count:,}")

st.divider()

# ---------- CHARTS ----------
left, right = st.columns(2)

with left:
    st.markdown("### Decision Distribution")
    decision_counts = filtered["decision"].value_counts()

    fig = go.Figure(data=[go.Pie(
        labels=decision_counts.index,
        values=decision_counts.values,
        hole=0.65,
        marker=dict(
            colors=["#22c55e", "#f59e0b", "#ef4444"]
        ),
        textinfo="label+value",
        hovertemplate="%{label}: %{value}<extra></extra>"
    )])
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=15, b=15, l=15, r=15),
        showlegend=False,
        height=330
    )
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown("### Fraud Rule Triggers")
    rule_data = pd.DataFrame({
        "Rule": ["R1", "R2", "R3", "R4"],
        "Triggers": [
            int(df[f"r{i}_triggered"].fillna(False).sum())
            for i in range(1, 5)
        ]
    })

    fig = px.bar(
        rule_data,
        x="Rule",
        y="Triggers",
        text="Triggers",
        color="Rule",
        color_discrete_sequence=[
            "#38bdf8", "#818cf8", "#f59e0b", "#ef4444"
        ]
    )
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        height=330,
        margin=dict(t=15, b=15)
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------- TIMELINE ----------
st.divider()
st.markdown("### Transaction Activity Timeline")

timeline = filtered.dropna(subset=["event_timestamp"]).copy()

if not timeline.empty:
    timeline["Time"] = timeline["event_timestamp"].dt.floor("15min")
    timeline_data = (
        timeline.groupby(["Time", "decision"])
        .size()
        .reset_index(name="Transactions")
    )

    fig = px.line(
        timeline_data,
        x="Time",
        y="Transactions",
        color="decision",
        markers=True,
        color_discrete_map={
            "APPROVE": "#22c55e",
            "REVIEW": "#f59e0b",
            "BLOCK": "#ef4444"
        }
    )
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=330,
        xaxis_title="Event Time",
        yaxis_title="Transaction Count"
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------- TRANSACTIONS ----------
st.divider()
st.markdown("### Transaction Monitoring")

search = st.text_input(
    "🔍 Search transaction ID, source account or destination account"
)

if search:
    mask = (
        filtered["transaction_id"].astype(str).str.contains(
            search, case=False, na=False
        )
        | filtered["source_account"].astype(str).str.contains(
            search, na=False
        )
        | filtered["destination_account"].astype(str).str.contains(
            search, na=False
        )
    )
    filtered = filtered[mask]

display_columns = [
    "transaction_id", "source_account", "destination_account",
    "event_timestamp", "amount", "currency",
    "r1_triggered", "r2_triggered", "r3_triggered",
    "r4_triggered", "r5_risk_score", "decision"
]

available_columns = [c for c in display_columns if c in filtered.columns]

st.caption(f"Showing {len(filtered):,} transactions")

st.dataframe(
    filtered[available_columns].sort_values(
        "event_timestamp", ascending=False
    ),
    use_container_width=True,
    hide_index=True,
    height=420
)

# ---------- ALERTS ----------
st.divider()
st.markdown("### 🚨 Fraud Alerts")

if not alerts.empty:
    alert_left, alert_right = st.columns([1, 2])

    with alert_left:
        st.markdown("#### Alert Summary")
        counts = alerts["rule_id"].value_counts().sort_index()

        for rule, count in counts.items():
            st.metric(f"{rule} Alerts", int(count))

    with alert_right:
        st.markdown("#### Alert Distribution")
        alert_chart = alerts["rule_id"].value_counts().reset_index()
        alert_chart.columns = ["Rule", "Alerts"]

        fig = px.bar(
            alert_chart,
            x="Rule",
            y="Alerts",
            text="Alerts",
            color="Rule",
            color_discrete_sequence=["#38bdf8", "#f59e0b", "#ef4444"]
        )
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
            height=280
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Alert Details")

    alert_search = st.text_input("Search alert ID or source account")

    shown_alerts = alerts.copy()
    if alert_search:
        mask = (
            shown_alerts["alert_id"].astype(str).str.contains(
                alert_search, case=False, na=False
            )
            | shown_alerts["source_account"].astype(str).str.contains(
                alert_search, na=False
            )
        )
        shown_alerts = shown_alerts[mask]

    st.dataframe(
        shown_alerts,
        use_container_width=True,
        hide_index=True,
        height=350
    )
else:
    st.success("No fraud alerts found.")

# ---------- FOOTER ----------
st.divider()
st.markdown(
    '<p class="subtitle">FraudWatch V2 | Distributed Fraud Monitoring System '
    '| Spark • Kafka • HDFS • Iceberg</p>',
    unsafe_allow_html=True
)
