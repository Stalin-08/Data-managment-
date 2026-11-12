
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# =====================================================
# PAGE CONFIGURATION
# =====================================================

st.set_page_config(
    page_title="Crypto Intelligence Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="collapsed"
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DB_PATH = PROJECT_ROOT / "database" / "crypto_data.db"
METRICS_PATH = PROJECT_ROOT / "monitoring" / "performance_metrics.csv"


# =====================================================
# CUSTOM DARK THEME
# =====================================================

st.markdown("""
<style>
    .stApp {
        background-color: #080b0d;
        color: #e8eeee;
    }

    [data-testid="stHeader"] {
        background-color: #080b0d;
    }

    [data-testid="stSidebar"] {
        background-color: #101517;
    }

    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1600px;
    }

    h1, h2, h3 {
        color: #f0f5f3 !important;
        font-weight: 650 !important;
    }

    p, label, span {
        color: #c1ceca;
    }

    [data-testid="stMetric"] {
        background: #11191a;
        border: 1px solid #20332e;
        border-radius: 12px;
        padding: 17px;
    }

    [data-testid="stMetricLabel"] {
        color: #9eada7 !important;
    }

    [data-testid="stMetricValue"] {
        color: #f0f5f3 !important;
    }

    [data-testid="stDataFrame"] {
        border: 1px solid #20332e;
        border-radius: 10px;
    }

    div[data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #111819;
        border: 1px solid #20332e;
        border-radius: 12px;
        padding: 12px;
    }

    .dashboard-subtitle {
        color: #82948d;
        font-size: 13px;
        margin-top: -12px;
        margin-bottom: 22px;
    }

    .section-label {
        color: #46d6a0;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 1.5px;
        margin-bottom: 5px;
    }

    .objective-card {
        background: #11191a;
        border: 1px solid #203b32;
        border-left: 4px solid #35c991;
        border-radius: 10px;
        padding: 17px;
        min-height: 145px;
    }

    .objective-title {
        color: #f1f6f3;
        font-size: 16px;
        font-weight: 650;
        margin-bottom: 9px;
    }

    .objective-description {
        color: #9caaa4;
        font-size: 13px;
        line-height: 1.6;
    }

    .status-dot {
        color: #35d99b;
        font-size: 12px;
        font-weight: 700;
    }

    hr {
        border-color: #20302b !important;
    }

    footer {
        visibility: hidden;
    }
</style>
""", unsafe_allow_html=True)


# =====================================================
# PLOTLY DARK CHART SETTINGS
# =====================================================

CHART_LAYOUT = {
    "template": "plotly_dark",
    "paper_bgcolor": "#111819",
    "plot_bgcolor": "#111819",
    "font": {"color": "#d9e4df"},
    "margin": {"l": 15, "r": 15, "t": 45, "b": 15},
    "legend": {"orientation": "h", "y": 1.12},
    "xaxis": {
        "gridcolor": "#26332f",
        "zerolinecolor": "#26332f"
    },
    "yaxis": {
        "gridcolor": "#26332f",
        "zerolinecolor": "#26332f"
    }
}

GREEN = "#35d99b"
BLUE = "#48a9e8"
RED = "#ff6577"


# =====================================================
# HEADER
# =====================================================

header_col1, header_col2 = st.columns([4, 1])

with header_col1:
    st.markdown(
        "<h1>₿ Crypto Intelligence</h1>",
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="dashboard-subtitle">'
        'REAL-TIME MARKET ANALYTICS · ANOMALY DETECTION · KAFKA STREAMING'
        '</div>',
        unsafe_allow_html=True
    )

with header_col2:
    st.markdown(
        '<p class="status-dot">● LIVE MONITORING</p>',
        unsafe_allow_html=True
    )

st.divider()


# =====================================================
# DATABASE FUNCTIONS
# =====================================================

def load_data():

    with sqlite3.connect(DB_PATH, timeout=30) as conn:

        trades = pd.read_sql_query(
            """
            SELECT *
            FROM trades
            ORDER BY id DESC
            LIMIT 2000
            """,
            conn
        )

        anomalies = pd.read_sql_query(
            """
            SELECT *
            FROM anomalies
            ORDER BY id DESC
            LIMIT 100
            """,
            conn
        )

        total_trades = pd.read_sql_query(
            "SELECT COUNT(*) AS total FROM trades",
            conn
        )["total"].iloc[0]

        total_anomalies = pd.read_sql_query(
            "SELECT COUNT(*) AS total FROM anomalies",
            conn
        )["total"].iloc[0]

    return trades, anomalies, total_trades, total_anomalies


def load_performance():

    if not METRICS_PATH.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(METRICS_PATH).tail(100)

    except (pd.errors.EmptyDataError, pd.errors.ParserError, OSError):
        return pd.DataFrame()


# =====================================================
# MAIN DASHBOARD
# =====================================================

@st.fragment(run_every=3)
def dashboard():

    if not DB_PATH.exists():
        st.error("Database not found. Start the Kafka consumer first.")
        return

    try:
        trades, anomalies, total_trades, total_anomalies = load_data()

    except Exception as error:
        st.error(f"Database error: {error}")
        return

    performance = load_performance()

    if trades.empty:
        st.info("Waiting for incoming cryptocurrency trades...")
        return

    trades = trades.sort_values("id").copy()

    # -------------------------------------------------
    # DATA CALCULATIONS
    # -------------------------------------------------

    anomaly_rate = (
        total_anomalies / total_trades * 100
        if total_trades else 0
    )

    btc_data = trades[trades["symbol"] == "BTCUSDT"]
    eth_data = trades[trades["symbol"] == "ETHUSDT"]

    latest_btc = (
        btc_data.iloc[-1]["price"]
        if not btc_data.empty else 0
    )

    latest_eth = (
        eth_data.iloc[-1]["price"]
        if not eth_data.empty else 0
    )

    latest_price = trades.iloc[-1]["price"]

    trading_value = trades["price"] * trades["quantity"]

    total_trading_value = trading_value.sum()

    # =================================================
    # BUSINESS OBJECTIVES
    # =================================================

    st.markdown(
        '<div class="section-label">BUSINESS OBJECTIVES</div>',
        unsafe_allow_html=True
    )

    obj1, obj2, obj3 = st.columns(3)

    with obj1:
        st.markdown("""
        <div class="objective-card">
            <div class="objective-title">
                01 · Market Monitoring
            </div>
            <div class="objective-description">
                Monitor BTC and ETH market prices, trading
                activity and transaction values in real time.
            </div>
        </div>
        """, unsafe_allow_html=True)

    with obj2:
        st.markdown("""
        <div class="objective-card">
            <div class="objective-title">
                02 · Anomaly Detection
            </div>
            <div class="objective-description">
                Identify unusual price movements and trading
                volumes using statistical anomaly detection.
            </div>
        </div>
        """, unsafe_allow_html=True)

    with obj3:
        st.markdown("""
        <div class="objective-card">
            <div class="objective-title">
                03 · System Performance
            </div>
            <div class="objective-description">
                Monitor Kafka throughput, processing time,
                end-to-end latency and consumer performance.
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.write("")

    # =================================================
    # KPI CARDS
    # =================================================

    st.markdown(
        '<div class="section-label">MARKET OVERVIEW</div>',
        unsafe_allow_html=True
    )

    k1, k2, k3, k4, k5 = st.columns(5)

    k1.metric("Total Trades", f"{total_trades:,}")
    k2.metric("Total Anomalies", f"{total_anomalies:,}")
    k3.metric("Anomaly Rate", f"{anomaly_rate:.2f}%")
    k4.metric("BTC Price", f"${latest_btc:,.2f}")
    k5.metric("ETH Price", f"${latest_eth:,.2f}")

    st.write("")

    # =================================================
    # MARKET CHARTS AND DATA TABLE
    # =================================================

    st.markdown(
        '<div class="section-label">MARKET ANALYTICS</div>',
        unsafe_allow_html=True
    )

    left, right = st.columns([1.6, 1])

    with left:

        st.subheader("Cryptocurrency Price Trends")

        price_data = trades[
            trades["symbol"].isin(["BTCUSDT", "ETHUSDT"])
        ].copy()

        if not price_data.empty:

            fig = px.line(
                price_data,
                x="id",
                y="price",
                color="symbol",
                color_discrete_map={
                    "BTCUSDT": GREEN,
                    "ETHUSDT": BLUE
                },
                labels={
                    "id": "Trade sequence",
                    "price": "Price (USDT)",
                    "symbol": "Asset"
                }
            )

            fig.update_layout(**CHART_LAYOUT)

            st.plotly_chart(
                fig,
                use_container_width=True,
                key="price_chart"
            )

    with right:

        st.subheader("Latest Market Prices")

        market_rows = []

        for symbol, symbol_data in [
            ("BTCUSDT", btc_data),
            ("ETHUSDT", eth_data)
        ]:

            if not symbol_data.empty:

                market_rows.append({
                    "Asset": symbol,
                    "Latest Price": symbol_data.iloc[-1]["price"],
                    "Latest Quantity": symbol_data.iloc[-1]["quantity"],
                    "Trades Loaded": len(symbol_data)
                })

        if market_rows:

            market_table = pd.DataFrame(market_rows)

            st.dataframe(
                market_table,
                use_container_width=True,
                hide_index=True
            )

        st.subheader("Trading Value by Asset")

        value_data = trades.copy()
        value_data["trading_value"] = (
            value_data["price"] * value_data["quantity"]
        )

        value_summary = (
            value_data.groupby("symbol")["trading_value"]
            .sum()
            .reset_index()
        )

        value_fig = px.bar(
            value_summary,
            x="symbol",
            y="trading_value",
            color="symbol",
            color_discrete_map={
                "BTCUSDT": GREEN,
                "ETHUSDT": BLUE
            },
            labels={
                "symbol": "Asset",
                "trading_value": "Trading value (USDT)"
            }
        )

        value_fig.update_layout(**CHART_LAYOUT)

        st.plotly_chart(
            value_fig,
            use_container_width=True,
            key="trading_value_chart"
        )

    st.divider()

    # =================================================
    # ANOMALY ANALYTICS
    # =================================================

    st.markdown(
        '<div class="section-label">OBJECTIVE 02 · ANOMALY DETECTION</div>',
        unsafe_allow_html=True
    )

    anomaly_col1, anomaly_col2 = st.columns([1, 1.5])

    with anomaly_col1:

        st.subheader("Anomaly Distribution")

        if not anomalies.empty:

            anomaly_distribution = (
                anomalies.groupby("symbol")
                .size()
                .reset_index(name="count")
            )

            anomaly_fig = px.pie(
                anomaly_distribution,
                names="symbol",
                values="count",
                hole=0.58,
                color="symbol",
                color_discrete_map={
                    "BTCUSDT": GREEN,
                    "ETHUSDT": BLUE
                }
            )

            anomaly_fig.update_layout(**CHART_LAYOUT)

            st.plotly_chart(
                anomaly_fig,
                use_container_width=True,
                key="anomaly_distribution"
            )

        else:
            st.info("No anomalies detected yet.")

    with anomaly_col2:

        st.subheader("Recent Anomaly Alerts")

        if not anomalies.empty:

            display_anomalies = anomalies[
                [
                    "symbol",
                    "price",
                    "quantity",
                    "price_zscore",
                    "volume_zscore",
                    "reasons",
                    "created_at"
                ]
            ].head(10)

            st.dataframe(
                display_anomalies,
                use_container_width=True,
                hide_index=True
            )

        else:
            st.success("No unusual trading activity detected.")

    st.divider()

    # =================================================
    # SYSTEM PERFORMANCE
    # =================================================

    st.markdown(
        '<div class="section-label">OBJECTIVE 03 · SYSTEM PERFORMANCE</div>',
        unsafe_allow_html=True
    )

    if not performance.empty:

        latest = performance.iloc[-1]

        p1, p2, p3, p4 = st.columns(4)

        p1.metric(
            "Throughput",
            f"{latest.get('current_throughput', 0):.2f} msg/sec"
        )

        p2.metric(
            "Average Latency",
            f"{latest.get('average_latency_ms', 0):.2f} ms"
        )

        p3.metric(
            "P95 Latency",
            f"{latest.get('p95_latency_ms', 0):.2f} ms"
        )

        p4.metric(
            "Processing Time",
            f"{latest.get('average_processing_ms', 0):.2f} ms"
        )

        performance_col1, performance_col2 = st.columns(2)

        with performance_col1:

            st.subheader("Throughput Over Time")

            throughput_fig = px.line(
                performance,
                x="timestamp_utc",
                y="current_throughput",
                labels={
                    "timestamp_utc": "Time",
                    "current_throughput": "Messages per second"
                }
            )

            throughput_fig.update_traces(
                line=dict(color=GREEN, width=2)
            )

            throughput_fig.update_layout(**CHART_LAYOUT)

            st.plotly_chart(
                throughput_fig,
                use_container_width=True,
                key="throughput_chart"
            )

        with performance_col2:

            st.subheader("Latency Over Time")

            latency_fig = go.Figure()

            latency_fig.add_trace(
                go.Scatter(
                    x=performance["timestamp_utc"],
                    y=performance["average_latency_ms"],
                    name="Average latency",
                    mode="lines",
                    line=dict(color=BLUE, width=2)
                )
            )

            latency_fig.add_trace(
                go.Scatter(
                    x=performance["timestamp_utc"],
                    y=performance["p95_latency_ms"],
                    name="P95 latency",
                    mode="lines",
                    line=dict(color=RED, width=2)
                )
            )

            latency_fig.update_layout(**CHART_LAYOUT)

            st.plotly_chart(
                latency_fig,
                use_container_width=True,
                key="latency_chart"
            )

    else:

        st.info(
            "Performance metrics are not available yet. "
            "Start the Kafka consumer to generate metrics."
        )

    st.divider()

    # =================================================
    # RECENT TRADES
    # =================================================

    st.markdown(
        '<div class="section-label">LIVE TRANSACTION RECORDS</div>',
        unsafe_allow_html=True
    )

    st.subheader("Recent Cryptocurrency Trades")

    recent_trades = trades.tail(50).iloc[::-1].copy()

    st.dataframe(
        recent_trades[
            [
                "symbol",
                "price",
                "quantity",
                "trade_time",
                "is_anomaly"
            ]
        ],
        use_container_width=True,
        hide_index=True
    )

    st.caption(
        f"Showing the latest 50 trades from {len(trades):,} "
        "recently loaded records. Dashboard refreshes every 3 seconds."
    )


# =====================================================
# RUN DASHBOARD
# =====================================================

dashboard()

st.markdown(
    """
    <hr>
    <div style="text-align:center; color:#71827b; font-size:12px;">
        CRYPTO INTELLIGENCE SYSTEM · POWERED BY APACHE KAFKA,
        PYTHON, SQLITE AND STREAMLIT
    </div>
    """,
    unsafe_allow_html=True
)
