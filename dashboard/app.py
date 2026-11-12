
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- CONFIGURATION ----------------

st.set_page_config(
    page_title="Crypto Anomaly Detection",
    page_icon="📊",
    layout="wide"
)

DB_PATH = (
    Path(__file__).resolve().parents[1]
    / "database"
    / "crypto_data.db"
)

st.title("₿ Real-Time Crypto Market Anomaly Detection")
st.caption(
    "Apache Kafka | Binance WebSocket | Python | Machine Learning"
)

# ---------------- DATABASE ----------------

def load_data():

    with sqlite3.connect(DB_PATH, timeout=10) as conn:

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


# ---------------- DASHBOARD ----------------

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

    if trades.empty:
        st.info("Waiting for incoming cryptocurrency trades...")
        return

    # Sort chronologically for charts
    trades = trades.sort_values("id")

    # ---------------- KPI METRICS ----------------

    anomaly_rate = (
        total_anomalies / total_trades * 100
        if total_trades else 0
    )

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Trades", f"{total_trades:,}")
    col2.metric("Detected Anomalies", f"{total_anomalies:,}")
    col3.metric("Anomaly Rate", f"{anomaly_rate:.2f}%")
    col4.metric("Latest Price", f"{trades.iloc[-1]['price']:,.2f}")

    st.divider()

    # ---------------- PRICE CHART ----------------

    st.subheader("Cryptocurrency Price Monitoring")

    for symbol in ["BTCUSDT", "ETHUSDT"]:

        symbol_data = trades[
            trades["symbol"] == symbol
        ].copy()

        if not symbol_data.empty:

            fig = px.line(
                symbol_data,
                x="id",
                y="price",
                title=f"{symbol} Live Trade Prices",
                labels={
                    "id": "Trade sequence",
                    "price": "Price (USDT)"
                }
            )

            st.plotly_chart(fig, use_container_width=True)

    # ---------------- TRADING VOLUME ----------------

    st.subheader("Trading Activity")

    volume_data = (
        trades.groupby("symbol")["quantity"]
        .sum()
        .reset_index()
    )

    volume_fig = px.bar(
        volume_data,
        x="symbol",
        y="quantity",
        color="symbol",
        title="Total Trading Quantity in Loaded Trades"
    )

    st.plotly_chart(volume_fig, use_container_width=True)

    # ---------------- ANOMALY ALERTS ----------------

    st.divider()
    st.subheader("🚨 Recent Anomaly Alerts")

    if not anomalies.empty:

        st.dataframe(
            anomalies[
                [
                    "symbol",
                    "price",
                    "quantity",
                    "price_zscore",
                    "volume_zscore",
                    "reasons",
                    "created_at"
                ]
            ],
            use_container_width=True,
            hide_index=True
        )

    else:
        st.success("No anomalies detected yet.")

    # ---------------- RECENT TRADES ----------------

    st.divider()
    st.subheader("Recent Cryptocurrency Trades")

    st.dataframe(
        trades[
            [
                "symbol",
                "price",
                "quantity",
                "trade_time",
                "is_anomaly"
            ]
        ].tail(50).iloc[::-1],
        use_container_width=True,
        hide_index=True
    )

    st.caption(
        "Dashboard refreshes automatically every 3 seconds. "
        "Charts display the latest stored trades."
    )


dashboard()
