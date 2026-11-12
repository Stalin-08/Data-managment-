
import sqlite3
import json
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "crypto_data.db"


def get_connection():
    connection = sqlite3.connect(
        DB_PATH,
        timeout=60
    )

    connection.row_factory = sqlite3.Row

    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=60000")
    connection.execute("PRAGMA synchronous=NORMAL")

    return connection


def init_db():

    with get_connection() as conn:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                quantity REAL NOT NULL,
                trade_id TEXT,
                trade_time INTEGER,
                received_at_ns INTEGER,
                is_anomaly INTEGER DEFAULT 0,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL,
                quantity REAL,
                price_zscore REAL,
                volume_zscore REAL,
                reasons TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()


def save_trade(trade, result, connection=None):

    # Reuse the supplied connection when available.
    owns_connection = connection is None
    conn = connection if connection is not None else get_connection()

    try:

        conn.execute("""
            INSERT INTO trades (
                symbol,
                price,
                quantity,
                trade_id,
                trade_time,
                received_at_ns,
                is_anomaly
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            trade["symbol"],
            trade["price"],
            trade["quantity"],
            str(trade.get("trade_id", "")),
            trade.get("trade_time"),
            trade.get("received_at_ns"),
            int(result["is_anomaly"])
        ))

        if result["is_anomaly"]:

            conn.execute("""
                INSERT INTO anomalies (
                    symbol,
                    price,
                    quantity,
                    price_zscore,
                    volume_zscore,
                    reasons
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                result["symbol"],
                result["price"],
                result["quantity"],
                result["price_zscore"],
                result["volume_zscore"],
                json.dumps(result["reasons"])
            ))

        # Commit every trade individually.
        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:

        # Close only connections created by this function.
        if owns_connection:
            conn.close()


def get_statistics():

    with get_connection() as conn:

        trades = conn.execute(
            "SELECT COUNT(*) FROM trades"
        ).fetchone()[0]

        anomalies = conn.execute(
            "SELECT COUNT(*) FROM anomalies"
        ).fetchone()[0]

        return {
            "total_trades": trades,
            "total_anomalies": anomalies
        }
