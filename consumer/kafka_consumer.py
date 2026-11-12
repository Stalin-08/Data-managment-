
import json
import time
import logging
import csv
from pathlib import Path
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException

from detection.anomaly_detector import CryptoAnomalyDetector
from database.db import init_db, get_connection, save_trade


# ==========================================
# KAFKA CONFIGURATION
# ==========================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "crypto-trades"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "crypto-anomaly-consumer",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "session.timeout.ms": 60000,
    "heartbeat.interval.ms": 20000,
    "max.poll.interval.ms": 300000
})


# ==========================================
# LOGGING CONFIGURATION
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ==========================================
# ANOMALY DETECTOR
# ==========================================

detector = CryptoAnomalyDetector()

total_messages = 0
anomaly_count = 0

latency_samples = deque(maxlen=10000)
processing_samples = deque(maxlen=10000)


# ==========================================
# PERFORMANCE CSV CONFIGURATION
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

MONITORING_DIR = PROJECT_ROOT / "monitoring"
MONITORING_DIR.mkdir(exist_ok=True)

METRICS_FILE = MONITORING_DIR / "performance_metrics.csv"


def percentile(values, percent):
    if not values:
        return 0.0

    ordered = sorted(values)

    index = int((percent / 100) * len(ordered))
    index = min(index, len(ordered) - 1)

    return ordered[index]


# ==========================================
# PERFORMANCE REPORTING
# ==========================================

def report_metrics(
    total_messages,
    anomaly_count,
    interval_count,
    interval_start,
    start_time,
    latency_samples,
    processing_samples,
    writer,
    csv_file
):

    now = time.monotonic()
    elapsed = now - start_time
    interval_elapsed = now - interval_start

    current_throughput = (
        interval_count / interval_elapsed
        if interval_elapsed > 0 else 0
    )

    average_throughput = (
        total_messages / elapsed
        if elapsed > 0 else 0
    )

    latencies = list(latency_samples)
    processing = list(processing_samples)

    avg_latency = (
        sum(latencies) / len(latencies)
        if latencies else 0
    )

    avg_processing = (
        sum(processing) / len(processing)
        if processing else 0
    )

    p95_latency = percentile(latencies, 95)
    p99_latency = percentile(latencies, 99)

    logging.info(
        "PERFORMANCE | Total: %s | "
        "Current Throughput: %.2f msg/sec | "
        "Average Throughput: %.2f msg/sec | "
        "Avg Latency: %.2f ms | "
        "P95: %.2f ms | P99: %.2f ms | "
        "Processing: %.2f ms | Anomalies: %s",

        total_messages,
        current_throughput,
        average_throughput,
        avg_latency,
        p95_latency,
        p99_latency,
        avg_processing,
        anomaly_count
    )

    writer.writerow({
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_messages": total_messages,
        "anomalies": anomaly_count,
        "current_throughput": round(current_throughput, 2),
        "average_throughput": round(average_throughput, 2),
        "average_latency_ms": round(avg_latency, 2),
        "p95_latency_ms": round(p95_latency, 2),
        "p99_latency_ms": round(p99_latency, 2),
        "average_processing_ms": round(avg_processing, 2)
    })

    csv_file.flush()

    latency_samples.clear()
    processing_samples.clear()

    return 0, now


# ==========================================
# MAIN CONSUMER
# ==========================================

def main():

    global total_messages, anomaly_count

    db_connection = None

    try:

        # Initialize database tables
        init_db()

        # Create one persistent database connection
        db_connection = get_connection()

        consumer.subscribe([KAFKA_TOPIC])

        logging.info("CRYPTO ANOMALY DETECTION STARTED")
        logging.info("Kafka broker: %s", KAFKA_BROKER)
        logging.info("Kafka topic: %s", KAFKA_TOPIC)
        logging.info("Database initialized")
        logging.info("Persistent SQLite connection established")
        logging.info("Metrics file: %s", METRICS_FILE)

        start_time = time.monotonic()
        interval_start = start_time
        interval_count = 0

        file_exists = (
            METRICS_FILE.exists()
            and METRICS_FILE.stat().st_size > 0
        )

        with open(
            METRICS_FILE,
            "a",
            newline="",
            encoding="utf-8"
        ) as csv_file:

            fieldnames = [
                "timestamp_utc",
                "total_messages",
                "anomalies",
                "current_throughput",
                "average_throughput",
                "average_latency_ms",
                "p95_latency_ms",
                "p99_latency_ms",
                "average_processing_ms"
            ]

            writer = csv.DictWriter(
                csv_file,
                fieldnames=fieldnames
            )

            if not file_exists:
                writer.writeheader()
                csv_file.flush()

            while True:

                msg = consumer.poll(1.0)

                if msg is None:

                    if time.monotonic() - interval_start >= 10:

                        interval_count, interval_start = report_metrics(
                            total_messages,
                            anomaly_count,
                            interval_count,
                            interval_start,
                            start_time,
                            latency_samples,
                            processing_samples,
                            writer,
                            csv_file
                        )

                    continue

                if msg.error():
                    logging.error("Kafka error: %s", msg.error())
                    continue

                try:

                    processing_start = time.perf_counter_ns()

                    trade = json.loads(
                        msg.value().decode("utf-8")
                    )

                    # End-to-end event latency
                    producer_timestamp = trade.get("received_at_ns")

                    if producer_timestamp is not None:

                        latency_ms = (
                            time.time_ns() - int(producer_timestamp)
                        ) / 1_000_000

                        if latency_ms >= 0:
                            latency_samples.append(latency_ms)

                    # Run anomaly detection
                    result = detector.process_trade(trade)

                    # Save using the persistent database connection
                    save_trade(
                        trade,
                        result,
                        connection=db_connection
                    )

                    processing_end = time.perf_counter_ns()

                    processing_ms = (
                        processing_end - processing_start
                    ) / 1_000_000

                    processing_samples.append(processing_ms)

                    total_messages += 1
                    interval_count += 1

                    if result["is_anomaly"]:

                        anomaly_count += 1

                        logging.warning(
                            "ANOMALY | %s | Price: %s | Reasons: %s",
                            result["symbol"],
                            result["price"],
                            ", ".join(result["reasons"])
                        )

                    elif total_messages % 100 == 0:

                        logging.info(
                            "NORMAL | %s | Price: %s",
                            result["symbol"],
                            result["price"]
                        )

                    # Report every 10 seconds
                    if time.monotonic() - interval_start >= 10:

                        interval_count, interval_start = report_metrics(
                            total_messages,
                            anomaly_count,
                            interval_count,
                            interval_start,
                            start_time,
                            latency_samples,
                            processing_samples,
                            writer,
                            csv_file
                        )

                except Exception:

                    logging.exception("Trade processing failed")

    except KeyboardInterrupt:

        logging.info("Stopping Kafka consumer...")

    except KafkaException:

        logging.exception("Kafka connection error")

    finally:

        # Close Kafka consumer
        consumer.close()

        # Close persistent database connection
        if db_connection is not None:
            db_connection.close()
            logging.info("SQLite connection closed")

        logging.info(
            "FINAL SUMMARY | Total messages: %s | Anomalies: %s",
            total_messages,
            anomaly_count
        )

        logging.info(
            "Performance report saved at: %s",
            METRICS_FILE
        )


if __name__ == "__main__":
    main()
