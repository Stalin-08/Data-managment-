
# Real-Time Cryptocurrency Anomaly Detection Using Apache Kafka

## 1. Project Overview

This project implements a real-time cryptocurrency market monitoring and anomaly detection system using Apache Kafka and Python.

It collects live cryptocurrency trade data for BTCUSDT and ETHUSDT from Binance WebSocket streams, processes the data through Kafka, detects unusual price movements and trading volumes, and stores the results in an SQLite database.

A Streamlit dashboard provides real-time visualization of cryptocurrency trades, detected anomalies, and system performance.

## 2. Project Objectives

- Collect real-time cryptocurrency trade data.
- Use Apache Kafka for distributed event streaming.
- Detect unusual price movements and trading volumes.
- Store trade records and anomaly alerts in SQLite.
- Visualize market activity through a Streamlit dashboard.
- Evaluate system performance using throughput and latency metrics.

## 3. System Architecture

Binance WebSocket API
        |
        v
Python Kafka Producer
        |
        v
Apache Kafka (crypto-trades topic)
        |
        v
Python Kafka Consumer
        |
        v
Crypto Anomaly Detection Engine
        |
        v
SQLite Database
        |
        v
Streamlit Dashboard

## 4. Technologies Used

- Python 3.12
- Apache Kafka
- Docker and Docker Compose
- Binance WebSocket API
- Confluent Kafka Python Client
- SQLite
- Streamlit
- Pandas
- NumPy
- Pytest

## 5. Anomaly Detection Methodology

The project uses statistical Z-score analysis to identify unusual cryptocurrency trading activity.

The detector maintains separate historical observations for each cryptocurrency.

Two indicators are monitored:

1. Price percentage changes.
2. Log-transformed trading volume.

The system uses:
- Rolling window size: 100 observations.
- Minimum historical observations: 30.
- Anomaly threshold: 3.5.

An event is flagged when the absolute price-change Z-score or the trading-volume Z-score reaches the configured threshold.

These alerts identify statistically unusual market activity and do not establish fraudulent trading.

## 6. Project Structure

crypto-kafka-anomaly-detection/
|
|-- producer/
|   |-- binance_producer.py
|
|-- consumer/
|   |-- kafka_consumer.py
|
|-- detection/
|   |-- anomaly_detector.py
|
|-- database/
|   |-- db.py
|
|-- dashboard/
|   |-- app.py
|
|-- monitoring/
|   |-- performance_test.py
|   |-- performance_metrics.csv
|
|-- tests/
|   |-- test_detector.py
|
|-- check_database.py
|-- docker-compose.yml
|-- requirements.txt
|-- README.md

## 7. Installation and Setup

### Prerequisites

- Python 3.12
- Docker Desktop
- Docker Compose
- Git

### Step 1: Clone the repository

git clone YOUR_GITHUB_REPOSITORY_URL

cd crypto-kafka-anomaly-detection

### Step 2: Create a virtual environment

python -m venv .venv

Activate it in Windows PowerShell:

.\.venv\Scripts\Activate.ps1

### Step 3: Install dependencies

pip install -r requirements.txt

### Step 4: Start Kafka

docker compose up -d

### Step 5: Start the Kafka producer

Open a new terminal:

python -m producer.binance_producer

### Step 6: Start the Kafka consumer

Open another terminal:

python -m consumer.kafka_consumer

### Step 7: Launch the dashboard

Open another terminal:

streamlit run dashboard/app.py

Open the dashboard at:

http://localhost:8501

## 8. Testing

The project uses Pytest for automated testing.

Run:

python -m pytest -v

The detector tests cover:

- Normal trade processing.
- Invalid trade values.
- Insufficient historical data.
- Unusual price movement detection.

## 9. Performance Monitoring

The project records performance metrics including:

- Total processed messages.
- Number of detected anomalies.
- Current throughput.
- Average throughput.
- Average end-to-end latency.
- P95 latency.
- P99 latency.
- Average processing time.

Performance results are stored in:

monitoring/performance_metrics.csv

Run the performance report:

python monitoring/performance_test.py

## 10. Database

SQLite is used to store cryptocurrency trades and detected anomalies.

The database is created locally when the application initializes.

The database file is excluded from GitHub and is generated during execution.

## 11. Limitations

- The project currently monitors BTCUSDT and ETHUSDT.
- Anomaly detection is based on statistical thresholds.
- Kafka is configured with a single broker.
- SQLite is used for local persistence.
- Detection results depend on the historical observations available to the system.
- Detected anomalies are statistical alerts, not confirmed market manipulation or fraud.

## 12. Conclusion

This project demonstrates a real-time cryptocurrency data processing pipeline using Apache Kafka, Python, statistical anomaly detection, and Streamlit.

It combines live market data ingestion, event streaming, anomaly detection, database storage, dashboard visualization, and performance monitoring in one integrated system.
