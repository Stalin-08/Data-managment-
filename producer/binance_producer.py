
import json
import time
import logging
from datetime import datetime, timezone

import websocket
from confluent_kafka import Producer


# ==========================================
# KAFKA CONFIGURATION
# ==========================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "crypto-trades"

# Explicit partition assignment
PARTITION_MAP = {
    "BTCUSDT": 0,
    "ETHUSDT": 1
}


# ==========================================
# BINANCE WEBSOCKET CONFIGURATION
# ==========================================

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=btcusdt@trade/ethusdt@trade"
)


# ==========================================
# LOGGING CONFIGURATION
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ==========================================
# KAFKA PRODUCER
# ==========================================

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "binance-crypto-producer",
    "acks": "all",
    "enable.idempotence": True
})

message_count = 0


# ==========================================
# DELIVERY CALLBACK
# ==========================================

def delivery_report(err, msg):

    if err:
        logging.error(
            "Kafka delivery failed: %s",
            err
        )

    else:
        logging.debug(
            "Delivered to %s partition %s",
            msg.topic(),
            msg.partition()
        )


# ==========================================
# PROCESS BINANCE TRADE EVENTS
# ==========================================

def on_message(ws, message):

    global message_count

    try:

        received_at = time.time_ns()

        payload = json.loads(message)
        trade = payload["data"]

        symbol = trade["s"]

        # Ignore unsupported symbols
        if symbol not in PARTITION_MAP:
            logging.warning(
                "Unsupported symbol received: %s",
                symbol
            )
            return

        # Prepare trade event
        event = {
            "symbol": symbol,
            "trade_id": trade["t"],
            "price": float(trade["p"]),
            "quantity": float(trade["q"]),
            "trade_time": trade["T"],
            "event_time": trade["E"],
            "buyer_is_maker": trade["m"],
            "source": "binance_websocket",
            "received_at_ns": received_at,
            "processed_at_utc": datetime.now(
                timezone.utc
            ).isoformat()
        }

        # Assign partition based on cryptocurrency
        assigned_partition = PARTITION_MAP[symbol]

        # Publish trade to Kafka
        producer.produce(
            topic=KAFKA_TOPIC,
            partition=assigned_partition,
            key=symbol,
            value=json.dumps(event),
            callback=delivery_report
        )

        producer.poll(0)

        message_count += 1

        if message_count % 10 == 0:

            logging.info(
                "Published %s trades | %s | Partition: %s | Price: %s",
                message_count,
                symbol,
                assigned_partition,
                event["price"]
            )

    except Exception:

        logging.exception(
            "Error processing trade event"
        )


# ==========================================
# WEBSOCKET ERROR HANDLER
# ==========================================

def on_error(ws, error):

    logging.error(
        "WebSocket error: %s",
        error
    )


# ==========================================
# WEBSOCKET CLOSE HANDLER
# ==========================================

def on_close(ws, close_status_code, close_message):

    logging.warning(
        "Binance connection closed: %s",
        close_message
    )


# ==========================================
# WEBSOCKET OPEN HANDLER
# ==========================================

def on_open(ws):

    logging.info(
        "Connected to Binance WebSocket"
    )

    logging.info(
        "Streaming BTCUSDT and ETHUSDT"
    )

    logging.info(
        "BTCUSDT -> Partition 0"
    )

    logging.info(
        "ETHUSDT -> Partition 1"
    )

    logging.info(
        "Kafka topic: %s",
        KAFKA_TOPIC
    )


# ==========================================
# MAIN PRODUCER FUNCTION
# ==========================================

def main():

    logging.info(
        "Starting Binance Kafka Producer"
    )

    while True:

        try:

            ws = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except KeyboardInterrupt:

            logging.info(
                "Stopping producer"
            )

            break

        except Exception:

            logging.exception(
                "Producer connection failed"
            )

        finally:

            producer.flush()

        logging.info(
            "Reconnecting in 5 seconds..."
        )

        time.sleep(5)

    producer.flush()

    logging.info(
        "Producer stopped successfully"
    )


# ==========================================
# ENTRY POINT
# ==========================================

if __name__ == "__main__":
    main()
