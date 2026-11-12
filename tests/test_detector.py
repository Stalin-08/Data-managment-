
from detection.anomaly_detector import CryptoAnomalyDetector


def test_normal_trade():
    detector = CryptoAnomalyDetector()

    result = detector.process_trade({
        "symbol": "BTCUSDT",
        "price": 74000,
        "quantity": 0.002
    })

    assert result["symbol"] == "BTCUSDT"
    assert result["is_anomaly"] is False
    assert result["status"] == "NORMAL"


def test_invalid_trade_values():
    detector = CryptoAnomalyDetector()

    result = detector.process_trade({
        "symbol": "BTCUSDT",
        "price": -100,
        "quantity": 0.002
    })

    assert result["is_anomaly"] is False
    assert result["reason"] == "Invalid trade values"


def test_insufficient_history():
    detector = CryptoAnomalyDetector()

    for i in range(10):
        result = detector.process_trade({
            "symbol": "BTCUSDT",
            "price": 74000 + i,
            "quantity": 0.002
        })

    assert result["is_anomaly"] is False


def test_unusual_price_movement():
    detector = CryptoAnomalyDetector(threshold=3.5)

    # Build historical data with small, alternating price movements.
    price = 74000

    for i in range(40):
        if i % 2 == 0:
            price *= 1.0001
        else:
            price *= 0.9999

        detector.process_trade({
            "symbol": "BTCUSDT",
            "price": price,
            "quantity": 0.002
        })

    # Introduce a sudden price increase.
    result = detector.process_trade({
        "symbol": "BTCUSDT",
        "price": 80000,
        "quantity": 0.002
    })

    assert result["is_anomaly"] is True
    assert "Unusual price movement" in result["reasons"]
