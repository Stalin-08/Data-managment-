
from collections import deque
import math
import statistics


class CryptoAnomalyDetector:

    def __init__(self, window_size=100, threshold=3.5):
        self.window_size = window_size
        self.threshold = threshold

        # Separate history for each cryptocurrency
        self.history = {}

    def calculate_zscore(self, value, history):

        if len(history) < 30:
            return 0.0

        mean = statistics.mean(history)
        std = statistics.pstdev(history)

        if std == 0:
            return 0.0

        return (value - mean) / std

    def process_trade(self, trade):

        symbol = trade["symbol"]
        price = float(trade["price"])
        quantity = float(trade["quantity"])

        if price <= 0 or quantity <= 0:
            return {
                "symbol": symbol,
                "is_anomaly": False,
                "reason": "Invalid trade values"
            }

        if symbol not in self.history:

            self.history[symbol] = {
                "prices": None,
                "returns": deque(maxlen=self.window_size),
                "volumes": deque(maxlen=self.window_size)
            }

        data = self.history[symbol]

        previous_price = data["prices"]

        # Calculate percentage price change
        if previous_price is None:
            price_change = 0.0
        else:
            price_change = (
                (price - previous_price) / previous_price
            ) * 100

        data["prices"] = price

        # Log transformed trading volume
        log_volume = math.log1p(quantity)

        # Calculate anomaly scores using previous observations
        return_score = self.calculate_zscore(
            price_change,
            data["returns"]
        )

        volume_score = self.calculate_zscore(
            log_volume,
            data["volumes"]
        )

        # Detect unusual behaviour
        reasons = []

        if len(data["returns"]) >= 30:

            if abs(return_score) >= self.threshold:
                reasons.append("Unusual price movement")

            if volume_score >= self.threshold:
                reasons.append("Unusual trading volume")

        is_anomaly = len(reasons) > 0

        # Update history after evaluating current trade
        data["returns"].append(price_change)
        data["volumes"].append(log_volume)

        return {
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "price_change_percent": round(price_change, 6),
            "price_zscore": round(return_score, 3),
            "volume_zscore": round(volume_score, 3),
            "is_anomaly": is_anomaly,
            "reasons": reasons,
            "status": "ANOMALY" if is_anomaly else "NORMAL"
        }
