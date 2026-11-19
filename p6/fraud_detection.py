"""
Practical 6 - Python Stateful Stream Simulation
Demonstrates Keyed State and Fraud Detection logic matching Flink's KeyedProcessFunction.
Rule: Detect two large transactions (>= ₹10,000) within 1 minute (60,000 ms) for the same customer.
"""
from datetime import datetime, timezone
from collections import defaultdict


class Transaction:
    def __init__(self, customer_id, amount, timestamp):
        self.customer_id = customer_id
        self.amount = amount
        self.timestamp = timestamp


class KeyedFraudDetector:
    def __init__(self):
        # Keyed state partitioned by customer_id
        self.state = defaultdict(lambda: None)

    def process_element(self, tx):
        alerts = []
        if tx.amount < 10000:
            return alerts

        prev = self.state[tx.customer_id]
        if prev is not None:
            gap = tx.timestamp - prev.timestamp
            if 0 <= gap <= 60000:
                alert = (
                    f"FRAUD ALERT: Customer={tx.customer_id}, "
                    f"Previous=₹{prev.amount}, "
                    f"Current=₹{tx.amount}, "
                    f"Gap={gap // 1000} seconds"
                )
                alerts.append(alert)

        self.state[tx.customer_id] = tx
        return alerts


if __name__ == "__main__":
    print("=" * 65)
    print("Running Stateful Fraud Detection Simulation (Flink Logic)")
    print("=" * 65)

    base = int(datetime(2026, 10, 19, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    transactions = [
        Transaction("C101", 15000, base + 10000),
        Transaction("C202", 5000,  base + 20000),
        Transaction("C101", 20000, base + 45000),  # Gap from 10000 = 35s -> ALERT!
        Transaction("C202", 12000, base + 50000),
        Transaction("C101", 3000,  base + 55000),  # Under 10k, ignored
        Transaction("C101", 18000, base + 90000),  # Gap from 45000 = 45s -> ALERT!
        Transaction("C202", 14000, base + 130000), # Gap from 50000 = 80s (>60s) -> No alert
    ]

    detector = KeyedFraudDetector()
    for tx in transactions:
        alerts = detector.process_element(tx)
        for alert in alerts:
            print(f"> {alert}")

    print("=" * 65)
    print("Job completed successfully!")
