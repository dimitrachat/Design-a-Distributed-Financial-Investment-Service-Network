import socket
import time
import random
import datetime
import json
from kafka import KafkaProducer
from datetime import date, timedelta

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'StockExchange'

# Stock list - remaining 12 stocks
stocks = [
    ('HPQ', 28.98), ('CSCO', 60.06), ('ZM', 73.47), ('QCOM', 154.98),
    ('ADBE', 435.08), ('VZ', 46.49), ('TXN', 186.49), ('CRM', 272.90),
    ('AVGO', 184.45), ('NVDA', 106.98), ('MSTR', 239.27), ('EBAY', 68.19)
]

# Greek holidays (simplified list)
greek_holidays = [
    # Format: (month, day)
    (1, 1),
    (1, 6),
    (3, 25),
    (5, 1),
    (8, 15),
    (10, 28),
    (12, 25),
    (12, 26)
]


# Function to check if a date is a holiday or weekend
def is_non_trading_day(check_date):
    # Check if it's a weekend
    if check_date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return True

    # Check if it's a Greek holiday
    for holiday in greek_holidays:
        if check_date.month == holiday[0] and check_date.day == holiday[1]:
            return True

    return False


# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Starting date
start_date = date(2000, 1, 1)
current_date = datetime.datetime.now().date()

print(f"Stock Exchange Server 2 started. Emitting data for {len(stocks)} stocks from {start_date} to {current_date}.")

# Iterate through all dates
simulation_date = start_date
while simulation_date <= current_date:
    # Skip non-trading days
    if not is_non_trading_day(simulation_date):
        for stock_info in stocks:
            ticker, base_price = stock_info

            # Add some random price variation
            random_factor = random.random() / 10 - 0.05  # -5% to +5%
            price = base_price * (1 + random_factor)

            # Create message
            msg = {
                "TICK": ticker,
                "PRICE": round(price, 2),
                "DATE": simulation_date.strftime("%Y-%m-%d"),
                "EXCHANGE": "SE2"
            }

            # Send to Kafka
            producer.send(TOPIC_NAME, msg)
            print(f"Sent: {msg}")

        # Flush to ensure all messages are sent
        producer.flush()

        # Sleep for at least 2 seconds between days
        time.sleep(2)

    # Move to next day
    simulation_date += timedelta(days=1)

print("Stock Exchange Server 2 completed emitting historical data.")