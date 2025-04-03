import json
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
CONSUMER_TOPIC = 'StockExchange'
PRODUCER_TOPIC = 'portfolios'

# Portfolio definitions for Investor 3
portfolios = {
    'P31': {
        'HPQ': 2200,
        'ZM': 1800,
        'DELL': 2400,
        'NVDA': 1200,
        'IBM': 1900,
        'INTC': 1600
    },
    'P32': {
        'VZ': 1800,
        'AVGO': 2900,
        'NVDA': 1600,
        'AAPL': 2200,
        'DELL': 2500,
        'ORCL': 2000
    }
}

# Initialize Kafka consumer
consumer = KafkaConsumer(
    CONSUMER_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='investor3',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Dictionary to store latest stock prices
stock_prices = {}

# Dictionary to store previous portfolio values
previous_portfolio_values = {
    'P31': {'date': None, 'value': 0},
    'P32': {'date': None, 'value': 0}
}

print("Investor 3 started. Managing portfolios P31 and P32.")

# Process incoming stock data
for message in consumer:
    stock_data = message.value

    # Update stock price
    ticker = stock_data['TICK']
    price = float(stock_data['PRICE'])
    date_str = stock_data.get('DATE')

    # Store the stock price
    if date_str not in stock_prices:
        stock_prices[date_str] = {}
    stock_prices[date_str][ticker] = price

    # Check if we have prices for all stocks in portfolios for this date
    all_stocks = set(portfolios['P31'].keys()).union(set(portfolios['P32'].keys()))

    if date_str in stock_prices and all(ticker in stock_prices[date_str] for ticker in all_stocks):
        # Calculate portfolio values
        for portfolio_id, portfolio_stocks in portfolios.items():
            # Calculate current portfolio value
            portfolio_value = sum(
                portfolio_stocks[ticker] * stock_prices[date_str][ticker]
                for ticker in portfolio_stocks
                if ticker in stock_prices[date_str]
            )

            # Calculate changes from previous day
            prev_value = previous_portfolio_values[portfolio_id]['value']
            value_change = portfolio_value - prev_value if prev_value > 0 else 0
            pct_change = (value_change / prev_value * 100) if prev_value > 0 else 0

            # Prepare message for Kafka
            portfolio_data = {
                'investor_id': 'Inv3',
                'portfolio_id': portfolio_id,
                'date': date_str,
                'value': round(portfolio_value, 2),
                'change': round(value_change, 2),
                'change_pct': round(pct_change, 2)
            }

            # Send to Kafka
            producer.send(PRODUCER_TOPIC, portfolio_data)
            print(f"Portfolio update: {portfolio_data}")

            # Update previous value for next calculation
            previous_portfolio_values[portfolio_id] = {
                'date': date_str,
                'value': portfolio_value
            }

        # Clear the stock prices for this date to avoid recalculation
        stock_prices[date_str] = {}