# Design-A-Distributed-Financial-Investment-Service-Network
A real-time stock portfolio analytics system using Kafka, MySQL, and PySpark. Simulates stock exchanges, tracks investor portfolios, and generates performance reports with statistical insights.

## 📌 Overview
This project simulates a stock market ecosystem with:
- **Stock Exchange Servers** (`se1_server.py`, `se2_server.py`): Generate mock stock prices for 24 stocks (2000–present) and publish to Kafka.
- **Investor Portfolio Managers** (`inv1.py`, `inv2.py`, `inv3.py`): Calculate portfolio values in real-time from stock data.
- **Database Layer** (`investorsDB.py`, `app1.py`): Stores portfolio NAVs in MySQL.
- **Analytics Engine** (`app2.py`): Uses PySpark to compute performance metrics (max/min changes, volatility, monthly trends).
 
 ----------------

## 🚀 Setup & Execution

## Instructions on how to run the network of applications:

## 1) RequirementS
    - Install Python 3.x, Kafka 4.0.0, MySQL, Spark
    - Install necessary Python packeages: kafka-python, mysql-connector-python, pyspark

## 2) Start Kafka 
    - Start zookeeper: sudo systemctl start zookeeper
    - Start Kafka: sudo systemctl start zookeeper
    - Ensure that both kafka and zookeeper are running: sudo systemctl status zookeeper/kafka 
    - Create Topics: 
	1) kafka-topics.sh --create  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic StockExchange
	2) kafka-topics.sh --create  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic portfolios

## 3) Start MySQL 
   - Run investorsDB.py to initialize tha database

Open PyCharm/in separate terminals the following: 

## 4) Run Stock Exchange Servers:
    - se1_server.py
    - se2_server.py

## 5) Run Investors:
   - inv1.py
   - inv2.py
   - inv3.py

## 6) Run app1.py to insert data from the topics to the database tables

## 7) once every other application has finished running, run application app2.py to produce the statistics required
