
import json
import mysql.connector
from mysql.connector import Error
from kafka import KafkaConsumer
from datetime import datetime

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
CONSUMER_TOPIC = 'portfolios'

# MySQL configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'itc6107',
    'password': 'itc6107',
    'database': 'InvestorsDB',
    'auth_plugin': 'mysql_native_password'
}

# Define number of shares for each portfolio
PORTFOLIO_SHARES = {
    'Inv1_P11': 12200,
    'Inv1_P12': 12700,
    'Inv2_P21': 11800,
    'Inv2_P22': 11900,
    'Inv3_P31': 11100,
    'Inv3_P32': 13000
}
def create_connection():
    #Create a connection to the MySQL database
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        print("MySQL connection successful")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def insert_portfolio_data(connection, investor_id, portfolio_id, date_str, total_value, daily_change,
                          daily_change_pct):
    #Insert portfolio data into the database
    try:
        cursor = connection.cursor()
        table_name = f"{investor_id}_{portfolio_id}"

        # Get number of shares for this portfolio
        portfolio_key = f"{investor_id}_{portfolio_id}"
        shares = PORTFOLIO_SHARES.get(portfolio_key, 1000)  # Default to 1000 if not found

        # Calculate NAV per share
        nav_per_share = total_value / shares

        # Calculate daily NAV change per share
        # Since the incoming daily_change is for the total portfolio,
        # we need to calculate it per share
        daily_nav_change = daily_change / shares if daily_change is not None else None

        # The daily_change_pct remains the same as percentage doesn't
        # depend on the number of shares
        # Check if the table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone()

        if not table_exists:
            print(f"Table {table_name} does not exist. Creating it...")
            create_table_query = f"""
            CREATE TABLE {table_name} (
                AsOf DATE PRIMARY KEY,
                NAVperShare DECIMAL(10, 2) NOT NULL,
                DailyNAVChange DECIMAL(10, 2),
                DailyNAVChangePercent DECIMAL(10, 2))
                """
            cursor.execute(create_table_query)
            connection.commit()

        # Convert the date string to a proper MySQL date format
        # Validate and convert the date string
        try:
            # Parse the input date string (assuming format is YYYY-MM-DD)
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            # Format to MySQL date format
            mysql_date = date_obj.strftime('%Y-%m-%d')

            # Insert or update the data
            query = f"""
            INSERT INTO {table_name} (AsOf, NAVperShare, DailyNAVChange, DailyNAVChangePercent)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                NAVperShare = VALUES(NAVperShare),
                DailyNAVChange = VALUES(DailyNAVChange),
                DailyNAVChangePercent = VALUES(DailyNAVChangePercent)"""
            
            values = (mysql_date, nav_per_share, daily_change, daily_change_pct)
            cursor.execute(query, values)
            connection.commit()
            print(f"Data inserted into {table_name} for date {mysql_date}")
            print(f"NAV per Share: {nav_per_share:.2f}, Daily Change: {daily_nav_change:.2f}, Change %: {daily_change_pct:.2f}%")
        except ValueError as ve:
            print(f"Invalid date format: {date_str}. Error: {ve}")
            print(f"Skipping this record for {investor_id}_{portfolio_id}")

    except Error as e:
        print(f"Error inserting data: {e}")
        print(
            f"Values attempted: investor_id={investor_id}, portfolio_id={portfolio_id}, date={date_str}, nav={nav_per_share}")


def main():
    # Create a database connection
    connection = create_connection()

    if connection is None:
        return

    # Create Kafka consumer
    consumer = KafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='app1',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Listening for messages on topic '{CONSUMER_TOPIC}'...")

    # Process incoming portfolio data
    for message in consumer:
        try:
            portfolio_data = message.value

            # Debug print the received message
            print(f"Received message: {portfolio_data}")

            # Extract data with validation
            investor_id = portfolio_data.get('investor_id')
            portfolio_id = portfolio_data.get('portfolio_id')
            date = portfolio_data.get('date')
            value = portfolio_data.get('value')
            change = portfolio_data.get('change')
            change_pct = portfolio_data.get('change_pct')

            # Validate required fields
            if not all([investor_id, portfolio_id, date, value is not None]):
                print(f"Missing required fields in message: {portfolio_data}")
                continue

            # Insert data into the database
            insert_portfolio_data(connection, investor_id, portfolio_id, date, value, change, change_pct)

        except Exception as e:
            print(f"Error processing message: {e}")
            continue


if __name__ == "__main__":
    main()

