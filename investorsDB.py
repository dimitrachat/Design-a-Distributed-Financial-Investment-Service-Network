import mysql.connector
from mysql.connector import Error


def create_connection():
    """Create a connection to the MySQL database"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='itc6107',
            password='itc6107',
            auth_plugin='mysql_native_password'
        )
        print("MySQL connection successful")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def create_database(connection, db_name):
    """Create a database"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' created or already exists")
    except Error as e:
        print(f"Error creating database: {e}")


def execute_query(connection, query):
    """Execute a query"""
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"Error executing query: {e}")


def main():
    # Create a connection
    connection = create_connection()

    if connection is not None:
        # Create database
        create_database(connection, "InvestorsDB")

        # Use the database
        execute_query(connection, "USE InvestorsDB")

        # Create Investors table
        investors_table = """
        CREATE TABLE IF NOT EXISTS Investors (
            Id VARCHAR(10) PRIMARY KEY,
            Name VARCHAR(100) NOT NULL,
            City VARCHAR(100) NOT NULL
        );
        """
        execute_query(connection, investors_table)

        # Create Portfolios table
        portfolios_table = """
        CREATE TABLE IF NOT EXISTS Portfolios (
            Id VARCHAR(10) PRIMARY KEY,
            Name VARCHAR(100) NOT NULL,
            Cumulative BOOLEAN NOT NULL
        );
        """
        execute_query(connection, portfolios_table)

        # Create Investors_Portfolios table
        investors_portfolios_table = """
        CREATE TABLE IF NOT EXISTS Investors_Portfolios (
            iid VARCHAR(10),
            pid VARCHAR(10),
            PRIMARY KEY (iid, pid),
            FOREIGN KEY (iid) REFERENCES Investors(Id),
            FOREIGN KEY (pid) REFERENCES Portfolios(Id)
        );
        """
        execute_query(connection, investors_portfolios_table)

        # Create portfolio performance tables for each investor/portfolio pair
        portfolio_tables = [
            "Inv1_P11", "Inv1_P12",
            "Inv2_P21", "Inv2_P22",
            "Inv3_P31", "Inv3_P32"
        ]

        for table_name in portfolio_tables:
            portfolio_table = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                AsOf DATE PRIMARY KEY,
                NAVperShare DECIMAL(10, 2) NOT NULL,
                DailyNAVChange DECIMAL(10, 2),
                DailyNAVChangePercent DECIMAL(10, 2)
            );
            """
            execute_query(connection, portfolio_table)

        # Insert sample data into Investors table
        insert_investors = """
        INSERT INTO Investors (Id, Name, City)
        VALUES 
            ('Inv1', 'Alpha Investments', 'New York'),
            ('Inv2', 'Beta Capital', 'London'),
            ('Inv3', 'Gamma Fund', 'Tokyo')
        ON DUPLICATE KEY UPDATE Name=VALUES(Name), City=VALUES(City);
        """
        execute_query(connection, insert_investors)

        # Insert sample data into Portfolios table
        insert_portfolios = """
        INSERT INTO Portfolios (Id, Name, Cumulative)
        VALUES 
            ('P11', 'Alpha Tech Growth', TRUE),
            ('P12', 'Alpha Value', TRUE),
            ('P21', 'Beta Tech Innovation', FALSE),
            ('P22', 'Beta High Growth', TRUE),
            ('P31', 'Gamma Balanced', FALSE),
            ('P32', 'Gamma Tech Leaders', TRUE)
        ON DUPLICATE KEY UPDATE Name=VALUES(Name), Cumulative=VALUES(Cumulative);
        """
        execute_query(connection, insert_portfolios)

        # Link investors with portfolios
        insert_investors_portfolios = """
        INSERT INTO Investors_Portfolios (iid, pid)
        VALUES 
            ('Inv1', 'P11'),
            ('Inv1', 'P12'),
            ('Inv2', 'P21'),
            ('Inv2', 'P22'),
            ('Inv3', 'P31'),
            ('Inv3', 'P32')
        ON DUPLICATE KEY UPDATE iid=VALUES(iid), pid=VALUES(pid);
        """
        execute_query(connection, insert_investors_portfolios)

        # Close the connection
        connection.close()
        print("MySQL connection closed")


if __name__ == "__main__":
    main()
