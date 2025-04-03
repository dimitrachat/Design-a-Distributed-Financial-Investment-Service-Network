from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark.sql.types as T


def create_spark_session():
    """Create a Spark session with MySQL connector"""
    return (SparkSession.builder
            .appName("PortfolioAnalysis")
            .config("spark.jars", "/home/itc6107/Downloads/mysql-connector-java-8.0.28.jar")
            .config("spark.driver.extraClassPath", "/home/itc6107/Downloads/mysql-connector-java-8.0.28.jar")
            .getOrCreate())


def load_portfolio_data(spark, investor_id, portfolio_id):
    """
    Load portfolio data from MySQL database
    : spark: SparkSession
    : investor_id: Investor identifier
    : portfolio_id: Portfolio identifier
    :return: Spark DataFrame with portfolio data
    """
    # MySQL connection properties
    db_properties = {
        "url": "jdbc:mysql://localhost:3306/InvestorsDB",
        "user": "itc6107",
        "password": "itc6107",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Table name
    table_name = f"{investor_id}_{portfolio_id}"

    # Read data from MySQL
    df = spark.read.jdbc(
        url=db_properties["url"],
        table=table_name,
        properties=db_properties
    )

    # Convert AsOf to date type and ensure proper sorting
    return (df.withColumn("AsOf", F.to_date(F.col("AsOf")))
            .orderBy("AsOf"))


def analyze_portfolio(spark, df, investor_id, portfolio_id):
    """
    Perform comprehensive portfolio analysis
    : spark: SparkSession
    :df: Portfolio DataFrame
    : investor_id: Investor identifier
    :portfolio_id: Portfolio identifier
    :return: Dictionary of analysis results
    """
    # Add year column
    df_with_year = df.withColumn("Year", F.year(F.col("AsOf")))

    # 1. Full history max/min changes
    overall_stats = {
        "MaxDailyChange": df.agg(F.max("DailyNAVChange")).collect()[0][0],
        "MinDailyChange": df.agg(F.min("DailyNAVChange")).collect()[0][0],
        "MaxDailyChangePercent": df.agg(F.max("DailyNAVChangePercent")).collect()[0][0],
        "MinDailyChangePercent": df.agg(F.min("DailyNAVChangePercent")).collect()[0][0]
    }

    # 2. Max/min changes per year
    yearly_stats = (df_with_year.groupBy("Year")
                    .agg(
        F.max("DailyNAVChange").alias("MaxDailyChange"),
        F.min("DailyNAVChange").alias("MinDailyChange"),
        F.max("DailyNAVChangePercent").alias("MaxDailyChangePercent"),
        F.min("DailyNAVChangePercent").alias("MinDailyChangePercent")
    )
                    .orderBy("Year")
                    .collect())

    # 3. Overall evaluation statistics
    eval_stats = {
        "AverageEvaluation": df.agg(F.mean("NAVperShare")).collect()[0][0],
        "StdDevEvaluation": df.agg(F.stddev("NAVperShare")).collect()[0][0]
    }

    # 4. Evaluation for specific period (2020-2024)
    period_df = df.filter(
        (F.col("AsOf") >= F.to_date(F.lit("2020-01-01"))) &
        (F.col("AsOf") <= F.to_date(F.lit("2024-12-31")))
    )
    period_stats = {
        "PeriodAverageEvaluation": period_df.agg(F.mean("NAVperShare")).collect()[0][0],
        "PeriodStdDevEvaluation": period_df.agg(F.stddev("NAVperShare")).collect()[0][0]
    }

    # 5. Monthly averages (most recent first)
    monthly_avg = (df.withColumn("Month", F.to_date(F.concat(
        F.year(F.col("AsOf")),
        F.lit("-"),
        F.lpad(F.month(F.col("AsOf")), 2, "0"),
        F.lit("-01")
    )))
                   .groupBy("Month")
                   .agg(F.mean("NAVperShare").alias("MonthlyAverage"))
                   .orderBy(F.col("Month").desc())
                   .collect())

    return {
        "OverallStats": overall_stats,
        "YearlyStats": yearly_stats,
        "EvaluationStats": eval_stats,
        "PeriodStats": period_stats,
        "MonthlyAverages": monthly_avg
    }


def write_analysis_to_file(analysis, investor_id, portfolio_id):
    """
    Write portfolio analysis to a file
    : analysis: Dictionary of analysis results
    : investor_id: Investor identifier
    :portfolio_id: Portfolio identifier
    """
    filename = f"{investor_id}_{portfolio_id}_stats.txt"

    with open(filename, "w") as f:
        # Overall Statistics
        f.write("OVERALL PORTFOLIO STATISTICS\n")
        f.write("=" * 40 + "\n\n")
        f.write("1. Full History Daily Changes:\n")
        f.write(f"   Max Daily Change: {analysis['OverallStats']['MaxDailyChange']:.2f}\n")
        f.write(f"   Min Daily Change: {analysis['OverallStats']['MinDailyChange']:.2f}\n")
        f.write(f"   Max Daily Change %: {analysis['OverallStats']['MaxDailyChangePercent']:.2f}%\n")
        f.write(f"   Min Daily Change %: {analysis['OverallStats']['MinDailyChangePercent']:.2f}%\n\n")

        # Yearly Statistics
        f.write("2. Yearly Daily Changes:\n")
        for year_stat in analysis['YearlyStats']:
            f.write(f"   Year {year_stat['Year']}:\n")
            f.write(f"     Max Daily Change: {year_stat['MaxDailyChange']:.2f}\n")
            f.write(f"     Min Daily Change: {year_stat['MinDailyChange']:.2f}\n")
            f.write(f"     Max Daily Change %: {year_stat['MaxDailyChangePercent']:.2f}%\n")
            f.write(f"     Min Daily Change %: {year_stat['MinDailyChangePercent']:.2f}%\n\n")

        # Evaluation Statistics
        f.write("3. Evaluation Statistics:\n")
        f.write(f"   Average NAV per Share: {analysis['EvaluationStats']['AverageEvaluation']:.2f}\n")
        f.write(f"   Standard Deviation: {analysis['EvaluationStats']['StdDevEvaluation']:.2f}\n\n")

        # Period Statistics
        f.write("4. Period (2020-2024) Statistics:\n")
        f.write(f"   Average NAV per Share: {analysis['PeriodStats']['PeriodAverageEvaluation']:.2f}\n")
        f.write(f"   Standard Deviation: {analysis['PeriodStats']['PeriodStdDevEvaluation']:.2f}\n\n")

        # Monthly Averages
        f.write("5. Monthly NAV per Share Averages (Most Recent First):\n")
        for monthly in analysis['MonthlyAverages']:
            f.write(f"   {monthly['Month']}: {monthly['MonthlyAverage']:.2f}\n")

    print(f"Analysis written to {filename}")


def main():
    # Create Spark session
    spark = create_spark_session()

    # Investors and their portfolios
    investors_portfolios = [
        ('Inv1', 'P11'),
        ('Inv1', 'P12'),
        ('Inv2', 'P21'),
        ('Inv2', 'P22'),
        ('Inv3', 'P31'),
        ('Inv3', 'P32')
    ]

    # Analyze each portfolio
    for investor_id, portfolio_id in investors_portfolios:
        try:
            # Load portfolio data
            portfolio_df = load_portfolio_data(spark, investor_id, portfolio_id)

            # Perform analysis
            analysis = analyze_portfolio(spark, portfolio_df, investor_id, portfolio_id)

            # Write analysis to file
            write_analysis_to_file(analysis, investor_id, portfolio_id)

        except Exception as e:
            print(f"Error processing {investor_id} {portfolio_id}: {e}")

    # Close Spark session
    spark.stop()


if __name__ == "__main__":
    main()