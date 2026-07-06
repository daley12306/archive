from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, quarter, year, month, day
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Create Date Dimension") \
        .config("spark.sql.catalog.nessie.ref", "backup") \
        .getOrCreate()
    
    current_year = datetime.now().year

    start_date = f"1990-01-01"
    end_date = f"{current_year + 10}-12-31"

    # Generate complete date range
    date_range = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date
    """)

    dim_time = date_range.withColumn("time_key", date_format("full_date", "yyyyMMdd").cast("int")) \
                        .withColumn("day", day("full_date")) \
                        .withColumn("day_name", date_format("full_date", "EEEE")) \
                        .withColumn("month", month("full_date")) \
                        .withColumn("month_name", date_format("full_date", "MMMM")) \
                        .withColumn("quarter", quarter("full_date")) \
                        .withColumn("year", year("full_date"))

    # Write to Iceberg table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    dim_time.write.format("iceberg").mode("overwrite").saveAsTable(f"nessie.gold.dim_time")