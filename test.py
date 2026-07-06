from pyspark.sql import SparkSession, Row
from datetime import date, datetime

spark = SparkSession.builder \
    .master('spark://spark-master:7077') \
    .appName("TestApp") \
    .getOrCreate()

# Create a DataFrame
df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
# Show DataFrame
df.show()

# Ensure the correct namespace exists
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.default")

# Create a Table based on DataFrame schema and save the data
df.write.format("iceberg").mode("overwrite").saveAsTable("nessie.ssdefault.sample_iceberg_table")

# Query the Data
result = spark.sql("SELECT * FROM nessie.default.sample_iceberg_table")
result.show()