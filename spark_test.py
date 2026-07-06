from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.config("spark.sql.catalog.nessie.ref", "backup").getOrCreate()
    df = spark.read.format("parquet").load("s3a://warehouse/bronze/topcv/*.parquet")
    df = df.filter(F.col("skills").isNotNull())
    df.show(100, truncate=False)