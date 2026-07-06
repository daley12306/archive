from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pathlib import Path
from py4j.java_gateway import java_import

def _glob_paths(spark: SparkSession, pattern: str) -> list[str]:
    sc = spark.sparkContext
    jvm = sc._jvm
    java_import(jvm, "org.apache.hadoop.fs.*")
    path = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
    stats = fs.globStatus(path) or []
    return [s.getPath().toString() for s in stats]

def init_tracker(spark):
    rows = []
    BRONZE_BASE = "s3a://warehouse/bronze"
    PLATFORMS = ["careerviet", "topcv", "vietnamworks"]

    for platform in PLATFORMS:
        paths = _glob_paths(spark, f"{BRONZE_BASE}/{platform}/*.parquet")

        for p in paths:
            rows.append((p, platform, "pending"))

    df = spark.createDataFrame(rows, ["file_path", "platform", "status"]) \
        .withColumn("created_at", F.current_timestamp()) \
        .withColumn("updated_at", F.current_timestamp())

    df.write.mode("overwrite").saveAsTable("nessie.meta.file_tracker")

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Spark Test") \
            .config("spark.sql.catalog.nessie.ref", "backup") \
            .getOrCreate()
    
    spark.sql('CREATE NAMESPACE IF NOT EXISTS nessie.meta')
    spark.sql('''
        CREATE TABLE IF NOT EXISTS nessie.meta.file_tracker (
        file_path STRING,
        platform STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
        )
        USING iceberg;
    ''')

    init_tracker(spark)

    spark.stop()