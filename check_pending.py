from pyspark.sql import SparkSession

SPARK_CONF = {
    "spark.sql.catalog.nessie.ref": "demo",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
    "spark.sql.catalog.nessie.authentication.type": "NONE",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.nessie.warehouse": "s3://warehouse",
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.cache-enabled": "false",
    "spark.sql.catalog.nessie.s3.access-key-id": "admin",
    "spark.sql.catalog.nessie.s3.secret-access-key": "password",
    "spark.sql.catalog.nessie.s3.path-style-access": "true",
    "spark.driver.extraJavaOptions": "-Daws.region=us-east-1",
    "spark.executor.extraJavaOptions": "-Daws.region=us-east-1",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.executor.memory": "4g",
    "spark.executor.memoryOverhead": "2g",
    "spark.driver.memory": "4g",
    "spark.sql.shuffle.partitions": "12",
    "spark.default.parallelism": "12",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

def main():
    builder = SparkSession.builder.appName("check_pending")
    for k, v in SPARK_CONF.items():
        builder = builder.config(k, v)
    spark = builder.config("spark.jars.packages", ICEBERG_PACKAGES).getOrCreate()
    count = spark.table("nessie.meta.file_tracker").filter("status = 'pending'").count()
    spark.stop()
    with open("/tmp/pending_count.txt", "w") as f:
        f.write(str(count))
    print(f"Pending files: {count}")

if __name__ == "__main__":
    main()