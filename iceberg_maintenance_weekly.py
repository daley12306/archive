from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

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
    # Maintenance không cần nhiều partition
    "spark.executor.memory": "3g",
    "spark.executor.memoryOverhead": "1g",
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.adaptive.enabled": "true",
}

with DAG(
    dag_id="iceberg_maintenance_weekly",
    description="Weekly Iceberg table maintenance: expire snapshots + compact files",
    start_date=pendulum.now("Asia/Saigon").subtract(days=1).start_of("day"),
    schedule_interval="0 2 * * 0",  # Chủ nhật 2:00 AM (ngoài giờ daily pipeline)
    catchup=False,
    max_active_runs=1,
) as dag:

    start = DummyOperator(task_id="start")

    maintenance = SparkSubmitOperator(
        task_id="iceberg_maintenance",
        application="/opt/airflow/scripts/iceberg_maintenance.py",
        name="iceberg_maintenance",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
        execution_timeout=None,  # Maintenance có thể mất lâu
    )

    end = DummyOperator(task_id="end")

    start >> maintenance >> end