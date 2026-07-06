from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

# -----------------------------------------------------------------------------
# Configuration for Spark
# -----------------------------------------------------------------------------
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
    "spark.executor.memory": "3g",
    "spark.executor.memoryOverhead": "1g",
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "4",
    "spark.default.parallelism": "8",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.parquet.enableVectorizedReader": "false",
    "spark.sql.parquet.filterPushdown": "false",
    "spark.hadoop.parquet.filter.columnindex.enabled": "false",
    "spark.sql.files.ignoreCorruptFiles": "true",
    "spark.sql.files.ignoreMissingFiles": "true",
}

DAG_ID = "jobs_pipeline_daily"

# -----------------------------------------------------------------------------
# Python callable to read pending count from file
# -----------------------------------------------------------------------------
def read_pending_result(**context):
    try:
        with open("/tmp/pending_count.txt") as f:
            count = int(f.read().strip())
    except Exception:
        count = 0
    print(f"Pending files count: {count}")
    return "trigger_silver_loop" if count > 0 else "end"

# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    description="Bronze → Silver → Gold pipeline, loop until no pending files",
    start_date=pendulum.now("Asia/Saigon").subtract(days=1).start_of("day"),
    schedule_interval="0 20 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    start = DummyOperator(task_id="start")

    # Silver ETL: reads pending bronze files, processes, writes to silver.jobs
    silver_etl = SparkSubmitOperator(
        task_id="silver_etl",
        application="/opt/airflow/scripts/silver/jobs_demo_v1.py",
        name="jobs_silver_v4",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
    )

    # Gold ETL: reads silver records with gold_processed=false, upserts dimensions and facts
    gold_etl = SparkSubmitOperator(
        task_id="gold_etl",
        application="/opt/airflow/scripts/gold/gold_demo.py",
        name="jobs_gold_daily",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
    )

    # Check pending files after processing batch (updates /tmp/pending_count.txt)
    check_pending_spark = SparkSubmitOperator(
        task_id="check_pending_spark",
        application="/opt/airflow/scripts/check_pending.py",
        name="check_pending",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
    )

    # Branch based on pending count
    check_pending = BranchPythonOperator(
        task_id="check_pending_files",
        python_callable=read_pending_result,
    )

    # Trigger the same DAG again if there are pending files
    trigger_silver_loop = TriggerDagRunOperator(
        task_id="trigger_silver_loop",
        trigger_dag_id=DAG_ID,
        wait_for_completion=False,
    )

    end = DummyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # DAG flow
    start >> silver_etl >> gold_etl >> check_pending_spark >> check_pending
    check_pending >> [trigger_silver_loop, end]
    trigger_silver_loop >> end