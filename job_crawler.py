from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

SPARK_CONF = {
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
    
    "spark.executor.memory": "2g",
    "spark.executor.memoryOverhead": "512m",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "1",
    "spark.sql.adaptive.enabled": "true",
}

def collect_file_paths(**context):
    ti = context["ti"]

    results = ti.xcom_pull(
        task_ids=["topcv_crawler", "careerviet_crawler", "vietnamworks_crawler"]
    )

    paths = []
    for r in results:
        if not r:
            continue
        if isinstance(r, list):
            paths.extend(r)
        else:
            paths.append(r)

    paths = [p.strip() for p in paths if p]

    return paths

with DAG(
    "job_crawler",
    start_date=pendulum.now('Asia/Saigon').subtract(days=1).start_of("day"),
    schedule_interval='0 8,17 * * *',
    catchup=False,
    max_active_runs=2,
) as dag:

    topcv = BashOperator(
        task_id="topcv_crawler",
        do_xcom_push=True,
        bash_command="cd /opt/airflow/scripts/bronze && python3 topcv_crawler.py"
    )

    careerviet = BashOperator(
        task_id="careerviet_crawler",
        do_xcom_push=True,
        bash_command="cd /opt/airflow/scripts/bronze && python3 careerviet_crawler.py",
    )

    vietnamworks = BashOperator(
        task_id="vietnamworks_crawler",
        do_xcom_push=True,
        bash_command="cd /opt/airflow/scripts/bronze && python3 vietnamworks_crawler.py",
    )

    collect_paths = PythonOperator(
        task_id="collect_paths",
        python_callable=collect_file_paths,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # insert_file_tracker = SparkSubmitOperator(
    #     task_id="insert_file_tracker",
    #     application="/opt/airflow/scripts/bronze/insert_file_tracker.py",
    #     conf=SPARK_CONF,
    #     packages=ICEBERG_PACKAGES,
    #     application_args=[
    #         "--paths",
    #         "{{ ti.xcom_pull(task_ids='collect_paths') | tojson }}"
    #     ],
    # )

    [topcv, careerviet, vietnamworks] >> collect_paths 
    # >> insert_file_tracker