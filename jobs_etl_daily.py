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

with DAG(
    dag_id="jobs_etl_daily",
    description="Transform jobs data from bronze to silver and then to gold",
    # giống style các DAG khác của bạn
    start_date=pendulum.now("Asia/Saigon").subtract(days=1).start_of("day"),
    schedule_interval="0 20,21 * * *",  
    catchup=False,
    max_active_runs=1,
) as dag:
    start = DummyOperator(task_id="start")

    jobs_silver = SparkSubmitOperator(
        task_id="jobs_silver",
        application="/opt/airflow/scripts/silver/jobs_silver.py",
        name="jobs_silver",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
    )

    dim_company = SparkSubmitOperator(
        task_id="dim_company",
        application="/opt/airflow/scripts/gold/dim_company.py",
        name="dim_company",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES,
    )

    fact_job = SparkSubmitOperator(
        task_id="fact_job",
        application="/opt/airflow/scripts/gold/fact_job.py",
        name="fact_job",
        conn_id="spark_default",
        conf=SPARK_CONF,    
        packages=ICEBERG_PACKAGES,
    )

    fact_job_skill = SparkSubmitOperator(
        task_id="fact_job_skill",   
        application="/opt/airflow/scripts/gold/fact_job_skill.py",  
        name="fact_job_skill",
        conn_id="spark_default",
        conf=SPARK_CONF,    
        packages=ICEBERG_PACKAGES,
    )

    end = DummyOperator(task_id="end")

    start >> jobs_silver >> dim_company >> fact_job >> fact_job_skill >> end
