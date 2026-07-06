from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
)

SPARK_CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
    "spark.sql.catalog.nessie.ref": "airflow_dev",
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
}

with DAG(
    "market_indicators",
    description="Crawl market indicators from various sources and transform to silver layer",
    start_date=pendulum.now('Asia/Saigon').subtract(days=1).start_of("day"),
    schedule_interval="0 0 10 * *",  # once a month
    catchup=False,
    max_active_runs=2,  # 2 run song song
) as dag:
    start = DummyOperator(task_id="start")
    
    vbma = BashOperator(
        task_id="vbma_crawler",
        bash_command=(
            "cd /opt/airflow/scripts/bronze && "
            "python3 vbma_crawler.py"  
        ),
    )

    ilo = BashOperator(
        task_id="ilo_crawler",
        bash_command=(
            "cd /opt/airflow/scripts/bronze && "
            "python3 ilo_crawler.py"  
        ),
    )

    vbma_silver = SparkSubmitOperator(
        task_id="vbma_silver",
        application="/opt/airflow/scripts/silver/vbma_silver.py",
        name="vbma_silver",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    ilo_silver = SparkSubmitOperator(
        task_id="ilo_silver",   
        application="/opt/airflow/scripts/silver/ilo_silver.py",
        name="ilo_silver",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    fact_macro = SparkSubmitOperator(
        task_id="fact_macro",
        application="/opt/airflow/scripts/gold/fact_macro.py",
        name="fact_macro",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    fact_labor_market = SparkSubmitOperator(
        task_id="fact_labor_market",
        application="/opt/airflow/scripts/gold/fact_labor_market.py",
        name="fact_labor_market",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    end = DummyOperator(task_id="end")

    start >> vbma >> vbma_silver >> fact_macro
    start >> ilo >> ilo_silver >> fact_labor_market
    [fact_macro, fact_labor_market] >> end