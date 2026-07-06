from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
    "dimension_init",
    description="Initialize dimension tables",
    schedule_interval=None, # triggered manually
    start_date=None,
    catchup=False,
    max_active_runs=2,  # 2 run song song
) as dag:
    start = DummyOperator(task_id="start")

    """
    DIMENSION FOR VBMA AND ILO
    """

    dim_time = SparkSubmitOperator(
        task_id="dim_time",
        application="/opt/airflow/scripts/gold/dim_time.py",
        name="dim_time",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    # dim_indicator = SparkSubmitOperator(
    #     task_id="dim_indicator",
    #     application="/opt/airflow/scripts/gold/dim_indicator.py",
    #     name="dim_indicator",
    #     conn_id="spark_default",
    #     conf=SPARK_CONF,
    #     packages=ICEBERG_PACKAGES
    # )

    # dim_demographic = SparkSubmitOperator(
    #     task_id="dim_demographic",
    #     application="/opt/airflow/scripts/gold/dim_demographic.py",
    #     name="dim_demographic",
    #     conn_id="spark_default",
    #     conf=SPARK_CONF,
    #     packages=ICEBERG_PACKAGES
    # )

    # dim_industry = SparkSubmitOperator(
    #     task_id="dim_industry",
    #     application="/opt/airflow/scripts/gold/dim_industry.py",
    #     name="dim_industry",
    #     conn_id="spark_default",
    #     conf=SPARK_CONF,
    #     packages=ICEBERG_PACKAGES
    # )

    """
    DIMENSION FOR JOBS
    """

    dim_company = SparkSubmitOperator(
        task_id="dim_company",
        application="/opt/airflow/scripts/gold/dim_company.py",
        name="dim_company",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )       

    dim_currency = SparkSubmitOperator(
        task_id="dim_currency",
        application="/opt/airflow/scripts/gold/dim_currency.py",
        name="dim_currency",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    dim_education = SparkSubmitOperator(
        task_id="dim_education",
        application="/opt/airflow/scripts/gold/dim_education.py",
        name="dim_education",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    dim_experience_type = SparkSubmitOperator(
        task_id="dim_experience_type",
        application="/opt/airflow/scripts/gold/dim_experience_type.py",
        name="dim_experience_type",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )   

    dim_job_category = SparkSubmitOperator(
        task_id="dim_job_category", 
        application="/opt/airflow/scripts/gold/dim_job_category.py",
        name="dim_job_category",
        conn_id="spark_default",    
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )      

    dim_platform = SparkSubmitOperator(
        task_id="dim_platform",
        application="/opt/airflow/scripts/gold/dim_platform.py",
        name="dim_platform",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    dim_salary_type = SparkSubmitOperator(
        task_id="dim_salary_type",
        application="/opt/airflow/scripts/gold/dim_salary_type.py",
        name="dim_salary_type",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    dim_work_form = SparkSubmitOperator(
        task_id="dim_work_form", 
        application="/opt/airflow/scripts/gold/dim_work_form.py",
        name="dim_work_form",
        conn_id="spark_default",    
        conf=SPARK_CONF,        
        packages=ICEBERG_PACKAGES       
    )

    dim_location = SparkSubmitOperator(
        task_id="dim_location",
        application="/opt/airflow/scripts/gold/dim_location.py",
        name="dim_location",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    dim_skills = SparkSubmitOperator(
        task_id="dim_skills",
        application="/opt/airflow/scripts/gold/dim_skills.py",
        name="dim_skills",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    dim_skill_alias = SparkSubmitOperator(
        task_id="dim_skill_alias",
        application="/opt/airflow/scripts/gold/dim_skill_alias.py",
        name="dim_skill_alias",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=ICEBERG_PACKAGES
    )

    end = DummyOperator(task_id="end")

    start >> [dim_company, dim_currency, dim_education, dim_experience_type, dim_job_category, dim_location, dim_platform, dim_salary_type, dim_work_form, dim_location, dim_time, dim_skills, dim_skill_alias] >> end