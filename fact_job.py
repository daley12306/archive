import os
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

SILVER_TABLE = os.getenv("SILVER_TABLE", "nessie.silver.jobs")

DIM_JOB_TABLE = os.getenv("DIM_JOB_TABLE", "nessie.gold.dim_job")
DIM_PLATFORM_TABLE = os.getenv("DIM_PLATFORM_TABLE", "nessie.gold.dim_platform")
DIM_COMPANY_TABLE = os.getenv("DIM_COMPANY_TABLE", "nessie.gold.dim_company")
DIM_LOCATION_TABLE = os.getenv("DIM_LOCATION_TABLE", "nessie.gold.dim_location")
DIM_CATEGORY_TABLE = os.getenv("DIM_CATEGORY_TABLE", "nessie.gold.dim_job_category")
DIM_CURRENCY_TABLE = os.getenv("DIM_CURRENCY_TABLE", "nessie.gold.dim_currency")
DIM_SALARY_TYPE_TABLE = os.getenv("DIM_SALARY_TYPE_TABLE", "nessie.gold.dim_salary_type")
DIM_EXP_TYPE_TABLE = os.getenv("DIM_EXP_TYPE_TABLE", "nessie.gold.dim_experience_type")
DIM_EDU_TABLE = os.getenv("DIM_EDU_TABLE", "nessie.gold.dim_education")
DIM_WORK_FORM_TABLE = os.getenv("DIM_WORK_FORM_TABLE", "nessie.gold.dim_work_form")
DIM_TIME_TABLE = os.getenv("DIM_TIME_TABLE", "nessie.gold.dim_time")


FACT_JOB_TABLE = os.getenv("FACT_JOB_TABLE", "nessie.gold.fact_job")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "build_fact_job")
NESSIE_REF = os.getenv("NESSIE_REF", "backup")
NESSIE_WAREHOUSE = os.getenv("NESSIE_WAREHOUSE", "s3a://warehouse")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .config("spark.sql.catalog.nessie.ref", NESSIE_REF)
        .config("spark.sql.catalog.nessie.warehouse", NESSIE_WAREHOUSE)
        .config("spark.sql.parquet.enableVectorizedReader", "false") 
        .config("spark.sql.iceberg.vectorization.enabled", "false") 
        .getOrCreate()
    )

def _safe_trim(col):
    return F.trim(F.col(col).cast("string"))

def main():
    spark = build_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    df = spark.read.table(SILVER_TABLE)

    needed = [
        "link",
        "platform",
        "company_clean",
        "location_clean",
        "category_name_final",
        "currency",
        "salary_type",
        "experience_type",
        "education_standard",
        "min_salary",
        "max_salary",
        "min_years",
        "max_years",
        "quantity_normalized",
        "work_form_standard",
        "expired_date_norm",
    ]
    df = df.select(*[c for c in needed if c in df.columns])

    df = (
        df
        .withColumn("link", _safe_trim("link"))
        .withColumn("platform", _safe_trim("platform"))
        .withColumn("company_clean", _safe_trim("company_clean"))
        .withColumn("location_clean", _safe_trim("location_clean"))
        .withColumn("category_name_final", _safe_trim("category_name_final"))
        .withColumn("currency", _safe_trim("currency"))
        .withColumn("salary_type", _safe_trim("salary_type"))
        .withColumn("experience_type", _safe_trim("experience_type"))
        .withColumn("education_standard", _safe_trim("education_standard"))
        .withColumn("work_form_standard", _safe_trim("work_form_standard"))
    )

    fallback_str = F.concat_ws(
        "||",
        F.coalesce(F.col("platform"), F.lit("")),
        F.coalesce(F.col("company_clean"), F.lit("")),
        F.coalesce(F.col("location_clean"), F.lit("")),
        F.coalesce(F.col("category_name_final"), F.lit("")),
        F.coalesce(F.col("expired_date_norm").cast("string"), F.lit("")),
    )

    df = df.withColumn(
        "job_key",
        F.sha2(
            F.when(F.col("link").isNotNull() & (F.col("link") != ""), F.col("link"))
             .otherwise(fallback_str),
            256,
        )
    )

    w = Window.partitionBy("job_key").orderBy(
        F.col("expired_date_norm").desc_nulls_last(),
        F.col("platform").asc_nulls_last(),
        F.col("link").asc_nulls_last(),
    )

    df = (
        df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    try:
        df_dim_job = spark.read.table(DIM_JOB_TABLE).select("job_key")
        df = df.join(df_dim_job, on="job_key", how="left")
    except Exception:
        pass

    df_dim_platform = spark.read.table(DIM_PLATFORM_TABLE).select(
        F.col("platform").alias("d_platform"),
        "platform_key",
    )

    df_dim_company = spark.read.table(DIM_COMPANY_TABLE).select(
        F.col("company_name").alias("d_company_name"),
        "company_key",
    )

    df_dim_location = spark.read.table(DIM_LOCATION_TABLE).select(
        F.col("location_name").alias("d_location_name"),
        "location_key",
    )

    df_dim_category = spark.read.table(DIM_CATEGORY_TABLE).select(
        F.col("category_name").alias("d_category_name"),
        "category_key",
    )

    df_dim_currency = spark.read.table(DIM_CURRENCY_TABLE).select(
        F.col("currency_code").alias("d_currency_code"),
        "currency_key",
    )

    df_dim_salary_type = spark.read.table(DIM_SALARY_TYPE_TABLE).select(
        F.col("salary_type_code").alias("d_salary_type_code"),
        "salary_type_key",
    )

    df_dim_exp_type = spark.read.table(DIM_EXP_TYPE_TABLE).select(
        F.col("experience_type_code").alias("d_exp_type_code"),
        "experience_type_key",
    )

    df_dim_edu = spark.read.table(DIM_EDU_TABLE).select(
        F.col("education_code").alias("d_edu_code"),
        "education_key",
    )

    df_dim_work_form = spark.read.table(DIM_WORK_FORM_TABLE).select(
        F.col("work_form_code").alias("d_work_form_code"),   # hoặc work_form_standard / work_form_name
        "work_form_key",
    )

    df_dim_time = spark.read.table(DIM_TIME_TABLE).select(
        F.col("full_date").alias("d_date"),
        "time_key"
    )

    df_fact = (
        df
        .join(df_dim_platform, df["platform"] == df_dim_platform["d_platform"], "left")
        .join(df_dim_company, df["company_clean"] == df_dim_company["d_company_name"], "left")
        .join(df_dim_location, df["location_clean"] == df_dim_location["d_location_name"], "left")
        .join(df_dim_category, df["category_name_final"] == df_dim_category["d_category_name"], "left")
        .join(df_dim_currency, df["currency"] == df_dim_currency["d_currency_code"], "left")
        .join(df_dim_salary_type, df["salary_type"] == df_dim_salary_type["d_salary_type_code"], "left")
        .join(df_dim_exp_type, df["experience_type"] == df_dim_exp_type["d_exp_type_code"], "left")
        .join(df_dim_edu, df["education_standard"] == df_dim_edu["d_edu_code"], "left")
        .join(df_dim_work_form, df["work_form_standard"] == df_dim_work_form["d_work_form_code"], "left")
        .join(df_dim_time, df["expired_date_norm"].cast("date") == df_dim_time["d_date"], "left")

    )

    snapshot_at = datetime.utcnow().isoformat()

    df_fact = (
        df_fact
        .withColumn("fact_job_key", F.col("job_key")) 
        .withColumn("snapshot_at", F.lit(snapshot_at))
        .select(
            "fact_job_key",
            "job_key",
            "platform_key",
            "company_key",
            "location_key",
            "category_key",
            "currency_key",
            "salary_type_key",
            "experience_type_key",
            "education_key",
            "work_form_key",
            F.col("min_salary").cast("bigint").alias("min_salary"),
            F.col("max_salary").cast("bigint").alias("max_salary"),
            F.col("min_years").cast("double").alias("min_years"),
            F.col("max_years").cast("double").alias("max_years"),
            F.col("quantity_normalized").cast("double").alias("quantity_normalized"),
            F.col("time_key").alias("expired_date_key"),
            F.col("snapshot_at").cast("string").alias("snapshot_at"),
        )
    )

    (
        df_fact
        .write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(FACT_JOB_TABLE)
    )

    spark.stop()


if __name__ == "__main__":
    main()
