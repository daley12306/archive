import os
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


SILVER_TABLE = os.getenv("SILVER_TABLE", "nessie.silver.jobs")
DIM_JOB_TABLE = os.getenv("DIM_JOB_TABLE", "nessie.gold.dim_job")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "build_dim_jobs")
NESSIE_REF = os.getenv("NESSIE_REF", "backup")
NESSIE_WAREHOUSE = os.getenv("NESSIE_WAREHOUSE", "s3a://warehouse")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .config("spark.sql.catalog.nessie.ref", NESSIE_REF)
        .config("spark.sql.catalog.nessie.warehouse", NESSIE_WAREHOUSE)
        .getOrCreate()
    )


def main():
    spark = build_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    df = spark.read.table(SILVER_TABLE)

    keep_cols = [
        "link",
        "platform",
        "title_clean",
        "location_clean",
        "company_clean",
        "min_salary",
        "max_salary",
        "currency",
        "salary_type",
        "min_years",
        "max_years",
        "experience_type",
        "education_standard",
        "work_form_standard",
        "quantity_normalized",
        "expired_date_norm",
        "industry_sector",
        "category_name_final",
    ]
    existing = [c for c in keep_cols if c in df.columns]
    df = df.select(*existing)

    fallback_str = F.concat_ws(
        "||",
        F.coalesce(F.col("platform").cast("string"), F.lit("")),
        F.coalesce(F.col("title_clean").cast("string"), F.lit("")),
        F.coalesce(F.col("company_clean").cast("string"), F.lit("")),
        F.coalesce(F.col("location_clean").cast("string"), F.lit("")),
        F.coalesce(F.col("category_name_final").cast("string"), F.lit("")),
        F.coalesce(F.col("industry_sector").cast("string"), F.lit("")),
        F.coalesce(F.col("expired_date_norm").cast("string"), F.lit("")),
    )

    df = df.withColumn(
        "job_key",
        F.sha2(
            F.when(F.col("link").isNotNull() & (F.trim(F.col("link")) != ""), F.col("link").cast("string"))
             .otherwise(fallback_str),
            256,
        )
    )

    order_cols = []
    if "expired_date_norm" in df.columns:
        order_cols.append(F.col("expired_date_norm").desc_nulls_last())
    if "platform" in df.columns:
        order_cols.append(F.col("platform").asc_nulls_last())
    if "link" in df.columns:
        order_cols.append(F.col("link").asc_nulls_last())

    w = Window.partitionBy("job_key").orderBy(*order_cols) if order_cols else Window.partitionBy("job_key")

    df_dim = (
        df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("created_at", F.lit(datetime.utcnow().isoformat()))
    )

    out_cols = ["job_key", "created_at"] + [c for c in keep_cols if c in df_dim.columns]
    df_dim = df_dim.select(*out_cols)

    (
        df_dim
        .write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(DIM_JOB_TABLE)
    )

    spark.stop()

if __name__ == "__main__":
    main()