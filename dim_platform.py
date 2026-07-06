import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

NESSIE_BRANCH = "backup"
SILVER_JOBS_TABLE = "nessie.silver.jobs"
DIM_PLATFORM_TABLE = "nessie.gold.dim_platform"


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Create Dim Platform")
        .config("spark.sql.catalog.nessie.ref", NESSIE_BRANCH)
        .getOrCreate()
    )
    return spark


def main():
    spark = build_spark_session()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    df_jobs = spark.read.table(SILVER_JOBS_TABLE)

    # df_dim = (
    #     df_jobs
    #     .select("platform")
    #     .where(F.col("platform").isNotNull() & (F.col("platform") != ""))
    #     .distinct()
    # )

    # df_dim = (
    #     df_dim
    #     .withColumn("platform_key", F.expr("uuid()"))
    #     .select("platform_key", "platform")
    #     .orderBy("platform") 
    # )

    # (
    #     df_dim.write
    #     .format("iceberg")
    #     .mode("overwrite")
    #     .saveAsTable(DIM_PLATFORM_TABLE)
    # )

    # Code mới
    df_dim = (
        df_jobs
        .select("platform")
        .where(F.col("platform").isNotNull() & (F.col("platform") != ""))
        .distinct()
        .withColumn("platform_key", F.sha2(F.col("platform"), 256))  # ← Deterministic hash
        .select("platform_key", "platform")
        .sort("platform")
    )

    (
        df_dim.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(DIM_PLATFORM_TABLE)
    )


    spark.stop()


if __name__ == "__main__":
    main()
