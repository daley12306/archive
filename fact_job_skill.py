import os
import re
from datetime import datetime, timezone

import unidecode
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

SILVER_TABLE = os.getenv("SILVER_TABLE", "nessie.silver.jobs")

DIM_SKILL_ALIAS_TABLE = os.getenv("DIM_SKILL_ALIAS_TABLE", "nessie.gold.dim_skill_alias")
DIM_CATEGORY_TABLE = os.getenv("DIM_CATEGORY_TABLE", "nessie.gold.dim_job_category")

FACT_SKILL_BY_CATEGORY_TABLE = os.getenv(
    "FACT_SKILL_BY_CATEGORY_TABLE", "nessie.gold.fact_skill_by_category"
)

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "build_fact_skill_by_category")
NESSIE_REF = os.getenv("NESSIE_REF", "backup")
NESSIE_WAREHOUSE = os.getenv("NESSIE_WAREHOUSE", "s3a://warehouse")

MIN_SKILL_LEN = int(os.getenv("MIN_SKILL_LEN", "2"))


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .config("spark.sql.catalog.nessie.ref", NESSIE_REF)
        .config("spark.sql.catalog.nessie.warehouse", NESSIE_WAREHOUSE)
        .getOrCreate()
    )


def normalize_skill_text(s: str | None) -> str | None:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None

    s = unidecode.unidecode(s).lower()
    s = re.sub(r"[\s_/|;,\n\r\t]+", " ", s)
    s = re.sub(r"[^0-9a-zA-Z#+\. ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    if not s or len(s) < MIN_SKILL_LEN:
        return None
    return s


normalize_skill_udf = F.udf(normalize_skill_text, StringType())


def _safe_trim(col: str):
    return F.trim(F.col(col).cast("string"))


def main():
    spark = build_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    created_at = datetime.now(timezone.utc).isoformat()

    df = spark.read.table(SILVER_TABLE)
    needed = ["link", "expired_date_norm", "category_name_final", "skills_all"]
    df = df.select(*[c for c in needed if c in df.columns])

    df = (
        df.withColumn("link", _safe_trim("link"))
          .withColumn("category_name_final", _safe_trim("category_name_final"))
    )

    # 2) job_key for countDistinct
    fallback_str = F.concat_ws(
        "||",
        F.coalesce(F.col("category_name_final"), F.lit("")),
        F.coalesce(F.col("expired_date_norm").cast("string"), F.lit("")),
        F.coalesce(F.col("link"), F.lit("")),
    )

    df = df.withColumn(
        "job_key",
        F.sha2(
            F.when(F.col("link").isNotNull() & (F.col("link") != ""), F.col("link"))
             .otherwise(fallback_str),
            256,
        )
    )

    df_skill = (
        df
        .withColumn("skill_raw", F.explode_outer(F.col("skills_all")))
        .withColumn("skill_raw", F.trim(F.col("skill_raw").cast("string")))
        .filter(F.col("job_key").isNotNull())
        .filter(F.col("category_name_final").isNotNull() & (F.col("category_name_final") != ""))
        .filter(F.col("skill_raw").isNotNull() & (F.col("skill_raw") != ""))
        .withColumn("skill_norm", normalize_skill_udf(F.col("skill_raw")))
        .filter(F.col("skill_norm").isNotNull() & (F.col("skill_norm") != ""))
        .select("job_key", "category_name_final", "skill_norm")
        .dropDuplicates(["job_key", "category_name_final", "skill_norm"])
    )

    df_alias = (
        spark.read.table(DIM_SKILL_ALIAS_TABLE)
        .select("skill_norm", "skill_key")
        .dropDuplicates(["skill_norm"])
    )

    df_cat = (
        spark.read.table(DIM_CATEGORY_TABLE)
        .select(F.col("category_name").alias("d_category_name"), "category_key")
        .dropDuplicates(["d_category_name"])
    )

    df_mapped = (
        df_skill
        .join(df_alias, on="skill_norm", how="left")
        .join(df_cat, df_skill["category_name_final"] == df_cat["d_category_name"], "left")
        .filter(F.col("skill_key").isNotNull())
        .filter(F.col("category_key").isNotNull())
        .select("category_key", "skill_key", "job_key")
    )

    df_skill_cnt = (
        df_mapped
        .groupBy("category_key", "skill_key")
        .agg(F.countDistinct("job_key").cast("int").alias("job_count"))
    )

    df_total = (
        df_mapped
        .groupBy("category_key")
        .agg(F.countDistinct("job_key").cast("int").alias("total_job_in_category"))
    )

    df_fact = (
        df_skill_cnt
        .join(df_total, on="category_key", how="left")
        .withColumn(
            "coverage",
            F.when(F.col("total_job_in_category") > 0,
                   (F.col("job_count") / F.col("total_job_in_category")).cast("double"))
             .otherwise(F.lit(None).cast("double"))
        )
        .withColumn("created_at", F.lit(created_at).cast("timestamp"))
        .select("category_key", "skill_key", "job_count", "coverage", "created_at")
    )

    (
        df_fact
        .write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(FACT_SKILL_BY_CATEGORY_TABLE)
    )

    spark.stop()


if __name__ == "__main__":
    main()
