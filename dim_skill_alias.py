import os
import re
from datetime import datetime

import unidecode
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


SILVER_TABLE = os.getenv("SILVER_TABLE", "nessie.silver.jobs")
DIM_SKILL_TABLE = os.getenv("DIM_SKILL_TABLE", "nessie.gold.dim_skill")
DIM_SKILL_ALIAS_TABLE = os.getenv("DIM_SKILL_ALIAS_TABLE", "nessie.gold.dim_skill_alias")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "build_dim_skill_alias")
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


def main():
    spark = build_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    # 1) all normalized skill variants observed in silver
    df_jobs = spark.read.table(SILVER_TABLE).select("skills_all")

    df_var = (
        df_jobs
        .withColumn("skill_raw", F.explode_outer(F.col("skills_all")))
        .withColumn("skill_raw", F.trim(F.col("skill_raw").cast("string")))
        .filter(F.col("skill_raw").isNotNull() & (F.col("skill_raw") != ""))
        .withColumn("skill_norm", normalize_skill_udf(F.col("skill_raw")))
        .filter(F.col("skill_norm").isNotNull() & (F.col("skill_norm") != ""))
        .select("skill_norm")
        .dropDuplicates(["skill_norm"])
    )

    df_dim = spark.read.table(DIM_SKILL_TABLE).select("skill_key", "skill_canon_norm")

    df_alias = (
        df_var
        .join(df_dim, df_var["skill_norm"] == df_dim["skill_canon_norm"], "left")
        .withColumn(
            "skill_key",
            F.when(F.col("skill_key").isNotNull(), F.col("skill_key"))
             .otherwise(F.sha2(F.col("skill_norm"), 256))
        )
        .withColumn("created_at", F.lit(datetime.utcnow().isoformat()))
        .select("skill_key", "skill_norm", "created_at")
    )

    (
        df_alias.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(DIM_SKILL_ALIAS_TABLE)
    )

    spark.stop()


if __name__ == "__main__":
    main()
