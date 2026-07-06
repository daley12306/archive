import os
import re
from datetime import datetime

import unidecode
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window  

from sentence_transformers import SentenceTransformer

SILVER_TABLE = os.getenv("SILVER_TABLE", "nessie.silver.jobs")
DIM_SKILL_TABLE = os.getenv("DIM_SKILL_TABLE", "nessie.gold.dim_skill")

EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "/opt/models/bge-m3")

TOPN_SKILL_FOR_CANON = int(os.getenv("TOPN", "8000"))
SIM_TH = float(os.getenv("SIM_TH", "0.88"))
MIN_SKILL_LEN = int(os.getenv("MIN_SKILL_LEN", "2"))

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "build_dim_skills")
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


def build_canon_map(model: SentenceTransformer, cand_skill_norms: list[str]) -> dict[str, str]:
    if not cand_skill_norms:
        return {}

    embs = model.encode(
        cand_skill_norms,
        convert_to_numpy=True,
        normalize_embeddings=True,
        batch_size=256,
        show_progress_bar=True,
    )

    canon_of: dict[str, str] = {}
    n = len(cand_skill_norms)

    for i in range(n):
        s = cand_skill_norms[i]
        if s in canon_of:
            continue

        canon_of[s] = s
        sims = embs[i] @ embs.T
        close_idx = np.where(sims >= SIM_TH)[0]
        for j in close_idx:
            sj = cand_skill_norms[int(j)]
            if sj not in canon_of:
                canon_of[sj] = s

    return canon_of


def main():
    spark = build_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    df_jobs = spark.read.table(SILVER_TABLE).select("link", "skills_all")

    df_jobs = df_jobs.withColumn(
        "job_id",
        F.sha2(F.coalesce(F.col("link").cast("string"), F.lit("")), 256)
    )

    df_skill = (
        df_jobs
        .withColumn("skill_raw", F.explode_outer(F.col("skills_all")))
        .withColumn("skill_raw", F.trim(F.col("skill_raw").cast("string")))
        .filter(F.col("skill_raw").isNotNull() & (F.col("skill_raw") != ""))
        .withColumn("skill_norm", normalize_skill_udf(F.col("skill_raw")))
        .filter(F.col("skill_norm").isNotNull() & (F.col("skill_norm") != ""))
        .select("job_id", "skill_raw", "skill_norm")
    )

    cand_pdf = (
        df_skill
        .groupBy("skill_norm")
        .agg(F.countDistinct("job_id").alias("n_jobs"))
        .orderBy(F.col("n_jobs").desc())
        .limit(TOPN_SKILL_FOR_CANON)
        .toPandas()
    )
    cand_skill_norms = cand_pdf["skill_norm"].astype(str).tolist()

    try:
        model_eb = SentenceTransformer(EMBED_MODEL_NAME, tokenizer_kwargs={"fix_mistral_regex": True})
    except TypeError:
        model_eb = SentenceTransformer(EMBED_MODEL_NAME)

    canon_map = build_canon_map(model_eb, cand_skill_norms)
    bc_map = spark.sparkContext.broadcast(canon_map)

    @F.udf(returnType=StringType())
    def map_to_canon(skill_norm: str | None) -> str | None:
        if skill_norm is None:
            return None
        m = bc_map.value
        return m.get(skill_norm, skill_norm)

    df_skill = df_skill.withColumn("skill_canon_norm", map_to_canon(F.col("skill_norm")))

    w = Window.partitionBy("skill_canon_norm").orderBy(F.col("cnt").desc(), F.col("skill_raw").asc())

    df_rep = (
        df_skill
        .groupBy("skill_canon_norm", "skill_raw")
        .agg(F.count("*").alias("cnt"))
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            "skill_canon_norm",
            F.col("skill_raw").alias("skill_canon_name"),
        )
    )

    df_dim_skill = (
        df_rep
        .withColumn("skill_key", F.sha2(F.col("skill_canon_norm"), 256))
        .withColumn("created_at", F.lit(datetime.utcnow().isoformat()))
        .select("skill_key", "skill_canon_norm", "skill_canon_name", "created_at")
    )

    (
        df_dim_skill
        .write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(DIM_SKILL_TABLE)
    )

    spark.stop()


if __name__ == "__main__":
    main()
