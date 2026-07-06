import json
import os
from datetime import date
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast, coalesce, lit

SILVER_TABLE = "nessie.silver.jobs"
GOLD_NAMESPACE = "nessie.gold"
SKILL_MAPPING_PATH = "/opt/airflow/scripts/skill_alias.json"

def load_skill_mapping(spark: SparkSession) -> DataFrame:
    if not os.path.exists(SKILL_MAPPING_PATH):
        print(f"[WARN] skill_alias.json not found at {SKILL_MAPPING_PATH}. Using empty mapping.")
        return spark.createDataFrame([], schema="raw_skill STRING, canonical_skill STRING, skill_type STRING")
    with open(SKILL_MAPPING_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    rows = [(raw.strip().lower(), info["canonical"], info["type"]) for raw, info in data.items()]
    return spark.createDataFrame(rows, schema="raw_skill STRING, canonical_skill STRING, skill_type STRING")


def ensure_skill_dim_and_alias(df_raw_skills: DataFrame, spark: SparkSession, mapping_df: DataFrame):
    df_raw = df_raw_skills \
        .select(F.lower(F.trim(F.col("raw_skill"))).alias("raw_skill")) \
        .distinct()

    df_mapped = df_raw.join(mapping_df, on="raw_skill", how="left") \
        .filter(
            F.col("canonical_skill").isNull() |          # chưa có trong mapping → giữ lại (dùng raw)
            (F.col("canonical_skill") != "__SKIP__")     # có trong mapping nhưng bị đánh dấu skip → loại
        ) \
        .select(
            F.col("raw_skill"),
            F.coalesce(F.col("canonical_skill"), F.col("raw_skill")).alias("canonical_skill"),
            F.coalesce(F.col("skill_type"), F.lit("unknown")).alias("skill_type"),
        )
    df_mapped.cache()
    df_mapped_count = df_mapped.count()  # materialize
    print(f"  Skills sau khi lọc __SKIP__: {df_mapped_count}")

    try:
        df_canon = df_mapped.select("canonical_skill", "skill_type").distinct()

        existing_names: set = {
            row.skill_name
            for row in spark.table(f"{GOLD_NAMESPACE}.dim_skill").select("skill_name").collect()
        }
        print(f"  dim_skill hiện tại: {len(existing_names)} skills")

        df_canon_pd = df_canon.toPandas()
        df_new_canon_pd = df_canon_pd[~df_canon_pd["canonical_skill"].isin(existing_names)]
        new_canon_count = len(df_new_canon_pd)
        print(f"  Skills mới cần insert: {new_canon_count}")

        if new_canon_count > 0:
            max_key = spark.sql(
                f"SELECT COALESCE(MAX(skill_key), 0) FROM {GOLD_NAMESPACE}.dim_skill"
            ).collect()[0][0]

            df_new_canon_pd = df_new_canon_pd.sort_values("canonical_skill").reset_index(drop=True)
            df_new_canon_pd["skill_key"] = range(int(max_key) + 1, int(max_key) + 1 + new_canon_count)

            df_new_canon = spark.createDataFrame(df_new_canon_pd[["skill_key", "canonical_skill", "skill_type"]]) \
                .withColumnRenamed("canonical_skill", "skill_name")

            df_new_canon.write.mode("append").saveAsTable(f"{GOLD_NAMESPACE}.dim_skill")
            print(f"  ✅ Inserted {new_canon_count} rows vào dim_skill")

        dim_skill_fresh = spark.table(f"{GOLD_NAMESPACE}.dim_skill") \
            .select(
                F.lower(F.trim(F.col("skill_name"))).alias("canonical_skill"),
                F.col("skill_key"),
            )

        existing_alias_set: set = {
            row.raw_skill
            for row in spark.table(f"{GOLD_NAMESPACE}.dim_skill_alias").select("raw_skill").collect()
        }
        print(f"  dim_skill_alias hiện tại: {len(existing_alias_set)} aliases")

        df_raw_no_alias = df_mapped \
            .filter(~F.col("raw_skill").isin(list(existing_alias_set))) \
            if existing_alias_set else df_mapped

        df_new_alias = df_raw_no_alias \
            .withColumn("canonical_skill_norm", F.lower(F.trim(F.col("canonical_skill")))) \
            .join(
                dim_skill_fresh.withColumnRenamed("canonical_skill", "canonical_skill_norm"),
                on="canonical_skill_norm",
                how="inner",
            ) \
            .select("raw_skill", "skill_key") \
            .distinct()

        new_alias_count = df_new_alias.count()
        print(f"  Alias mới cần insert: {new_alias_count}")

        if new_alias_count > 0:
            df_new_alias.write.mode("append").saveAsTable(f"{GOLD_NAMESPACE}.dim_skill_alias")
            print(f"  ✅ Inserted {new_alias_count} rows vào dim_skill_alias")

    finally:
        df_mapped.unpersist()


def upsert_dim_job(df_updates: DataFrame, spark: SparkSession):
    df_updates.createOrReplaceTempView("new_jobs")
    spark.sql(f"""
        MERGE INTO {GOLD_NAMESPACE}.dim_job AS target
        USING new_jobs AS source
        ON target.job_key = source.job_key
        WHEN MATCHED THEN UPDATE SET
            title_clean = source.title_clean,
            description = source.description,
            requirement = source.requirement,
            link = source.link
        WHEN NOT MATCHED THEN INSERT *
    """)

def upsert_dim_company(df_updates: DataFrame, spark: SparkSession):
    df_comp = df_updates.select("company_clean").distinct() \
        .withColumn("company_key", F.sha2(F.col("company_clean"), 256)) \
        .withColumnRenamed("company_clean", "company_name")
    df_comp.createOrReplaceTempView("new_companies")
    spark.sql(f"""
        MERGE INTO {GOLD_NAMESPACE}.dim_company AS target
        USING new_companies AS source
        ON target.company_key = source.company_key
        WHEN NOT MATCHED THEN INSERT *
    """)

def upsert_fact_jobs(df_updates: DataFrame, spark: SparkSession):
    df_updates.createOrReplaceTempView("new_facts")
    spark.sql(f"""
        MERGE INTO {GOLD_NAMESPACE}.fact_jobs AS target
        USING new_facts AS source
        ON target.job_key = source.job_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def refresh_fact_job_skills(df_jobs: DataFrame, spark: SparkSession):
    job_keys = [row.job_key for row in df_jobs.select("job_key").distinct().collect()]
    if not job_keys:
        return

    keys_str = ", ".join(f"'{k}'" for k in job_keys)
    spark.sql(f"""
        DELETE FROM {GOLD_NAMESPACE}.fact_job_skills
        WHERE job_key IN ({keys_str})
    """)

    df_tech = df_jobs.select("job_key", F.explode("tech_skills").alias("raw_skill")).withColumn("original_type", F.lit("tech"))
    df_soft = df_jobs.select("job_key", F.explode("soft_skills").alias("raw_skill")).withColumn("original_type", F.lit("soft"))
    df_skills = df_tech.union(df_soft) \
        .filter(F.col("raw_skill").isNotNull() & (F.trim(F.col("raw_skill")) != ""))

    if df_skills.isEmpty():
        return

    df_skills = df_skills.withColumn("raw_skill_norm", F.lower(F.trim(F.col("raw_skill"))))
    alias_df = spark.table(f"{GOLD_NAMESPACE}.dim_skill_alias").withColumnRenamed("raw_skill", "alias_raw")
    df_mapped = df_skills.join(alias_df, df_skills.raw_skill_norm == alias_df.alias_raw, "inner") \
        .select("job_key", "skill_key")

    dim_skill = spark.table(f"{GOLD_NAMESPACE}.dim_skill").select("skill_key", "skill_type")
    df_mapped = df_mapped.join(dim_skill, on="skill_key", how="left") \
        .select("job_key", "skill_key", "skill_type")

    if not df_mapped.isEmpty():
        df_mapped.write.mode("append").saveAsTable(f"{GOLD_NAMESPACE}.fact_job_skills")

def main():
    spark = SparkSession.builder.appName("jobs_gold_daily").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    mapping_df = load_skill_mapping(spark)
    BATCH_LIMIT = 800

    df_silver = spark.table(SILVER_TABLE).filter(F.col("gold_processed") == False)
    has_pending = df_silver.limit(1).count() > 0
    if not has_pending:
        print("No pending silver records.")
        spark.stop()
        return

    df_silver = df_silver.limit(BATCH_LIMIT)
    print(f"Processing up to {BATCH_LIMIT} records...")

    df_silver = df_silver.repartition(4, "link")
    window    = Window.partitionBy("link").orderBy(F.col("processed_date").desc())
    df_latest = df_silver.withColumn("rn", F.row_number().over(window)) \
                         .filter(F.col("rn") == 1).drop("rn")

    df_latest.cache()
    df_latest.count()
    print("Materialized df_latest into cache.")

    try:
        # Skill dimensions
        df_tech_raw = df_latest \
            .select(F.explode("tech_skills").alias("raw_skill")) \
            .filter(F.col("raw_skill").isNotNull() & (F.trim(F.col("raw_skill")) != ""))
        df_soft_raw = df_latest \
            .select(F.explode("soft_skills").alias("raw_skill")) \
            .filter(F.col("raw_skill").isNotNull() & (F.trim(F.col("raw_skill")) != ""))
        df_all_raw = df_tech_raw.union(df_soft_raw).distinct()

        raw_count = df_all_raw.count()
        print(f"Raw skills from batch: {raw_count}")

        if raw_count > 0:
            ensure_skill_dim_and_alias(df_all_raw, spark, mapping_df)
        else:
            print("[WARN] Batch không có skill nào — NER có chạy đúng trong silver ETL không?")

        # Other dimensions
        df_dim_job = df_latest.select(
            F.col("link").alias("job_key"),
            "title_clean", "description", "requirement", "link",
        )
        upsert_dim_job(df_dim_job, spark)
        upsert_dim_company(df_latest.select("company_clean"), spark)

        # Fact tables
        loc_df = broadcast(spark.table(f"{GOLD_NAMESPACE}.dim_location").select("province_name", "location_key"))
        cat_df = broadcast(spark.table(f"{GOLD_NAMESPACE}.dim_category").select("category_name", "category_key"))
        level_df = broadcast(spark.table(f"{GOLD_NAMESPACE}.dim_level").select("level_code", "level_key"))
        edu_df = broadcast(spark.table(f"{GOLD_NAMESPACE}.dim_education").select("education_code", "education_key"))
        comp_df = broadcast(spark.table(f"{GOLD_NAMESPACE}.dim_company").select("company_name", "company_key"))

        df_fact = df_latest \
            .withColumn("job_key", F.col("link")) \
            .withColumn("_level_code",
                F.when(F.col("level_standard").isNull(), F.lit("staff")).otherwise(F.col("level_standard"))) \
            .withColumn("_edu_code",
                F.when(F.col("education_standard").isNull(), F.lit("0")).otherwise(F.col("education_standard")))

        df_fact = df_fact \
            .join(level_df, df_fact["_level_code"] == level_df["level_code"], "left") \
            .join(edu_df,   df_fact["_edu_code"]   == edu_df["education_code"],   "left") \
            .join(loc_df,   df_fact["location_clean"]      == loc_df["province_name"],  "left") \
            .join(cat_df,   df_fact["category_name_final"] == cat_df["category_name"],  "left") \
            .join(comp_df,  df_fact["company_clean"]       == comp_df["company_name"],  "left") \
            .withColumn("job_key", F.col("link"))

        df_fact = df_fact \
            .withColumn("processed_time_key",
                        F.date_format(F.col("processed_date"), "yyyyMMdd").cast(IntegerType())) \
            .withColumn("expired_time_key",
                        F.when(F.col("expired_date_norm").isNotNull(),
                               F.date_format(F.col("expired_date_norm"), "yyyyMMdd").cast(IntegerType()))
                         .otherwise(lit(None).cast(IntegerType()))) \
            .withColumn("min_salary",          coalesce(F.col("min_salary"),          lit(0))) \
            .withColumn("max_salary",          coalesce(F.col("max_salary"),          lit(0))) \
            .withColumn("min_years",           coalesce(F.col("min_years"),           lit(0.0))) \
            .withColumn("max_years",           coalesce(F.col("max_years"),           lit(0.0))) \
            .withColumn("quantity_normalized", coalesce(F.col("quantity_normalized"), lit(1.0))) \
            .withColumn("platform",            coalesce(F.col("platform"),            lit("unknown"))) \
            .withColumn("work_form_standard",  coalesce(F.col("work_form_standard"),  lit("full_time"))) \
            .withColumn("salary_type",         coalesce(F.col("salary_type"),         lit("unknown"))) \
            .withColumn("currency",            coalesce(F.col("currency"),            lit("VND"))) \
            .withColumn("experience_type",     coalesce(F.col("experience_type"),     lit("none")))

        fact_cols = [
            "job_key", "processed_time_key", "expired_time_key", "location_key",
            "company_key", "category_key", "level_key", "education_key", "platform",
            "work_form_standard", "salary_type", "currency", "experience_type",
            "min_salary", "max_salary", "min_years", "max_years", "quantity_normalized"
        ]
        df_fact = df_fact.select(*[c for c in fact_cols if c in df_fact.columns])
        upsert_fact_jobs(df_fact, spark)

        df_for_skills = df_latest.withColumn("job_key", F.col("link"))
        refresh_fact_job_skills(df_for_skills, spark)

        # Mark processed
        processed_links = [row.link for row in df_latest.select("link").distinct().collect()]
        if processed_links:
            spark.createDataFrame(
                [(lnk,) for lnk in processed_links], ["link"]
            ).createOrReplaceTempView("_gold_processed_links")

            spark.sql(f"""
                MERGE INTO {SILVER_TABLE} AS target
                USING _gold_processed_links AS source
                ON target.link = source.link
                WHEN MATCHED THEN UPDATE SET gold_processed = true
            """)

        print(f"Gold ETL completed. Processed {len(processed_links)} records.")
    finally:
        df_latest.unpersist()

    spark.stop()
    print("Gold ETL done.")

if __name__ == "__main__":
    main()