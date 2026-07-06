# export_for_kaggle.py
# Chạy trong môi trường có Spark + Iceberg/Nessie catalog (ví dụ Jupyter container 
# hoặc spark-submit trong docker-compose của bạn)

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

SPARK_CONF = {
    "spark.sql.catalog.nessie.ref": "demo",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
    "spark.sql.catalog.nessie.authentication.type": "NONE",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.nessie.warehouse": "s3://warehouse",
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.s3.access-key-id": "admin",
    "spark.sql.catalog.nessie.s3.secret-access-key": "password",
    "spark.sql.catalog.nessie.s3.path-style-access": "true",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

builder = SparkSession.builder.appName("export_kaggle")
for k, v in SPARK_CONF.items():
    builder = builder.config(k, v)
spark = builder.config("spark.jars.packages", ICEBERG_PACKAGES).getOrCreate()

# ── Join fact_jobs với dim_job để lấy requirement + title + skill keys ──
df = spark.sql("""
    SELECT
        f.job_key,
        j.title_clean,
        j.requirement,
        f.category_key,
        f.level_key,
        c.category_name,
        l.level_name_vn
    FROM nessie.gold.fact_jobs f
    JOIN nessie.gold.dim_job j ON f.job_key = j.job_key
    LEFT JOIN nessie.gold.dim_category c ON f.category_key = c.category_key
    LEFT JOIN nessie.gold.dim_level l ON f.level_key = l.level_key
    WHERE j.requirement IS NOT NULL AND length(j.requirement) > 50
""")

# ── Lấy skill keys cho mỗi job (gộp thành list) ──
df_skills = spark.sql("""
    SELECT
        fs.job_key,
        collect_list(s.skill_name) as canonical_skills,
        collect_list(fs.skill_type) as skill_types
    FROM nessie.gold.fact_job_skills fs
    JOIN nessie.gold.dim_skill s ON fs.skill_key = s.skill_key
    GROUP BY fs.job_key
""")

df_final = df.join(df_skills, on="job_key", how="left")

# ── Lọc job có ít nhất 3 skill (theo điều kiện đã thảo luận) ──
df_final = df_final.filter(
    F.size(F.coalesce(F.col("canonical_skills"), F.array())) >= 3
)

# ── Sample khoảng 1500-2000 job để cân bằng giữa đủ data và file nhẹ ──
df_sample = df_final.orderBy(F.rand(seed=42)).limit(2000)

# ── Convert sang Pandas rồi export CSV (vì list column cần xử lý riêng) ──
pdf = df_sample.toPandas()
pdf['canonical_skills'] = pdf['canonical_skills'].apply(
    lambda x: '|'.join(x) if x else ''
)
pdf['skill_types'] = pdf['skill_types'].apply(
    lambda x: '|'.join(x) if x else ''
)

OUTPUT_PATH = '/tmp/careerlake_export_kaggle.csv'
pdf.to_csv(OUTPUT_PATH, index=False)
print(f"Exported {len(pdf)} rows to {OUTPUT_PATH}")