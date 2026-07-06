from datetime import datetime
from tqdm import tqdm

from pyspark.sql import SparkSession
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from qdrant_client.http import models
from qdrant_client.http.models import VectorParams, Distance
from sentence_transformers import SentenceTransformer

spark = (
    SparkSession.builder
    .appName("QdrantIndexer")
    .config("spark.sql.catalog.nessie.ref", "demo")
    .getOrCreate()
)

query = """
SELECT
    job_key,
    title_clean,
    company_clean,
    location_clean,
    category_name_final,
    work_form_standard,
    min_salary,
    max_salary,
    currency,
    min_years,
    max_years,
    skills_all,
    expired_date_norm,
    link
FROM nessie.silver.jobs
"""

print("Loading data from Spark...")

df = spark.sql(query)

total_rows = df.count()     # biết tổng record
print("Total rows:", total_rows)

rows = df.collect()

print("Rows loaded")

qdrant = QdrantClient(
    host="qdrant",
    port=6333
)


# qdrant.create_collection(
#     collection_name="all_jobs",
#     vectors_config=VectorParams(
#         size=768,
#         distance=Distance.COSINE
#     )
# )

model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")

points = []

print("Generating embeddings...")

for idx, row in enumerate(
        tqdm(rows, desc="Embedding", unit="job")
):

    skills = row["skills_all"] or []

    embedding_text = " | ".join([
        row["title_clean"] or "",
        " ".join(skills)
    ])

    vector = model.encode(
        embedding_text,
        normalize_embeddings=True
    ).tolist()

    payload = {
        "job_key": row["job_key"],
        "title_clean": row["title_clean"],
        "company_clean": row["company_clean"],
        "location_clean": row["location_clean"],
        "category_name_final": row["category_name_final"],
        "work_form_standard": row["work_form_standard"],
        "min_salary": row["min_salary"],
        "max_salary": row["max_salary"],
        "currency": row["currency"],
        "min_years": row["min_years"],
        "max_years": row["max_years"],
        "skills_all": skills,
        "expired_date_norm": (
            row["expired_date_norm"].isoformat()
            if row["expired_date_norm"]
            else None
        ),
        "link": row["link"],
    }

    points.append(
        PointStruct(
            id=idx,
            vector=vector,
            payload=payload
        )
    )

print("Uploading to Qdrant...")

BATCH = 500

for i in tqdm(
        range(0, len(points), BATCH),
        desc="Upsert",
        unit="batch"
):

    qdrant.upsert(
        collection_name="all_jobs",
        points=points[i:i+BATCH]
    )

print("Done")