"""
indexer.py — Đọc gold layer từ Dremio → embed → index vào Qdrant

Chạy thủ công:   python indexer.py
Trong Airflow:   gọi hàm index_pipeline()
"""
import uuid
from datetime import date

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, PayloadSchemaType,
)

from config import (
    GOLD_SPACE, COLLECTION_NAME, EMBED_MODEL, VECTOR_SIZE,
    PASSAGE_PREFIX, BATCH_SIZE, QDRANT_HOST, QDRANT_PORT,
)
from dremio_client import DremioClient


# ── Query SQL ─────────────────────────────────────────────────────────────────

JOB_SQL = f"""
    SELECT
        j.job_key,
        j.title_clean,
        j.category_name_final           AS category_name,
        j.link,
        wf.work_form_code,
        l.location_name,
        CAST(fj.min_years  AS DOUBLE)   AS min_years,
        CAST(fj.max_years  AS DOUBLE)   AS max_years,
        et.experience_type_code,
        CAST(d.full_date   AS VARCHAR)  AS expired_date,
        c.company_name,
        p.platform,
        CAST(fj.min_salary AS BIGINT)   AS min_salary,
        CAST(fj.max_salary AS BIGINT)   AS max_salary,
        cur.currency_code,
        st.salary_type_code,
        fj.category_key
    FROM {GOLD_SPACE}.fact_job fj
    JOIN {GOLD_SPACE}.dim_job             j   ON fj.job_key             = j.job_key
    JOIN {GOLD_SPACE}.dim_work_form       wf  ON fj.work_form_key       = wf.work_form_key
    JOIN {GOLD_SPACE}.dim_location        l   ON fj.location_key        = l.location_key
    JOIN {GOLD_SPACE}.dim_experience_type et  ON fj.experience_type_key = et.experience_type_key
    JOIN {GOLD_SPACE}.dim_time            d   ON fj.expired_date_key    = d.time_key
    JOIN {GOLD_SPACE}.dim_company         c   ON fj.company_key         = c.company_key
    JOIN {GOLD_SPACE}.dim_platform        p   ON fj.platform_key        = p.platform_key
    JOIN {GOLD_SPACE}.dim_currency        cur ON fj.currency_key        = cur.currency_key
    JOIN {GOLD_SPACE}.dim_salary_type     st  ON fj.salary_type_key     = st.salary_type_key
    ORDER BY d.full_date DESC
"""
# Lý do bỏ WHERE expired: load tất cả job vào Qdrant, filter expired chạy ở searcher.py
# Điều này giúp test được ngay cả khi data cũ, và đúng hơn về mặt kiến trúc
# (Qdrant là serving index, không phải ETL filter layer)

SKILL_SQL_TEMPLATE = """
    SELECT
        fsc.category_key,
        ds.skill_canon_name AS skill_name
    FROM {space}.fact_skill_by_category fsc
    JOIN {space}.dim_skill_alias  sa  ON fsc.skill_key = sa.skill_key
    JOIN {space}.dim_skill        ds  ON sa.skill_key  = ds.skill_key
    WHERE fsc.category_key IN ({keys})
      AND fsc.coverage > 0.1
    ORDER BY fsc.coverage DESC
"""


# ── Load data ─────────────────────────────────────────────────────────────────

def load_jobs(dremio: DremioClient, limit: int | None = None) -> list[dict]:
    sql = JOB_SQL
    if limit:
        sql = sql + f" LIMIT {limit}"

    print(f"  Query jobs từ {GOLD_SPACE}...")
    jobs = dremio.query(sql)

    if not jobs:
        raise ValueError(
            "Không có job nào trả về.\n"
            f"Thử chạy trong Dremio SQL Runner: SELECT * FROM {GOLD_SPACE}.fact_job LIMIT 5"
        )
    print(f"  Lấy được {len(jobs)} jobs")

    # Gắn skill tags theo category
    category_keys = list({j["category_key"] for j in jobs if j.get("category_key")})
    if category_keys:
        keys_str  = ", ".join(f"'{k}'" for k in category_keys)
        skill_sql = SKILL_SQL_TEMPLATE.format(space=GOLD_SPACE, keys=keys_str)
        skill_rows = dremio.query(skill_sql)

        skill_map: dict[str, list[str]] = {}
        for row in skill_rows:
            cat = row["category_key"]
            name = row.get("skill_name") or ""
            if name:
                skill_map.setdefault(cat, []).append(name)

        for job in jobs:
            job["skill_tags"] = skill_map.get(job.get("category_key") or "", [])
    else:
        for job in jobs:
            job["skill_tags"] = []

    return jobs


# ── Qdrant setup ──────────────────────────────────────────────────────────────

def setup_collection(client: QdrantClient, recreate: bool = False):
    exists = client.collection_exists(COLLECTION_NAME)
    if exists and not recreate:
        print(f"  Collection '{COLLECTION_NAME}' đã tồn tại, dùng upsert.")
        return
    if exists:
        client.delete_collection(COLLECTION_NAME)
        print(f"  Đã xóa collection cũ.")

    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
    )

    # Payload indexes để filter nhanh — quan trọng, không bỏ qua
    indexes = {
        "expired_date":         PayloadSchemaType.DATETIME,
        "work_form_code":       PayloadSchemaType.KEYWORD,
        "location_name":        PayloadSchemaType.KEYWORD,
        "experience_type_code": PayloadSchemaType.KEYWORD,
        "min_years":            PayloadSchemaType.FLOAT,
        "max_years":            PayloadSchemaType.FLOAT,
    }
    for field, schema in indexes.items():
        client.create_payload_index(COLLECTION_NAME, field, schema)

    print(f"  Tạo collection '{COLLECTION_NAME}' (vector_size={VECTOR_SIZE})")


# ── Embed + index ─────────────────────────────────────────────────────────────

def build_embed_text(job: dict) -> str:
    """
    Ghép title + category + skills thành passage để embed.
    Format phải nhất quán với CV (dùng query: prefix ở searcher.py).
    """
    skills = " ".join(job.get("skill_tags") or [])
    return f"{PASSAGE_PREFIX}{job['title_clean']} {job['category_name']} {skills}".strip()


def build_payload(job: dict) -> dict:
    expired = job.get("expired_date") or ""
    return {
        # filter fields
        "expired_date":         str(expired)[:10],
        "work_form_code":       job.get("work_form_code")       or "unknown",
        "location_name":        job.get("location_name")        or "unknown",
        "min_years":            float(job["min_years"])  if job.get("min_years")  is not None else 0.0,
        "max_years":            float(job["max_years"])  if job.get("max_years")  is not None else 99.0,
        "experience_type_code": job.get("experience_type_code") or "unknown",
        # display fields
        "job_key":    job.get("job_key") or "",
        "title":      job.get("title_clean") or "",
        "company":    job.get("company_name") or "",
        "category":   job.get("category_name") or "",
        "link":       job.get("link") or "",
        "platform":   job.get("platform") or "",
        "min_salary": int(job["min_salary"]) if job.get("min_salary") else None,
        "max_salary": int(job["max_salary"]) if job.get("max_salary") else None,
        "currency":   job.get("currency_code") or "VND",
        "salary_type":job.get("salary_type_code") or "",
        "skill_tags": [s for s in (job.get("skill_tags") or []) if s],
    }


def index_jobs(jobs: list[dict], client: QdrantClient, model: SentenceTransformer):
    total = len(jobs)
    for start in range(0, total, BATCH_SIZE):
        batch  = jobs[start : start + BATCH_SIZE]
        texts  = [build_embed_text(j) for j in batch]
        vecs   = model.encode(texts, normalize_embeddings=True, show_progress_bar=False).tolist()

        points = [
            PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_DNS, job["job_key"])),
                vector=vec,
                payload=build_payload(job),
            )
    cache crc ����PendingChanges     1 data ��� AchievementTimes 0 &� j1 ��!j2 �*j3 3}-j4 >�7j5 ��Cj6 �!j7 �`!j8 f�%j9 D"!j10 e
!j11 1I*j12 k�)j17 �"!j18 F#!j19 g"j20 �1j21 ��!j22 Aj23 Aj24 Aj25 $�Cj26 $�Cj27 ��*j28 U!j29 �'j30 �K"j31 n�7j 2 data �   3 data     4 data �  state     5 data �    6 data     7 data [    8 data     11 data �/  state     12 data �  state     13 data -   14 data |  state     16 data �  state     17 data �    18 data     19 data R    20 data _    21 data <    24 data A   25 data �   26 data     27 data     28 data &    29 data     30 data     31 data U    32 data �
state     33 data ��W 34 data �<state     35 data �i  state     36 data [
   37 data    38 data �   39 data �    40 data �&   41 data �0   42 data P   43 data 	    44 data �    45 data Q   46 data �    47 data Q   48 data �   49 data �   50 data �   51 data �   52 data T   53 data F   54 data     55 data     57 data �    58 data �    59 data �    60 data �    61 data 4    63 data (9   64 data Q   67 data 9Z   68 data �    69 data ��� AchievementTimes 0 }M"j1  +#j2 �L!j3 /#j4 �n+j5 ��3j6 ��7j7 q�;j8 ��@j9 �*!j15 �&j18 w !j19 c�"j20 X6j22 u