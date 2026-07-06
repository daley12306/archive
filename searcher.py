"""
searcher.py — Nhận CV profile → query Qdrant → trả về job recommend

Dùng trực tiếp hoặc import vào FastAPI.
"""
import re
from datetime import date
from typing import Literal

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Filter, FieldCondition, MatchValue,
    Range, DatetimeRange,
)

from config import COLLECTION_NAME, QUERY_PREFIX


# ── CV Profile ────────────────────────────────────────────────────────────────

class CVProfile:
    """
    Output của CV parser sau khi đọc PDF.
    experience_source cho serving layer biết cách fallback.
    """
    def __init__(
        self,
        raw_text: str,
        candidate_location: str | None = None,
        years_of_experience: float | None = None,
        experience_source: Literal["explicit", "inferred", "default"] = "default",
        extracted_skills: set | None = None,
    ):
        self.raw_text            = raw_text
        self.candidate_location  = candidate_location
        self.years_of_experience = years_of_experience
        self.experience_source   = experience_source
        self.extracted_skills    = extracted_skills or set()


# ── CV Parser (placeholder — thay bằng NER sau) ───────────────────────────────

def parse_cv_text(raw_text: str) -> CVProfile:
    """
    Parse CV text thành CVProfile.
    Hiện tại dùng regex đơn giản.
    Sau khi NER mới của Người A xong → thay thế phần extract skills.
    """
    text = raw_text.lower()

    # Tầng 1 — Explicit: tìm "X năm kinh nghiệm" hoặc "X years experience"
    patterns = [
        r'(\d+(?:\.\d+)?)\s*năm\s*kinh\s*nghiệm',
        r'(\d+(?:\.\d+)?)\s*years?\s*(?:of\s*)?experience',
    ]
    years = None
    source = "default"
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            years  = float(m.group(1))
            source = "explicit"
            break

    # Tầng 2 — Inferred: đếm số lần xuất hiện năm bắt đầu làm việc
    if source == "default":
        year_mentions = re.findall(r'20\d{2}\s*[-–]\s*(?:20\d{2}|nay|present)', text)
        if len(year_mentions) >= 1:
            # Ước lượng từ năm đầu tiên đến nay
            first_years = re.findall(r'(20\d{2})\s*[-–]', " ".join(year_mentions))
            if first_years:
                earliest = min(int(y) for y in first_years)
                years  = float(date.today().year - earliest)
                source = "inferred"

    # Extract skills đơn giản (placeholder)
    COMMON_SKILLS = {
        "python", "java", "javascript", "typescript", "react", "reactjs",
        "nodejs", "fastapi", "django", "spring", "docker", "kubernetes",
        "sql", "postgresql", "mysql", "mongodb", "redis", "git",
        "aws", "gcp", "azure", "spark", "airflow", "kafka",
        "excel", "sap", "misa", "power bi", "tableau",
    }
    skills = {s for s in COMMON_SKILLS if s in text}

    # Extract location đơn giản
    PROVINCES = [
        "hồ chí minh", "hà nội", "đà nẵng", "bình dương",
        "đồng nai", "hải phòng", "cần thơ", "long an",
    ]
    location = None
    for prov in PROVINCES:
        if prov in text:
            location = prov.title()
            break

    return CVProfile(
        raw_text=raw_text,
        candidate_location=location,
        years_of_experience=years,
        experience_source=source,
        extracted_skills=skills,
    )


# ── Filter logic ──────────────────────────────────────────────────────────────

def build_filter(cv: CVProfile, filter_expired: bool = True) -> Filter:
    """
    filter_expired=False khi test với data cũ (tất cả đã hết hạn).
    filter_expired=True  khi production.
    """
    today = date.today().isoformat()
    must  = []

    if filter_expired:
        must.append(
            FieldCondition(key="expired_date", range=DatetimeRange(gte=today))
        )

    # Location filter — chỉ dùng tỉnh/thành sau normalize
    # work_form remote/hybrid/onsite không có trong data → bỏ hoàn toàn
    if cv.candidate_location:
        must.append(
            FieldCondition(key="location_name", match=MatchValue(value=cv.candidate_location))
        )

    # Experience
    if cv.experience_source in ("explicit", "inferred") and cv.years_of_experience is not None:
        yoe   = cv.years_of_experience
        slack = 1.0 if cv.experience_source == "inferred" else 0.0
        must += [
            FieldCondition(key="min_years", range=Range(lte=yoe + slack)),
            FieldCondition(key="max_years", range=Range(gte=max(0.0, yoe - slack))),
        ]
    elif cv.experience_source == "default":
        # Không biết kinh nghiệm → chỉ lấy job fresher
        must.append(
            FieldCondition(key="experience_type_code", match=MatchValue(value="fresher"))
        )

    return Filter(must=must)


# ── Search ────────────────────────────────────────────────────────────────────

def recommend(
    cv: CVProfile,
    client: QdrantClient,
    model: SentenceTransformer,
    top_k: int = 10,
    filter_expired: bool = True,
) -> list[dict]:
    cv_vec = model.encode(
        f"{QUERY_PREFIX}{cv.raw_text[:512]}",
        normalize_embeddings=True,
    ).tolist()

    hits = client.query_points(
        collection_name=COLLECTION_NAME,
        query=cv_vec,
        query_filter=build_filter(cv, filter_expired=filter_expired),
        limit=top_k,
        with_payload=True,
    ).points

    results = []
    for hit in hits:
        p = hit.payload

        job_skills = set(p.get("skill_tags") or [])
        matched    = sorted(job_skills & cv.extracted_skills)
        missing    = sorted(job_skills - cv.extracted_skills)

        try:
            days_left = (date.fromisoformat(str(p["expired_date"])[:10]) - date.today()).days
        except Exception:
            days_left = -1

        mn, mx = p.get("min_salary"), p.get("max_salary")
        if mn and mx:
            salary = f"{mn:,}–{mx:,} {p.get('currency','VND')}/{p.get('salary_type','')}"
        else:
            salary = "Thỏa thuận"

        results.append({
            # Display
            "job_key":          p.get("job_key"),
            "title":            p.get("title"),
            "company":          p.get("company"),
            "category":         p.get("category"),
            "location":         p.get("location_name"),
            "employment_type":  p.get("work_form_code"),  # full_time/part_time/internship
            "exp_range":        f"{p.get('min_years',0):.0f}–{p.get('max_years',99):.0f} năm",
            "salary":           salary,
            "days_until_expiry":days_left,
            "link":             p.get("link"),
            "platform":         p.get("platform"),
            # Explain — quan trọng cho demo và hội đồng
            "match_score":      round(hit.score, 4),
            "matched_skills":   matched,
            "missing_skills":   missing[:5],
            "experience_match": _explain_exp(cv, p),
        })

    return results


def _explain_exp(cv: CVProfile, payload: dict) -> str:
    if cv.experience_source == "default":
        return "fresher_fallback"
    yoe = cv.years_of_experience or 0
    if yoe < (payload.get("min_years") or 0):
        return "underqualified"
    if yoe > (payload.get("max_years") or 99):
        return "overqualified"
    return "exact"
