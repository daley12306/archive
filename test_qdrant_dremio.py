"""
test_qdrant_dremio.py — Test end-to-end với data thật từ Dremio

Chạy: python test_qdrant_dremio.py
"""
from collections import Counter
from datetime import date

from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

from config import QDRANT_HOST, QDRANT_PORT, COLLECTION_NAME, EMBED_MODEL
from dremio_client import DremioClient
from indexer import load_jobs, setup_collection, index_jobs
from searcher import CVProfile, recommend, build_filter

SAMPLE_LIMIT = 30   # tăng lên 100-200 khi test đầy đủ hơn


# ── Overview ──────────────────────────────────────────────────────────────────

def print_overview(jobs: list[dict]):
    print(f"\n{'─'*55}")
    print(f"  DATA OVERVIEW — {len(jobs)} jobs từ Dremio")
    print(f"{'─'*55}")
    wf  = Counter(j.get("work_form_code") for j in jobs)
    exp = Counter(j.get("experience_type_code") for j in jobs)
    loc = Counter(j.get("location_name") for j in jobs)
    print(f"  Work form   : {dict(wf)}")
    print(f"  Experience  : {dict(exp)}")
    print(f"  Top location: {dict(loc.most_common(5))}")
    print()
    print(f"  Sample 3 jobs:")
    for j in jobs[:3]:
        print(f"    [{j.get('experience_type_code')}] {j['title_clean']} @ {j.get('company_name')}")
        print(f"    {j.get('work_form_code')} | hết hạn: {j.get('expired_date')}")
        print(f"    Skills: {(j.get('skill_tags') or [])[:5]}")
        print()


# ── Print search results ──────────────────────────────────────────────────────

def print_results(results: list[dict]):
    if not results:
        print("  Không có kết quả — filter quá chặt hoặc sample nhỏ")
        print("  Thử tăng SAMPLE_LIMIT hoặc bỏ filter location")
        return
    for i, r in enumerate(results, 1):
        bar = "█" * int(r["match_score"] * 20)
        print(f"  {i}. [{r['match_score']:.4f}] {bar}")
        print(f"     {r['title']} @ {r['company']}")
        print(f"     {r['category']}")
        print(f"     {r['location']} | {r['employment_type']} | {r['exp_range']} | {r['salary']}")
        print(f"     Matched : {r['matched_skills'] or '(chưa có NER)'}")
        print(f"     Missing : {r['missing_skills'] or '(chưa có NER)'}")
        print(f"     Exp fit : {r['experience_match']} | còn {r['days_until_expiry']} ngày")
        print()


# ── Test scenarios ────────────────────────────────────────────────────────────

def run_tests(jobs: list[dict], client: QdrantClient, model: SentenceTransformer):
    top_loc = Counter(
        j.get("location_name") for j in jobs
        if j.get("location_name") not in ("unknown", None, "Remote", "Toàn quốc")
    ).most_common(1)
    loc1 = top_loc[0][0] if top_loc else "Hồ Chí Minh"

    top_cat = Counter(j.get("category_name") for j in jobs if j.get("category_name")).most_common(1)
    cat1 = top_cat[0][0] if top_cat else "IT - Phần mềm"

    # Kiểm tra data có còn hạn không
    today = date.today()
    expired_count = sum(
        1 for j in jobs
        if j.get("expired_date") and
        date.fromisoformat(str(j["expired_date"])[:10]) < today
    )
    filter_expired = expired_count < len(jobs) * 0.9  # tắt filter nếu >90% đã hết hạn

    if not filter_expired:
        print(f"\n  Lưu ý: {expired_count}/{len(jobs)} jobs đã hết hạn")
        print(f"  Tắt filter expired để test embedding + filter logic khác")
        print(f"  (Khi có data mới, bật lại filter_expired=True)")

    print(f"\n  location='{loc1}', category='{cat1}'")

    scenarios = [
        {
            "name":   f"CV junior 2 năm, {loc1}",
            "cv":     CVProfile(
                raw_text=f"2 năm kinh nghiệm {cat1}",
                candidate_location=None,  # tắt location filter — chưa normalize
                years_of_experience=2.0,
                experience_source="explicit",
            ),
            "expect": f"Junior/mid jobs tại {loc1} hoặc remote, exp range khớp",
        },
        {
            "name":   "Fresher — không rõ kinh nghiệm",
            "cv":     CVProfile(
                raw_text=f"Sinh viên mới tốt nghiệp {cat1}",
                candidate_location=None,  # tắt location filter — chưa normalize
                years_of_experience=None,
                experience_source="default",
            ),
            "expect": "Chỉ job fresher (experience_type_code=fresher)",
        },
        {
            "name":   "Senior 5 năm, không filter location",
            "cv":     CVProfile(
                raw_text=f"Senior 5 năm kinh nghiệm {cat1}",
                candidate_location=None,
                years_of_experience=5.0,
                experience_source="explicit",
            ),
            "expect": "Senior jobs, bao gồm cả remote",
        },
    ]

    for s in scenarios:
        print(f"\n{'='*55}")
        print(f"  {s['name']}")
        print(f"  Kỳ vọng: {s['expect']}")
        print(f"{'─'*55}")
        results = recommend(
            s["cv"], client, model, top_k=5,
            filter_expired=filter_expired,
        )
        print_results(results)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # 1. Kết nối
    print("\n[1] Kết nối Qdrant và Dremio...")
    qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    try:
        qdrant.get_collections()
        print(f"  Qdrant OK")
    except Exception as e:
        print(f"  Qdrant thất bại: {e}")
        print(f"  Chạy: docker ps | grep qdrant")
        exit(1)

    dremio = DremioClient()

    # 2. Load data
    print(f"\n[2] Load {SAMPLE_LIMIT} jobs từ Dremio...")
    jobs = load_jobs(dremio, limit=SAMPLE_LIMIT)
    print_overview(jobs)

    # 3. Load model
    print(f"\n[3] Load embedding model '{EMBED_MODEL}'...")
    model    = SentenceTransformer(EMBED_MODEL)
    print(f"  Model OK — vector_size={model.get_sentence_embedding_dimension()}")

    # 4. Index
    print(f"\n[4] Index vào Qdrant (recreate=True)...")
    setup_collection(qdrant, recreate=True)
    index_jobs(jobs, qdrant, model)

    # 5. Test search
    print(f"\n[5] Test search...")
    run_tests(jobs, qdrant, model)

    # 6. Summary
    count = qdrant.get_collection(COLLECTION_NAME).points_count
    print(f"\n{'='*55}")
    print(f"  Tổng vectors trong Qdrant : {count}")
    print(f"  Qdrant UI                 : http://localhost:{QDRANT_PORT}/dashboard")
    print(f"\n  Bước tiếp theo:")
    print(f"  1. Score có hợp lý không? Top 1 rõ ràng hơn bottom 5?")
    print(f"  2. Filter hoạt động? Fresher CV không nhận senior job?")
    print(f"  3. Paste output ra nếu score bất thường để debug")
    print(f"{'='*55}")
