import streamlit as st
import pandas as pd
import numpy as np
import re
import math
from io import StringIO
from typing import List, Dict, Tuple, Set
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Optional parsers
try:
    import PyPDF2  # for PDF
except Exception:
    PyPDF2 = None

try:
    import docx  # python-docx (for DOCX)
except Exception:
    docx = None


# =========================
# --------- DATA ----------
# =========================

@st.cache_data
def load_mock_jobs() -> pd.DataFrame:
    """Mock job dataset. In thực tế: load từ Lakehouse (Gold/Silver)."""
    data = [
        {
            "job_id": "J001",
            "title": "Data Engineer",
            "company": "VietData",
            "location": "Hồ Chí Minh",
            "salary_min": 2000,
            "salary_max": 3000,
            "seniority": "Mid",
            "skills": ["Python", "SQL", "Airflow", "Spark", "Docker", "GCP"],
            "description": "Thiết kế và vận hành data pipeline trên GCP, dùng Airflow, Spark. Yêu cầu Python/SQL tốt, kinh nghiệm Docker."
        },
        {
            "job_id": "J002",
            "title": "Machine Learning Engineer",
            "company": "AIWorks",
            "location": "Hà Nội",
            "salary_min": 2200,
            "salary_max": 3500,
            "seniority": "Senior",
            "skills": ["Python", "TensorFlow", "PyTorch", "MLOps", "Docker", "Kubernetes"],
            "description": "Xây dựng và triển khai mô hình ML, tối ưu hóa inference, MLOps trên Kubernetes, viết Python."
        },
        {
            "job_id": "J003",
            "title": "Backend Engineer (FastAPI)",
            "company": "FinTechX",
            "location": "Hồ Chí Minh",
            "salary_min": 1500,
            "salary_max": 2300,
            "seniority": "Junior",
            "skills": ["Python", "FastAPI", "PostgreSQL", "Redis", "Docker", "CI/CD"],
            "description": "Phát triển API hiệu năng cao với FastAPI, quản lý DB PostgreSQL, cache với Redis, CI/CD."
        },
        {
            "job_id": "J004",
            "title": "Data Analyst (BI)",
            "company": "InsightVN",
            "location": "Đà Nẵng",
            "salary_min": 1000,
            "salary_max": 1800,
            "seniority": "Mid",
            "skills": ["SQL", "Power BI", "Excel", "DAX", "Storytelling"],
            "description": "Xây dựng dashboard Power BI, phân tích dữ liệu với SQL, trình bày insight."
        },
        {
            "job_id": "J005",
            "title": "ML Ops / Data Platform Engineer",
            "company": "CloudEdge",
            "location": "Hồ Chí Minh",
            "salary_min": 2500,
            "salary_max": 3800,
            "seniority": "Senior",
            "skills": ["Python", "Airflow", "Kafka", "Spark", "Kubernetes", "Terraform", "GCP"],
            "description": "Thiết kế data platform, streaming với Kafka, orchestration Airflow, IaC bằng Terraform, chạy trên GCP/K8s."
        },
    ]
    return pd.DataFrame(data)


def get_all_skill_vocab(jobs_df: pd.DataFrame) -> Set[str]:
    vocab = set()
    for skills in jobs_df["skills"].tolist():
        vocab.update(skills)
    return vocab


# =========================
# ----- CV PARSING --------
# =========================

def extract_text_from_pdf(file_bytes: bytes) -> str:
    if PyPDF2 is None:
        return ""
    text = []
    try:
        reader = PyPDF2.PdfReader(StringIO(file_bytes.decode("latin-1")))
    except Exception:
        # fallback reading from raw bytes
        import io
        reader = PyPDF2.PdfReader(io.BytesIO(file_bytes))
    for page in reader.pages:
        try:
            text.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(text)


def extract_text_from_docx(file_bytes: bytes) -> str:
    if docx is None:
        return ""
    import io
    f = io.BytesIO(file_bytes)
    document = docx.Document(f)
    paras = [p.text for p in document.paragraphs]
    return "\n".join(paras)


def normalize_text(s: str) -> str:
    s = s.replace("\x00", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def simple_skill_extractor(text: str, skill_vocab: Set[str]) -> List[str]:
    """
    Cực kỳ đơn giản: khớp theo từ/cụm từ trong vocab (case-insensitive).
    Gợi ý: có thể nâng cấp bằng NER (BERT/PhoBERT) sau.
    """
    text_lower = " " + text.lower() + " "
    matched = []
    # Sort theo độ dài để ưu tiên cụm từ dài (vd: 'machine learning' trước 'learning')
    for sk in sorted(skill_vocab, key=lambda x: -len(x)):
        pattern = r"\b" + re.escape(sk.lower()) + r"\b"
        if re.search(pattern, text_lower):
            matched.append(sk)
    return sorted(set(matched))


# =========================
# ---- RECOMMENDER --------
# =========================

def build_job_corpus(jobs_df: pd.DataFrame) -> List[str]:
    """
    Kết hợp title + description + skills thành văn bản cho TF-IDF.
    """
    corpus = []
    for _, row in jobs_df.iterrows():
        parts = [
            str(row["title"]),
            str(row["description"]),
            " ".join(row["skills"]),
            str(row["location"]),
            str(row["seniority"])
        ]
        corpus.append(" ".join(parts))
    return corpus


def recommend_jobs(
    jobs_df: pd.DataFrame,
    cv_text: str,
    cv_skills: List[str],
    alpha: float = 0.6,
    top_k: int = 5,
) -> pd.DataFrame:
    """
    Score = alpha * cosine_similarity(TFIDF) + (1 - alpha) * skill_overlap
    skill_overlap = |CV ∩ JOB| / |JOB|
    """
    corpus = build_job_corpus(jobs_df)
    vectorizer = TfidfVectorizer()
    job_mat = vectorizer.fit_transform(corpus)

    cv_vec = vectorizer.transform([cv_text])
    sim = cosine_similarity(cv_vec, job_mat).flatten()  # shape: (n_jobs,)

    # skill overlap
    job_skills_list = jobs_df["skills"].tolist()
    overlap_scores = []
    for skills in job_skills_list:
        if len(skills) == 0:
            overlap_scores.append(0.0)
        else:
            inter = len(set([s.lower() for s in cv_skills]) & set([s.lower() for s in skills]))
            overlap_scores.append(inter / float(len(skills)))
    overlap_scores = np.array(overlap_scores, dtype=float)

    final_score = alpha * sim + (1 - alpha) * overlap_scores

    result = jobs_df.copy()
    result["sim_score"] = sim
    result["skill_overlap"] = overlap_scores
    result["score"] = final_score
    result = result.sort_values("score", ascending=False).head(top_k).reset_index(drop=True)
    return result


def highlight_skill_badges(job_skills: List[str], matched_skills: Set[str]) -> str:
    """
    Render HTML badges, highlight kỹ năng trùng bằng màu nổi bật.
    """
    html_parts = []
    for sk in job_skills:
        if sk.lower() in {s.lower() for s in matched_skills}:
            # matched → badge xanh lá
            html_parts.append(
                f'<span style="display:inline-block;background:#16a34a;color:#fff;'
                f'padding:4px 8px;border-radius:12px;margin:2px;font-size:12px;">{sk}</span>'
            )
        else:
            # not matched → badge xám
            html_parts.append(
                f'<span style="display:inline-block;background:#e5e7eb;color:#111827;'
                f'padding:4px 8px;border-radius:12px;margin:2px;font-size:12px;">{sk}</span>'
            )
    return " ".join(html_parts)


# =========================
# --------- UI ------------
# =========================

st.set_page_config(page_title="Job Recommender", page_icon="🧭", layout="wide")
st.title("🧭 Job Recommendation System (Lakehouse-ready)")

# Load data
jobs_df = load_mock_jobs()
skill_vocab = get_all_skill_vocab(jobs_df)

# ---- Sidebar Filters ----
with st.sidebar:
    st.header("🔎 Filters")
    keyword = st.text_input("Keyword (title/description)")
    locations = ["(All)"] + sorted(jobs_df["location"].unique().tolist())
    loc = st.selectbox("Location", locations, index=0)
    min_salary = st.number_input("Min salary (USD)", min_value=0, value=0, step=100)
    selected_skills = st.multiselect("Filter by skills", sorted(skill_vocab))

    st.markdown("---")
    st.subheader("⚙️ Pagination")
    page_size = st.selectbox("Page size", [5, 10, 20], index=0)
    if "page" not in st.session_state:
        st.session_state.page = 1
    # Reset page when filters change (simple heuristic)
    if st.button("Reset page"):
        st.session_state.page = 1

# ---- Filter logic ----
filtered = jobs_df.copy()

if keyword:
    mask_kw = (
        filtered["title"].str.contains(keyword, case=False, na=False) |
        filtered["description"].str.contains(keyword, case=False, na=False)
    )
    filtered = filtered[mask_kw]

if loc and loc != "(All)":
    filtered = filtered[filtered["location"] == loc]

if min_salary and min_salary > 0:
    filtered = filtered[filtered["salary_max"] >= min_salary]

if selected_skills:
    # Require at least one selected skill present
    filtered = filtered[filtered["skills"].apply(lambda s: any(sk in s for sk in selected_skills))]

filtered = filtered.reset_index(drop=True)

# ---- List jobs (paginated) ----
st.subheader("📋 Current Jobs")
total = len(filtered)
total_pages = max(1, math.ceil(total / page_size))

cols = st.columns([1, 1, 1, 2, 1])
with cols[0]:
    st.markdown(f"**Total:** {total}")
with cols[1]:
    st.markdown(f"**Pages:** {total_pages}")
with cols[2]:
    go_prev = st.button("⬅ Prev", use_container_width=True)
with cols[3]:
    page_num = st.number_input("Page", min_value=1, max_value=total_pages, value=st.session_state.page, step=1)
with cols[4]:
    go_next = st.button("Next ➡", use_container_width=True)

if go_prev and st.session_state.page > 1:
    st.session_state.page -= 1
if go_next and st.session_state.page < total_pages:
    st.session_state.page += 1
if page_num != st.session_state.page:
    st.session_state.page = page_num

start_idx = (st.session_state.page - 1) * page_size
end_idx = start_idx + page_size
page_df = filtered.iloc[start_idx:end_idx]

def render_job_card(row: pd.Series):
    st.markdown(f"### {row['title']} • **{row['company']}**")
    st.markdown(f"**Location:** {row['location']}  |  **Seniority:** {row['seniority']}  |  "
                f"**Salary:** ${row['salary_min']}–${row['salary_max']}")
    st.markdown(f"{row['description']}")
    # skill chips (no highlighting in the list view)
    badges = highlight_skill_badges(row["skills"], matched_skills=set())
    st.markdown(badges, unsafe_allow_html=True)
    st.markdown("---")

for _, r in page_df.iterrows():
    render_job_card(r)

# =========================
# ---- CV & Recommend -----
# =========================

st.subheader("📄 Upload CV & Get Recommendations")

uploaded = st.file_uploader("Upload your CV (PDF/DOCX/TXT)", type=["pdf", "docx", "txt"])
cv_text = ""
cv_skills: List[str] = []

col_left, col_right = st.columns([1, 1])

with col_left:
    if uploaded:
        bytes_data = uploaded.read()
        if uploaded.type == "application/pdf":
            cv_text = extract_text_from_pdf(bytes_data)
        elif uploaded.type in ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                               "application/msword"):
            cv_text = extract_text_from_docx(bytes_data)
        else:
            # txt
            try:
                cv_text = bytes_data.decode("utf-8", errors="ignore")
            except Exception:
                cv_text = ""

        cv_text = normalize_text(cv_text)
        if not cv_text:
            st.warning("Could not extract text from your file. Please try another format.")
        else:
            st.text_area("CV Text (preview)", cv_text[:2000], height=200)
            cv_skills = simple_skill_extractor(cv_text, skill_vocab)
            st.markdown("**Detected skills:** " + (", ".join(cv_skills) if cv_skills else "_(none)_"))

with col_right:
    alpha = st.slider("Weight: Text similarity vs Skill overlap (alpha)", 0.0, 1.0, 0.6, 0.05)
    top_k = st.slider("Top-K recommendations", 1, 10, 5, 1)
    btn = st.button("✨ Recommend Jobs", type="primary")

if btn:
    if not uploaded or not cv_text:
        st.error("Please upload a valid CV first.")
    else:
        rec_df = recommend_jobs(jobs_df, cv_text, cv_skills, alpha=alpha, top_k=top_k)

        st.markdown("## ✅ Recommended Jobs")
        for _, row in rec_df.iterrows():
            st.markdown(f"### {row['title']} • **{row['company']}**  "
                        f"<span style='color:#6b7280'>(Score: {row['score']:.3f} = "
                        f"{alpha:.0%}×sim {row['sim_score']:.3f} + "
                        f"{(1-alpha):.0%}×overlap {row['skill_overlap']:.3f})</span>",
                        unsafe_allow_html=True)
            st.markdown(f"**Location:** {row['location']}  |  **Seniority:** {row['seniority']}  |  "
                        f"**Salary:** ${row['salary_min']}–${row['salary_max']}")
            st.markdown(row["description"])

            # Highlight matching skills
            matched = set([s for s in row["skills"] if s.lower() in {x.lower() for x in cv_skills}])
            st.markdown("**Skills:**", unsafe_allow_html=True)
            st.markdown(highlight_skill_badges(row["skills"], matched), unsafe_allow_html=True)

            # Reasons / Explain
            if matched:
                st.markdown(f"**Matched skills:** {', '.join(sorted(matched))}")
            else:
                st.markdown("_No skill matched. Consider upskilling or broadening filters._")

            st.markdown("---")


# =========================
# ------- FOOTER ----------
# =========================

with st.expander("ℹ️ Notes & Next Steps"):
    st.markdown(
        """
- Đây là bản **demo baseline**: TF‑IDF + cosine + skill-overlap. Bạn có thể nâng cấp:
  - **Embeddings**: Sentence‑BERT / SimCSE cho tiếng Việt.
  - **Vector Search**: FAISS / OpenSearch KNN.
  - **Skill Extractor**: NER (BERT/PhoBERT) thay vì từ điển đơn giản.
  - **Lakehouse Integration**: Đọc jobs từ Iceberg/Delta, refresh theo lịch (Airflow/dbt).
- UX: highlight matching skills bằng badge màu xanh lá; kỹ năng không match là xám.
- Tối ưu điểm số: thử trọng số khác nhau cho sim vs overlap, thêm **seniority/location match**.
        """
    )