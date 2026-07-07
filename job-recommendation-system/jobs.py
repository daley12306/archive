import streamlit as st
import pandas as pd
import math
from typing import List, Set

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

def truncate_text(text: str, max_length: int = 80) -> str:
    """Cắt ngắn text và thêm ... nếu quá dài."""
    if len(text) <= max_length:
        return text
    return text[:max_length].rsplit(' ', 1)[0] + "..."

# =========================
# --------- UI ------------
# =========================

st.set_page_config(page_title="List Jobs", page_icon="👜", layout="wide")
st.title("👜 List Jobs")

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

@st.dialog("📄 Job Details", width="large")
def show_job_detail(row: pd.Series):
    """Hiển thị modal với đầy đủ thông tin job."""
    st.markdown(f"## {row['title']}")
    st.markdown(f"### 🏢 {row['company']}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"**📍 Location:**")
        st.markdown(f"{row['location']}")
    with col2:
        st.markdown(f"**📊 Seniority:**")
        st.markdown(f"{row['seniority']}")
    with col3:
        st.markdown(f"**💰 Salary:**")
        st.markdown(f"${row['salary_min']:,} – ${row['salary_max']:,}")
    
    st.markdown("---")
    
    st.markdown("### 📝 Description")
    st.markdown(row['description'])
    
    st.markdown("---")
    
    st.markdown("### 🛠️ Required Skills")
    badges = highlight_skill_badges(row["skills"], matched_skills=set())
    st.markdown(badges, unsafe_allow_html=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        st.button("📨 Apply Now", type="primary", use_container_width=True)
    with col2:
        st.button("💾 Save Job", use_container_width=True)

def render_job_card(row: pd.Series, idx: int):
    """Render job card với description ngắn gọn và nút xem chi tiết."""
    with st.container():
        col_main, col_btn = st.columns([5, 1])
        
        with col_main:
            st.markdown(f"### {row['title']} • **{row['company']}**")
            st.markdown(f"**📍** {row['location']}  |  **📊** {row['seniority']}  |  "
                        f"**💰** ${row['salary_min']:,}–${row['salary_max']:,}")
            # Hiển thị description đã cắt ngắn
            st.markdown(f"*{truncate_text(row['description'], 100)}*")
            # Skill badges (chỉ hiển thị 4 skills đầu tiên)
            display_skills = row["skills"][:4]
            remaining = len(row["skills"]) - 4
            badges = highlight_skill_badges(display_skills, matched_skills=set())
            if remaining > 0:
                badges += f' <span style="color:#6b7280;font-size:12px;">+{remaining} more</span>'
            st.markdown(badges, unsafe_allow_html=True)
        
        with col_btn:
            st.write("")  # Spacing
            if st.button("🔍 View", key=f"view_{row['job_id']}_{idx}", use_container_width=True):
                show_job_detail(row)
        
        st.markdown("---")

for idx, (_, r) in enumerate(page_df.iterrows()):
    render_job_card(r, idx)