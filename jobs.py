import streamlit as st
import pandas as pd
import math
import requests
import time
from typing import List, Set

# =========================
# --------- CONFIG --------
# =========================

DREMIO_URL = "http://dremio:9047"  # Đổi thành http://dremio:9047 nếu chạy trong Docker
DREMIO_USER = "daley12306"
DREMIO_PASS = "TPhuc2306@"

# =========================
# --------- DATA ----------
# =========================

@st.cache_data(ttl=300)
def load_jobs_from_dremio() -> pd.DataFrame:
    """Load jobs từ Dremio view v_fact_job_posting."""
    try:
        # 1. Login
        auth_resp = requests.post(
            f"{DREMIO_URL}/apiv2/login",
            json={"userName": DREMIO_USER, "password": DREMIO_PASS},
            timeout=10
        )
        auth_resp.raise_for_status()
        token = auth_resp.json()["token"]
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # 2. Submit SQL query
        sql = """
            SELECT 
                fact_job_key as job_id,
                CONCAT(category_name, ' at ', company_name) as title,
                company_name as company,
                location_name as location,
                min_salary_vnd / 1000000.0 as salary_min,
                max_salary_vnd / 1000000.0 as salary_max,
                experience_type_name as seniority,
                CAST(NULL AS VARCHAR) as skills,
                CONCAT(
                    'Level: ', education_name, ' | ',
                    'Experience: ', COALESCE(CAST(min_years AS VARCHAR), 'N/A'), '-', COALESCE(CAST(max_years AS VARCHAR), 'N/A'), ' years | ',
                    'Work form: ', work_form_name, ' | ',
                    'Quantity: ', CAST(quantity_normalized AS VARCHAR)
                ) as description,
                platform_name,
                expired_date,
                salary_bucket,
                exp_bucket
            FROM Nessie.gold.v_fact_job_posting
            WHERE snapshot_at = (SELECT MAX(snapshot_at) FROM Nessie.gold.v_fact_job_posting) and expired_date >= CURRENT_DATE
        """
        
        job_resp = requests.post(
            f"{DREMIO_URL}/api/v3/sql",
            headers=headers,
            json={"sql": sql},
            timeout=30
        )
        job_resp.raise_for_status()
        job_id = job_resp.json()["id"]
        
        # 3. Poll job status với logging chi tiết
        max_wait = 60  # Tăng timeout lên 60s
        for i in range(max_wait):
            status_resp = requests.get(
                f"{DREMIO_URL}/api/v3/job/{job_id}",
                headers=headers,
                timeout=10
            )
            status_resp.raise_for_status()
            job_info = status_resp.json()
            status = job_info["jobState"]
            
            # Debug log
            if i % 5 == 0:  # Log mỗi 5s
                st.info(f"⏳ Query status: {status} ({i}s elapsed)")
            
            if status == "COMPLETED":
                # Kiểm tra rowCount trước khi fetch
                row_count = job_info.get("rowCount", 0)
                st.success(f"✅ Query completed. Row count: {row_count}")
                break
            elif status in ["FAILED", "CANCELED"]:
                error_msg = job_info.get("errorMessage", "Unknown error")
                st.error(f"❌ Query {status}: {error_msg}")
                raise RuntimeError(f"Query failed: {status} - {error_msg}")
            
            time.sleep(1)
        else:
            # Timeout
            st.warning(f"⚠️ Query timeout after {max_wait}s. Last status: {status}")
            raise RuntimeError(f"Query timeout. Status: {status}")
        
        # 4. Fetch results với error handling
        try:
            # Dùng API v2 thay vì v3 (tương thích hơn)
            results_url = f"{DREMIO_URL}/api/v3/job/{job_id}/results"
            
            # Fetch từng batch nhỏ nếu data lớn
            all_rows = []
            offset = 0
            limit = 500  # Giảm limit xuống 500
            
            while True:
                batch_resp = requests.get(
                    f"{results_url}?offset={offset}&limit={limit}",
                    headers=headers,
                    timeout=30
                )
                batch_resp.raise_for_status()
                batch_data = batch_resp.json()
                
                rows = batch_data.get("rows", [])
                if not rows:
                    break
                
                all_rows.extend(rows)
                offset += len(rows)
                
                # Stop nếu đã lấy đủ hoặc hết data
                if len(rows) < limit or offset >= 1000:
                    break
            
            st.success(f"✅ Fetched {len(all_rows)} rows")
            df = pd.DataFrame(all_rows)
            
        except requests.exceptions.HTTPError as e:
            st.error(f"❌ Failed to fetch results: {e}")
            st.error(f"Response: {batch_resp.text if 'batch_resp' in locals() else 'N/A'}")
            raise
        
        # Parse skills từ JSON (nếu có dim_skill join)
        if "skills" in df.columns:
            df["skills"] = df["skills"].fillna("[]").apply(
                lambda x: eval(x) if isinstance(x, str) and x.startswith("[") else []
            )
        else:
            # Mock skills từ category_name (tạm thời)
            df["skills"] = df.apply(
                lambda row: [row.get("platform_name", "Unknown"), 
                             row.get("seniority", "Mid"),
                             row.get("exp_bucket", "Entry")],
                axis=1
            )
        
        # Convert salary sang USD (giả sử 1 USD = 25,000 VND)
        if "salary_min" in df.columns:
            df["salary_min"] = df["salary_min"] / 25.0
            df["salary_max"] = df["salary_max"] / 25.0
        
        return df
        
    except Exception as e:
        st.error(f"⚠️ Failed to load from Dremio: {e}")
        st.info("Falling back to mock data...")
        return load_mock_jobs()

@st.cache_data
def load_mock_jobs() -> pd.DataFrame:
    """Mock job dataset (fallback)."""
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
    ]
    return pd.DataFrame(data)

def get_all_skill_vocab(jobs_df: pd.DataFrame) -> Set[str]:
    vocab = set()
    for skills in jobs_df["skills"].tolist():
        if isinstance(skills, list):
            vocab.update(skills)
    return vocab

def highlight_skill_badges(job_skills: List[str], matched_skills: Set[str]) -> str:
    """Render HTML badges, highlight kỹ năng trùng bằng màu nổi bật."""
    html_parts = []
    for sk in job_skills:
        if sk.lower() in {s.lower() for s in matched_skills}:
            html_parts.append(
                f'<span style="display:inline-block;background:#16a34a;color:#fff;'
                f'padding:4px 8px;border-radius:12px;margin:2px;font-size:12px;">{sk}</span>'
            )
        else:
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
try:
    jobs_df = load_jobs_from_dremio()
    skill_vocab = get_all_skill_vocab(jobs_df)
except Exception as e:
    st.error(f"Critical error: {e}")
    st.stop()

# ---- Sidebar Filters ----
with st.sidebar:
    st.header("🔎 Filters")
    keyword = st.text_input("Keyword (title/description)")
    locations = ["(All)"] + sorted(jobs_df["location"].dropna().unique().tolist())
    loc = st.selectbox("Location", locations, index=0)
    min_salary = st.number_input("Min salary (USD)", min_value=0, value=0, step=100)
    selected_skills = st.multiselect("Filter by skills", sorted(skill_vocab))

    st.markdown("---")
    st.subheader("⚙️ Pagination")
    page_size = st.selectbox("Page size", [5, 10, 20], index=0)
    if "page" not in st.session_state:
        st.session_state.page = 1
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
        st.markdown(f"${row['salary_min']:,.0f} – ${row['salary_max']:,.0f}")
    
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
                        f"**💰** ${row['salary_min']:,.0f}–${row['salary_max']:,.0f}")
            st.markdown(f"*{truncate_text(row['description'], 100)}*")
            display_skills = row["skills"][:4] if isinstance(row["skills"], list) else []
            remaining = len(row["skills"]) - 4 if isinstance(row["skills"], list) else 0
            badges = highlight_skill_badges(display_skills, matched_skills=set())
            if remaining > 0:
                badges += f' <span style="color:#6b7280;font-size:12px;">+{remaining} more</span>'
            st.markdown(badges, unsafe_allow_html=True)
        
        with col_btn:
            st.write("")
            if st.button("🔍 View", key=f"view_{row['job_id']}_{idx}", use_container_width=True):
                show_job_detail(row)
        
        st.markdown("---")

for idx, (_, r) in enumerate(page_df.iterrows()):
    render_job_card(r, idx)