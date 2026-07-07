import streamlit as st

# Set page config
st.set_page_config(page_title="Job Recommender", page_icon="🧭", layout="wide")

jobs_page = st.Page("jobs.py", title="List Jobs")
recommendation_page = st.Page("recommendation_system.py", title="Job Recommender")

pg = st.navigation(
    {
        "Jobs": [jobs_page],
        "Recommendations": [recommendation_page]
    }
)

pg.run()