"""
dags/job_indexing_dag.py — Airflow DAG tự động hóa index pipeline

DAG 1: job_indexing_pipeline   — chạy hàng ngày lúc 2h sáng (upsert)
DAG 2: full_reindex_after_ner  — trigger thủ công sau khi NER mới xong
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner":          "careerlake",
    "retries":        1,
    "retry_delay":    timedelta(minutes=5),
    "email_on_failure": False,
}


# ── DAG 1: Hàng ngày ─────────────────────────────────────────────────────────

with DAG(
    dag_id="job_indexing_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",   # 2h sáng hàng ngày
    catchup=False,
    tags=["recommend", "qdrant"],
) as daily_dag:

    check_qdrant = BashOperator(
        task_id="check_qdrant_health",
        bash_command="curl -f http://qdrant:6333/healthz || exit 1",
    )

    def run_daily_index(**context):
        import sys
        sys.path.append("/opt/airflow/plugins/recommend")
        from indexer import index_pipeline
        count = index_pipeline(recreate=False)   # upsert, không xóa data cũ
        print(f"Daily index done: {count} vectors")

    daily_index = PythonOperator(
        task_id="embed_and_index_jobs",
        python_callable=run_daily_index,
    )

    def verify(**context):
        import sys
        sys.path.append("/opt/airflow/plugins/recommend")
        from config import COLLECTION_NAME, QDRANT_HOST, QDRANT_PORT
        from qdrant_client import QdrantClient
        client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        count  = client.get_collection(COLLECTION_NAME).points_count
        print(f"Verify OK: {count} vectors")
        if count == 0:
            raise ValueError("Collection rỗng sau index!")

    verify_index = PythonOperator(
        task_id="verify_index",
        python_callable=verify,
    )

    check_qdrant >> daily_index >> verify_index


# ── DAG 2: Manual full reindex ────────────────────────────────────────────────
# Trigger thủ công sau khi Người A cải thiện NER và re-run silver/gold pipeline

with DAG(
    dag_id="full_reindex_after_ner_update",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # chỉ trigger thủ công
    catchup=False,
    tags=["recommend", "qdrant", "manual"],
) as reindex_dag:

    def run_full_reindex(**context):
        import sys
        sys.path.append("/opt/airflow/plugins/recommend")
        from indexer import index_pipeline
        # recreate=True: xóa collection cũ và tạo lại với NER mới
        count = index_pipeline(recreate=True)
        print(f"Full reindex done: {count} vectors")

    full_reindex = PythonOperator(
        task_id="full_reindex",
        python_callable=run_full_reindex,
    )
