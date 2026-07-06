from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "example_parallel",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=2,  # 2 run song song
) as dag:
    t1 = BashOperator(task_id="t1", bash_command="sleep 15")
    t2 = BashOperator(task_id="t2", bash_command="sleep 15")
    t3 = BashOperator(task_id="t3", bash_command="sleep 15")