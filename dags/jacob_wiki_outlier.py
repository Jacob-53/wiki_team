from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

DAG_ID = "wiki_outlier"

with DAG(
    DAG_ID,
    max_active_runs=1,
    max_active_tasks=1,
    description="wiki outlier",
    schedule="10 10 * * *",
    start_date=datetime(2024, 3, 1),
    end_date=datetime(2024, 4, 1),
    catchup=True,
    tags=["wiki", "outlier", "ko"],
) as dag:
    start = EmptyOperator(task_id="start")
    wiki_outlier = BashOperator(
        task_id='run_bash',
        bash_command="""
        ssh -i ~/.ssh/jacob_gcp_key jacob8753@34.64.85.205 \
        "/home/jacob8753/code/outlier/run.sh {{ ds_nodash }} /home/jacob8753/code/outlier/wiki_outlier.py"
        """,
    )
    end = EmptyOperator(task_id="end")
    start >> wiki_outlier >> end
