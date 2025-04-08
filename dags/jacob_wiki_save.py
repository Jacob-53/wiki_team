from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "wiki_save"

with DAG(
    DAG_ID,
    max_active_runs=1,
    max_active_tasks=1,
    description="wiki spark gcp submit",
    schedule="10 10 * * *",
    start_date=datetime(2024, 3, 1),
    end_date=datetime(2024, 4, 1),
    catchup=True,
    tags=["spark", "sh", "wiki"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_bash = BashOperator(
        task_id='run_bash',
        bash_command="""
        ssh -i ~/.ssh/jacob_gcp_key jacob8753@34.47.89.22 \
        "/home/jacob8753/code/test/run.sh {{ ds }} /home/jacob8753/code/test/test_save_parquet.py"
        """,
    )
    end = EmptyOperator(task_id="end")
    start >> run_bash >> end