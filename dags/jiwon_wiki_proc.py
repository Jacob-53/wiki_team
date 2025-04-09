from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.timezone import datetime

DAG_ID = "wiki_proc"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="wiki data processing",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 5),
    catchup=True,
    tags=["spark", "submit", "wiki"],
) as dag:
    SPARK_HOME = "/home/jiwon/app/spark-3.5.1-bin-hadoop3"

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    process_data = BashOperator(
        task_id="wiki.proc",
        bash_command="""
        DATE_STR={{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m%dT%H%M%S') }}
        ssh -i ~/.ssh/jiwon_spark juper7619@34.64.59.16 \
        "/home/juper7619/code/wiki/run.sh $DATE_STR /home/juper7619/code/wiki/processing.py"
    """
    )

start >> process_data >> end
