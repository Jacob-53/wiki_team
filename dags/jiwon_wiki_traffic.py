from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.timezone import datetime

DAG_ID = "wiki_traffic"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="wiki data",
    schedule="10 10 * * *",
    start_date=datetime(2024, 2, 1),
    end_date=datetime(2024, 3, 1),
    catchup=True,
    tags=["wiki", "ko", "traffic"],
) as dag:
    SPARK_HOME = "/home/jiwon/app/spark-3.5.1-bin-hadoop3"

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    traffic_data = BashOperator(
        task_id="wiki.traffic",
        bash_command="""
        ssh -i ~/.ssh/jiwon_spark juper7619@34.22.81.199 \
        "/home/juper7619/code/wiki_2/run.sh {{ ds_nodash }} /home/juper7619/code/wiki_2/processing.py"
        """
    )


    start >> traffic_data >> end
