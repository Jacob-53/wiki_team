from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# 기본 설정값
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": True
}

with DAG(
    dag_id="convert_txt_to_parquet",
    default_args=default_args,
    schedule= "10 10 * * *",
    catchup=True,
    start_date= datetime(2024, 2, 1),
    end_date=datetime(2024, 3, 1),
    max_active_runs=1,
    max_active_tasks=1,
    description="Convert TXT files to Parquet and store in GCP",
) as dag:

    SPARK_HOME="/home/gmlwls5168/app/spark-3.5.1-bin-hadoop3"

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    convert_task = BashOperator(
        task_id="convert_text_to_parquet",
        bash_command="""
        ssh -i ~/.ssh/gcp_key gmlwls5168@34.22.71.87 "/home/gmlwls5168/code/test/run.sh {{ ds }} /home/gmlwls5168/code/test/test_parquet.py"
        """
        )

start >> convert_task >> end

if __name__ == "__main__":
    dag.test()
