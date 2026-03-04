from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="bank_etl_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_spark_pipeline = BashOperator(
        task_id="run_spark_etl",
        bash_command="python /Users/kushalmaddala/Documents/bank-etl-de1/Spark/ingest_csv.py"
    )