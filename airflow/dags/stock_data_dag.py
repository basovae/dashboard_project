from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "ekaterina",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "stock_data_pipeline",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    catchup=False
)

def fetch_stock_data():
    subprocess.run(["python", "/app/fetch_stock_data.py"])

fetch_task = PythonOperator(
    task_id="fetch_stock_data",
    python_callable=fetch_stock_data,
    dag=dag
)

fetch_task
