from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess


default_args = {
    "owner": "ekaterina",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "stock_data_pipeline",
    default_args=default_args,
    schedule_interval="*/30 * * * *",  # ✅ Every 30 minutes (change to '0 * * * *' for hourly)
    catchup=False
)

def run_update():
    """Run stock data update and log output."""
    result = subprocess.run(
        ["docker", "exec", "stock_project-stock-fetcher", "python", "update_stock_data.py"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    print(f"Exit Code: {result.returncode}")  # ✅ Show actual exit code
    
    if result.returncode != 0:
        raise Exception(f"Update stock data failed with exit code {result.returncode}: {result.stderr}")


update_task = PythonOperator(
    task_id="update_stock_data",
    python_callable=run_update,
    dag=dag
)
