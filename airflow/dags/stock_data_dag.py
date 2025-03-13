from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

default_args = {
    "owner": "ekaterina",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 9),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "stock_data_pipeline",
    default_args=default_args,
    schedule_interval="*/30 * * * *",  # Every 30 minutes
    catchup=False,
    tags=["stocks", "data"]
)

# Configure logging
logger = logging.getLogger("airflow.task")

def run_backfill():
    """Run stock data backfill job and log output."""
    result = subprocess.run(
        ["docker", "exec", "stock_project-stock-fetcher", "python", "backfill_stock_data.py"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    
    logger.info("STDOUT: %s", result.stdout)
    logger.info("STDERR: %s", result.stderr)
    logger.info("Exit Code: %d", result.returncode)
    
    if result.returncode != 0:
        raise Exception(f"Backfill stock data failed with exit code {result.returncode}: {result.stderr}")


def run_update():
    """Run stock data update job and log output."""
    result = subprocess.run(
        ["docker", "exec", "stock_project-stock-fetcher", "python", "update_stock_data.py"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    
    logger.info("STDOUT: %s", result.stdout)
    logger.info("STDERR: %s", result.stderr)
    logger.info("Exit Code: %d", result.returncode)
    
    if result.returncode != 0:
        raise Exception(f"Update stock data failed with exit code {result.returncode}: {result.stderr}")


def run_index_manager():
    """Run index manager to add new symbols."""
    result = subprocess.run(
        ["docker", "exec", "stock_project-stock-fetcher", "python", "add_default_indexes.py"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    
    logger.info("STDOUT: %s", result.stdout)
    logger.info("STDERR: %s", result.stderr)
    logger.info("Exit Code: %d", result.returncode)
    
    if result.returncode != 0:
        raise Exception(f"Index manager failed with exit code {result.returncode}: {result.stderr}")


# Create DAG tasks
index_manager_task = PythonOperator(
    task_id="add_default_indexes",
    python_callable=run_index_manager,
    dag=dag
)

backfill_task = PythonOperator(
    task_id="backfill_stock_data",
    python_callable=run_backfill,
    dag=dag
)

update_task = PythonOperator(
    task_id="update_stock_data",
    python_callable=run_update,
    dag=dag
)

# Set dependencies between tasks
index_manager_task >> backfill_task >> update_task