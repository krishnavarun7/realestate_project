from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the include directory to sys.path
sys.path.insert(0, os.path.abspath('/usr/local/airflow/include'))

# Import main function
from main import main  # Ensure main() exists in main.py

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('run_main_script_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    task = PythonOperator(
        task_id='run_main_script',
        python_callable=main  # Run the main function from main.py
    )
