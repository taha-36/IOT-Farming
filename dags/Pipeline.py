from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer
from datetime import datetime, timedelta
import subprocess
import time
import json
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Define DAG
with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 11, 4),
    schedule_interval=None,
    catchup=False
) as dag:

    def run_data_generator():
        """Run DataGenerator.py for exactly 1 minute then terminate"""
        script_path = os.path.join(os.path.dirname(__file__), 'DataGenerator.py')
        script_dir = os.path.dirname(__file__)
        result = subprocess.run(
            ["python", script_path],
            cwd=script_dir,
            capture_output=True,
            text=True
        )
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        result.check_returncode()

    def run_extract():
        """Run ExtractData.py until it finishes execution"""
        script_path = os.path.join(os.path.dirname(__file__), 'ExtractData.py')
        script_dir = os.path.dirname(__file__)
        result = subprocess.run(
            ["python", script_path],
            cwd=script_dir,
            capture_output=True,
            text=True
        )
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        result.check_returncode()

    def run_transform():
        """Run TransformData.py until it finishes execution"""
        script_path = os.path.join(os.path.dirname(__file__), 'TransformData.py')
        script_dir = os.path.dirname(__file__)
        result = subprocess.run(
            ["python", script_path],
            cwd=script_dir,
            capture_output=True,
            text=True
        )
        print("Data Transformation Completed Successfully")

    def run_load():
        """Run LoadData.py until it finishes execution"""
        script_path = os.path.join(os.path.dirname(__file__), 'LoadData.py')
        script_dir = os.path.dirname(__file__)
        result = subprocess.run(
            ["python", script_path],
            cwd=script_dir,
            capture_output=True,
            text=True
        )
        print("Data Loaded Successfully")

    # Define tasks
    task_generate = PythonOperator(
        task_id='generate_data',
        python_callable=run_data_generator
    )

    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=run_extract
    )


    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=run_transform
    )

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=run_load
    )

    task_generate >> task_extract >> task_transform >> task_load