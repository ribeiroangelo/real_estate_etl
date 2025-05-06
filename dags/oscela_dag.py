from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.crawler.osceola_crawler import OsceolaCrawler
import os
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def crawl_osceola():
    """Download Osceola Excel file."""
    crawler = OsceolaCrawler()
    file_path = crawler.crawl()
    return file_path

def run_etl(county, file_path):
    """Run PySpark ETL for the county."""
    cmd = [
        'spark-submit',
        os.path.join(os.path.dirname(__file__), '..', 'main.py'),
        '--county', county,
        '--file', file_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"ETL failed: {result.stderr}")

with DAG(
    'osceola_property_sales',
    default_args=default_args,
    description='ETL pipeline for Osceola County property sales',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 1),
    catchup=False,
) as dag:
    
    crawl_task = PythonOperator(
        task_id='crawl_osceola_data',
        python_callable=crawl_osceola,
    )

    etl_task = PythonOperator(
        task_id='run_osceola_etl',
        python_callable=run_etl,
        op_kwargs={'county': 'osceola', 'file_path': crawl_task.output},
    )

    crawl_task >> etl_task