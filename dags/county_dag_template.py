from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from importlib import import_module
import os
import subprocess

def create_county_dag(county: str, crawler_class: str, schedule: str = '@daily'):
    """Create a DAG for a county."""
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    }

    def crawl_county():
        """Download county data."""
        module = import_module(f'src.crawler.{county}_crawler')
        crawler_class = getattr(module, crawler_class)
        crawler = crawler_class()
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
        f'{county}_property_sales',
        default_args=default_args,
        description=f'ETL pipeline for {county} County property sales',
        schedule_interval=schedule,
        start_date=datetime(2025, 5, 1),
        catchup=False,
    ) as dag:
        
        crawl_task = PythonOperator(
            task_id=f'crawl_{county}_data',
            python_callable=crawl_county,
        )

        etl_task = PythonOperator(
            task_id=f'run_{county}_etl',
            python_callable=run_etl,
            op_kwargs={'county': county, 'file_path': crawl_task.output},
        )

        crawl_task >> etl_task

    return dag

# Example usage for Miami-Dade
miami_dade_dag = create_county_dag(
    county='miami_dade',
    crawler_class='MiamiDadeCrawler',
    schedule='@weekly'
)