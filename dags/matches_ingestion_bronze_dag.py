import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)
    
from bot.match_extractor import run_ingestion
from src.utils.db_handler import DBHandler
from src.bronze import tb_atp_matches as bronze_matches


default_args = {
    'owner': 'paulorugani',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id='matches_ingestion_bronze_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='0 21 * * *', 
    catchup=False,
    tags=['bronze', 'matches', 'raw']
)
def matches_ingestion_bronze():

    @task
    def task_get_file():
        run_ingestion()

    @task
    def task_setup_database():
        db_handler = DBHandler()
        db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS bronze")

    @task
    def task_run_bronze_matches():
        bronze_matches.run(init_run=False)

    download_upload_task = task_get_file()
    setup_database_task = task_setup_database()
    bronze_task = task_run_bronze_matches()

    download_upload_task >> setup_database_task >> bronze_task

dag = matches_ingestion_bronze()

