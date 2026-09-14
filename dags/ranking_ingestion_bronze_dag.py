import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)
    
from bot.ranking_extractor import run_ingestion
from src.utils.db_handler import DBHandler
from src.bronze import tb_atp_ranking

default_args = {
    'owner': 'paulorugani',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id='ranking_ingestion_bronze_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='0 4 * * 1', #every monday
    catchup=False,
    tags=['bronze', 'ranking', 'raw']
)
def ranking_ingestion_bronze():

    @task
    def task_get_file():
        run_ingestion()

    @task
    def task_setup_database():
        db_handler = DBHandler()
        db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS bronze")

    @task
    def task_run_bronze_rankings():
        tb_atp_ranking.run(init_run=False)

    get_file_task = task_get_file()
    setup_database_task = task_setup_database()
    bronze_task = task_run_bronze_rankings()

    get_file_task >> setup_database_task >> bronze_task

dag = ranking_ingestion_bronze()
