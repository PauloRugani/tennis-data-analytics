import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
# pyrefly: ignore [missing-import]
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)
    
from bot.match_extractor import run_ingestion
from src.bronze import tb_atp_matches as bronze_matches


default_args = {
    'owner': 'paulorugani',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id='ingestion_bronze_match_dag',
    default_args=default_args,
    start_date=datetime(2026, 9, 15),
    schedule='0 21 * * *', 
    catchup=False,
    tags=['bronze', 'match', 'raw', 'ingestion']
)
def run_ingestion_bronze():

    @task_group("ingestion")
    def run_ingestion():
        @task
        def task_get_file():
            run_ingestion()
        task_get_file()

    @task_group("bronze")
    def run_bronze():
        @task
        def task_run_bronze_matches():
            bronze_matches.run()
        task_run_bronze_matches()

    task_run_dag_silver_gold = TriggerDagRunOperator(
        task_id='run_silver_gold_match_dag',
        trigger_dag_id='silver_gold_match_dag',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    task_run_ingestion = run_ingestion()
    task_run_bronze = run_bronze()

    task_run_ingestion >> task_run_bronze >> task_run_dag_silver_gold

dag = run_ingestion_bronze()

