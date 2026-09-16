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
    @task
    def load_variables():
        from airflow.models import Variable
        return {
            "bucket_endpoint": Variable.get("BUCKET_ENDPOINT"),
            "bucket_access_key": Variable.get("BUCKET_ACCESS_KEY"),
            "bucket_secret_key": Variable.get("BUCKET_SECRET_KEY"),
            "bucket_name": Variable.get("TENNIS_BUCKET_NAME")
        }
    
    connection_vars = load_variables()

    @task_group("ingestion")
    def run_ingestion(connection_vars):
        @task
        def task_get_file(conn_vars):
            run_ingestion(conn_vars)
        task_get_file(connection_vars)

    @task_group("bronze")
    def run_bronze(connection_vars):
        @task
        def task_run_bronze_matches(conn_vars):
            bronze_matches.run(init_run=True, conn_vars=conn_vars)
        task_run_bronze_matches(connection_vars)

    task_run_dag_silver_gold = TriggerDagRunOperator(
        task_id='run_silver_gold_match_dag',
        trigger_dag_id='silver_gold_match_dag',
        wait_for_completion=True,
        reset_dag_run=False,
    )

    task_run_ingestion = run_ingestion(connection_vars)
    task_run_bronze = run_bronze(connection_vars)

    task_run_ingestion >> task_run_bronze >> task_run_dag_silver_gold

dag = run_ingestion_bronze()

