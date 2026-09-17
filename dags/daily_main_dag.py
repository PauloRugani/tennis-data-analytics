from datetime import datetime
from airflow.decorators import dag
# pyrefly: ignore [missing-import]
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from src.utils.notification import on_success_callback, on_failure_callback

default_args = {
    'owner': 'paulorugani',
}

@dag(
    dag_id='daily_main_dag',
    default_args=default_args,
    start_date=datetime(2026, 9, 15),
    schedule='0 21 * * *', 
    catchup=False,
    tags=['main', 'daily'],
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback
)

def run_daily_pipeline():
    run_dag_ingestion_bronze = TriggerDagRunOperator(
        task_id='run_dag_ingestion_bronze',
        trigger_dag_id='ingestion_bronze_match_dag',
        wait_for_completion=True,
        reset_dag_run=False,
    )

    run_dag_silver_gold = TriggerDagRunOperator(
        task_id='run_dag_silver_gold',
        trigger_dag_id='silver_gold_match_dag',
        wait_for_completion=True,
        reset_dag_run=False,
    )

    run_dag_ingestion_bronze >> run_dag_silver_gold

dag = run_daily_pipeline()