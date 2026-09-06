import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)
    
from bot.ranking_extractor import run_airflow, AIRFLOW_TEMP_DIR
from src.utils.push_github import push_to_github
from src.utils.clean_airflow_tmp import clean_airflow_tmp
from src.utils.fetch_from_github import fetch_file_from_github
from src.bronze import tb_atp_ranking

def push():
    if not os.path.exists(AIRFLOW_TEMP_DIR):
        return

    for root, _, files in os.walk(AIRFLOW_TEMP_DIR):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, AIRFLOW_TEMP_DIR)
            repo_target_path = f"data/raw/{relative_path}"

            push_to_github(
                local_file_path=local_file_path,
                repo_file_path=repo_target_path,
                repo_name="PauloRugani/tennis-data-analytics",
                branch="main"
            )

def fetch():
    today = datetime.now()
    current_monday = today - timedelta(days=today.weekday())
    current_year = current_monday.year
    filename = f"tb_incremental_ranking_{current_year}.csv"
    repo_file_path = f"data/raw/incremental/{filename}"

    incremental_dir = os.path.join(AIRFLOW_TEMP_DIR, "incremental")
    os.makedirs(incremental_dir, exist_ok=True)

    csv_path = os.path.join(incremental_dir, filename)

    fetch_file_from_github("PauloRugani/tennis-data-analytics", repo_file_path, csv_path)


def run_ranking_bronze():
    tb_atp_ranking.run(init_run=False)

default_args = {
    'owner': 'paulorugani',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='ranking_ingestion_bronze_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='0 4 * * 1', #every monday
    catchup=False,
) as dag:

    task_fetch_ranking_file = PythonOperator(
        task_id='task_fetch_ranking_file',
        python_callable=fetch,
    )

    task_download = PythonOperator(
        task_id='task_download',
        python_callable=run_airflow,
    )

    task_upload_github = PythonOperator(
        task_id='task_upload_github',
        python_callable=push,
    )

    task_run_bronze_rankings = PythonOperator(
        task_id='task_run_bronze_rankings',
        python_callable=run_ranking_bronze,
    )

    task_cleanup = PythonOperator(
        task_id='task_cleanup',
        python_callable=clean_airflow_tmp,
        trigger_rule='all_done',
    )

    task_fetch_ranking_file >> task_download >> task_upload_github >> task_run_bronze_rankings >> task_cleanup