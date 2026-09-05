import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bot.match_extractor import run_airflow, AIRFLOW_TEMP_DIR
from src.utils.push_github import push_to_github
from src.utils.clean_airflow_tmp import clean_airflow_tmp

def push():
    if not os.path.exists(AIRFLOW_TEMP_DIR):
        print("Nenhum arquivo no staging para upload.")
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

default_args = {
    'owner': 'paulorugani',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='tennis_data_ingestion',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task_download = PythonOperator(
        task_id='extract_matches',
        python_callable=run_airflow,
    )

    task_upload_github = PythonOperator(
        task_id='upload_matches_to_github',
        python_callable=push,
    )

    task_cleanup = PythonOperator(
        task_id='cleanup_tmp_files',
        python_callable=clean_airflow_tmp,
        trigger_rule='all_done',
    )

    task_download >> task_upload_github >> task_cleanup