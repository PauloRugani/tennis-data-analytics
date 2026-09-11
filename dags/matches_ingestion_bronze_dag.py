import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)
    
from bot.match_extractor import run_ingestion, AIRFLOW_TEMP_DIR
from src.utils.github_handler import GithubHandler
from src.utils.clean_airflow_tmp import clean_airflow_tmp
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
    def task_download():
        run_ingestion()

    @task
    def task_upload_github():
        if not os.path.exists(AIRFLOW_TEMP_DIR):
            return

        github_handler = GithubHandler()

        for root, _, files in os.walk(AIRFLOW_TEMP_DIR):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, AIRFLOW_TEMP_DIR)
                repo_target_path = f"data/raw/{relative_path}"

                github_handler.push_file(
                    local_file_path=local_file_path,
                    repo_file_path=repo_target_path
                )

    @task
    def task_setup_database():
        db_handler = DBHandler()
        db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS bronze")

    @task
    def task_run_bronze_matches():
        bronze_matches.run(init_run=False)

    @task(trigger_rule='all_done')
    def task_clean_tmp():
        clean_airflow_tmp()

    download_task = task_download()
    upload_task = task_upload_github()
    setup_database_task = task_setup_database()
    bronze_task = task_run_bronze_matches()
    clean_tmp_task = task_clean_tmp()

    download_task >> upload_task >> setup_database_task >> bronze_task >> clean_tmp_task

dag = matches_ingestion_bronze()
