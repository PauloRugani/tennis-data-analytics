import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)
    
from bot.ranking_extractor import run_ingestion, AIRFLOW_TEMP_DIR
from src.utils.github_handler import GithubHandler
from src.utils.clean_airflow_tmp import clean_airflow_tmp
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
    def task_fetch_ranking_file():
        today = datetime.now()
        current_monday = today - timedelta(days=today.weekday())
        current_year = current_monday.year
        filename = f"tb_incremental_ranking_{current_year}.csv"
        repo_file_path = f"data/raw/incremental/{filename}"

        incremental_dir = os.path.join(AIRFLOW_TEMP_DIR, "incremental")
        os.makedirs(incremental_dir, exist_ok=True)

        csv_path = os.path.join(incremental_dir, filename)

        github_handler = GithubHandler()
        github_handler.fetch_file(repo_file_path, csv_path)

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
    def task_run_bronze_rankings():
        tb_atp_ranking.run(init_run=False)

    @task(trigger_rule='all_done')
    def task_clean_tmp():
        clean_airflow_tmp()

    fetch_task = task_fetch_ranking_file()
    download_task = task_download()
    upload_task = task_upload_github()
    setup_database_task = task_setup_database()
    bronze_task = task_run_bronze_rankings()
    clean_tmp_task = task_clean_tmp()

    fetch_task >> download_task >> upload_task >> setup_database_task >> bronze_task >> clean_tmp_task

dag = ranking_ingestion_bronze()
