from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
# pyrefly: ignore [missing-import]
from src.utils.notification import on_success_callback, on_failure_callback

default_args = {
    'owner': 'paulorugani',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': on_failure_callback
}

@dag(
    dag_id='ingestion_bronze_player_dag',
    default_args=default_args,
    start_date=datetime(2026, 9, 15),
    schedule=None,
    catchup=False,
    tags=['bronze', 'player', 'raw', 'ingestion'],
    on_success_callback=on_success_callback
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
    def ingestion(connection_vars):
        @task
        def run_get_file(conn_vars):
            from bot.player_extractor import run_ingestion
            run_ingestion(conn_vars)
        run_get_file(connection_vars)

    @task_group("bronze")
    def bronze(connection_vars):
        @task
        def run_bronze_rankings(conn_vars):
            from src.bronze import tb_atp_players
            tb_atp_players.run(conn_vars=conn_vars)
        run_bronze_rankings(connection_vars)

    run_ingestion = ingestion(connection_vars)
    run_bronze = bronze(connection_vars)

    run_ingestion >> run_bronze

dag = run_ingestion_bronze()