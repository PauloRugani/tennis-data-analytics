import os
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

default_args = {
    'owner': 'paulorugani',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id='silver_gold_ranking_dag',
    default_args=default_args,
    start_date=datetime(2026, 9, 15),
    schedule=None, 
    catchup=False,
    tags=['silver', 'gold', 'ranking']
)
def run_silver_gold():
    @task
    def load_variables():
        from airflow.models import Variable
        return {
            "bucket_endpoint": Variable.get("BUCKET_ENDPOINT"),
            "bucket_access_key": Variable.get("BUCKET_ACCESS_KEY"),
            "bucket_secret_key": Variable.get("BUCKET_SECRET_KEY"),
            "bucket_name": Variable.get("TENNIS_BUCKET_NAME")
        }
    
    bucket_connection_vars = load_variables()

    @task_group(group_id='silver')
    def run_silver(bucket_connection_vars):
        @task
        def run_rankings(bcv):
            from src.silver import tb_atp_rankings
            tb_atp_rankings.run(bcv)

        run_rankings(bucket_connection_vars)

    @task_group(group_id='gold')
    def run_gold(bucket_connection_vars):    
        
        @task
        def setup_database():
            ...
            
        @task_group(group_id='fact')
        def run_fact(bucket_connection_vars):
            @task
            def run_fact_player_ranking(bcv):
                from src.gold.fact import fact_player_ranking
                fact_player_ranking.run(bcv)
            
            run_fact_player_ranking(bucket_connection_vars)

        @task_group(group_id='create_view')
        def run_create_view():
            @task
            def run_create_fact_view():
                ...

            run_create_fact_view()

        task_setup_database = setup_database()
        task_run_fact = run_fact(bucket_connection_vars)
        task_run_create_view = run_create_view()
        
        task_setup_database  >> task_run_fact >> task_run_create_view

    task_run_silver = run_silver(bucket_connection_vars) 
    task_run_gold = run_gold(bucket_connection_vars)

    task_run_silver >> task_run_gold

dag = run_silver_gold()