import os
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
# pyrefly: ignore [missing-import]
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
            "bucket_name": Variable.get("TENNIS_BUCKET_NAME"),

            "jdbc_url": Variable.get("JDBC_URL"),
            "jdbc_user": Variable.get("JDBC_USER"),
            "jdbc_password": Variable.get("JDBC_PASSWORD")
        }
    
    connection_vars = load_variables()

    @task_group(group_id='silver')
    def run_silver(connection_vars):
        @task
        def run_rankings(conn_vars):
            from src.silver import tb_atp_rankings
            tb_atp_rankings.run(conn_vars)

        run_rankings(connection_vars)

    @task_group(group_id='gold')
    def run_gold(connection_vars):    
        
        @task
        def setup_database():
            hook = PostgresHook(
                postgres_conn_id="POSTGRES_CONNECTION"
            )
            hook.run("CREATE SCHEMA IF NOT EXISTS gold;")

        @task_group(group_id='fact')
        def run_fact(connection_vars):
            @task
            def run_fact_player_ranking(conn_vars):
                from src.gold.fact import fact_player_ranking
                fact_player_ranking.run(conn_vars)
            
            run_fact_player_ranking(connection_vars)

        @task_group(group_id='create_view')
        def run_create_view():
            @task
            def run_create_fact_view():
                hook = PostgresHook(
                    postgres_conn_id="POSTGRES_CONNECTION"
                )
                hook.run("""
                            CREATE OR REPLACE VIEW gold.vw_fact_player_ranking AS 
                            SELECT * FROM gold.fact_player_ranking;
                        """
                        )

            run_create_fact_view()

        task_setup_database = setup_database()
        task_run_fact = run_fact(connection_vars)
        task_run_create_view = run_create_view()
        
        task_setup_database  >> task_run_fact >> task_run_create_view

    task_run_silver = run_silver(connection_vars) 
    task_run_gold = run_gold(connection_vars)

    task_run_silver >> task_run_gold

dag = run_silver_gold()