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

    @task_group(group_id='silver')
    def run_silver():
        @task
        def run_rankings():
            from src.silver import tb_atp_rankings
            tb_atp_rankings.run()

        task_run_rankings = run_rankings()

    @task_group(group_id='gold')
    def run_gold():    
        
        @task
        def setup_database():
            from src.utils.db_handler import DBHandler
            db_handler = DBHandler()
            db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS gold;")

        @task_group(group_id='fact')
        def run_fact():
            @task
            def run_fact_player_ranking():
                from src.gold.fact import fact_player_ranking
                fact_player_ranking.run()
            
            task_run_fact_player_ranking = run_fact_player_ranking()

        @task_group(group_id='create_view')
        def run_create_view():
            from src.utils.db_handler import DBHandler
            db_handler = DBHandler()
            
            @task
            def run_create_fact_view():
                tables = ["fact_player_ranking"]
                for table in tables:
                    print(f"Creating {table} view...")
                    db_handler.execute_query(f"""
                        CREATE OR REPLACE VIEW vw_{table} AS 
                        SELECT * FROM gold.{table};
                    """)
                    print(f"{table} view created")

            run_create_fact_view()

        task_setup_database = setup_database()
        task_run_fact = run_fact()
        task_run_create_view = run_create_view()
        
        task_setup_database  >> task_run_fact >> task_run_create_view

    task_run_silver = run_silver() 
    task_run_gold = run_gold()

    task_run_silver >> task_run_gold

dag = run_silver_gold()