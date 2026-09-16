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
    dag_id='silver_gold_match_dag',
    default_args=default_args,
    start_date=datetime(2026, 9, 15),
    schedule=None, 
    catchup=False,
    tags=['silver', 'gold', 'match']
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
        def run_matches(conn_vars):
            from src.silver import tb_atp_matches
            tb_atp_matches.run(conn_vars)

        @task
        def run_player_match(conn_vars):
            from src.silver import tb_atp_player_match
            tb_atp_player_match.run(conn_vars)

        @task
        def run_players(conn_vars):
            from src.silver import tb_atp_players
            tb_atp_players.run(conn_vars)

        @task
        def run_tournaments(conn_vars):
            from src.silver import tb_atp_tournaments
            tb_atp_tournaments.run(conn_vars)

        task_run_matches = run_matches(connection_vars)
        task_run_player_match = run_player_match(connection_vars)
        task_run_players = run_players(connection_vars)
        task_run_tournaments = run_tournaments(connection_vars)

    @task_group(group_id='gold')
    def run_gold(connection_vars):    
        
        @task
        def setup_database():
            ...
            
        @task_group(group_id='dimension')
        def run_dimension(conn_vars):
            @task
            def run_dim_date(conn_vars):
                from src.gold.dimension import dim_date
                dim_date.run(conn_vars)
            
            @task
            def run_dim_entry(conn_vars):
                from src.gold.dimension import dim_entry
                dim_entry.run(conn_vars)
            
            @task
            def run_dim_players(conn_vars):
                from src.gold.dimension import dim_players
                dim_players.run(conn_vars)
            
            @task
            def run_dim_tournaments(conn_vars):
                from src.gold.dimension import dim_tournaments
                dim_tournaments.run(conn_vars)

            run_dim_date(conn_vars)
            run_dim_entry(conn_vars)
            run_dim_players(conn_vars)
            run_dim_tournaments(conn_vars)

        @task_group(group_id='fact')
        def run_fact(conn_vars):
            @task
            def run_fact_player_match_stats(conn_vars):
                from src.gold.fact import fact_player_match_stats
                fact_player_match_stats.run(conn_vars)
            
            @task
            def run_fact_player_season(conn_vars):
                from src.gold.fact import fact_player_season
                fact_player_season.run(conn_vars)
            
            @task
            def run_fact_player_tournament_stats(conn_vars):
                from src.gold.fact import fact_player_tournament_stats
                fact_player_tournament_stats.run(conn_vars)

            run_fact_player_match_stats(conn_vars)
            run_fact_player_season(conn_vars)
            run_fact_player_tournament_stats(conn_vars)

    
        @task_group(group_id='create_view')
        def run_create_view():
            @task
            def run_create_dimension_view():
                ...
            
            @task
            def run_create_fact_view():
                ...

            run_create_dimension_view()
            run_create_fact_view()

        task_setup_database = setup_database()
        task_run_dimension = run_dimension(connection_vars)
        task_run_fact = run_fact(connection_vars)
        task_run_create_view = run_create_view()
        task_setup_database >> task_run_dimension >> task_run_fact >> task_run_create_view

    task_run_silver = run_silver(connection_vars) 
    task_run_gold = run_gold(connection_vars)

    task_run_silver >> task_run_gold

dag = run_silver_gold()