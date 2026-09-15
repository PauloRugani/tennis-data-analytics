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

    @task_group(group_id='silver')
    def run_silver():
        @task
        def run_matches():
            from src.silver import tb_atp_matches
            tb_atp_matches.run()

        @task
        def run_player_match():
            from src.silver import tb_atp_player_match
            tb_atp_player_match.run()

        @task
        def run_players():
            from src.silver import tb_atp_players
            tb_atp_players.run()

        @task
        def run_tournaments():
            from src.silver import tb_atp_tournaments
            tb_atp_tournaments.run()

        task_run_matches = run_matches()
        task_run_player_match = run_player_match()
        task_run_players = run_players()
        task_run_tournaments = run_tournaments()

    @task_group(group_id='gold')
    def run_gold():    
        
        @task
        def setup_database():
            from src.utils.db_handler import DBHandler
            db_handler = DBHandler()
            db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS gold;")

        @task_group(group_id='dimension')
        def run_dimension():
            @task
            def run_dim_date():
                from src.gold.dimension import dim_date
                dim_date.run()
            
            @task
            def run_dim_entry():
                from src.gold.dimension import dim_entry
                dim_entry.run()
            
            @task
            def run_dim_players():
                from src.gold.dimension import dim_players
                dim_players.run()
            
            @task
            def run_dim_tournaments():
                from src.gold.dimension import dim_tournaments
                dim_tournaments.run()

            task_run_dim_date = run_dim_date()
            task_run_dim_entry = run_dim_entry()
            task_run_dim_players = run_dim_players()
            task_run_dim_tournaments = run_dim_tournaments()

        @task_group(group_id='fact')
        def run_fact():
            @task
            def run_fact_player_match_stats():
                from src.gold.fact import fact_player_match_stats
                fact_player_match_stats.run()
            
            @task
            def run_fact_player_season():
                from src.gold.fact import fact_player_season
                fact_player_season.run()
            
            @task
            def run_fact_player_tournament_stats():
                from src.gold.fact import fact_player_tournament_stats
                fact_player_tournament_stats.run()

            task_run_fact_player_match = run_fact_player_match_stats()
            task_run_fact_player_season = run_fact_player_season()
            task_run_fact_player_tournament = run_fact_player_tournament_stats()

    
        @task_group(group_id='create_view')
        def run_create_view():
            from src.utils.db_handler import DBHandler
            db_handler = DBHandler()

            @task
            def run_create_dimension_view():
                tables = ["dim_date", "dim_entry", "dim_player", "dim_tournament"]
                for table in tables:
                    print(f"Creating {table} view...")
                    db_handler.execute_query(f"""
                        CREATE OR REPLACE VIEW vw_{table} AS 
                        SELECT * FROM gold.{table};
                    """)
                    print(f"{table} view created")
            
            @task
            def run_create_fact_view():
                tables = ["fact_player_match_stats", "fact_player_season", "fact_player_tournament_stats"]
                for table in tables:
                    print(f"Creating {table} view...")
                    db_handler.execute_query(f"""
                        CREATE OR REPLACE VIEW vw_{table} AS 
                        SELECT * FROM gold.{table};
                    """)
                    print(f"{table} view created")

            run_create_dimension_view()
            run_create_fact_view()

        task_setup_database = setup_database()
        task_run_dimension = run_dimension()
        task_run_fact = run_fact()
        task_run_create_view = run_create_view()
        task_setup_database >> task_run_dimension >> task_run_fact >> task_run_create_view

    task_run_silver = run_silver() 
    task_run_gold = run_gold()

    task_run_silver >> task_run_gold

dag = run_silver_gold()