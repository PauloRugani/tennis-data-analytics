import os
import sys
import pandas as pd
from pyspark.sql import functions as f
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pyspark_handler import PySparkHandler

load_dotenv()
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def load_tables(handler, init_run=False):
    try:
        if not init_run:
            tb_atp_matches = handler.load_data(
                spark=handler.spark,
                path="s3a://tennis-data-lake/bronze/tb_atp_matches/",
                format="parquet"
            )
        else: 
            tb_atp_matches = None
            
        tb_ongoing_tourneys = handler.load_data(
            spark=handler.spark,
            path=r"s3a://tennis-data-lake/raw/incremental/tb_ongoing_tourneys.csv",
            format="csv",
            header="true"
        )

        historical_matches = "s3a://tennis-data-lake/raw/historical/matches/"
    except Exception as e:
        print(e)
        raise

    return tb_atp_matches, tb_ongoing_tourneys, historical_matches

def run_transformation(handler, init_run, tb_atp_matches, tb_ongoing_tourneys, historical_matches):
    try:
        print("Running transformations...")
        if not init_run:
            df = tb_atp_matches
        else:
            df = handler.load_data(
                spark=handler.spark,
                path=historical_matches,
                format="csv",
                header="true",
                inferSchema="false"
            )

        new_matches = (
            df.alias("tb_matches")
            .join(
                tb_ongoing_tourneys.alias("tb_ongoing"),
                [
                    f.col("tb_matches.tourney_date") == f.col("tb_ongoing.tourney_date"), 
                    f.col("tb_matches.winner_name") == f.col("tb_ongoing.winner_name"),
                    f.col("tb_matches.loser_name") == f.col("tb_ongoing.loser_name")
                ],
                'right'
                )
            .where(f.col("tb_matches.tourney_date").isNull())
            .select(
                "tb_ongoing.*",
            )
        )


        if not init_run:
            df_final = new_matches.withColumn("DATE_INGESTION", f.lit(f.current_date()))
        else:
            df_final = df.unionByName(new_matches, allowMissingColumns=True).withColumn("DATE_INGESTION", f.lit(f.current_date()))

        df_final = df_final.where(
                """
                    tourney_name not like '%Davis Cup%' and 
                    tourney_name not like '%Olymp%' and 
                    tourney_name not like '%Laver Cup%' and 
                    tourney_name not like '%Next Gen%' and
                    tourney_name not like '%Atp Cup%' and 
                    tourney_name not like '%United Cup%' and 
                    tourney_name not in ('Kingston', 'Dusseldorf', 'Nations Cup')
                """
                )
        print("Transformations completed")
        return df_final
    except Exception as e:
        print(e)
        raise

def save_table(handler, init_run, df_final):
    try:
        if not init_run:
            save_mode = "append"
        else:
            save_mode = "overwrite"
            
        if df_final.count() > 0:
            handler.save_data(
                df=df_final,
                path="s3a://tennis-data-lake/bronze/tb_atp_matches/",
                format="parquet",
                mode=save_mode
            )
    except Exception as e:
        print(e)    
        raise

def run(init_run: bool):
    handler = None
    try:
        handler = PySparkHandler(app_name="tb_atp_matches_bronze")
        tb_atp_matches, tb_ongoing_tourneys, historical_matches = load_tables(handler, init_run)
        df_final = run_transformation(handler, init_run, tb_atp_matches, tb_ongoing_tourneys, historical_matches)
        save_table(handler, init_run, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run(init_run=True)