import os
import sys
import pandas as pd
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils.pyspark_handler import PySparkHandler

load_dotenv()
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def load_tables(handler):
    try:
        tb_player_match = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/silver/tb_atp_player_match/",
            format="parquet"
        )
        tb_tournaments = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/gold/dimension/dim_tournaments/",
            format="parquet"
        )
        tb_players = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/gold/dimension/dim_players/",
            format="parquet"
        )
        return tb_player_match, tb_tournaments, tb_players
    except Exception as e:
        print(e)
        raise

def run_transformation(handler, tb_player_match, tb_tournaments, tb_players):
    try:
        round_order = (
            f.when(f.col("DES_MATCH_ROUND") == "F", 11) # Final
             .when(f.col("DES_MATCH_ROUND") == "SF", 10) # Semi Final
             .when(f.col("DES_MATCH_ROUND") == "BR", 9) # Third Place
             .when(f.col("DES_MATCH_ROUND") == "QF", 8) # Quarte final
             .when(f.col("DES_MATCH_ROUND") == "R16", 7) # Round of 16
             .when(f.col("DES_MATCH_ROUND") == "R32", 6) # Round of 32
             .when(f.col("DES_MATCH_ROUND") == "R64", 5) # Round of 64
             .when(f.col("DES_MATCH_ROUND") == "R128", 4) # Round of 128
             .when(f.col("DES_MATCH_ROUND") == "ER", 3) # Early Round
             .when(f.col("DES_MATCH_ROUND") == "RR", 2) # Round Robin
             .otherwise(1)
        )

        df = (
            tb_player_match.alias("p_m")
            .join(tb_tournaments.alias("t"), "COD_TOURNEY_ID", 'left')
            .join(
                tb_players.alias("p"),
                (f.col("p_m.COD_PLAYER_ID").cast("string") == f.col("p.COD_PLAYER_ID").cast("string"))
                | (
                    f.col("p_m.COD_PLAYER_ID").cast("string")
                    == f.col("p.COD_PLAYER_ID_OLD").cast("string")
                ),
                "left",
            )
            .groupBy(f.col("SK_PLAYER"), f.concat(f.substring(f.col("DATE_MATCH"), 1, 4), f.lit('0101')).alias("SK_DATE"))
            .agg(
                f.count("COD_MATCH_ID").alias("NUM_TOTAL_MATCHES"),
                f.countDistinct("COD_TOURNEY_ID").alias("NUM_TOTAL_TOURNAMENTS"),
                f.sum(f.when(f.col("FLAG_PLAYER_IS_WINNER") == True, f.lit(1)).otherwise(f.lit(0))).alias("NUM_TOTAL_WINS"),
                f.sum(f.when(f.col("FLAG_PLAYER_IS_WINNER") == False, f.lit(1)).otherwise(f.lit(0))).alias("NUM_TOTAL_LOSSES"),
                f.sum(
                    f.when(
                        (f.col("FLAG_PLAYER_IS_WINNER") == True) & (f.col("DES_MATCH_ROUND") == 'F'), f.lit(1)
                    ).otherwise(0)
                ).alias("NUM_TOTAL_TITLES"),
                f.coalesce(f.max_by(
                    f.when(f.col("DES_TOURNEY_NAME") == "Australian Open", f.col("DES_MATCH_ROUND")), 
                    f.when(f.col("DES_TOURNEY_NAME") == "Australian Open", round_order)
                ), f.lit('-')).alias("DES_AUS_OPEN_RESULT"),
                f.coalesce(f.max_by(
                    f.when(f.col("DES_TOURNEY_NAME") == "Roland Garros", f.col("DES_MATCH_ROUND")), 
                    f.when(f.col("DES_TOURNEY_NAME") == "Roland Garros", round_order)
                ), f.lit('-')).alias("DES_ROLAND_GARROS_RESULT"),
                f.coalesce(f.max_by(
                    f.when(f.col("DES_TOURNEY_NAME") == "Wimbledon", f.col("DES_MATCH_ROUND")), 
                    f.when(f.col("DES_TOURNEY_NAME") == "Wimbledon", round_order)
                ), f.lit('-')).alias("DES_WIMBLEDON_RESULT"),
                f.coalesce(f.max_by(
                    f.when(f.col("DES_TOURNEY_NAME") == "US Open", f.col("DES_MATCH_ROUND")), 
                    f.when(f.col("DES_TOURNEY_NAME") == "US Open", round_order)
                ), f.lit('-')).alias("DES_US_OPEN_RESULT"),
                f.min_by(f.col("NUM_PLAYER_RANK"), f.col("DATE_MATCH")).cast('int').alias("NUM_PLAYER_START_RANK"),
                f.max_by(f.col("NUM_PLAYER_RANK"), f.col("DATE_MATCH")).cast('int').alias("NUM_PLAYER_FINAL_RANK"),
                f.min(f.col("NUM_PLAYER_RANK").cast('int')).alias("NUM_PLAYER_BEST_RANK"),
                f.max(f.col("NUM_PLAYER_RANK").cast('int')).alias("NUM_PLAYER_WORST_RANK"),
                f.min_by(f.col("NUM_PLAYER_RANK_PTS"), f.col("DATE_MATCH")).cast('int').alias("NUM_PLAYER_START_RANK_PTS"),
                f.max_by(f.col("NUM_PLAYER_RANK_PTS"), f.col("DATE_MATCH")).cast('int').alias("NUM_PLAYER_FINAL_RANK_PTS"),
                f.max(f.col("NUM_PLAYER_RANK_PTS").cast('int')).alias("NUM_PLAYER_BEST_RANK_PTS"),
                f.min(f.col("NUM_PLAYER_RANK_PTS").cast('int')).alias("NUM_PLAYER_WORST_RANK_PTS"),
                f.lit(f.current_date()).alias("DATE_LOAD")
            )
            .orderBy("SK_DATE")
        )
        return df
    except Exception as e:
        print(e)
        raise

def save_table(handler, df):
    try:
        handler.save_data(
            df=df,
            path="s3a://tennis-data-lake/gold/fact/fact_player_season/",
            format="parquet",
            mode="overwrite"
        )
        
        (
            df.write
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "gold.fact_player_season")
            .option("user", os.getenv("DB_USER"))
            .option("password", os.getenv("DB_PASSWORD"))
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
    except Exception as e:
        print(e)
        raise

def run():
    handler = None
    try:
        handler = PySparkHandler(app_name="fact_player_season")
        tb_player_match, tb_tournaments, tb_players = load_tables(handler)
        df_final = run_transformation(handler, tb_player_match, tb_tournaments, tb_players)
        save_table(handler, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run()
