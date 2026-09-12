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
        tb_date = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/gold/dimension/dim_date/",
            format="parquet"
        )
        tb_entry = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/gold/dimension/dim_entry/",
            format="parquet"
        )
        tb_players = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/gold/dimension/dim_players/",
            format="parquet"
        )
        return tb_player_match, tb_tournaments, tb_date, tb_entry, tb_players
    except Exception as e:
        print(e)
        raise

def run_transformation(handler, tb_player_match, tb_tournaments, tb_date, tb_entry, tb_players):
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

        tourney_stats = (
            tb_player_match
            .groupBy("COD_TOURNEY_ID", "COD_PLAYER_ID")
            .agg(
                f.first("COD_PLAYER_ENTRY").alias("COD_PLAYER_ENTRY"),
                f.coalesce(f.first("NUM_PLAYER_SEED").cast('int'), f.lit(-1)).alias("NUM_PLAYER_SEED"),
                f.coalesce(f.first("NUM_PLAYER_RANK_PTS").cast('int'), f.lit(0)).alias("NUM_PLAYER_RANK_PTS"),
                f.coalesce(f.first("NUM_PLAYER_RANK").cast('int'), f.lit(0)).alias("NUM_PLAYER_RANK"),
                f.max(
                    f.when((f.col("DES_MATCH_ROUND") == "F") & (f.col("FLAG_PLAYER_IS_WINNER") == True), 1).otherwise(0)
                ).alias("FLAG_IS_CHAMPION"),
                f.max_by(f.col("DES_MATCH_ROUND"), round_order).alias("DES_LAST_ROUND_PLAYED"),
                f.count("COD_MATCH_ID").alias("NUM_TOTAL_MATCHES"),
                f.coalesce(f.sum(f.col("NUM_MATCH_DURATION_M").cast("int")), f.lit(0)).alias("NUM_TOTAL_MIN_IN_GAME"),
                f.coalesce(f.max(f.col("NUM_MATCH_DURATION_M").cast("int")), f.lit(0)).alias("NUM_LONGEST_MATCH"),
                f.coalesce(f.sum(f.col("NUM_PLAYER_ACES").cast('int')), f.lit(0)).alias("NUM_TOTAL_ACES"), 
                f.coalesce(f.sum(f.col("NUM_PLAYER_DB_FAULTS").cast('int')), f.lit(0)).alias("NUM_TOTAL_DB_FAULTS"), 
                f.coalesce(f.sum(f.col("NUM_PLAYER_SERVE_PTS").cast('int')), f.lit(0)).alias("NUM_TOTAL_SERVE_PTS"), 
                f.coalesce(f.sum(f.col("NUM_PLAYER_1ST_SERVES_IN").cast('int')), f.lit(0)).alias("NUM_TOTAL_1ST_SERVES_IN"), 
                f.coalesce(f.sum(f.col("NUM_PLAYER_1ST_SERVE_PTS_WON").cast('int')), f.lit(0)).alias("NUM_TOTAL_1ST_SERVE_PTS_WON"), 
                f.coalesce(f.sum(f.col("NUM_PLAYER_2ND_SERVE_PTS_WON").cast('int')), f.lit(0)).alias("NUM_TOTAL_2ND_SERVE_PTS_WON"), 
                f.coalesce(f.sum(f.col("NUM_PLAYER_SERVE_GAMES").cast('int')), f.lit(0)).alias("NUM_TOTAL_SERVE_GAMES"), 
                f.coalesce(f.sum(f.col("NUM_PLAYER_BP_SAVED").cast('int')), f.lit(0)).alias("NUM_TOTAL_BP_SAVED"), 
                f.coalesce(f.sum(f.col("NUM_PLAYER_BP_FACED").cast('int')), f.lit(0)).alias("NUM_TOTAL_BP_FACED"),
                f.first("DATE_INGESTION").alias("DATE_INGESTION")
            )
        )

        df = (
            tourney_stats.alias("s")
            .join(tb_tournaments.alias("t"), f.expr("s.COD_TOURNEY_ID <=> t.COD_TOURNEY_ID"), 'left')
            .join(
                tb_players.alias("p"),
                (f.col("s.COD_PLAYER_ID").cast("string") == f.col("p.COD_PLAYER_ID").cast("string"))
                | (
                    f.col("s.COD_PLAYER_ID").cast("string")
                    == f.col("p.COD_PLAYER_ID_OLD").cast("string")
                ),
                "left",
            )
            .join(tb_entry.alias("e"), f.expr("s.COD_PLAYER_ENTRY <=> e.COD_PLAYER_ENTRY"), 'left')
            .withColumn("SK_DATE", f.concat(f.substring(f.col("s.COD_TOURNEY_ID"), 1, 4), f.lit('0101')))
            .select(
                f.col("p.SK_PLAYER"),
                f.col("t.SK_TOURNEY"),
                f.col("e.SK_ENTRY_TYPE"),
                f.col("SK_DATE"),
                f.col("s.NUM_PLAYER_SEED"),
                f.col("s.NUM_PLAYER_RANK_PTS"),
                f.col("s.NUM_PLAYER_RANK"),
                f.col("s.FLAG_IS_CHAMPION"),
                f.col("s.DES_LAST_ROUND_PLAYED"),
                f.col("s.NUM_TOTAL_MATCHES"),
                f.col("s.NUM_TOTAL_MIN_IN_GAME"),
                f.col("s.NUM_LONGEST_MATCH"),
                f.col("s.NUM_TOTAL_ACES"), 
                f.col("s.NUM_TOTAL_DB_FAULTS"), 
                f.col("s.NUM_TOTAL_SERVE_PTS"), 
                f.col("s.NUM_TOTAL_1ST_SERVES_IN"), 
                f.col("s.NUM_TOTAL_1ST_SERVE_PTS_WON"), 
                f.col("s.NUM_TOTAL_2ND_SERVE_PTS_WON"), 
                f.col("s.NUM_TOTAL_SERVE_GAMES"), 
                f.col("s.NUM_TOTAL_BP_SAVED"), 
                f.col("s.NUM_TOTAL_BP_FACED"),
                f.lit(f.current_date()).alias("DATE_LOAD")
            )
            .distinct()
        )
        return df
    except Exception as e:
        print(e)
        raise

def save_table(handler, df):
    try:
        handler.save_data(
            df=df,
            path="s3a://tennis-data-lake/gold/fact/fact_player_tournament_stats/",
            format="parquet",
            mode="overwrite"
        )
        
        (
            df.write
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "gold.fact_player_tournament_stats")
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
        handler = PySparkHandler(app_name="fact_player_tournament")
        tb_player_match, tb_tournaments, tb_date, tb_entry, tb_players = load_tables(handler)
        df_final = run_transformation(handler, tb_player_match, tb_tournaments, tb_date, tb_entry, tb_players)
        save_table(handler, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run()
