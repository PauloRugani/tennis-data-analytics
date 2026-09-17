import os
import sys
from pyspark.sql import functions as f
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils.pyspark_handler import PySparkHandler

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def load_tables(handler, bucket_name):
    try:
        tb_player_match = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/silver/tb_atp_player_match/",
            format="parquet"
        )
        tb_tournaments = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/gold/dimension/dim_tournaments/",
            format="parquet"
        )
        tb_date = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/gold/dimension/dim_date/",
            format="parquet"
        )
        tb_players = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/gold/dimension/dim_players/",
            format="parquet"
        )
        return tb_player_match, tb_tournaments, tb_date, tb_players
    except Exception as e:
        print(e)
        raise

def run_transformation(handler, tb_player_match, tb_tournaments, tb_date, tb_players):
    try:
        df = (
            tb_player_match.alias("p_m")
            .join(tb_tournaments.alias("t"), f.expr("p_m.COD_TOURNEY_ID <=> t.COD_TOURNEY_ID"), 'left')
            .join(
                tb_players.alias("pw"),
                (f.col("p_m.COD_PLAYER_ID").cast("string") == f.col("pw.COD_PLAYER_ID").cast("string"))
                | (
                    f.col("p_m.COD_PLAYER_ID").cast("string")
                    == f.col("pw.COD_PLAYER_ID_OLD").cast("string")
                ),
                "left",
            )
            .join(
                tb_players.alias("po"),
                (f.col("p_m.COD_PLAYER_OPPONENT_ID").cast("string") == f.col("po.COD_PLAYER_ID").cast("string"))
                | (
                    f.col("p_m.COD_PLAYER_OPPONENT_ID").cast("string")
                    == f.col("po.COD_PLAYER_ID_OLD").cast("string")
                ),
                "left",
            )
            .join(
                tb_date.alias("d"),
                f.col("p_m.DATE_MATCH") == f.col("d.DATE")
            )
            .select(
                f.col("p_m.COD_MATCH_ID"),
                f.col("pw.SK_PLAYER"),
                f.col("po.SK_PLAYER").alias("SK_PLAYER_OPPONENT"),
                f.col("t.SK_TOURNEY"),
                f.col("d.SK_DATE"),
                
                f.col("p_m.FLAG_PLAYER_IS_WINNER").alias("FLAG_PLAYER_IS_WINNER"),
                f.coalesce(f.col("p_m.NUM_MATCH"), f.lit(0)).alias("NUM_MATCH"),
                f.coalesce(f.col("p_m.DES_MATCH_SCORE"), f.lit('-')).alias("DES_MATCH_SCORE"),
                f.coalesce(f.col("p_m.NUM_MATCH_BEST_OF"), f.lit(3)).alias("NUM_MATCH_BEST_OF"),
                f.coalesce(f.col("p_m.DES_MATCH_ROUND"), f.lit('-')).alias("DES_MATCH_ROUND"), 
                f.coalesce(f.col("p_m.NUM_MATCH_DURATION_M"), f.lit(0)).alias("NUM_MATCH_DURATION_M"),
                f.coalesce(f.col("p_m.NUM_PLAYER_ACES"), f.lit(0)).alias("NUM_PLAYER_ACES"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_DB_FAULTS"), f.lit(0)).alias("NUM_PLAYER_DB_FAULTS"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_SERVE_PTS"), f.lit(0)).alias("NUM_PLAYER_SERVE_PTS"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_1ST_SERVES_IN"), f.lit(0)).alias("NUM_PLAYER_1ST_SERVES_IN"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_1ST_SERVE_PTS_WON"), f.lit(0)).alias("NUM_PLAYER_1ST_SERVE_PTS_WON"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_2ND_SERVE_PTS_WON"), f.lit(0)).alias("NUM_PLAYER_2ND_SERVE_PTS_WON"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_SERVE_GAMES"), f.lit(0)).alias("NUM_PLAYER_SERVE_GAMES"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_BP_SAVED"), f.lit(0)).alias("NUM_PLAYER_BP_SAVED"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_BP_FACED"), f.lit(0)).alias("NUM_PLAYER_BP_FACED"),

                f.lit(f.current_date()).alias("DATE_LOAD")
            )
            .distinct()
        )

        return df
    except Exception as e:
        print(e)
        raise

def save_table(handler, df, bucket_name, jdbc_url, jdbc_user, jdbc_password):
    try:
        handler.save_data(
            df=df,
            path=f"s3a://{bucket_name}/gold/fact/fact_player_match_stats/",
            format="parquet",
            mode="overwrite"
        )
        
        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "gold.fact_player_match_stats")
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
    except Exception as e:
        print(e)
        raise

def run(conn_vars: dict = None):
    handler = None
    try:
        handler = PySparkHandler(
            app_name="fact_player_match_stats",
            bucket_endpoint=conn_vars.get("bucket_endpoint"),
            bucket_access_key=conn_vars.get("bucket_access_key"),
            bucket_secret_key=conn_vars.get("bucket_secret_key")
        )
        bucket_name = conn_vars.get("bucket_name")
        jdbc_url = conn_vars.get("jdbc_url")
        jdbc_user = conn_vars.get("jdbc_user")
        jdbc_password = conn_vars.get("jdbc_password")
        
        tb_player_match, tb_tournaments, tb_date, tb_players = load_tables(handler, bucket_name)
        df_final = run_transformation(handler, tb_player_match, tb_tournaments, tb_date, tb_players)
        save_table(handler, df_final, bucket_name, jdbc_url, jdbc_user, jdbc_password)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run()
