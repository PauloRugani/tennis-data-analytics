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
        tb_atp_rankings = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/silver/tb_atp_rankings/",
            format="parquet"
        )
        tb_date = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/gold/dimension/dim_date/",
            format="parquet"
        )
        tb_players = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/gold/dimension/dim_players/",
            format="parquet"
        )
        return tb_atp_rankings, tb_date, tb_players
    except Exception as e:
        print(e)
        raise

def run_transformation(handler, tb_atp_rankings, tb_date, tb_players):
    try:
        df = (
            tb_atp_rankings.alias("r")
            .join(
                tb_date.alias("d"),
                f.col("r.DATE_WEEK_RANKING") == f.col("d.DATE"),
                'inner'
            )
            .join(
                tb_players.alias("p"),
                f.initcap(f.trim(f.regexp_replace(f.col("r.DES_PLAYER_NAME"), "-", " "))) == f.col("p.DES_PLAYER_NAME"),
                'inner'
            )
            .select(
                "d.SK_DATE",
                "p.SK_PLAYER",
                f.col("r.NUM_PLAYER_RANK").cast("int").alias("NUM_PLAYER_RANK"),
                f.col("r.NUM_PLAYER_RANK_PTS").cast('int').alias("NUM_PLAYER_RANK_PTS"),
                "r.NUM_PLAYER_LE_PTS",
                "r.NUM_PLAYER_DROP_PTS",
                "r.NUM_PLAYER_NEXT_BEST",
                f.lit(f.current_date()).alias("DATE_LOAD")
            )
            .dropDuplicates(["SK_PLAYER", "SK_DATE"])
        )
        return df
    except Exception as e:
        print(e)
        raise

def save_table(handler, df):
    try:
        handler.save_data(
            df=df,
            path="s3a://tennis-data-lake/gold/fact/fact_player_ranking/",
            format="parquet",
            mode="overwrite"
        )

        (
            df.write
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "gold.fact_player_ranking")
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
        handler = PySparkHandler(app_name="fact_player_ranking")
        tb_atp_rankings, tb_date, tb_players = load_tables(handler)
        df_final = run_transformation(handler, tb_atp_rankings, tb_date, tb_players)
        save_table(handler, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run()
