import os
import sys
import pandas as pd
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pyspark_handler import PySparkHandler

load_dotenv()
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def load_tables(handler):
    try:
        tb_atp_rankings = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/bronze/tb_atp_rankings/",
            format="parquet"
        )
        return tb_atp_rankings
    except Exception as e:
        print(e)
        raise

def run_transformation(handler, tb_atp_rankings):
    try:
        df = (
            tb_atp_rankings
            .select(
                f.date_format(f.to_date(f.col("date"), 'yyyyMMdd'), 'yyyy-MM-dd').alias("DATE_WEEK_RANKING"),
                f.col("rank").cast("int").alias("NUM_PLAYER_RANK"),
                f.col("name").alias("DES_PLAYER_NAME"),
                f.col("id").alias("COD_PLAYER_ID"),
                f.col("age").cast("int").alias("NUM_PLAYER_AGE"),
                f.regexp_replace(f.col("points"), ",", "").cast("int").alias("NUM_PLAYER_RANK_PTS"),
                f.col("lost_earned_points").cast("string").alias("NUM_PLAYER_LE_PTS"),
                f.col("tourn_played").cast("int").alias("NUM_PLAYER_TOURNEY_PLAYED"),
                f.regexp_replace(f.col("dropping"), ",", "").cast("int").alias("NUM_PLAYER_DROP_PTS"),
                f.col("next_best").cast("int").alias("NUM_PLAYER_NEXT_BEST"),

                f.col("DATE_INGESTION").alias("DATE_INGESTION")
            )
            .dropDuplicates(["DATE_WEEK_RANKING", "COD_PLAYER_ID"])
        )
        return df
    except Exception as e:
        print(e)
        raise

def save_table(handler, df):
    try:
        handler.save_data(
            df=df,
            path="s3a://tennis-data-lake/silver/tb_atp_rankings/",
            format="parquet",
            mode="overwrite"
        )
    except Exception as e:
        print(e)
        raise

def run():
    handler = None
    try:
        handler = PySparkHandler(app_name="tb_atp_ranking_silver")
        tb_atp_rankings = load_tables(handler)
        df_final = run_transformation(handler, tb_atp_rankings)
        save_table(handler, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run()
