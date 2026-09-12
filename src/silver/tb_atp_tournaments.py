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
        tb_atp_matches = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/bronze/tb_atp_matches/",
            format="parquet"
        )
        return tb_atp_matches
    except Exception as e:
        print(e)
        raise

def run_transformation(handler, tb_atp_matches):
    try:
        df = (
            tb_atp_matches
            .withColumn(
                "TOURNEY_NAME",
                f.when(f.col("tourney_name").contains("Davis Cup"), f.lit("Davis Cup"))
                .when(f.col("tourney_id") == "2026-416", f.lit("Rome Masters"))
                .otherwise(f.col("tourney_name"))
            )
            .withColumn(
                "TOURNEY_LEVEL",
                f.when(f.col("tourney_id") == "2026-416", f.lit("M"))
                .when(f.col("tourney_name").contains("Olympics"), f.lit("O"))
                .when(f.col("tourney_name").contains("Finals"), f.lit("F"))
                .otherwise(f.col("tourney_level"))
            )
            .withColumn(
                "TOURNEY_IS_INDOOR",
                f.when(f.col("indoor") == "I", f.lit(True))
                .when(f.col("surface") == "Carpet", f.lit(True))
                .when(f.col("indoor") == "O", f.lit(False))
                .when(f.col("tourney_name").contains("Indoor"), f.lit(True))
                .otherwise(f.lit(False))
            )
            .groupBy(f.col("tourney_id").alias("COD_TOURNEY_ID"))
            .agg(
                f.first("TOURNEY_NAME", ignorenulls=True).alias("DES_TOURNEY_NAME"),
                f.first("TOURNEY_LEVEL", ignorenulls=True).alias("DES_TOURNEY_LEVEL"),
                f.first(f.col("draw_size").cast("int"), ignorenulls=True).cast("int").alias("NUM_TOURNEY_DRAW_SIZE"),
                f.first("surface", ignorenulls=True).alias("DES_TOURNEY_SURFACE"),
                f.first("TOURNEY_IS_INDOOR", ignorenulls=True).alias("FLAG_TOURNEY_IS_INDOOR"),
                f.substring(f.first("tourney_date", ignorenulls=True).cast("string"), 1, 4).alias("REF_YEAR"),
                f.first("DATE_INGESTION").alias("DATE_INGESTION")
            )
            .dropDuplicates(["COD_TOURNEY_ID"])
        )
        return df
    except Exception as e:
        print(e)
        raise

def save_table(handler, df):
    try:
        handler.save_data(
            df=df,
            path="s3a://tennis-data-lake/silver/tb_atp_tournaments/",
            format="parquet",
            mode="overwrite"
        )
    except Exception as e:
        print(e)
        raise

def run():
    handler = None
    try:
        handler = PySparkHandler(app_name="tb_atp_tournament_silver")
        tb_atp_matches = load_tables(handler)
        df_final = run_transformation(handler, tb_atp_matches)
        save_table(handler, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run()
