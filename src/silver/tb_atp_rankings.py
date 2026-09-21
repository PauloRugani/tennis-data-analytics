import os
import sys
from pyspark.sql import functions as f
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pyspark_handler import PySparkHandler
from utils.logger import get_logger

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

def load_tables(handler, bucket_name):
    try:
        logger.info("Loading silver rankings source table...")
        tb_atp_rankings = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/bronze/tb_atp_rankings/",
            format="parquet"
        )
        return tb_atp_rankings
    except Exception as e:
        logger.error(f"Failed to load silver rankings source table: {e}")
        raise

def run_transformation(tb_atp_rankings):
    try:
        df = (
            tb_atp_rankings
            .select(
                f.date_format(f.to_date(f.col("date"), 'yyyyMMdd'), 'yyyy-MM-dd').alias("DATE_WEEK_RANKING"),
                f.col("rank").cast("int").alias("NUM_PLAYER_RANK"),
                f.col("name").alias("DES_PLAYER_NAME"),
                f.col("age").cast("int").alias("NUM_PLAYER_AGE"),
                f.regexp_replace(f.col("points"), ",", "").cast("int").alias("NUM_PLAYER_RANK_PTS"),
                f.col("lost_earned_points").cast("string").alias("NUM_PLAYER_LE_PTS"),
                f.col("tourn_played").cast("int").alias("NUM_PLAYER_TOURNEY_PLAYED"),
                f.regexp_replace(f.col("dropping"), ",", "").cast("int").alias("NUM_PLAYER_DROP_PTS"),
                f.col("next_best").cast("int").alias("NUM_PLAYER_NEXT_BEST"),

                f.col("DATE_INGESTION").alias("DATE_INGESTION")
            )
            .dropDuplicates(["DATE_WEEK_RANKING", "DES_PLAYER_NAME"])
        )
        logger.info("Silver rankings transformation completed")
        return df
    except Exception as e:
        logger.error(f"Failed to transform silver rankings: {e}")
        raise

def save_table(handler, df, bucket_name):
    try:
        handler.save_data(
            df=df,
            path=f"s3a://{bucket_name}/silver/tb_atp_rankings/",
            format="parquet",
            mode="overwrite"
        )
    except Exception as e:
        logger.error(f"Failed to save silver rankings: {e}")
        raise

def run(conn_vars: dict = None):
    handler = None
    try:
        logger.info("Starting silver rankings run")
        handler = PySparkHandler(
            app_name="tb_atp_ranking_silver",
            bucket_endpoint=conn_vars.get("bucket_endpoint"),
            bucket_access_key=conn_vars.get("bucket_access_key"),
            bucket_secret_key=conn_vars.get("bucket_secret_key")
        )
        bucket_name = conn_vars.get("bucket_name")
        tb_atp_rankings = load_tables(handler, bucket_name)
        df_final = run_transformation(tb_atp_rankings)
        save_table(handler, df_final, bucket_name)
        logger.info("Finished silver rankings run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run()
