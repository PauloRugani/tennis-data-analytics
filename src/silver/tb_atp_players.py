import os
import sys
from pyspark.sql import functions as f
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pyspark_handler import PySparkHandler
from utils.logger import get_logger

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

def load_tables(handler, bucket_name):
    try:
        logger.info("Loading silver players source table...")
        tb_atp_players = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/bronze/tb_atp_players/",
            format="parquet"
        )
        return tb_atp_players
    except Exception as e:
        logger.error(f"Failed to load silver players source table: {e}")
        raise

def run_transformation(tb_atp_players):
    try:

        df = (
            tb_atp_players
            .select(
                f.col("id").alias("COD_PLAYER_ID"),
                f.col("player").alias("DES_PLAYER_NAME"),
                f.col("atpname").alias("DES_PLAYER_ATP_NAME"),
                f.date_format(f.to_date(f.col("birthdate"), 'yyyyMMdd'), 'yyyy-MM-dd').alias("DATE_PLAYER_BIRTHDATE"),
                f.coalesce(f.col("height"), f.lit(-1)).alias("NUM_PLAYER_HEIGHT"),
                f.col("turnedpro").alias("DATE_PLAYER_TURNED_PRO_YEAR"),
                f.coalesce(f.col("birthplace"), f.lit("-")).alias("DES_PLAYER_BIRTH_PLACE"),
                f.coalesce(f.col("coaches"), f.lit("-")).alias("DES_PLAYER_COACHES"),
                f.coalesce(f.col("hand"), f.lit("-")).alias("DES_PLAYER_HAND"),
                f.coalesce(f.col("backhand"), f.lit("-")).alias("DES_PLAYER_BACKHAND_TYPE"),
                f.coalesce(f.col("ioc"), f.lit("N/A")).alias("DES_PLAYER_COUNTRY"),
            )
        )

        logger.info("Silver players transformation completed")
        return df
    except Exception as e:
        logger.error(f"Failed to transform silver players: {e}")
        raise

def save_table(handler, df, bucket_name):
    try:
        handler.save_data(
            df=df,
            path=f"s3a://{bucket_name}/silver/tb_atp_players/",
            format="parquet",
            mode="overwrite"
        )
    except Exception as e:
        logger.error(f"Failed to save silver players: {e}")
        raise

def run(conn_vars: dict = None):
    handler = None
    try:
        logger.info("Starting silver players run")
        handler = PySparkHandler(
            app_name="tb_atp_players_silver",
            bucket_endpoint=conn_vars.get("bucket_endpoint"),
            bucket_access_key=conn_vars.get("bucket_access_key"),
            bucket_secret_key=conn_vars.get("bucket_secret_key")
        )
        bucket_name = conn_vars.get("bucket_name")
        tb_atp_players = load_tables(handler, bucket_name)
        df_final = run_transformation(tb_atp_players)
        save_table(handler, df_final, bucket_name)
        logger.info("Finished silver players run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run()
