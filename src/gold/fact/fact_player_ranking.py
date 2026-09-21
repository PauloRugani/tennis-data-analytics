import os
import sys
from pyspark.sql import functions as f
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils.pyspark_handler import PySparkHandler
from utils.logger import get_logger

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

def load_tables(handler, bucket_name):
    try:
        logger.info("Loading fact_player_ranking source tables...")
        tb_atp_rankings = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/silver/tb_atp_rankings/",
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
        return tb_atp_rankings, tb_date, tb_players
    except Exception as e:
        logger.error(f"Failed to load fact_player_ranking source tables: {e}")
        raise

def run_transformation(tb_atp_rankings, tb_date, tb_players):
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
                f.col("d.SK_DATE").cast('string').alias("SK_DATE"),
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
        logger.info("fact_player_ranking transformation completed")
        return df
    except Exception as e:
        logger.error(f"Failed to transform fact_player_ranking: {e}")
        raise

def save_table(handler, df, bucket_name, jdbc_url, jdbc_user, jdbc_password):
    try:
        handler.save_data(
            df=df,
            path=f"s3a://{bucket_name}/gold/fact/fact_player_ranking/",
            format="parquet",
            mode="overwrite"
        )

        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "gold.fact_player_ranking")
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
    except Exception as e:
        logger.error(f"Failed to save fact_player_ranking: {e}")
        raise

def run(conn_vars: dict = None):
    handler = None
    try:
        logger.info("Starting fact_player_ranking run")
        handler = PySparkHandler(
            app_name="fact_player_ranking",
            bucket_endpoint=conn_vars.get("bucket_endpoint"),
            bucket_access_key=conn_vars.get("bucket_access_key"),
            bucket_secret_key=conn_vars.get("bucket_secret_key")
        )
        bucket_name = conn_vars.get("bucket_name")
        jdbc_url = conn_vars.get("jdbc_url")
        jdbc_user = conn_vars.get("jdbc_user")
        jdbc_password = conn_vars.get("jdbc_password")

        tb_atp_rankings, tb_date, tb_players = load_tables(handler, bucket_name)
        df_final = run_transformation(tb_atp_rankings, tb_date, tb_players)
        save_table(handler, df_final, bucket_name, jdbc_url, jdbc_user, jdbc_password)
        logger.info("Finished fact_player_ranking run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run()
