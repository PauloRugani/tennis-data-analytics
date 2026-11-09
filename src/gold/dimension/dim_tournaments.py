import os
import sys
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.pyspark_handler import PySparkHandler
from src.utils.logger import get_logger

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

def load_tables(handler, bucket_name):
    try:
        logger.info("Loading dim_tournaments source tables...")
        atp_tournaments = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/silver/tb_atp_tournaments/",
            format="parquet"
        )
        atp_player_match = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/silver/tb_atp_player_match/",
            format="parquet"
        )
        return atp_tournaments, atp_player_match
    except Exception as e:
        logger.error(f"Failed to load dim_tournaments source tables: {e}")
        raise

def run_transformation(atp_tournaments, atp_player_match):
    try:
        window_tourney = Window.partitionBy("t.COD_TOURNEY_ID", "t.REF_YEAR")

        df = (
            atp_tournaments.alias("t")
            .join(atp_player_match.alias("m"),
                     [
                        f.col("t.COD_TOURNEY_ID") == f.split(f.col("m.COD_TOURNEY_ID"), "-", 2).getItem(1),
                        f.col("t.REF_YEAR") == f.split(f.col("m.COD_TOURNEY_ID"), "-", 2).getItem(0)
                     ], "left")
            .withColumn(
                "DES_TOURNEY_STATUS",
                f.when(
                    f.max(
                        f.when(~f.col("DES_MATCH_ROUND").isin("F", "RR"), 1)
                        .otherwise(0)
                    ).over(window_tourney)
                    == 1,
                    "In Progress",
                ).otherwise("Finished"),
            )
            .select(
                f.md5(f.concat_ws("-", f.col("t.COD_TOURNEY_ID"), f.col("t.REF_YEAR"))).alias("SK_TOURNEY"),
                f.col("t.COD_TOURNEY_ID"),
                f.col("DES_TOURNEY_NAME"),
                f.when(f.col("DES_TOURNEY_LEVEL") == "G", "Grand Slam")
                .when(f.col("DES_TOURNEY_LEVEL") == "M", "Masters 1000")
                .when(f.col("DES_TOURNEY_LEVEL") == "500", "ATP 500")
                .when(f.col("DES_TOURNEY_LEVEL") == "250", "ATP 250")
                .when(f.col("DES_TOURNEY_LEVEL") == "A", "Other")
                .when(f.col("DES_TOURNEY_LEVEL") == "F", "ATP Finals")
                .when(f.col("DES_TOURNEY_LEVEL") == "O", "Olympics Games")
                .when(f.col("DES_TOURNEY_LEVEL") == "D", "Davis Cup")
                .otherwise(f.lit("-"))
                .alias("DES_TOURNEY_LEVEL"),
                f.coalesce(f.col("m.NUM_TOURNEY_DRAW_SIZE"), f.lit(-1))
                .cast("int")
                .alias("NUM_TOURNEY_DRAW_SIZE"),
                f.coalesce(f.col("DES_TOURNEY_SURFACE"), f.lit("-")).alias(
                    "DES_TOURNEY_SURFACE"
                ),
                f.when(f.col("FLAG_TOURNEY_IS_INDOOR") == True, "Yes")
                .when(f.col("FLAG_TOURNEY_IS_INDOOR") == False, "No")
                .otherwise(f.lit("-"))
                .alias("FLAG_TOURNEY_IS_INDOOR"),
                f.col("DES_TOURNEY_STATUS"),
                f.coalesce(f.col("DATE_TOURNEY_START"), f.lit("-")).alias(
                    "DATE_TOURNEY_START"
                ),
                f.coalesce(f.col("t.REF_YEAR"), f.lit("-")).alias("REF_YEAR"),
                f.lit(f.current_date()).alias("DATE_LOAD")
            )
            .dropDuplicates(["COD_TOURNEY_ID", "REF_YEAR"])
        )
        logger.info("dim_tournaments transformation completed")
        return df
    except Exception as e:
        logger.error(f"Failed to transform dim_tournaments: {e}")
        raise

def save_table(handler, df, bucket_name, jdbc_url, jdbc_user, jdbc_password):
    try:
        handler.save_data(
            df=df,
            path=f"s3a://{bucket_name}/gold/dimension/dim_tournaments/",
            format="parquet",
            mode="overwrite"
        )

        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "gold.dim_tournaments")
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
    except Exception as e:
        logger.error(f"Failed to save dim_tournaments: {e}")
        raise

def run(conn_vars: dict = None):
    handler = None
    try:
        logger.info("Starting dim_tournaments run")
        handler = PySparkHandler(
            app_name="dim_tournament",
            bucket_endpoint=conn_vars.get("bucket_endpoint"),
            bucket_access_key=conn_vars.get("bucket_access_key"),
            bucket_secret_key=conn_vars.get("bucket_secret_key")
        )
        bucket_name = conn_vars.get("bucket_name")
        jdbc_url = conn_vars.get("jdbc_url")
        jdbc_user = conn_vars.get("jdbc_user")
        jdbc_password = conn_vars.get("jdbc_password")

        atp_tournaments, atp_player_match = load_tables(handler, bucket_name)
        df_final = run_transformation(atp_tournaments, atp_player_match)
        save_table(handler, df_final, bucket_name, jdbc_url, jdbc_user, jdbc_password)
        logger.info("Finished dim_tournaments run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run()
