import os
from pyspark.sql import functions as f
from src.utils.pyspark_handler import PySparkHandler
from src.utils.logger import get_logger
import boto3

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

def load_tables(handler, init_run, bucket_name):
    try:
        logger.info(f"Loading bronze match tables (init_run={init_run})...")
        if not init_run:
            tb_atp_matches = handler.load_data(
                spark=handler.spark,
                path=f"s3a://{bucket_name}/bronze/tb_atp_matches/",
                format="parquet"
            )
        else: 
            tb_atp_matches = None
            
        tb_ongoing_tourneys = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/raw/incremental/tb_ongoing_tourneys.csv",
            format="csv",
            header="true"
        )

        historical_matches = f"s3a://{bucket_name}/raw/historical/matches/"
    except Exception as e:
        logger.error(f"Failed to load bronze match tables: {e}")
        raise

    return tb_atp_matches, tb_ongoing_tourneys, historical_matches

def run_transformation(handler, init_run, s3_client, bucket_name, tb_atp_matches, tb_ongoing_tourneys, historical_matches):
    try:
        logger.info("Running match transformations...")
        if not init_run:
            df = tb_atp_matches
        else:
            prefix = "raw/historical/matches/"
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            files = [
                obj["Key"]
                for obj in response.get("Contents", [])
                if obj["Key"].endswith(".csv")
            ]

            df = None
            for index, file_key in enumerate(files):
                final_path = f"s3a://{bucket_name}/{file_key}"
                match_data = handler.load_data(spark=handler.spark, path=final_path, format="csv", header="true")
                if index == 0:
                    df = match_data
                else:
                    df = df.unionByName(match_data, allowMissingColumns=True)


        new_matches = (
            df.alias("tb_matches")
            .join(
                tb_ongoing_tourneys.alias("tb_ongoing"),
                [
                    f.col("tb_matches.tourney_date") == f.col("tb_ongoing.tourney_date"), 
                    f.col("tb_matches.winner_name") == f.col("tb_ongoing.winner_name"),
                    f.col("tb_matches.loser_name") == f.col("tb_ongoing.loser_name")
                ],
                'right'
                )
            .where(f.col("tb_matches.tourney_date").isNull())
            .select(
                "tb_ongoing.*",
            )
        )


        if not init_run:
            df_final = new_matches.withColumn("DATE_INGESTION", f.lit(f.current_date()))
        else:
            df_final = df.unionByName(new_matches, allowMissingColumns=True).withColumn("DATE_INGESTION", f.lit(f.current_date()))

        df_final = df_final.where(
                """
                    tourney_name not like '%Davis Cup%' and 
                    tourney_name not like '%Olymp%' and 
                    tourney_name not like '%Laver Cup%' and 
                    tourney_name not like '%Next Gen%' and
                    tourney_name not like '%Atp Cup%' and 
                    tourney_name not like '%United Cup%' and 
                    tourney_name not in ('Kingston', 'Dusseldorf', 'Nations Cup')
                """
                )
        logger.info("Match transformations completed")
        return df_final
    except Exception as e:
        logger.error(f"Failed to transform match data: {e}")
        raise

def save_table(handler, init_run, df_final, bucket_name):
    try:
        if not init_run:
            save_mode = "append"
        else:
            save_mode = "overwrite"
            
        new_rows = df_final.count()
        if new_rows > 0:
            logger.info(f"Saving {new_rows} match rows (mode={save_mode})")
            handler.save_data(
                df=df_final,
                path=f"s3a://{bucket_name}/bronze/tb_atp_matches/",
                format="parquet",
                mode=save_mode
            )
        else:
            logger.info("No new match rows to save")
    except Exception as e:
        logger.error(f"Failed to save bronze match data: {e}")
        raise

def run(init_run: bool, conn_vars: dict = None):
    handler = None
    try:
        logger.info("Starting bronze matches run")
        handler = PySparkHandler(
            app_name="tb_atp_matches_bronze",
            bucket_endpoint=conn_vars.get("bucket_endpoint"),
            bucket_access_key=conn_vars.get("bucket_access_key"),
            bucket_secret_key=conn_vars.get("bucket_secret_key")
        )
        s3_client = boto3.client(
            "s3",
            endpoint_url=conn_vars.get("bucket_endpoint"),
            aws_access_key_id=conn_vars.get("bucket_access_key"),
            aws_secret_access_key=conn_vars.get("bucket_secret_key")
        )

        bucket_name = conn_vars.get("bucket_name")
        tb_atp_matches, tb_ongoing_tourneys, historical_matches = load_tables(handler, init_run, bucket_name)
        df_final = run_transformation(handler, init_run, s3_client, bucket_name, tb_atp_matches, tb_ongoing_tourneys, historical_matches)
        save_table(handler, init_run, df_final, bucket_name)
        logger.info("Finished bronze matches run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run(init_run=True)