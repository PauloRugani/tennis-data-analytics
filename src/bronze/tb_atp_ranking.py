import os
import sys
from pyspark.sql import functions as f
from datetime import datetime
from src.utils.pyspark_handler import PySparkHandler
from src.utils.logger import get_logger
import boto3

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

def load_tables(handler, init_run, bucket_name):
    try:
        logger.info(f"Loading bronze ranking tables (init_run={init_run})...")
        if not init_run:
            tb_atp_rankings = handler.load_data(
                spark=handler.spark,
                path=f"s3a://{bucket_name}/bronze/tb_atp_rankings/",
                format="parquet"
            )
        else:
            tb_atp_rankings = None

        tb_incremental_rankings = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/raw/incremental/tb_incremental_ranking_{datetime.now().year}.csv",
            format="csv",
            header="true"
        )
    except Exception as e:
        logger.error(f"Failed to load bronze ranking tables: {e}")
        raise
    
    return tb_atp_rankings, tb_incremental_rankings

def run_transformation(handler, init_run, s3_client, bucket_name, tb_atp_rankings, tb_incremental_rankings):
    try:
        logger.info("Running ranking transformations...")
        if not init_run:
            df = tb_atp_rankings
        else:
            prefix = "raw/historical/ranking/"
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            files = [
                obj["Key"]
                for obj in response.get("Contents", [])
                if obj["Key"].endswith(".csv")
            ]

            df = None
            for index, file_key in enumerate(files):
                final_path = f"s3a://{bucket_name}/{file_key}"
                ranking_data = handler.load_data(spark=handler.spark, path=final_path, format="csv", header="true")
                if index == 0:
                    df = ranking_data
                else:
                    df = df.unionByName(ranking_data, allowMissingColumns=True)

        new_rankings = (
            df.alias("h_r")
            .join(
                tb_incremental_rankings.alias("i_r"),
                [
                    f.col("h_r.date") == f.col("i_r.date"), 
                    f.col("h_r.name") == f.col("i_r.name")
                ],
                'right'
                )
            .where(f.col("h_r.date").isNull())
            .select(
                "i_r.*",
            )
        )

        if not init_run:
            df_final = new_rankings.withColumn("DATE_INGESTION", f.lit(f.current_date()))
        else:
            df_final = df.unionByName(new_rankings, allowMissingColumns=True).withColumn("DATE_INGESTION", f.lit(f.current_date()))

        logger.info("Ranking transformations completed")
        return df_final
    except Exception as e:
        logger.error(f"Failed to transform ranking data: {e}")
        raise

def save_table(handler, init_run, df_final, bucket_name):
    try:
        if not init_run:
            save_mode = "append"
        else:
            save_mode = "overwrite"

        new_rows = df_final.count()
        if new_rows > 0:
            logger.info(f"Saving {new_rows} ranking rows (mode={save_mode})")
            handler.save_data(
                df=df_final,
                path=f"s3a://{bucket_name}/bronze/tb_atp_rankings/",
                format="parquet",
                mode=save_mode
            )
        else:
            logger.info("No new ranking rows to save")
    except Exception as e:
        logger.error(f"Failed to save bronze ranking data: {e}")
        raise

def run(init_run: bool, conn_vars: dict = None):
    handler = None
    try:
        logger.info("Starting bronze ranking run")
        handler = PySparkHandler(
            app_name="tb_atp_ranking_bronze",
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
        tb_atp_rankings, tb_incremental_rankings = load_tables(handler, init_run, bucket_name)
        df_final = run_transformation(handler, init_run, s3_client, bucket_name, tb_atp_rankings, tb_incremental_rankings)
        save_table(handler, init_run, df_final, bucket_name)
        logger.info("Finished bronze ranking run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run(init_run=True)