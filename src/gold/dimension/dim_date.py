import os
import sys
from datetime import datetime
from pyspark.sql import functions as f
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.pyspark_handler import PySparkHandler
from src.utils.logger import get_logger

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

def run_transformation(handler):
    try:
        start_date = '1960-01-01'
        end_date = datetime.now().strftime('%Y-%m-%d')

        df_date = (
            handler.spark.range(1)
            .select(
                f.sequence(
                    f.to_date(f.lit(start_date)),
                    f.to_date(f.lit(end_date)),
                    f.expr("INTERVAL 1 DAY"),
                ).alias("DATE")
            )
            .select(f.explode("DATE").alias("DATE"))
        )

        df = (
            df_date
            .withColumn("SK_DATE", f.date_format("DATE", "yyyyMMdd").cast("string"))
            .withColumn("NUM_YEAR", f.year("DATE"))
            .withColumn("NUM_MONTH", f.month("DATE"))
            .withColumn("DES_MONTH", f.date_format("DATE", "MMMM"))
            .withColumn("DES_MONTH_SHORT", f.date_format("DATE", "MMM"))
            .withColumn("NUM_DAY", f.dayofmonth("DATE"))
            .withColumn("DES_DAY", f.date_format("DATE", "EEEE"))
            .withColumn("DATE_LOAD", f.lit(f.current_date()))
        )
        logger.info("dim_date transformation completed")
        return df
    except Exception as e:
        logger.error(f"Failed to transform dim_date: {e}")
        raise

def save_table(handler, df, bucket_name, jdbc_url, jdbc_user, jdbc_password):
    try:
        handler.save_data(
            df=df,
            path=f"s3a://{bucket_name}/gold/dimension/dim_date/",
            format="parquet",
            mode="overwrite"
        )
        
        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "gold.dim_date")
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
    except Exception as e:
        logger.error(f"Failed to save dim_date: {e}")
        raise

def run(conn_vars: dict = None):
    handler = None
    try:
        logger.info("Starting dim_date run")
        handler = PySparkHandler(
            app_name="dim_date",
            bucket_endpoint=conn_vars.get("bucket_endpoint"),
            bucket_access_key=conn_vars.get("bucket_access_key"),
            bucket_secret_key=conn_vars.get("bucket_secret_key")
        )
        bucket_name = conn_vars.get("bucket_name")
        jdbc_url = conn_vars.get("jdbc_url")
        jdbc_user = conn_vars.get("jdbc_user")
        jdbc_password = conn_vars.get("jdbc_password")
        
        df_final = run_transformation(handler)
        save_table(handler, df_final, bucket_name, jdbc_url, jdbc_user, jdbc_password)
        logger.info("Finished dim_date run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run()
