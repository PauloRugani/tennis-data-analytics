import os
import sys
from datetime import datetime
from pyspark.sql import functions as f
from pyspark.sql.window import Window
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.pyspark_handler import PySparkHandler
from src.utils.logger import get_logger

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

def load_tables(handler, bucket_name):
    try:
        logger.info("Loading dim_date source tables...")
        tb_rankings = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/silver/tb_atp_rankings/",
            format="parquet"
        )
        return tb_rankings
    except Exception as e:
        logger.error(f"Failed to load dim_date source tables: {e}")
        raise

def run_transformation(handler, tb_rankings):
    try:
        start_date = '1968-01-01'
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
            .select(f.explode("DATE").alias("DATE_FORMATED"))
        )

        # Datas em que a ATP publicou ranking (nem toda semana tem: dados antigos e 2020)
        ranking_dates = (
            tb_rankings
            .select(f.to_date(f.col("DATE_WEEK_RANKING")).alias("DATE_FORMATED"))
            .distinct()
            .withColumn("DATE_PUBLISHED", f.col("DATE_FORMATED"))
        )

        # Para cada dia, o ultimo ranking publicado ate aquele dia
        window_ranking = (
            Window.orderBy("DATE_FORMATED")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        df = (
            df_date
            .join(ranking_dates, ["DATE_FORMATED"], "left")
            .withColumn("SK_DATE", f.date_format("DATE_FORMATED", "yyyyMMdd").cast("string"))
            .withColumn("NUM_YEAR", f.year("DATE_FORMATED"))
            .withColumn("NUM_MONTH", f.month("DATE_FORMATED"))
            .withColumn("DES_MONTH", f.date_format("DATE_FORMATED", "MMMM"))
            .withColumn("DES_MONTH_SHORT", f.date_format("DATE_FORMATED", "MMM"))
            .withColumn("NUM_DAY", f.dayofmonth("DATE_FORMATED"))
            .withColumn("DES_DAY", f.date_format("DATE_FORMATED", "EEEE"))
            .withColumn("DATE_RANKING", f.last("DATE_PUBLISHED", ignorenulls=True).over(window_ranking))
            .withColumn("SK_DATE_RANKING", f.date_format("DATE_RANKING", "yyyyMMdd").cast("string"))
            .withColumn("DATE_LOAD", f.lit(f.current_date()))
            .drop("DATE_PUBLISHED")
            .orderBy("DATE_FORMATED")
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
        
        tb_rankings = load_tables(handler, bucket_name)
        df_final = run_transformation(handler, tb_rankings)
        save_table(handler, df_final, bucket_name, jdbc_url, jdbc_user, jdbc_password)
        logger.info("Finished dim_date run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run()
