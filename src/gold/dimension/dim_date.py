import os
import sys
import pandas as pd
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils.pyspark_handler import PySparkHandler

load_dotenv()
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def load_tables(handler):
    try:
        tb_player_match = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/silver/tb_atp_player_match/",
            format="parquet"
        )
        return tb_player_match
    except Exception as e:
        print(e)
        raise

def run_transformation(handler, tb_player_match):
    try:
        start_date = '1960-01-01'
        end_date = tb_player_match.select(f.max("DATE_MATCH")).collect()[0][0]

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
            .withColumn("SK_DATE", f.date_format("DATE", "yyyyMMdd").cast("int"))
            .withColumn("NUM_YEAR", f.year("DATE"))
            .withColumn("NUM_MONTH", f.month("DATE"))
            .withColumn("DES_MONTH", f.date_format("DATE", "MMMM"))
            .withColumn("DES_MONTH_SHORT", f.date_format("DATE", "MMM"))
            .withColumn("NUM_DAY", f.dayofmonth("DATE"))
            .withColumn("DES_DAY", f.date_format("DATE", "EEEE"))
            .withColumn("DATE_LOAD", f.lit(f.current_date()))
        )
        return df
    except Exception as e:
        print(e)
        raise

def save_table(handler, df):
    try:
        handler.save_data(
            df=df,
            path="s3a://tennis-data-lake/gold/dimension/dim_date/",
            format="parquet",
            mode="overwrite"
        )
        
        (
            df.write
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "gold.dim_date")
            .option("user", os.getenv("DB_USER"))
            .option("password", os.getenv("DB_PASSWORD"))
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
    except Exception as e:
        print(e)
        raise

def run():
    handler = None
    try:
        handler = PySparkHandler(app_name="dim_date")
        tb_player_match = load_tables(handler)
        df_final = run_transformation(handler, tb_player_match)
        save_table(handler, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run()
