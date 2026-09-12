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
        df = (
            tb_player_match
            .select("COD_PLAYER_ENTRY")
            .distinct()
            .withColumn(
                "DES_ENTRY_TYPE",
                f.when(f.col("COD_PLAYER_ENTRY") == "WC", f.lit("Wild Card"))
                .when(f.col("COD_PLAYER_ENTRY") == "Q", f.lit("Qualifier"))
                .when(f.col("COD_PLAYER_ENTRY") == "LL", f.lit("Lucky Loser"))
                .when(f.col("COD_PLAYER_ENTRY") == "ITF", f.lit("ITF Entry"))
                .when(f.col("COD_PLAYER_ENTRY") == "UP", f.lit("Next Gen / Unranked Performance"))
                .when(f.col("COD_PLAYER_ENTRY") == "W", f.lit("Wild Card"))
                .when(f.col("COD_PLAYER_ENTRY") == "SE", f.lit("Special Exempt"))
                .when(f.col("COD_PLAYER_ENTRY") == "PR", f.lit("Protected Ranking"))
                .when(f.col("COD_PLAYER_ENTRY") == "S", f.lit("Exempt Special / Special"))
                .when(f.col("COD_PLAYER_ENTRY") == "NG", f.lit("Next Gen Accelerator"))
                .otherwise(f.lit("Direct Acceptance / Regular"))
            )
            .distinct()
            .withColumn("SK_ENTRY_TYPE", f.monotonically_increasing_id() + 1)
            .select(
                f.col("SK_ENTRY_TYPE"),
                f.col("COD_PLAYER_ENTRY"),
                f.col("DES_ENTRY_TYPE"),
                f.lit(f.current_date()).alias("DATE_LOAD")
            )
        )
        return df
    except Exception as e:
        print(e)
        raise

def save_table(handler, df):
    try:
        handler.save_data(
            df=df,
            path="s3a://tennis-data-lake/gold/dimension/dim_entry/",
            format="parquet",
            mode="overwrite"
        )

        (
            df.write
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "gold.dim_entry")
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
        handler = PySparkHandler(app_name="dim_entry")
        tb_player_match = load_tables(handler)
        df_final = run_transformation(handler, tb_player_match)
        save_table(handler, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run()
