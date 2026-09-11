import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from dotenv import load_dotenv

load_dotenv()
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def create_spark_session():
    try:
        print("Creating spark session...")
        spark = (
            SparkSession.builder.appName("dim_entry")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .getOrCreate()
        )
        
        spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
        spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 200)
        spark.conf.set("spark.sql.repl.eagerEval.truncate", 50)
        return spark
    except Exception as e:
        print(e)
        raise

def load_tables(spark):
    try:
        print("Loading data...")
        tb_player_match = (
            spark.read
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "silver.tb_atp_player_match")
            .option("user", os.getenv("DB_USER"))
            .option("password", os.getenv("DB_PASSWORD"))
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        print("Data loaded")
        return tb_player_match
    except Exception as e:
        print(e)
        raise

def run_transformation(spark, tb_player_match):
    try:
        print("Running transformations...")
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
        print("Transformations completed")
        return df
    except Exception as e:
        print(e)
        raise

def save_table(df):
    try:
        print("Saving data...")
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
        print("Data saved to database")
    except Exception as e:
        print(e)
        raise

def run():
    spark = None
    try:
        spark = create_spark_session()
        tb_player_match = load_tables(spark)
        df_final = run_transformation(spark, tb_player_match)
        save_table(df_final)
    finally:
        if spark:
            print("Stopping spark session...")
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    run()
