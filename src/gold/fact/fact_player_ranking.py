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
            SparkSession.builder.appName("fact_player_ranking")
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
        tb_atp_rankings = (
            spark.read
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "silver.tb_atp_rankings")
            .option("user", os.getenv("DB_USER"))
            .option("password", os.getenv("DB_PASSWORD"))
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        tb_date = (
            spark.read
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "gold.dim_date")
            .option("user", os.getenv("DB_USER"))
            .option("password", os.getenv("DB_PASSWORD"))
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        tb_players = (
            spark.read
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "gold.dim_players")
            .option("user", os.getenv("DB_USER"))
            .option("password", os.getenv("DB_PASSWORD"))
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        print("Data loaded")
        return tb_atp_rankings, tb_date, tb_players
    except Exception as e:
        print(e)
        raise

def run_transformation(spark, tb_atp_rankings, tb_date, tb_players):
    try:
        print("Running transformations...")
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
                "d.SK_DATE",
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
            .option("dbtable", "gold.fact_player_ranking")
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
        tb_atp_rankings, tb_date, tb_players = load_tables(spark)
        df_final = run_transformation(spark, tb_atp_rankings, tb_date, tb_players)
        save_table(df_final)
    finally:
        if spark:
            print("Stopping spark session...")
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    run()
