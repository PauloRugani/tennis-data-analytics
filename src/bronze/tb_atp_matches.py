from pyspark.sql import SparkSession
from pyspark.sql import functions as f

import pandas as pd

import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from dotenv import load_dotenv
load_dotenv()

def create_spark_session():
    try:
        print("Creating spark session...")
        spark = (
            SparkSession.builder.appName("bronze")
            .config("spark.driver.memory", "3500m")
            .config("spark.executor.memory", "3500m")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .config(
                "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
            )
            .getOrCreate()
        )
        return spark
        print("Spark session created")
    except Exception as e:
        print(e)
        raise

def load_tables(spark, init_run=False):
    try:
        print("Loading data...")
        if not init_run:
            tb_atp_matches = (
                spark.read
                .format("jdbc")
                .option("url", os.getenv("JDBC_URL"))
                .option("dbtable", "bronze.tb_atp_matches")
                .option("user", os.getenv("DB_USER"))
                .option("password", os.getenv("DB_PASSWORD"))
                .option("driver", "org.postgresql.Driver")
                .load()
            )

        else: 
            tb_atp_matches = None
        try:
            tb_ongoing_tourneys = spark.read.format("csv").option("header", "true").load(r"data/raw/incremental/tb_ongoing_tourneys.csv")
        except Exception as e:
            tb_ongoing_tourneys = spark.read.format("csv").option("header", "true").load(r"/tmp/airflow_staging/incremental/tb_ongoing_tourneys.csv")

        historical_matches = os.path.join("data", "raw", "historical", "matches")
        print("Data loades")

    except Exception as e:
        print(e)
        raise

    return tb_atp_matches, tb_ongoing_tourneys, historical_matches

def run_transformation(spark, init_run, tb_atp_matches, tb_ongoing_tourneys, historical_matches):
    try:
        print("Running transformations...")
        if not init_run:
            df = tb_atp_matches
        else:
            df = (
                spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "false")
                .load(historical_matches)
            )

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
        print("Transformations completed")
        return df_final
    except Exception as e:
        print(e)
        raise

def save_table(init_run, df_final):
    try:
        print("Saving data...")
        if not init_run:
            save_mode = "append"
        else:
            save_mode = "overwrite"
            
        if df_final.count() > 0:
            (
                df_final.write
                .format("jdbc")
                .option("url", os.getenv("JDBC_URL"))
                .option("dbtable", "bronze.tb_atp_matches")
                .option("user", os.getenv("DB_USER"))
                .option("password", os.getenv("DB_PASSWORD"))
                .option("driver", "org.postgresql.Driver")
                .mode(save_mode)
                .save()
            )
        print("Data saved to database")
    except Exception as e:
        print(e)    
        raise

def run(init_run: bool):
    spark = None
    try:
        spark = create_spark_session()
        tb_atp_matches, tb_ongoing_tourneys, historical_matches = load_tables(spark, init_run)
        df_final = run_transformation(spark, init_run, tb_atp_matches, tb_ongoing_tourneys, historical_matches)
        save_table(init_run, df_final)
    finally:
        if spark:
            print("Stopping spark session...")
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    run(init_run=False)