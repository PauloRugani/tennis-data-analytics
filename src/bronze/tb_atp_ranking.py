from pyspark.sql import SparkSession
from pyspark.sql import functions as f

import os
from datetime import datetime

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from dotenv import load_dotenv
load_dotenv()

def create_spark_session():
    try:
        print("Creating spark session...")
        spark = (
            SparkSession.builder.appName("atp_ranking")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .getOrCreate()
        )
        return spark
    except Exception as e:
        print(e)
        raise

def load_tables(spark, init_run=False):
    try:
        print("Loading data...")
        if not init_run:
            tb_atp_rankings = (
                spark.read
                .format("jdbc")
                .option("url", os.getenv("JDBC_URL"))
                .option("dbtable", "bronze.tb_atp_rankings")
                .option("user", os.getenv("DB_USER"))
                .option("password", os.getenv("DB_PASSWORD"))
                .option("driver", "org.postgresql.Driver")
                .load()
            )
        else:
            tb_atp_rankings = None

        try:
            tb_incremental_rankings = (
                spark.read
                .format("csv")
                .option("header", "true")
                .load(fr"data/raw/incremental/tb_incremental_ranking_{datetime.now().year}.csv")
            )
        except Exception as e:
            tb_incremental_rankings = (
                spark.read
                .format("csv")
                .option("header", "true")
                .load(fr"/tmp/airflow_staging/incremental/tb_incremental_ranking_{datetime.now().year}.csv")
            )

        historical_ranking = r"data/raw/historical/ranking"
        print("Data loaded")
    except Exception as e:
        print(e)
        raise
    
    return tb_atp_rankings, tb_incremental_rankings, historical_ranking

def run_transformation(spark, init_run, tb_atp_rankings, tb_incremental_rankings, historical_ranking):
    try:
        print("Running transformations...")
        if not init_run:
            df = tb_atp_rankings
        else:
            df = (
                spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "false")
                .load(historical_ranking)
            )

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
                .option("dbtable", "bronze.tb_atp_rankings")
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
        tb_atp_rankings, tb_incremental_rankings, historical_ranking = load_tables(spark, init_run)
        df_final = run_transformation(spark, init_run, tb_atp_rankings, tb_incremental_rankings, historical_ranking)
        save_table(init_run, df_final)
    finally:
        if spark:
            print("Stopping spark session...")
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    run(init_run=False)
