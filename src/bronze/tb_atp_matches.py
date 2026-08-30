from pyspark.sql import SparkSession
from pyspark.sql import functions as f

import pandas as pd

import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from dotenv import load_dotenv
load_dotenv()


try:
    spark = (
        SparkSession.builder.appName("bronze")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
except Exception as e:
    print(e)


tb_atp_matches = spark.read.format("parquet").load(r"../../data/bronze/tb_atp_matches/")


tb_ongoing_tourneys = spark.read.format("csv").option("header", "true").load(r"../../data/raw/incremental/tb_ongoing_tourneys.csv")


historical_matches = r"../../data/raw/historical/matches"


for index, file_name in enumerate(os.listdir(historical_matches)):
    final_path = os.path.join(historical_matches, file_name)
    match_data = spark.read.format("csv").option("header", "true").load(final_path)
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


if df_final.count() > 0:
    (
        df_final.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(r"../../data/bronze/tb_atp_matches")
    )

    (
        df_final.write
        .format("jdbc")
        .option("url", os.getenv("JDBC_URL"))
        .option("dbtable", "bronze.tb_atp_matches")
        .option("user", os.getenv("DB_USER"))
        .option("password", os.getenv("DB_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )    

