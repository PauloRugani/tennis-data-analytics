from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import pandas as pd

import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from dotenv import load_dotenv
load_dotenv()


try:
    spark = (
        SparkSession.builder.appName("silver_atp_ranking")
        .config("spark.driver.memory", "3500m")
        .config("spark.executor.memory", "3500m")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
        )
        .getOrCreate()
    )
except Exception as e:
    print(e)


spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 200)
spark.conf.set("spark.sql.repl.eagerEval.truncate", 50)


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

# tb_atp_rankings = spark.read.format("parquet").load(r"../../data/bronze/tb_atp_rankings/")


df = (
    tb_atp_rankings
    .select(
        f.date_format(f.to_date(f.col("date"), 'yyyyMMdd'), 'yyyy-MM-dd').alias("DATE_WEEK_RANKING"),
        f.col("rank").cast("int").alias("NUM_PLAYER_RANK"),
        f.col("name").alias("DES_PLAYER_NAME"),
        f.col("id").alias("COD_PLAYER_ID"),
        f.col("age").cast("int").alias("NUM_PLAYER_AGE"),
        f.regexp_replace(f.col("points"), ",", "").cast("int").alias("NUM_PLAYER_RANK_PTS"),
        f.col("lost_earned_points").cast("string").alias("NUM_PLAYER_LE_PTS"),
        f.col("tourn_played").cast("int").alias("NUM_PLAYER_TOURNEY_PLAYED"),
        f.regexp_replace(f.col("dropping"), ",", "").cast("int").alias("NUM_PLAYER_DROP_PTS"),
        f.col("next_best").cast("int").alias("NUM_PLAYER_NEXT_BEST"),

        f.col("DATE_INGESTION").alias("DATE_INGESTION")
    )
    .dropDuplicates(["DATE_WEEK_RANKING", "COD_PLAYER_ID"])
)


(
    df.coalesce(4).write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"../../data/silver/tb_atp_rankings")
)


(
df.write
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "silver.tb_atp_rankings")
    .option("user", os.getenv("DB_USER"))
    .option("password", os.getenv("DB_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

