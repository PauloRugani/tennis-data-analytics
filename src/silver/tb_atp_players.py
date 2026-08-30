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
        SparkSession.builder.appName("silver_atp_players")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
except Exception as e:
    print(e)


spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 200)
spark.conf.set("spark.sql.repl.eagerEval.truncate", 50)


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


# tb_atp_matches = spark.read.format("parquet").load(r"../../data/bronze/tb_atp_matches/")


final_tb_atp_matches = (
        tb_atp_matches 
        .withColumn(
            "PLAYER",
            f.when(
                f.col("winner_name").contains("Shevchenko"), "Aleksandr Shevchenko"
            ).otherwise(f.col("winner_name"))
        )
        .withColumn(
            "loser_name",
            f.when(
                f.col("loser_name").contains("Shevchenko"), "Aleksandr Shevchenko"
            ).otherwise(f.col("loser_name"))
        )
    )


window_winner_name = Window.partitionBy("winner_name")  

winners = (
        final_tb_atp_matches 
        .select(
            f.coalesce(
                f.col("winner_id"),
                f.first("winner_id", ignorenulls=True).over(window_winner_name)
            ).cast("string").alias("COD_PLAYER_ID"),
            f.col("winner_name").alias("DES_PLAYER_NAME"),
            f.col("winner_hand").alias("DES_PLAYER_HAND"),
            f.col("winner_ht").cast('int').alias("NUM_PLAYER_HEIGHT"),
            f.col("winner_ioc").alias("DES_PLAYER_COUNTRY"),
            f.date_format(f.to_date(f.col("tourney_date").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").alias("DATE_MATCH"),
            f.col("winner_age").cast("double").alias("NUM_PLAYER_AGE"),

            f.col("DATE_INGESTION")
        )
        .distinct()
    )


window_loser_name = Window.partitionBy("loser_name")  

losers = (
    final_tb_atp_matches 
    .select(
        f.coalesce(
            f.col("loser_id"),
            f.first("loser_id", ignorenulls=True).over(window_loser_name)
        ).cast("string").alias("COD_PLAYER_ID"),
        f.col("loser_name").alias("DES_PLAYER_NAME"),
        f.col("loser_hand").alias("DES_PLAYER_HAND"),
        f.col("loser_ht").cast("int").alias("NUM_PLAYER_HEIGHT"),
        f.col("loser_ioc").alias("DES_PLAYER_COUNTRY"),
        f.date_format(f.to_date(f.col("tourney_date").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").alias("DATE_MATCH"),
        f.col("loser_age").cast("double").alias("NUM_PLAYER_AGE"),

        f.col("DATE_INGESTION")
    )
)


from pyspark.sql.window import Window
window_spec = Window.partitionBy("COD_PLAYER_ID").orderBy(f.col("DATE_MATCH").desc())

df = (
    winners.unionByName(losers)
    .withColumn(
        "DATE_PLAYER_BIRTH",
        f.expr("date_sub(DATE_MATCH, cast(NUM_PLAYER_AGE * 365.25 as int))")
    )
    .withColumn("row_num", f.row_number().over(window_spec))
    .filter(f.col("row_num") == 1)

    .groupBy("COD_PLAYER_ID")
    .agg(
        f.first("DES_PLAYER_NAME").alias("DES_PLAYER_NAME"),
        f.first("DES_PLAYER_HAND").alias("DES_PLAYER_HAND"),
        f.first("NUM_PLAYER_HEIGHT").alias("NUM_PLAYER_HEIGHT"),
        f.first("DES_PLAYER_COUNTRY").alias("DES_PLAYER_COUNTRY"),
        f.first("DATE_PLAYER_BIRTH").alias("DATE_PLAYER_BIRTH"),

        f.first("DATE_INGESTION").alias("DATE_INGESTION")
    )
)


(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"../../data/silver/tb_atp_players")
)


(
df.write
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "silver.tb_atp_players")
    .option("user", os.getenv("DB_USER"))
    .option("password", os.getenv("DB_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

