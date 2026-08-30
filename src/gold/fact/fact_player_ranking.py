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
        SparkSession.builder.appName("fact_player_ranking")
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


# # silver
# tb_atp_rankings = (
#     spark.read
#     .format("jdbc")
#     .option("url", os.getenv("JDBC_URL"))
#     .option("dbtable", "silver.tb_atp_rankings")
#     .option("user", os.getenv("DB_USER"))
#     .option("password", os.getenv("DB_PASSWORD"))
#     .option("driver", "org.postgresql.Driver")
#     .load()
#     )

# # gold
# tb_date = (
#     spark.read
#     .format("jdbc")
#     .option("url", os.getenv("JDBC_URL"))
#     .option("dbtable", "gold.dim_date")
#     .option("user", os.getenv("DB_USER"))
#     .option("password", os.getenv("DB_PASSWORD"))
#     .option("driver", "org.postgresql.Driver")
#     .load()
#     )

# tb_players = (
#     spark.read
#     .format("jdbc")
#     .option("url", os.getenv("JDBC_URL"))
#     .option("dbtable", "gold.dim_players")
#     .option("user", os.getenv("DB_USER"))
#     .option("password", os.getenv("DB_PASSWORD"))
#     .option("driver", "org.postgresql.Driver")
#     .load()
#     )

tb_atp_rankings = spark.read.format("parquet").load(r"../../../data/silver/tb_atp_rankings/")
tb_date = spark.read.format("parquet").load(r"../../../data/gold/dimension/dim_date/")
tb_players = spark.read.format("parquet").load(r"../../../data/gold/dimension/dim_players/")
tb_atp_rankings_2 = spark.read.format("parquet").load(r"../../../data/bronze/tb_atp_rankings/")


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


(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"../../../data/gold/fact/fact_player_ranking")
)


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

