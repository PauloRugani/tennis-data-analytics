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
        SparkSession.builder.appName("fact_player_season")
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


# silver
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

# gold
tb_tournaments = (
    spark.read
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "gold.dim_tournaments")
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

# tb_player_match = spark.read.format("parquet").load(r"../../../data/silver/tb_atp_player_match/")
# tb_tournaments = spark.read.format("parquet").load(r"../../../data/gold/dimension/dim_tournaments/")
# tb_players = spark.read.format("parquet").load(r"../../../data/gold/dimension/dim_players/")


round_order = (
    f.when(f.col("DES_MATCH_ROUND") == "F", 11) # Final
     .when(f.col("DES_MATCH_ROUND") == "SF", 10) # Semi Final
     .when(f.col("DES_MATCH_ROUND") == "BR", 9) # Third Place
     .when(f.col("DES_MATCH_ROUND") == "QF", 8) # Quarte final
     .when(f.col("DES_MATCH_ROUND") == "R16", 7) # Round of 16
     .when(f.col("DES_MATCH_ROUND") == "R32", 6) # Round of 32
     .when(f.col("DES_MATCH_ROUND") == "R64", 5) # Round of 64
     .when(f.col("DES_MATCH_ROUND") == "R128", 4) # Round of 128
     .when(f.col("DES_MATCH_ROUND") == "ER", 3) # Early Round
     .when(f.col("DES_MATCH_ROUND") == "RR", 2) # Round Robin
     .otherwise(1)
)

df = (
    tb_player_match.alias("p_m")
    .join(tb_tournaments.alias("t"), "COD_TOURNEY_ID", 'left')
    .join(
        tb_players.alias("p"),
        (f.col("p_m.COD_PLAYER_ID").cast("string") == f.col("p.COD_PLAYER_ID").cast("string"))
        | (
            f.col("p_m.COD_PLAYER_ID").cast("string")
            == f.col("p.COD_PLAYER_ID_OLD").cast("string")
        ),
        "left",
    )
    .groupBy(f.col("SK_PLAYER"), f.concat(f.substring(f.col("DATE_MATCH"), 1, 4), f.lit('0101')).alias("SK_DATE"))
    .agg(
        f.count("COD_MATCH_ID").alias("NUM_TOTAL_MATCHES"),
        f.countDistinct("COD_TOURNEY_ID").alias("NUM_TOTAL_TOURNAMENTS"),
        f.sum(f.when(f.col("FLAG_PLAYER_IS_WINNER") == True, f.lit(1)).otherwise(f.lit(0))).alias("NUM_TOTAL_WINS"),
        f.sum(f.when(f.col("FLAG_PLAYER_IS_WINNER") == False, f.lit(1)).otherwise(f.lit(0))).alias("NUM_TOTAL_LOSSES"),

        f.sum(
            f.when(
                (f.col("FLAG_PLAYER_IS_WINNER") == True) & (f.col("DES_MATCH_ROUND") == 'F'), f.lit(1)
                ).otherwise(0)
        ).alias("NUM_TOTAL_TITLES"),

        f.coalesce(f.max_by(
            f.when(f.col("DES_TOURNEY_NAME") == "Australian Open", f.col("DES_MATCH_ROUND")), 
            f.when(f.col("DES_TOURNEY_NAME") == "Australian Open", round_order)
        ), f.lit('-')).alias("DES_AUS_OPEN_RESULT"),
        
        f.coalesce(f.max_by(
            f.when(f.col("DES_TOURNEY_NAME") == "Roland Garros", f.col("DES_MATCH_ROUND")), 
            f.when(f.col("DES_TOURNEY_NAME") == "Roland Garros", round_order)
        ), f.lit('-')).alias("DES_ROLAND_GARROS_RESULT"),
        
        f.coalesce(f.max_by(
            f.when(f.col("DES_TOURNEY_NAME") == "Wimbledon", f.col("DES_MATCH_ROUND")), 
            f.when(f.col("DES_TOURNEY_NAME") == "Wimbledon", round_order)
        ), f.lit('-')).alias("DES_WIMBLEDON_RESULT"),
        
        f.coalesce(f.max_by(
            f.when(f.col("DES_TOURNEY_NAME") == "US Open", f.col("DES_MATCH_ROUND")), 
            f.when(f.col("DES_TOURNEY_NAME") == "US Open", round_order)
        ), f.lit('-')).alias("DES_US_OPEN_RESULT"),

        f.min_by(f.col("NUM_PLAYER_RANK"), f.col("DATE_MATCH")).cast('int').alias("NUM_PLAYER_START_RANK"),
        f.max_by(f.col("NUM_PLAYER_RANK"), f.col("DATE_MATCH")).cast('int').alias("NUM_PLAYER_FINAL_RANK"),
        f.min(f.col("NUM_PLAYER_RANK").cast('int')).alias("NUM_PLAYER_BEST_RANK"),
        f.max(f.col("NUM_PLAYER_RANK").cast('int')).alias("NUM_PLAYER_WORST_RANK"),

        f.min_by(f.col("NUM_PLAYER_RANK_PTS"), f.col("DATE_MATCH")).cast('int').alias("NUM_PLAYER_START_RANK_PTS"),
        f.max_by(f.col("NUM_PLAYER_RANK_PTS"), f.col("DATE_MATCH")).cast('int').alias("NUM_PLAYER_FINAL_RANK_PTS"),
        f.max(f.col("NUM_PLAYER_RANK_PTS").cast('int')).alias("NUM_PLAYER_BEST_RANK_PTS"),
        f.min(f.col("NUM_PLAYER_RANK_PTS").cast('int')).alias("NUM_PLAYER_WORST_RANK_PTS"),

        f.lit(f.current_date()).alias("DATE_LOAD")
    )
    .orderBy("SK_DATE")
)


(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"../../../data/gold/fact/fact_player_season")
)


(
df.write
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "gold.fact_player_season")
    .option("user", os.getenv("DB_USER"))
    .option("password", os.getenv("DB_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

