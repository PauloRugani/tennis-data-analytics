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
        SparkSession.builder.appName("fact_player_tournament")
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

tb_entry = (
    spark.read
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "gold.dim_entry")
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

# tb_player_match = spark.read.format("parquet").load(r"data/silver/tb_atp_player_match/")
# tb_tournaments = spark.read.format("parquet").load(r"data/gold/dimension/dim_tournaments/")
# tb_date = spark.read.format("parquet").load(r"data/gold/dimension/dim_date/")
# tb_entry = spark.read.format("parquet").load(r"data/gold/dimension/dim_entry/")
# tb_players = spark.read.format("parquet").load(r"data/gold/dimension/dim_players/")


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

tourney_stats = (
    tb_player_match
    .groupBy("COD_TOURNEY_ID", "COD_PLAYER_ID")
    .agg(
        f.first("COD_PLAYER_ENTRY").alias("COD_PLAYER_ENTRY"),
        f.coalesce(f.first("NUM_PLAYER_SEED").cast('int'), f.lit(-1)).alias("NUM_PLAYER_SEED"),
        f.coalesce(f.first("NUM_PLAYER_RANK_PTS").cast('int'), f.lit(0)).alias("NUM_PLAYER_RANK_PTS"),
        f.coalesce(f.first("NUM_PLAYER_RANK").cast('int'), f.lit(0)).alias("NUM_PLAYER_RANK"),

        f.max(
            f.when((f.col("DES_MATCH_ROUND") == "F") & (f.col("FLAG_PLAYER_IS_WINNER") == True), 1).otherwise(0)
        ).alias("FLAG_IS_CHAMPION"),
        
        f.max_by(f.col("DES_MATCH_ROUND"), round_order).alias("DES_LAST_ROUND_PLAYED"),

        f.count("COD_MATCH_ID").alias("NUM_TOTAL_MATCHES"),
        f.coalesce(f.sum(f.col("NUM_MATCH_DURATION_M").cast("int")), f.lit(0)).alias("NUM_TOTAL_MIN_IN_GAME"),
        f.coalesce(f.max(f.col("NUM_MATCH_DURATION_M").cast("int")), f.lit(0)).alias("NUM_LONGEST_MATCH"),


        f.coalesce(f.sum(f.col("NUM_PLAYER_ACES").cast('int')), f.lit(0)).alias("NUM_TOTAL_ACES"), 
        f.coalesce(f.sum(f.col("NUM_PLAYER_DB_FAULTS").cast('int')), f.lit(0)).alias("NUM_TOTAL_DB_FAULTS"), 
        f.coalesce(f.sum(f.col("NUM_PLAYER_SERVE_PTS").cast('int')), f.lit(0)).alias("NUM_TOTAL_SERVE_PTS"), 
        f.coalesce(f.sum(f.col("NUM_PLAYER_1ST_SERVES_IN").cast('int')), f.lit(0)).alias("NUM_TOTAL_1ST_SERVES_IN"), 
        f.coalesce(f.sum(f.col("NUM_PLAYER_1ST_SERVE_PTS_WON").cast('int')), f.lit(0)).alias("NUM_TOTAL_1ST_SERVE_PTS_WON"), 
        f.coalesce(f.sum(f.col("NUM_PLAYER_2ND_SERVE_PTS_WON").cast('int')), f.lit(0)).alias("NUM_TOTAL_2ND_SERVE_PTS_WON"), 
        f.coalesce(f.sum(f.col("NUM_PLAYER_SERVE_GAMES").cast('int')), f.lit(0)).alias("NUM_TOTAL_SERVE_GAMES"), 
        f.coalesce(f.sum(f.col("NUM_PLAYER_BP_SAVED").cast('int')), f.lit(0)).alias("NUM_TOTAL_BP_SAVED"), 
        f.coalesce(f.sum(f.col("NUM_PLAYER_BP_FACED").cast('int')), f.lit(0)).alias("NUM_TOTAL_BP_FACED"),

        f.first("DATE_INGESTION").alias("DATE_INGESTION")
    )
)


df = (
    tourney_stats.alias("s")
    .join(tb_tournaments.alias("t"), f.expr("s.COD_TOURNEY_ID <=> t.COD_TOURNEY_ID"), 'left')
    .join(
        tb_players.alias("p"),
        (f.col("s.COD_PLAYER_ID").cast("string") == f.col("p.COD_PLAYER_ID").cast("string"))
        | (
            f.col("s.COD_PLAYER_ID").cast("string")
            == f.col("p.COD_PLAYER_ID_OLD").cast("string")
        ),
        "left",
    )
    .join(tb_entry.alias("e"), f.expr("s.COD_PLAYER_ENTRY <=> e.COD_PLAYER_ENTRY"), 'left')
    
    .withColumn("SK_DATE", f.concat(f.substring(f.col("s.COD_TOURNEY_ID"), 1, 4), f.lit('0101')))

    .select(
        f.col("p.SK_PLAYER"),
        f.col("t.SK_TOURNEY"),
        f.col("e.SK_ENTRY_TYPE"),
        f.col("SK_DATE"),

        f.col("s.NUM_PLAYER_SEED"),
        f.col("s.NUM_PLAYER_RANK_PTS"),
        f.col("s.NUM_PLAYER_RANK"),
        f.col("s.FLAG_IS_CHAMPION"),
        f.col("s.DES_LAST_ROUND_PLAYED"),
        f.col("s.NUM_TOTAL_MATCHES"),
        f.col("s.NUM_TOTAL_MIN_IN_GAME"),
        f.col("s.NUM_LONGEST_MATCH"),
        f.col("s.NUM_TOTAL_ACES"), 
        f.col("s.NUM_TOTAL_DB_FAULTS"), 
        f.col("s.NUM_TOTAL_SERVE_PTS"), 
        f.col("s.NUM_TOTAL_1ST_SERVES_IN"), 
        f.col("s.NUM_TOTAL_1ST_SERVE_PTS_WON"), 
        f.col("s.NUM_TOTAL_2ND_SERVE_PTS_WON"), 
        f.col("s.NUM_TOTAL_SERVE_GAMES"), 
        f.col("s.NUM_TOTAL_BP_SAVED"), 
        f.col("s.NUM_TOTAL_BP_FACED"),
    
        f.lit(f.current_date()).alias("DATE_LOAD")
    )
    .distinct()
)


(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"data/gold/fact/fact_player_tournament_stats")
)


(
df.write
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "gold.fact_player_tournament_stats")
    .option("user", os.getenv("DB_USER"))
    .option("password", os.getenv("DB_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

