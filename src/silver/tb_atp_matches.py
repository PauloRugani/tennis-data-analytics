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
        SparkSession.builder.appName("silver_atp_matches")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
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

# tb_atp_matches = spark.read.format("parquet").load(r"data/bronze/tb_atp_matches/")


from pyspark.sql import functions as f
from pyspark.sql.window import Window

matches_cleaned = (
    tb_atp_matches
    .withColumn(
        "loser_id",
        f.when(
            f.col("loser_id").isNull() & f.col("loser_name").contains("Alejandro Davidovich Fokina"), '200221'
        ).when(
            f.col("loser_id").isNull() & f.col("loser_name").contains("Shevchenko"), '207686'
        ).when(
            f.col("loser_id").isNull() & f.col("loser_name").contains("Jesper De Jong"), '207411'
        ).otherwise(f.col("loser_id"))
    )
    .withColumn(
        'tourney_id',
        f.when(
            (f.col("tourney_id") == '2026-416') & (f.col('tourney_name') == 'Munich'),
            f.lit("2026-308")
        ).otherwise(f.col("tourney_id"))
    )
)

window_spec = Window.partitionBy("tourney_id").orderBy(
    f.to_date(f.col("tourney_date").cast("string"), "yyyyMMdd").asc(),
    f.col("match_num").asc_nulls_last()
)

df = (
    matches_cleaned
    .withColumn(
        "match_num",
        f.coalesce(
            f.col("match_num").cast("int"),
            f.row_number().over(window_spec)
        )
    )
    .alias('m')
        
    .withColumn("COD_MATCH_ID", f.concat_ws('-', f.col("m.tourney_id"), f.col("m.match_num")))

    .select(
        # match / tourney columns
        f.col("COD_MATCH_ID"),
        f.col("m.tourney_id").alias("COD_TOURNEY_ID"),
        f.col("m.draw_size").cast("int").alias("NUM_TOURNEY_DRAW_SIZE"),
        f.date_format(f.to_date(f.col("m.tourney_date").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").alias("DATE_MATCH"),
        f.col("m.match_num").cast("int").cast('int').alias("NUM_MATCH"),
        f.col("m.score").alias("DES_MATCH_SCORE"),
        f.col("m.best_of").cast("int").alias("NUM_MATCH_BEST_OF"),
        f.col("m.round").alias("DES_MATCH_ROUND"),
        f.col("m.minutes").cast("int").alias("NUM_MATCH_DURATION_M"),

        # winner columns
        f.col("m.winner_id").cast('string').alias("COD_W_ID"),
        f.col("m.winner_rank").cast("int").alias("NUM_W_RANK"),
        f.col("m.winner_rank_points").cast("int").alias("NUM_W_RANK_PTS"),
        f.col("m.winner_seed").alias("NUM_W_SEED"),
        f.upper(f.col("m.winner_entry")).alias("COD_W_ENTRY"),
        f.col("m.w_ace").cast("int").alias("NUM_W_ACES"),
        f.col("m.w_df").cast("int").alias("NUM_W_DB_FAULTS"),
        f.col("m.w_svpt").cast("int").alias("NUM_W_SERVE_PTS"),
        f.col("m.w_1stIn").cast("int").alias("NUM_W_1ST_SERVES_IN"),
        f.col("m.w_1stWon").cast("int").alias("NUM_W_1ST_SERVE_PTS_WON"),
        f.col("m.w_2ndWon").cast("int").alias("NUM_W_2ND_SERVE_PTS_WON"),
        f.col("m.w_SvGms").cast("int").alias("NUM_W_SERVE_GAMES"),
        f.col("m.w_bpSaved").cast("int").alias("NUM_W_BP_SAVED"),
        f.col("m.w_bpFaced").cast("int").alias("NUM_W_BP_FACED"),
        
        # loser columns
        f.col("m.loser_id").cast("string").alias("COD_L_ID"),
        f.col("m.loser_rank").cast("int").alias("NUM_L_RANK"),
        f.col("m.loser_rank_points").cast("int").alias("NUM_L_RANK_PTS"),
        f.col("m.loser_seed").alias("NUM_L_SEED"),
        f.upper(f.col("m.loser_entry")).alias("COD_L_ENTRY"),
        f.col("m.l_ace").cast("int").alias("NUM_L_ACES"),
        f.col("m.l_df").cast("int").alias("NUM_L_DB_FAULTS"),
        f.col("m.l_svpt").cast("int").alias("NUM_L_SERVE_PTS"),
        f.col("m.l_1stIn").cast("int").alias("NUM_L_1ST_SERVES_IN"),
        f.col("m.l_1stWon").cast("int").alias("NUM_L_1ST_SERVE_PTS_WON"),
        f.col("m.l_2ndWon").cast("int").alias("NUM_L_2ND_SERVE_PTS_WON"),
        f.col("m.l_SvGms").cast("int").alias("NUM_L_SERVE_GAMES"),
        f.col("m.l_bpSaved").cast("int").alias("NUM_L_BP_SAVED"),
        f.col("m.l_bpFaced").cast("int").alias("NUM_L_BP_FACED"),

        f.col("m.DATE_INGESTION").alias("DATE_INGESTION")
    )
    .orderBy(["DATE_MATCH", "NUM_MATCH"], ascending=False)
    .dropDuplicates(["COD_MATCH_ID"])
)


(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"data/silver/tb_atp_matches")
)


(
df.write
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "silver.tb_atp_matches")
    .option("user", os.getenv("DB_USER"))
    .option("password", os.getenv("DB_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

