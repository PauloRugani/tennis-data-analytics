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
        SparkSession.builder.appName("silver_atp_tournament")
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


# tb_atp_matches = spark.read.format("parquet").load(r"data/bronze/tb_atp_matches/")


df = (
    tb_atp_matches
    .withColumn(
        "TOURNEY_NAME",
        f.when(f.col("tourney_name").contains("Davis Cup"), f.lit("Davis Cup"))
         .when(f.col("tourney_id") == "2026-416", f.lit("Rome Masters"))
         .otherwise(f.col("tourney_name"))
    )
    .withColumn(
        "TOURNEY_LEVEL",
        f.when(f.col("tourney_id") == "2026-416", f.lit("M"))
         .when(f.col("tourney_name").contains("Olympics"), f.lit("O"))
         .when(f.col("tourney_name").contains("Finals"), f.lit("F"))
         .otherwise(f.col("tourney_level"))
    )
    .withColumn(
        "TOURNEY_IS_INDOOR",
        f.when(f.col("indoor") == "I", f.lit(True))
         .when(f.col("surface") == "Carpet", f.lit(True))
         .when(f.col("indoor") == "O", f.lit(False))
         .when(f.col("tourney_name").contains("Indoor"), f.lit(True))
         .otherwise(f.lit(False))
    )
    .groupBy(f.col("tourney_id").alias("COD_TOURNEY_ID"))
    .agg(
        f.first("TOURNEY_NAME", ignorenulls=True).alias("DES_TOURNEY_NAME"),
        f.first("TOURNEY_LEVEL", ignorenulls=True).alias("DES_TOURNEY_LEVEL"),
        f.first(f.col("draw_size").cast("int"), ignorenulls=True).cast("int").alias("NUM_TOURNEY_DRAW_SIZE"),
        f.first("surface", ignorenulls=True).alias("DES_TOURNEY_SURFACE"),
        f.first("TOURNEY_IS_INDOOR", ignorenulls=True).alias("FLAG_TOURNEY_IS_INDOOR"),
        f.substring(f.first("tourney_date", ignorenulls=True).cast("string"), 1, 4).alias("REF_YEAR"),
        f.first("DATE_INGESTION").alias("DATE_INGESTION")
    )
    .dropDuplicates(["COD_TOURNEY_ID"])
)


(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"../../data/silver/tb_atp_tournaments")
)


(
df.write
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "silver.tb_atp_tournaments")
    .option("user", os.getenv("DB_USER"))
    .option("password", os.getenv("DB_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

