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
        SparkSession.builder.appName("dim_entry")
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

# tb_player_match = spark.read.format("parquet").load(r"../../../data/silver/tb_atp_player_match/")


df = (
    tb_player_match
    .select("COD_PLAYER_ENTRY")
    .distinct()
    .withColumn(
        "DES_ENTRY_TYPE",
        f.when(f.col("COD_PLAYER_ENTRY") == "WC", f.lit("Wild Card"))
        .when(f.col("COD_PLAYER_ENTRY") == "Q", f.lit("Qualifier"))
        .when(f.col("COD_PLAYER_ENTRY") == "LL", f.lit("Lucky Loser"))
        .when(f.col("COD_PLAYER_ENTRY") == "ITF", f.lit("ITF Entry"))
        .when(f.col("COD_PLAYER_ENTRY") == "UP", f.lit("Next Gen / Unranked Performance"))
        .when(f.col("COD_PLAYER_ENTRY") == "W", f.lit("Wild Card"))
        .when(f.col("COD_PLAYER_ENTRY") == "SE", f.lit("Special Exempt"))
        .when(f.col("COD_PLAYER_ENTRY") == "PR", f.lit("Protected Ranking"))
        .when(f.col("COD_PLAYER_ENTRY") == "S", f.lit("Exempt Special / Special"))
        .when(f.col("COD_PLAYER_ENTRY") == "NG", f.lit("Next Gen Accelerator"))
        .otherwise(f.lit("Direct Acceptance / Regular"))
    )
    .distinct()
    .withColumn("SK_ENTRY_TYPE", f.monotonically_increasing_id() + 1)
    .select(
        f.col("SK_ENTRY_TYPE"),
        f.col("COD_PLAYER_ENTRY"),
        f.col("DES_ENTRY_TYPE"),
        f.lit(f.current_date()).alias("DATE_LOAD")
    )
)


(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"../../../data/gold/dimension/dim_entry")
)


(
df.write
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "gold.dim_entry")
    .option("user", os.getenv("DB_USER"))
    .option("password", os.getenv("DB_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

