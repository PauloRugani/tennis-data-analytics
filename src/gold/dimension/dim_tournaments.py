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
        SparkSession.builder.appName("dim_tournament")
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


# atp_tournaments = (
#     spark.read
#     .format("jdbc")
#     .option("url", os.getenv("JDBC_URL"))
#     .option("dbtable", "silver.tb_atp_tournaments")
#     .option("user", os.getenv("DB_USER"))
#     .option("password", os.getenv("DB_PASSWORD"))
#     .option("driver", "org.postgresql.Driver")
#     .load()
#     )

# atp_matches = (
#     spark.read
#     .format("jdbc")
#     .option("url", os.getenv("JDBC_URL"))
#     .option("dbtable", "silver.tb_atp_matches")
#     .option("user", os.getenv("DB_USER"))
#     .option("password", os.getenv("DB_PASSWORD"))
#     .option("driver", "org.postgresql.Driver")
#     .load()
# )

atp_tournaments = spark.read.format("parquet").load(r"../../../data/silver/tb_atp_tournaments/")
atp_matches = spark.read.format("parquet").load(r"../../../data/silver/tb_atp_matches/")


from datetime import datetime

window_tourney = Window.partitionBy("COD_TOURNEY_ID")

df = (
    atp_tournaments.alias("t")
    .join(atp_matches.alias("m"), "COD_TOURNEY_ID", "right")
    .withColumn(
        "MIN_DATE",
        f.coalesce(
            f.min(f.to_date(f.col("DATE_MATCH").cast("string"))).over(
                window_tourney
            ),
            f.to_date(f.col("m.DATE_MATCH").cast("string")),
        ),
    )
    .withColumn(
        "DATE_TOURNEY_START",
        f.date_format(f.date_trunc("week", f.col("MIN_DATE")), "yyyy-MM-dd"),
    )
    .withColumn(
        "DES_TOURNEY_STATUS",
        f.when(
            f.max(
                f.when(f.col("DES_MATCH_ROUND").isin("F", "RR"), 1)
                .when(f.col("REF_YEAR") < datetime.now().year, 1)
                .otherwise(0)
            ).over(window_tourney)
            == 1,
            "Finished",
        ).otherwise("In Progress"),
    )
    .select(
        f.col("COD_TOURNEY_ID"),
        f.col("DES_TOURNEY_NAME"),
        f.when(f.col("DES_TOURNEY_LEVEL") == "G", "Grand Slam")
        .when(f.col("DES_TOURNEY_LEVEL") == "M", "Masters 1000")
        .when(f.col("DES_TOURNEY_LEVEL") == "500", "ATP 500")
        .when(f.col("DES_TOURNEY_LEVEL") == "250", "ATP 250")
        .when(f.col("DES_TOURNEY_LEVEL") == "A", "Other")
        .when(f.col("DES_TOURNEY_LEVEL") == "F", "ATP Finals")
        .when(f.col("DES_TOURNEY_LEVEL") == "O", "Olympics Games")
        .when(f.col("DES_TOURNEY_LEVEL") == "D", "Davis Cup")
        .otherwise(f.lit("-"))
        .alias("DES_TOURNEY_LEVEL"),
        f.coalesce(f.col("m.NUM_TOURNEY_DRAW_SIZE"), f.lit(-1))
        .cast("int")
        .alias("NUM_TOURNEY_DRAW_SIZE"),
        f.coalesce(f.col("DES_TOURNEY_SURFACE"), f.lit("-")).alias(
            "DES_TOURNEY_SURFACE"
        ),
        f.when(f.col("FLAG_TOURNEY_IS_INDOOR") == True, "Yes")
        .when(f.col("FLAG_TOURNEY_IS_INDOOR") == False, "No")
        .otherwise(f.lit("-"))
        .alias("FLAG_TOURNEY_IS_INDOOR"),
        f.col("DES_TOURNEY_STATUS"),
        f.coalesce(f.col("DATE_TOURNEY_START"), f.lit("-")).alias(
            "DATE_TOURNEY_START"
        ),
        f.coalesce(f.col("REF_YEAR"), f.lit("-")).alias("REF_YEAR"),
        f.lit(f.current_date()).alias("DATE_LOAD")
    )
    .distinct()
    .withColumn("SK_TOURNEY", f.monotonically_increasing_id() + 1)
)


(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"../../../data/gold/dimension/dim_tournaments")
)


(
df.write
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "gold.dim_tournaments")
    .option("user", os.getenv("DB_USER"))
    .option("password", os.getenv("DB_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

