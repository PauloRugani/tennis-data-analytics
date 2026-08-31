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
        SparkSession.builder.appName("dim_date")
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

# tb_player_match = spark.read.format("parquet").load(r"data/silver/tb_atp_player_match/")


start_date = '1960-01-01'
end_date = tb_player_match.select(f.max("DATE_MATCH")).collect()[0][0]


df_date = (
    spark.range(1)
    .select(
        f.sequence(
            f.to_date(f.lit(start_date)),
            f.to_date(f.lit(end_date)),
            f.expr("INTERVAL 1 DAY"),
        ).alias("DATE")
    )
    .select(f.explode("DATE").alias("DATE"))
)


df = (
    df_date
    .withColumn("SK_DATE", f.date_format("DATE", "yyyyMMdd").cast("int"))
    .withColumn("NUM_YEAR", f.year("DATE"))
    .withColumn("NUM_MONTH", f.month("DATE"))
    .withColumn("DES_MONTH", f.date_format("DATE", "MMMM"))
    .withColumn("DES_MONTH_SHORT", f.date_format("DATE", "MMM"))
    .withColumn("NUM_DAY", f.dayofmonth("DATE"))
    .withColumn("DES_DAY", f.date_format("DATE", "EEEE"))
    .withColumn("DATE_LOAD", f.lit(f.current_date()))
)


(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(r"data/gold/dimension/dim_date")
)


(
df.write
    .format("jdbc")
    .option("url", os.getenv("JDBC_URL"))
    .option("dbtable", "gold.dim_date")
    .option("user", os.getenv("DB_USER"))
    .option("password", os.getenv("DB_PASSWORD"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

