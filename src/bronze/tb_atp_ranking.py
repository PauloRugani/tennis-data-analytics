from pyspark.sql import SparkSession
from pyspark.sql import functions as f

import pandas as pd

import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from dotenv import load_dotenv
load_dotenv()


try:
    spark = (
        SparkSession.builder.appName("atp_ranking")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
except Exception as e:
    print(e)


try:
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
    table_exists = True
    save_mode = "append"
except:
    table_exists = False
    save_mode = "overwrite"


from datetime import datetime
tb_incremental_rankings = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load(fr"data/raw/incremental/tb_incremental_ranking_{datetime.now().year}.csv")
)


historical_matches = r"data/raw/historical/ranking"


if table_exists:
    df = tb_atp_rankings
else:
    for index, file_name in enumerate(os.listdir(historical_matches)):
        final_path = os.path.join(historical_matches, file_name)
        match_data = spark.read.format("csv").option("header", "true").load(final_path)
        if index == 0:
            df = match_data 
        else:
            df = df.unionByName(match_data, allowMissingColumns=True)


new_rankings = (
    df.alias("h_r")
    .join(
        tb_incremental_rankings.alias("i_r"),
        [
            f.col("h_r.date") == f.col("i_r.date"), 
            f.col("h_r.name") == f.col("i_r.name")
        ],
        'right'
        )
    .where(f.col("h_r.date").isNull())
    .select(
        "i_r.*",
    )
)


if table_exists:
    df_final = new_rankings.withColumn("DATE_INGESTION", f.lit(f.current_date()))
else:
    df_final = df.unionByName(new_rankings, allowMissingColumns=True).withColumn("DATE_INGESTION", f.lit(f.current_date()))


if df_final.count() > 0:
    (
        df_final.write
        .mode(save_mode)
        .option("compression", "snappy")
        .parquet(r"data/bronze/tb_atp_rankings")
    )

    (
        df_final.write
        .format("jdbc")
        .option("url", os.getenv("JDBC_URL"))
        .option("dbtable", "bronze.tb_atp_rankings")
        .option("user", os.getenv("DB_USER"))
        .option("password", os.getenv("DB_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .mode(save_mode)
        .save()
    )    

