import os
import sys
import pandas as pd
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils.pyspark_handler import PySparkHandler

load_dotenv()
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def load_tables(handler):
    try:
        atp_tournaments = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/silver/tb_atp_tournaments/",
            format="parquet"
        )
        atp_matches = handler.load_data(
            spark=handler.spark,
            path="s3a://tennis-data-lake/silver/tb_atp_matches/",
            format="parquet"
        )
        return atp_tournaments, atp_matches
    except Exception as e:
        print(e)
        raise

def run_transformation(handler, atp_tournaments, atp_matches):
    try:
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
        return df
    except Exception as e:
        print(e)
        raise

def save_table(handler, df):
    try:
        handler.save_data(
            df=df,
            path="s3a://tennis-data-lake/gold/dimension/dim_tournaments/",
            format="parquet",
            mode="overwrite"
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
    except Exception as e:
        print(e)
        raise

def run():
    handler = None
    try:
        handler = PySparkHandler(app_name="dim_tournament")
        atp_tournaments, atp_matches = load_tables(handler)
        df_final = run_transformation(handler, atp_tournaments, atp_matches)
        save_table(handler, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run()
