import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from dotenv import load_dotenv

load_dotenv()
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def create_spark_session():
    try:
        print("Creating spark session...")
        spark = (
            SparkSession.builder.appName("fact_player_match_stats")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .getOrCreate()
        )
        
        spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
        spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 200)
        spark.conf.set("spark.sql.repl.eagerEval.truncate", 50)
        return spark
    except Exception as e:
        print(e)
        raise

def load_tables(spark):
    try:
        print("Loading data...")
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
        print("Data loaded")
        return tb_player_match, tb_tournaments, tb_date, tb_players

    except Exception as e:
        print(e)
        raise

def run_transformation(spark, tb_player_match, tb_tournaments, tb_date, tb_players):
    try:
        print("Running transformations...")
        df = (
            tb_player_match.alias("p_m")
            .join(tb_tournaments.alias("t"), f.expr("p_m.COD_TOURNEY_ID <=> t.COD_TOURNEY_ID"), 'left')
            .join(
                tb_players.alias("pw"),
                (f.col("p_m.COD_PLAYER_ID").cast("string") == f.col("pw.COD_PLAYER_ID").cast("string"))
                | (
                    f.col("p_m.COD_PLAYER_ID").cast("string")
                    == f.col("pw.COD_PLAYER_ID_OLD").cast("string")
                ),
                "left",
            )
            .join(
                tb_players.alias("po"),
                (f.col("p_m.COD_PLAYER_OPPONENT_ID").cast("string") == f.col("po.COD_PLAYER_ID").cast("string"))
                | (
                    f.col("p_m.COD_PLAYER_OPPONENT_ID").cast("string")
                    == f.col("po.COD_PLAYER_ID_OLD").cast("string")
                ),
                "left",
            )
            .join(
                tb_date.alias("d"),
                f.col("p_m.DATE_MATCH") == f.col("d.DATE")
            )
            .select(
                f.col("p_m.COD_MATCH_ID"),
                f.col("pw.SK_PLAYER"),
                f.col("po.SK_PLAYER").alias("SK_PLAYER_OPPONENT"),
                f.col("t.SK_TOURNEY"),
                f.col("d.SK_DATE"),
                
                f.col("p_m.FLAG_PLAYER_IS_WINNER").alias("FLAG_PLAYER_IS_WINNER"),
                f.coalesce(f.col("p_m.NUM_MATCH"), f.lit(0)).alias("NUM_MATCH"),
                f.coalesce(f.col("p_m.DES_MATCH_SCORE"), f.lit('-')).alias("DES_MATCH_SCORE"),
                f.coalesce(f.col("p_m.NUM_MATCH_BEST_OF"), f.lit(3)).alias("NUM_MATCH_BEST_OF"),
                f.coalesce(f.col("p_m.DES_MATCH_ROUND"), f.lit('-')).alias("DES_MATCH_ROUND"), 
                f.coalesce(f.col("p_m.NUM_MATCH_DURATION_M"), f.lit(0)).alias("NUM_MATCH_DURATION_M"),
                f.coalesce(f.col("p_m.NUM_PLAYER_ACES"), f.lit(0)).alias("NUM_PLAYER_ACES"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_DB_FAULTS"), f.lit(0)).alias("NUM_PLAYER_DB_FAULTS"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_SERVE_PTS"), f.lit(0)).alias("NUM_PLAYER_SERVE_PTS"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_1ST_SERVES_IN"), f.lit(0)).alias("NUM_PLAYER_1ST_SERVES_IN"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_1ST_SERVE_PTS_WON"), f.lit(0)).alias("NUM_PLAYER_1ST_SERVE_PTS_WON"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_2ND_SERVE_PTS_WON"), f.lit(0)).alias("NUM_PLAYER_2ND_SERVE_PTS_WON"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_SERVE_GAMES"), f.lit(0)).alias("NUM_PLAYER_SERVE_GAMES"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_BP_SAVED"), f.lit(0)).alias("NUM_PLAYER_BP_SAVED"), 
                f.coalesce(f.col("p_m.NUM_PLAYER_BP_FACED"), f.lit(0)).alias("NUM_PLAYER_BP_FACED"),

                f.lit(f.current_date()).alias("DATE_LOAD")
            )
            .distinct()
        )

        if tb_player_match.count() == df.count():
            print('ok')
        else: 
            raise Exception("Count mismatch between source and transformed data")
            
        print("Transformations completed")
        return df
    except Exception as e:
        print(e)
        raise

def save_table(df):
    try:
        print("Saving data...")
        (
            df.write
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "gold.fact_player_match_stats")
            .option("user", os.getenv("DB_USER"))
            .option("password", os.getenv("DB_PASSWORD"))
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
        print("Data saved to database")
    except Exception as e:
        print(e)
        raise

def run():
    spark = None
    try:
        spark = create_spark_session()
        tb_player_match, tb_tournaments, tb_date, tb_players = load_tables(spark)
        df_final = run_transformation(spark, tb_player_match, tb_tournaments, tb_date, tb_players)
        save_table(df_final)
    finally:
        if spark:
            print("Stopping spark session...")
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    run()
