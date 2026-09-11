import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from itertools import chain
from dotenv import load_dotenv

load_dotenv()
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def create_spark_session():
    try:
        print("Creating spark session...")
        spark = (
            SparkSession.builder.appName("dim_players")
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
        tb_players = (
            spark.read
            .format("jdbc")
            .option("url", os.getenv("JDBC_URL"))
            .option("dbtable", "silver.tb_atp_players")
            .option("user", os.getenv("DB_USER"))
            .option("password", os.getenv("DB_PASSWORD"))
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        print("Data loaded")
        return tb_players
    except Exception as e:
        print(e)
        raise

def run_transformation(spark, tb_players):
    try:
        print("Running transformations...")
        country_mapping = {
            "ALG": "dz", "ARG": "ar", "ARM": "am", "AUS": "au", "AUT": "at",
            "AZE": "az", "BAH": "bs", "BAR": "bb", "BEL": "be", "BER": "bm",
            "BIH": "ba", "BLR": "by", "BOL": "bo", "BRA": "br", "BUL": "bg",
            "BUR": "bf", "CAN": "ca", "CAR": "cw", "CHI": "cl", "CHN": "cn",
            "CIV": "ci", "COL": "co", "CRC": "cr", "CRO": "hr", "CUB": "cu",
            "CUW": "cw", "CYP": "cy", "CZE": "cz", "DEN": "dk", "DOM": "do",
            "ECU": "ec", "EGY": "eg", "ESA": "sv", "ESP": "es", "EST": "ee",
            "FIN": "fi", "FRA": "fr", "FRG": "de", "GBR": "gb", "GDR": "de",
            "GEO": "ge", "GER": "de", "GRE": "gr", "HAI": "ht", "HKG": "hk",
            "HUN": "hu", "INA": "id", "IND": "in", "IRI": "ir", "IRL": "ie",
            "ISR": "il", "ITA": "it", "JAM": "jm", "JOR": "jo", "JPN": "jp",
            "KAZ": "kz", "KEN": "ke", "KOR": "kr", "KUW": "kw", "LAT": "lv",
            "LBN": "lb", "LTU": "lt", "LUX": "lu", "MAR": "ma", "MAS": "my",
            "MDA": "md", "MEX": "mx", "MKD": "mk", "MON": "mc", "NED": "nl",
            "NGR": "ng", "NIG": "ne", "NOR": "no", "NZL": "nz", "PAK": "pk",
            "PAN": "pa", "PAR": "py", "PER": "pe", "PHI": "ph", "POL": "pl",
            "POR": "pt", "PRT": "pt", "PUR": "pr", "QAT": "qa", "RHO": "zw",
            "ROU": "ro", "RSA": "za", "RUS": "ru", "SEN": "sn", "SLO": "si",
            "SRB": "rs", "SRI": "lk", "SUD": "sd", "SUI": "ch", "SUR": "sr",
            "SVK": "sk", "SVN": "si", "SWE": "se", "TCH": "cz", "THA": "th",
            "TPE": "tw", "TUN": "tn", "TUR": "tr", "UAE": "ae", "UKR": "ua",
            "UNK": "un", "URS": "ru", "URU": "uy", "USA": "us", "UZB": "uz",
            "VEN": "ve", "VIE": "vn", "YUG": "rs", "ZIM": "zw"
        }

        mapping_expr = f.create_map([f.lit(x) for x in chain(*country_mapping.items())])

        df = (
            tb_players 
            .withColumn(
                "DES_PLAYER_HAND",
                f.when(f.col("DES_PLAYER_HAND") == 'L', "Left-Handed")
                .when(f.col("DES_PLAYER_HAND") == 'R', "Right-Handed")
                .when(f.col("DES_PLAYER_HAND") == 'A', "Ambidextrous")
                .when(f.col("DES_PLAYER_HAND") == 'U', "Unknown")
                .otherwise("Unknown")
            )
            .withColumn(
                "DES_PLAYER_COUNTRY_ISO2",
                mapping_expr[f.upper(f.trim(f.col("DES_PLAYER_COUNTRY")))]
            )
            .groupBy(f.initcap(f.trim(f.regexp_replace(f.col("DES_PLAYER_NAME"), "-", " "))).alias("DES_PLAYER_NAME"))
            .agg(
                f.last("COD_PLAYER_ID").alias("COD_PLAYER_ID"),
                f.first("COD_PLAYER_ID").alias("COD_PLAYER_ID_OLD"),
                f.last("DES_PLAYER_HAND").alias("DES_PLAYER_HAND"),
                f.last("NUM_PLAYER_HEIGHT").alias("NUM_PLAYER_HEIGHT"),
                f.last("DES_PLAYER_COUNTRY").alias("DES_PLAYER_COUNTRY"),
                f.last("DES_PLAYER_COUNTRY_ISO2").alias("DES_PLAYER_COUNTRY_ISO2"),
                f.last("DATE_PLAYER_BIRTH").alias("DATE_PLAYER_BIRTH"),
            )
            .distinct()
            .withColumn("SK_PLAYER", f.monotonically_increasing_id() + 1)
            .withColumn("DATE_LOAD", f.lit(f.current_date()))
            .select(
                "SK_PLAYER",
                f.col("DES_PLAYER_NAME"),
                f.col("COD_PLAYER_ID").cast('string'),
                f.col("COD_PLAYER_ID_OLD").cast('string'),
                f.col("DES_PLAYER_HAND").cast('string'),
                f.col("NUM_PLAYER_HEIGHT").cast('int'),
                f.col("DES_PLAYER_COUNTRY").cast('string'),
                f.col("DES_PLAYER_COUNTRY_ISO2").cast('string'),
                f.to_date(f.col("DATE_PLAYER_BIRTH"), 'yyyy-MM-dd').alias("DATE_PLAYER_BIRTH"),
                f.col("DATE_LOAD")
            )
        )
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
            .option("dbtable", "gold.dim_players")
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
        tb_players = load_tables(spark)
        df_final = run_transformation(spark, tb_players)
        save_table(df_final)
    finally:
        if spark:
            print("Stopping spark session...")
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    run()
