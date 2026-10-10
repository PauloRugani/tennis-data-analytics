import os
import sys
from pyspark.sql import functions as f
from itertools import chain
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.pyspark_handler import PySparkHandler
from src.utils.logger import get_logger

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

def load_tables(handler, bucket_name):
    try:
        logger.info("Loading dim_players source table...")
        tb_players = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/silver/tb_atp_players/",
            format="parquet"
        )
        tb_atp_player_match = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/silver/tb_atp_player_match/",
            format="parquet"
        )
        return tb_players, tb_atp_player_match
    except Exception as e:
        logger.error(f"Failed to load dim_players source table: {e}")
        raise

def run_transformation(tb_players, tb_atp_player_match):
    try:
        country_mapping = {
            "AHO": "an", "ALG": "dz", "AND": "ad", "ANT": "ag", "ARG": "ar",
            "ARM": "am", "AUS": "au", "AUT": "at", "AZE": "az", "BAH": "bs",
            "BAN": "bd", "BAR": "bb", "BEL": "be", "BEN": "bj", "BER": "bm",
            "BFA": "bf", "BIH": "ba", "BLR": "by", "BOL": "bo", "BRA": "br",
            "BRN": "bh", "BUL": "bg", "BUR": "bf", "CAN": "ca", "CAR": "cw",
            "CGO": "cg", "CHI": "cl", "CHN": "cn", "CIV": "ci", "CMR": "cm",
            "COK": "ck", "COL": "co", "CRC": "cr", "CRO": "hr", "CUB": "cu",
            "CUR": "cw", "CUW": "cw", "CYP": "cy", "CZE": "cz", "DEN": "dk",
            "DOM": "do", "ECA": "Unk", "ECU": "ec", "EGY": "eg", "ESA": "sv",
            "ESP": "es", "EST": "ee", "FIJ": "fj", "FIN": "fi", "FRA": "fr",
            "FRG": "de", "GBR": "gb", "GDR": "de", "GEO": "ge", "GER": "de",
            "GHA": "gh", "GRE": "gr", "GUA": "gt", "GUM": "gu", "HAI": "ht",
            "HKG": "hk", "HUN": "hu", "INA": "id", "IND": "in", "IRI": "ir",
            "IRQ": "iq", "IRL": "ie", "ISR": "il", "ITA": "it", "JAM": "jm",
            "JOR": "jo", "JPN": "jp", "KAZ": "kz", "KEN": "ke", "KOR": "kr",
            "KSA": "sa", "KUW": "kw", "LAT": "lv", "LBA": "ly", "LBN": "lb",
            "LIB": "lb", "LTU": "lt", "LUX": "lu", "MAD": "mg", "MAR": "ma",
            "MAS": "my", "MDA": "md", "MEX": "mx", "MHL": "mh", "MKD": "mk",
            "MLI": "ml", "MLT": "mt", "MNE": "me", "MON": "mc", "NAM": "na",
            "NED": "nl", "NGR": "ng", "NIG": "ne", "NMI": "mp", "NOR": "no",
            "NZL": "nz", "OMA": "om", "PAK": "pk", "PAN": "pa", "PAR": "py",
            "PER": "pe", "PHI": "ph", "PNG": "pg", "POL": "pl", "POR": "pt",
            "PRT": "pt", "PRY": "py", "PUR": "pr", "QAT": "qa", "RHO": "zw",
            "ROU": "ro", "RSA": "za", "RUS": "ru", "SAM": "ws", "SDN": "sd",
            "SEN": "sn", "SGP": "sg", "SIN": "sg", "SLO": "si", "SOL": "sb",
            "SRB": "rs", "SRI": "lk", "SUD": "sd", "SUI": "ch", "SUR": "sr",
            "SVK": "sk", "SVN": "si", "SWE": "se", "SYR": "sy", "TCH": "cz",
            "TGO": "tg", "THA": "th", "TJK": "tj", "TOG": "tg", "TPE": "tw",
            "TRI": "tt", "TUN": "tn", "TUR": "tr", "UAE": "ae", "UGA": "ug",
            "UKR": "ua", "UNK": "un", "URS": "ru", "URU": "uy", "URY": "uy",
            "USA": "us", "UZB": "uz", "VAN": "vu", "VEN": "ve", "VIE": "vn",
            "YUG": "rs", "ZAM": "zm", "ZIM": "zw", "N/A": "Unk"
        }

        mapping_expr = f.create_map([f.lit(x) for x in chain(*country_mapping.items())])

        players_from_matches = (
            tb_atp_player_match.orderBy(f.col("DATE_MATCH"))
            .groupBy("DES_PLAYER_NAME")
            .agg(
                f.first("COD_PLAYER_ID", ignorenulls=True).cast("string").alias("COD_PLAYER_ID"),
                f.coalesce(f.last("DES_PLAYER_HAND"), f.lit("-")).alias("DES_PLAYER_HAND"),
                f.coalesce(f.last("NUM_PLAYER_HEIGHT"), f.lit(-1)).cast('int').alias("NUM_PLAYER_HEIGHT"),
                f.coalesce(f.last("DES_PLAYER_COUNTRY"), f.lit("N/A")).alias("DES_PLAYER_COUNTRY"),
            )
        )
        
        df = (
            tb_players.alias("p")
            .join(
                players_from_matches.alias("pm"),
                "DES_PLAYER_NAME",
                'full'
            )
            .select(
                f.coalesce(f.col("pm.COD_PLAYER_ID"), f.col("p.COD_PLAYER_ID")).alias("COD_PLAYER_ID"),
                f.coalesce(f.col("p.COD_PLAYER_ID"), f.col("pm.COD_PLAYER_ID")).alias("COD_PLAYER_ID_NEW"),
                f.coalesce(f.col("p.DES_PLAYER_NAME"), f.col("pm.DES_PLAYER_NAME")).alias("DES_PLAYER_NAME"),
                f.coalesce(f.col("p.DES_PLAYER_ATP_NAME"), f.col("pm.DES_PLAYER_NAME")).alias("DES_PLAYER_ATP_NAME"),
                f.coalesce(f.col("p.DATE_PLAYER_BIRTHDATE"), f.lit(None)).alias("DATE_PLAYER_BIRTHDATE"),
                f.coalesce(f.col("p.NUM_PLAYER_HEIGHT"), f.col("pm.NUM_PLAYER_HEIGHT")).alias("NUM_PLAYER_HEIGHT"),
                f.coalesce(f.col("p.DATE_PLAYER_TURNED_PRO_YEAR"), f.lit(None)).alias("DATE_PLAYER_TURNED_PRO_YEAR"),
                f.coalesce(f.col("p.DES_PLAYER_BIRTH_PLACE"), f.lit("-")).alias("DES_PLAYER_BIRTH_PLACE"),
                f.coalesce(f.col("p.DES_PLAYER_COACHES"), f.lit("-")).alias("DES_PLAYER_COACHES"),
                f.coalesce(f.col("p.DES_PLAYER_HAND"), f.col("pm.DES_PLAYER_HAND")).alias("DES_PLAYER_HAND"),
                f.coalesce(f.col("p.DES_PLAYER_BACKHAND_TYPE"), f.lit("-")).alias("DES_PLAYER_BACKHAND_TYPE"),
                f.coalesce(f.col("p.DES_PLAYER_COUNTRY"), f.col("pm.DES_PLAYER_COUNTRY")).alias("DES_PLAYER_COUNTRY")
            )
            .withColumn(
                "DES_PLAYER_HAND",
                f.when(f.col("DES_PLAYER_HAND") == "L", f.lit("Left-Handed"))
                .when(f.col("DES_PLAYER_HAND") == "R", f.lit("Right-Handed"))
                .when(f.col("DES_PLAYER_HAND") == "A", f.lit("Ambidextrous"))
                .otherwise(f.lit("Unknown"))
            )
            .withColumn(
                "DES_PLAYER_BACKHAND_TYPE",
                f.when(f.col("DES_PLAYER_BACKHAND_TYPE") == "1H", f.lit("One-Handed Backhand"))
                .when(f.col("DES_PLAYER_BACKHAND_TYPE") == "2H", f.lit("Two-Handed Backhand"))
                .otherwise(f.lit("Unknown backhand"))
            )
            .withColumn(
                "DES_PLAYER_COUNTRY_ISO2",
                mapping_expr[f.upper(f.trim(f.col("DES_PLAYER_COUNTRY")))]
            )
            .withColumn("SK_PLAYER", f.md5(f.col("COD_PLAYER_ID")))

            .groupBy("SK_PLAYER")
            .agg(
                f.first("COD_PLAYER_ID").alias("COD_PLAYER_ID"),
                f.max_by("COD_PLAYER_ID_NEW", f.when(f.col("COD_PLAYER_ID_NEW").rlike("[A-Za-z]"), 1).otherwise(0)).alias("COD_PLAYER_ID_NEW"),
                f.first("DES_PLAYER_NAME").alias("DES_PLAYER_NAME"),
                f.first("DES_PLAYER_ATP_NAME").alias("DES_PLAYER_ATP_NAME"),
                f.first("DATE_PLAYER_BIRTHDATE").alias("DATE_PLAYER_BIRTHDATE"),
                f.first("NUM_PLAYER_HEIGHT").alias("NUM_PLAYER_HEIGHT"),
                f.first("DATE_PLAYER_TURNED_PRO_YEAR").alias("DATE_PLAYER_TURNED_PRO_YEAR"),
                f.first("DES_PLAYER_BIRTH_PLACE").alias("DES_PLAYER_BIRTH_PLACE"),
                f.first("DES_PLAYER_COACHES").alias("DES_PLAYER_COACHES"),
                f.first("DES_PLAYER_HAND").alias("DES_PLAYER_HAND"),
                f.first("DES_PLAYER_BACKHAND_TYPE").alias("DES_PLAYER_BACKHAND_TYPE"),
                f.first("DES_PLAYER_COUNTRY").alias("DES_PLAYER_COUNTRY"),
                f.first("DES_PLAYER_COUNTRY_ISO2").alias("DES_PLAYER_COUNTRY_ISO2"),
            )
        )

        logger.info("dim_players transformation completed")
        return df
    except Exception as e:
        logger.error(f"Failed to transform dim_players: {e}")
        raise

def save_table(handler, df, bucket_name, jdbc_url, jdbc_user, jdbc_password):
    try:
        handler.save_data(
            df=df,
            path=f"s3a://{bucket_name}/gold/dimension/dim_players/",
            format="parquet",
            mode="overwrite"
        )

        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "gold.dim_players")
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
    except Exception as e:
        logger.error(f"Failed to save dim_players: {e}")
        raise

def run(conn_vars: dict = None):
    handler = None
    try:
        logger.info("Starting dim_players run")
        handler = PySparkHandler(
            app_name="dim_players",
            bucket_endpoint=conn_vars.get("bucket_endpoint"),
            bucket_access_key=conn_vars.get("bucket_access_key"),
            bucket_secret_key=conn_vars.get("bucket_secret_key")
        )
        bucket_name = conn_vars.get("bucket_name")
        jdbc_url = conn_vars.get("jdbc_url")
        jdbc_user = conn_vars.get("jdbc_user")
        jdbc_password = conn_vars.get("jdbc_password")
        
        tb_players, tb_atp_player_match = load_tables(handler, bucket_name)
        df_final = run_transformation(tb_players, tb_atp_player_match)
        save_table(handler, df_final, bucket_name, jdbc_url, jdbc_user, jdbc_password)
        logger.info("Finished dim_players run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run()
