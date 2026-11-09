import os
import sys
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from itertools import chain
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.pyspark_handler import PySparkHandler
from src.utils.logger import get_logger
from src.utils.player_names import normalize_player_name, player_name_key

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
logger = get_logger(__name__)

SYNTHETIC_ID_PREFIX = "NOID_"
PLACEHOLDER_NAME_KEYS = ["unknownunknown", "qualifierqualifier"]
MAX_ACTIVE_AGE_YEARS = 50

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
        tb_atp_rankings = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/silver/tb_atp_rankings/",
            format="parquet"
        )
        
        return tb_players, tb_atp_player_match, tb_atp_rankings
    except Exception as e:
        logger.error(f"Failed to load dim_players source table: {e}")
        raise

def group_ids(id_pairs):
    parent = {}

    def find(player_id):
        parent.setdefault(player_id, player_id)
        while parent[player_id] != player_id:
            parent[player_id] = parent[parent[player_id]]
            player_id = parent[player_id]
        return player_id

    for id_a, id_b in id_pairs:
        root_a, root_b = sorted([find(id_a), find(id_b)])
        parent[root_b] = root_a

    return {player_id: find(player_id) for player_id in list(parent)}

def find_same_player_pairs(id_names, id_profile):
    is_legacy_id = f.col("COD_PLAYER_ID").rlike(r"^\d+$")
    is_synthetic_id = f.col("COD_PLAYER_ID").startswith(SYNTHETIC_ID_PREFIX)
    real_names = id_names.where(~is_synthetic_id)
    profile_a = id_profile.select([f.col(c).alias(f"A_{c}") for c in id_profile.columns])
    profile_b = id_profile.select([f.col(c).alias(f"B_{c}") for c in id_profile.columns])

    candidates = (
        real_names.where(is_legacy_id).withColumnRenamed("COD_PLAYER_ID", "ID_A")
        .join(real_names.where(~is_legacy_id).withColumnRenamed("COD_PLAYER_ID", "ID_B"), "NAME_KEY")
        .select("ID_A", "ID_B")
        .distinct()
        .join(profile_a, f.col("ID_A") == f.col("A_COD_PLAYER_ID"))
        .join(profile_b, f.col("ID_B") == f.col("B_COD_PLAYER_ID"))
    )

    gap_days = f.greatest(
        f.datediff(f.col("B_FIRST_ACTIVITY"), f.col("A_LAST_ACTIVITY")),
        f.datediff(f.col("A_FIRST_ACTIVITY"), f.col("B_LAST_ACTIVITY")),
    )
    age_at_last_activity = f.datediff(
        f.greatest(f.col("A_LAST_ACTIVITY"), f.col("B_LAST_ACTIVITY")),
        f.coalesce(f.col("A_BIRTHDATE"), f.col("B_BIRTHDATE")),
    ) / 365.25

    contradiction = (
        f.coalesce(f.col("A_BIRTHDATE") != f.col("B_BIRTHDATE"), f.lit(False))
        | f.coalesce(f.col("A_COUNTRY_KEY") != f.col("B_COUNTRY_KEY"), f.lit(False))
        | f.coalesce(gap_days <= 0, f.lit(False))
        | f.coalesce(age_at_last_activity > MAX_ACTIVE_AGE_YEARS, f.lit(False))
    )

    real_pairs = (
        candidates.where(~contradiction)
        .withColumn("QTY_NEW_IDS", f.count("*").over(Window.partitionBy("ID_A")))
        .withColumn("QTY_OLD_IDS", f.count("*").over(Window.partitionBy("ID_B")))
        .where((f.col("QTY_NEW_IDS") == 1) & (f.col("QTY_OLD_IDS") == 1))
        .select("ID_A", "ID_B")
    )

    real_id_by_name = (
        real_names.groupBy("NAME_KEY")
        .agg(f.count_distinct("COD_PLAYER_ID").alias("QTY"), f.first("COD_PLAYER_ID").alias("ID_B"))
        .where(f.col("QTY") == 1)
    )
    synthetic_pairs = (
        id_names.where(is_synthetic_id)
        .join(real_id_by_name, "NAME_KEY")
        .select(f.col("COD_PLAYER_ID").alias("ID_A"), "ID_B")
    )

    return [(row["ID_A"], row["ID_B"]) for row in real_pairs.unionByName(synthetic_pairs).distinct().collect()]

def run_transformation(tb_players, tb_atp_player_match, tb_atp_rankings):
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

        def without_placeholder(coluna, placeholder):
            coluna = f.col(coluna) if isinstance(coluna, str) else coluna
            return f.when(coluna != placeholder, coluna)

        country_code = f.upper(f.trim(f.col("DES_PLAYER_COUNTRY")))
        country_key = without_placeholder(f.coalesce(mapping_expr[country_code], country_code), "Unk")

        registry = (
            tb_players
            .select(
                "COD_PLAYER_ID",
                "DES_PLAYER_ATP_NAME",
                f.to_date("DATE_PLAYER_BIRTHDATE").alias("DATE_PLAYER_BIRTHDATE"),
                f.col("DATE_PLAYER_TURNED_PRO_YEAR"),
                without_placeholder(f.col("NUM_PLAYER_HEIGHT").cast("int"), -1).alias("REG_HEIGHT"),
                without_placeholder("DES_PLAYER_BIRTH_PLACE", "-").alias("DES_PLAYER_BIRTH_PLACE"),
                without_placeholder("DES_PLAYER_COACHES", "-").alias("DES_PLAYER_COACHES"),
                without_placeholder("DES_PLAYER_HAND", "-").alias("REG_HAND"),
                without_placeholder("DES_PLAYER_BACKHAND_TYPE", "-").alias("DES_PLAYER_BACKHAND_TYPE"),
                without_placeholder("DES_PLAYER_COUNTRY", "N/A").alias("REG_COUNTRY"),
                f.lit(True).alias("IN_REGISTRY"),
            )
        )

        latest_known = lambda coluna: f.max_by(coluna, f.when(f.col(coluna).isNotNull(), f.col("DATE_MATCH")))
        from_matches = (
            tb_atp_player_match
            .withColumn("DES_PLAYER_HAND", f.when(f.col("DES_PLAYER_HAND").isin("L", "R", "A"), f.col("DES_PLAYER_HAND")))
            .withColumn("NUM_PLAYER_HEIGHT", f.when(f.col("NUM_PLAYER_HEIGHT") > 0, f.col("NUM_PLAYER_HEIGHT")))
            .groupBy("COD_PLAYER_ID")
            .agg(
                latest_known("DES_PLAYER_HAND").alias("MATCH_HAND"),
                latest_known("NUM_PLAYER_HEIGHT").alias("MATCH_HEIGHT"),
                latest_known("DES_PLAYER_COUNTRY").alias("MATCH_COUNTRY"),
            )
        )

        activity = (
            tb_atp_player_match.select("COD_PLAYER_ID", f.to_date("DATE_MATCH").alias("ACTIVITY_DATE"))
            .unionByName(tb_atp_rankings.select("COD_PLAYER_ID", f.to_date("DATE_WEEK_RANKING").alias("ACTIVITY_DATE")))
            .groupBy("COD_PLAYER_ID")
            .agg(f.min("ACTIVITY_DATE").alias("FIRST_ACTIVITY"), f.max("ACTIVITY_DATE").alias("LAST_ACTIVITY"))
        )

        name_observations = (
            tb_players.select("COD_PLAYER_ID", f.col("DES_PLAYER_NAME"), f.lit(1).alias("PRIORITY"), f.lit(1).alias("QTY"))
            .unionByName(tb_players.select("COD_PLAYER_ID", f.col("DES_PLAYER_ATP_NAME").alias("DES_PLAYER_NAME"), f.lit(2).alias("PRIORITY"), f.lit(1).alias("QTY")))
            .unionByName(tb_atp_player_match.groupBy("COD_PLAYER_ID", "DES_PLAYER_NAME").agg(f.count("*").cast("int").alias("QTY")).withColumn("PRIORITY", f.lit(3)))
            .unionByName(tb_atp_rankings.groupBy("COD_PLAYER_ID", "DES_PLAYER_NAME").agg(f.count("*").cast("int").alias("QTY")).withColumn("PRIORITY", f.lit(4)))
            .where(f.col("COD_PLAYER_ID").isNotNull() & f.col("DES_PLAYER_NAME").isNotNull())
            .withColumn("NAME_KEY", player_name_key(f.col("DES_PLAYER_NAME")))
            .where(f.col("NAME_KEY") != "")
        )

        best_name = (
            name_observations
            .where(~f.col("NAME_KEY").isin(PLACEHOLDER_NAME_KEYS))
            .withColumn("RN", f.row_number().over(Window.partitionBy("COD_PLAYER_ID").orderBy("PRIORITY", f.desc("QTY"), "DES_PLAYER_NAME")))
            .where(f.col("RN") == 1)
            .select("COD_PLAYER_ID", normalize_player_name(f.col("DES_PLAYER_NAME")).alias("DES_PLAYER_NAME"))
        )

        id_profile = (
            name_observations.select("COD_PLAYER_ID").distinct()
            .join(best_name, "COD_PLAYER_ID", "left")
            .join(registry, "COD_PLAYER_ID", "left")
            .join(from_matches, "COD_PLAYER_ID", "left")
            .join(activity, "COD_PLAYER_ID", "left")
            .withColumn("DES_PLAYER_COUNTRY", f.coalesce(f.col("REG_COUNTRY"), f.col("MATCH_COUNTRY")))
            .withColumn("NUM_PLAYER_HEIGHT", f.coalesce(f.col("REG_HEIGHT"), f.col("MATCH_HEIGHT")))
            .withColumn("DES_PLAYER_HAND", f.coalesce(f.col("REG_HAND"), f.col("MATCH_HAND")))
            .withColumn("COUNTRY_KEY", country_key)
            .cache()
        )

        comparison_profile = id_profile.select(
            "COD_PLAYER_ID", "FIRST_ACTIVITY", "LAST_ACTIVITY",
            f.col("DATE_PLAYER_BIRTHDATE").alias("BIRTHDATE"), "COUNTRY_KEY",
        )
        id_names = (
            name_observations
            .where(~f.col("NAME_KEY").isin(PLACEHOLDER_NAME_KEYS))
            .select("COD_PLAYER_ID", "NAME_KEY")
            .distinct()
        )

        same_player_groups = group_ids(find_same_player_pairs(id_names, comparison_profile))
        group_schema = "COD_PLAYER_ID string, PLAYER_GROUP string"
        id_to_group = tb_players.sparkSession.createDataFrame(list(same_player_groups.items()), group_schema)

        is_synthetic = f.col("COD_PLAYER_ID").startswith(SYNTHETIC_ID_PREFIX)
        is_numeric = f.col("COD_PLAYER_ID").rlike(r"^\d+$")
        group_order = Window.partitionBy("PLAYER_GROUP").orderBy(
            is_synthetic.asc(),
            is_numeric.asc(),
            f.col("LAST_ACTIVITY").desc_nulls_last(),
            f.col("IN_REGISTRY").asc_nulls_last(),
            f.col("COD_PLAYER_ID").asc(),
        )
        whole_group = group_order.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        first_known = lambda coluna: f.first(coluna, ignorenulls=True).over(whole_group)
        real_ids = f.collect_list(f.when(~is_synthetic, f.col("COD_PLAYER_ID"))).over(whole_group)
        unknown_player_name = f.concat(f.lit("Unknown-"), f.col("COD_PLAYER_ID"))

        df = (
            id_profile
            .join(id_to_group, "COD_PLAYER_ID", "left")
            .withColumn("PLAYER_GROUP", f.coalesce(f.col("PLAYER_GROUP"), f.col("COD_PLAYER_ID")))
            .withColumn("RN", f.row_number().over(group_order))
            .withColumn("COD_PLAYER_ID_OLD", real_ids.getItem(1))
            .withColumn("COD_PLAYER_ID_ALL", f.concat_ws(",", f.collect_list("COD_PLAYER_ID").over(whole_group)))
            .withColumn("DES_PLAYER_NAME", first_known("DES_PLAYER_NAME"))
            .withColumn("DES_PLAYER_ATP_NAME", first_known("DES_PLAYER_ATP_NAME"))
            .withColumn("DATE_PLAYER_BIRTHDATE", first_known("DATE_PLAYER_BIRTHDATE"))
            .withColumn("DATE_PLAYER_TURNED_PRO_YEAR", first_known("DATE_PLAYER_TURNED_PRO_YEAR"))
            .withColumn("NUM_PLAYER_HEIGHT", f.coalesce(first_known("NUM_PLAYER_HEIGHT"), f.lit(-1)))
            .withColumn("DES_PLAYER_BIRTH_PLACE", f.coalesce(first_known("DES_PLAYER_BIRTH_PLACE"), f.lit("-")))
            .withColumn("DES_PLAYER_COACHES", f.coalesce(first_known("DES_PLAYER_COACHES"), f.lit("-")))
            .withColumn("DES_PLAYER_HAND", first_known("DES_PLAYER_HAND"))
            .withColumn("DES_PLAYER_BACKHAND_TYPE", first_known("DES_PLAYER_BACKHAND_TYPE"))
            .withColumn("DES_PLAYER_COUNTRY", f.coalesce(first_known("DES_PLAYER_COUNTRY"), f.lit("N/A")))
            .where(f.col("RN") == 1)
            .withColumn("DATE_PLAYER_BIRTHDATE", f.when(f.col("DES_PLAYER_NAME").isNotNull(), f.col("DATE_PLAYER_BIRTHDATE")))
            .withColumn("DES_PLAYER_NAME", f.coalesce(f.col("DES_PLAYER_NAME"), unknown_player_name))
            .withColumn(
                "DES_PLAYER_ATP_NAME",
                f.when(
                    player_name_key(f.col("DES_PLAYER_ATP_NAME")).isin(PLACEHOLDER_NAME_KEYS), unknown_player_name
                ).otherwise(f.col("DES_PLAYER_ATP_NAME"))
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
            .select(
                "SK_PLAYER",
                "COD_PLAYER_ID",
                "COD_PLAYER_ID_OLD",
                "COD_PLAYER_ID_ALL",
                "DES_PLAYER_NAME",
                "DES_PLAYER_ATP_NAME",
                f.date_format("DATE_PLAYER_BIRTHDATE", "yyyy-MM-dd").alias("DATE_PLAYER_BIRTHDATE"),
                "NUM_PLAYER_HEIGHT",
                "DATE_PLAYER_TURNED_PRO_YEAR",
                "DES_PLAYER_BIRTH_PLACE",
                "DES_PLAYER_COACHES",
                "DES_PLAYER_HAND",
                "DES_PLAYER_BACKHAND_TYPE",
                "DES_PLAYER_COUNTRY",
                "DES_PLAYER_COUNTRY_ISO2",
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
        
        tb_players, tb_atp_player_match, tb_atp_rankings = load_tables(handler, bucket_name)
        df_final = run_transformation(tb_players, tb_atp_player_match, tb_atp_rankings)
        save_table(handler, df_final, bucket_name, jdbc_url, jdbc_user, jdbc_password)
        logger.info("Finished dim_players run")
    finally:
        logger.info("Stopping spark session...")
        handler.spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run()
