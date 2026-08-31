import os
from dotenv import load_dotenv
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

load_dotenv()
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# 1. Spark Session
spark = (
    SparkSession.builder.appName("test_pipeline_sanity")
    .config("spark.driver.memory", "3500m")
    .config("spark.executor.memory", "3500m")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .config("spark.sql.shuffle.partitions", "4")
    .config(
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# 2. Dados de Teste
sample_data = [
    ("2026-001", "20260105", 1, "Novak Djokovic", "Carlos Alcaraz", "6-4 6-4", "F", "WC"),
    ("2026-001", "20260104", 2, "Novak Djokovic", "Daniil Medvedev", "7-5 6-3", "SF", "DA"),
    ("2026-002", "20260112", 1, "Jannik Sinner", "Alexander Zverev", "6-3 6-2", "F", None),
]
columns = ["tourney_id", "tourney_date", "match_num", "winner_name", "loser_name", "score", "round", "winner_entry"]

df_raw = spark.createDataFrame(sample_data, columns)

# 3. Transformações
window_sk = Window.orderBy("COD_MATCH_ID")

df_transformed = (
    df_raw
    .withColumn("COD_MATCH_ID", f.concat_ws("-", f.col("tourney_id"), f.col("match_num")))
    .withColumn("DATE_MATCH", f.to_date(f.col("tourney_date"), "yyyyMMdd"))
    .withColumn("SK_TEST_KEY", f.row_number().over(window_sk).cast("int"))
    .withColumn("TEST_TIMESTAMP", f.current_timestamp())
)

# 4. Gravação Parquet Local (Ficará visível na pasta data/silver/)
output_parquet_path = os.path.abspath("./data/silver/tb_atp_matches_test").replace("\\", "/")

(
    df_transformed.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(output_parquet_path)
)

print(f"✅ Arquivos Parquet gravados e mantidos em: {output_parquet_path}")

# 5. Gravação JDBC (Supabase/PostgreSQL)
jdbc_url = os.getenv("JDBC_URL")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

if jdbc_url and db_user and db_password:
    (
        df_transformed.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "silver.tb_atp_matches_test")
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    print("✅ Tabela gravada com sucesso no PostgreSQL/Supabase!")

spark.stop()