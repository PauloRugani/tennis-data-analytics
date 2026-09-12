import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from typing import Literal
load_dotenv()

class PySparkHandler:
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark = self.init_spark_session()

    def init_spark_session(self):
        try:
            print("Creating spark session...")
            spark = (
                SparkSession.builder
                .appName(self.app_name)
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config(
                    "spark.jars.packages",
                    "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
                )
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
                .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .getOrCreate()
            )
            
            spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
            spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 200)
            spark.conf.set("spark.sql.repl.eagerEval.truncate", 50)
            print("Spark session created")
            return spark
        except Exception as e:
            print(e)
            raise

    def save_data(self, df, path: str, format: str = "parquet", mode: str = "overwrite", **options):
        try:
            print(f"Saving data...")
            (
                df.write
                .mode(mode)
                .format(format)
                .options(**options)
                .save(path)
            )
            print(f"Data saved")
        except Exception as e:
            print(e)
            raise

    def load_data(self, spark, path: str, format: str = "parquet", **options):
        try:
            print(f"Loading data...")
            df = (
                spark.read
                .format(format)
                .options(**options)
                .load(path)
            )
            print(f"Data loaded")
            return df
        except Exception as e:
            print(e)
            raise