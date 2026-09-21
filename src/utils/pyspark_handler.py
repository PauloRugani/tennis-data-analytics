from pyspark.sql import SparkSession
from dotenv import load_dotenv
from src.utils.logger import get_logger
load_dotenv()

logger = get_logger(__name__)

class PySparkHandler:
    def __init__(self, app_name: str, bucket_endpoint: str, bucket_access_key: str, bucket_secret_key: str):
        self.app_name = app_name
        self.bucket_endpoint = bucket_endpoint
        self.bucket_access_key = bucket_access_key
        self.bucket_secret_key = bucket_secret_key
        self.spark = self.init_spark_session()

    def init_spark_session(self):
        try:
            logger.info(f"Creating spark session for app '{self.app_name}'...")
            spark = (
                SparkSession.builder
                .appName(self.app_name)
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config(
                    "spark.jars.packages",
                    "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
                )
                .config("spark.hadoop.fs.s3a.endpoint", self.bucket_endpoint)
                .config("spark.hadoop.fs.s3a.access.key", self.bucket_access_key)
                .config("spark.hadoop.fs.s3a.secret.key", self.bucket_secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .getOrCreate()
            )
            
            spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
            spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 200)
            spark.conf.set("spark.sql.repl.eagerEval.truncate", 50)
            logger.info("Spark session created")
            return spark
        except Exception as e:
            logger.error(f"Failed to create spark session: {e}")
            raise

    def save_data(self, df, path: str, format: str = "parquet", mode: str = "overwrite", **options):
        try:
            logger.info(f"Saving data to {path} (format={format}, mode={mode})...")
            (
                df.write
                .mode(mode)
                .format(format)
                .options(**options)
                .save(path)
            )
            logger.info(f"Data saved to {path}")
        except Exception as e:
            logger.error(f"Failed to save data to {path}: {e}")
            raise

    def load_data(self, spark, path: str, format: str = "parquet", **options):
        try:
            logger.info(f"Loading data from {path} (format={format})...")
            df = (
                spark.read
                .format(format)
                .options(**options)
                .load(path)
            )
            logger.info(f"Data loaded from {path}")
            return df
        except Exception as e:
            logger.error(f"Failed to load data from {path}: {e}")
            raise