import os
from src.utils.pyspark_handler import PySparkHandler

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def load_tables(handler, bucket_name):
    try:
        print(f"Loading raw player tables...")
        tb_players = handler.load_data(
            spark=handler.spark,
            path=f"s3a://{bucket_name}/raw/incremental/tb_players.csv",
            header="true",
            format="csv"
        )
    except Exception as e:
        print(f"Failed to load bronze player tables: {e}")
        raise
    
    return tb_players


def save_table(handler, df_final, bucket_name):
    try:
        print(f"Saving player rows")
        handler.save_data(
            df=df_final,
            path=f"s3a://{bucket_name}/bronze/tb_atp_players/",
            format="parquet",
            mode='overwrite'
        )
    except Exception as e:
        print(f"Failed to save bronze player data: {e}")
        raise

def run(conn_vars: dict = None):
    handler = None
    try:
        print("Starting bronze player run")
        handler = PySparkHandler(
            app_name="tb_atp_players_bronze",
            bucket_endpoint=conn_vars.get("bucket_endpoint"),
            bucket_access_key=conn_vars.get("bucket_access_key"),
            bucket_secret_key=conn_vars.get("bucket_secret_key")
        )

        bucket_name = conn_vars.get("bucket_name")
        tb_players = load_tables(handler, bucket_name)
        save_table(handler, tb_players, bucket_name)
        print("Finished bronze player run")
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    run()