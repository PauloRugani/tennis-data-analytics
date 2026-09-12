import os
import sys
from pyspark.sql import functions as f
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pyspark_handler import PySparkHandler

load_dotenv()
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def load_tables(handler, init_run=False):
    try:
        if not init_run:
            tb_atp_rankings = handler.load_data(
                spark=handler.spark,
                path="s3a://tennis-data-lake/bronze/tb_atp_rankings/",
                format="parquet"
            )
        else:
            tb_atp_rankings = None

        tb_incremental_rankings = handler.load_data(
            spark=handler.spark,
            path=fr"s3a://tennis-data-lake/raw/incremental/tb_incremental_ranking_{datetime.now().year}.csv",
            format="csv",
            header="true"
        )

        historical_ranking = "s3a://tennis-data-lake/raw/historical/ranking/"
    except Exception as e:
        print(e)
        raise
    
    return tb_atp_rankings, tb_incremental_rankings, historical_ranking

def run_transformation(handler, init_run, tb_atp_rankings, tb_incremental_rankings, historical_ranking):
    try:
        print("Running transformations...")
        if not init_run:
            df = tb_atp_rankings
        else:
            df = handler.load_data(
                spark=handler.spark,
                path=historical_ranking,
                format="csv",
                header="true",
                inferSchema="false"
            )

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

        if not init_run:
            df_final = new_rankings.withColumn("DATE_INGESTION", f.lit(f.current_date()))
        else:
            df_final = df.unionByName(new_rankings, allowMissingColumns=True).withColumn("DATE_INGESTION", f.lit(f.current_date()))

        print("Transformations completed")
        return df_final
    except Exception as e:
        print(e)
        raise

def save_table(handler, init_run, df_final):
    try:
        if not init_run:
            save_mode = "append"
        else:
            save_mode = "overwrite"

        if df_final.count() > 0:
            handler.save_data(
                df=df_final,
                path="s3a://tennis-data-lake/bronze/tb_atp_rankings/",
                format="parquet",
                mode=save_mode
            )
    except Exception as e:
        print(e)
        raise

def run(init_run: bool):
    handler = None
    try:
        handler = PySparkHandler(app_name="tb_atp_ranking_bronze")
        tb_atp_rankings, tb_incremental_rankings, historical_ranking = load_tables(handler, init_run)
        df_final = run_transformation(handler, init_run, tb_atp_rankings, tb_incremental_rankings, historical_ranking)
        save_table(handler, init_run, df_final)
    finally:
        print("Stopping spark session...")
        handler.spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run(init_run=True)
