import os
import shutil

AIRFLOW_TEMP_DIR = "/tmp/airflow_staging"

def clean_airflow_tmp():
    if os.path.exists(AIRFLOW_TEMP_DIR):
        shutil.rmtree(AIRFLOW_TEMP_DIR)
        print(f"[Airflow] {AIRFLOW_TEMP_DIR} removed.")