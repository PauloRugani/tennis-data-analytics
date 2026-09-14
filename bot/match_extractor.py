import os
import io
import boto3
from botocore.client import Config
from datetime import datetime
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

def get_file(page, role_name: str, relative_path: str, s3_client, bucket: str):
    with page.expect_download(timeout=60000) as download_info:
        page.get_by_role("link", name=role_name).click()

    download = download_info.value
    tmp_path = download.path()
    
    with open(tmp_path, "rb") as f:
        file_bytes = f.read()
        
    s3_client.upload_fileobj(io.BytesIO(file_bytes), bucket, relative_path)
    print(f"{relative_path} saved successfully")
    
    download.delete()
    return relative_path

def extract_bot():
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        config=Config(signature_version='s3v4')
    )

    try:
        s3_client.head_bucket(Bucket=os.getenv("MINIO_BUCKET"))
    except:
        s3_client.create_bucket(Bucket=os.getenv("MINIO_BUCKET"))

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto("https://stats.tennismylife.org/tennis-match-database", wait_until="networkidle")
            year = datetime.now().year

            file_curr = f"raw/historical/matches/atp_matches_{year}.csv"
            file_ongoing = "raw/incremental/tb_ongoing_tourneys.csv"

            path_curr = get_file(page, f"Download {year}.csv", file_curr, s3_client, os.getenv("MINIO_BUCKET"))

            path_ongoing = get_file(page, "Download ongoing_tourneys.csv", file_ongoing, s3_client, os.getenv("MINIO_BUCKET"))

        finally:
            context.close()
            browser.close()

def run_ingestion():
    print(f"[Airflow] Download matches starts...")
    extract_bot()

if __name__ == "__main__":
    extract_bot()