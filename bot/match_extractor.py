import io
import boto3
from botocore.client import Config
from datetime import datetime
from playwright.sync_api import sync_playwright

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

def extract_bot(conn_vars):
    s3_client = boto3.client(
        's3',
        endpoint_url=conn_vars["bucket_endpoint"],
        aws_access_key_id=conn_vars["bucket_access_key"],
        aws_secret_access_key=conn_vars["bucket_secret_key"],
        config=Config(signature_version='s3v4')
    )

    bucket = conn_vars["bucket_name"]
    try:
        s3_client.head_bucket(Bucket=bucket)
    except:
        s3_client.create_bucket(Bucket=bucket)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto("https://stats.tennismylife.org/tennis-match-database", wait_until="networkidle")
            year = datetime.now().year

            file_curr = f"raw/historical/matches/atp_matches_{year}.csv"
            file_ongoing = "raw/incremental/tb_ongoing_tourneys.csv"

            path_curr = get_file(page, f"Download {year}.csv", file_curr, s3_client, bucket)

            path_ongoing = get_file(page, "Download ongoing_tourneys.csv", file_ongoing, s3_client, bucket)

        finally:
            context.close()
            browser.close()

def run_ingestion(conn_vars):
    print(f"[Airflow] Download matches starts...")
    extract_bot(conn_vars)

if __name__ == "__main__":
    extract_bot()