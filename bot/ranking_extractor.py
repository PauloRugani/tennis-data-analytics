import csv
import io
import re
import logging
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from curl_cffi import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

INCREMENTAL_PREFIX = "raw/incremental/tb_incremental_ranking_"
HISTORICAL_PREFIX = "raw/historical/ranking/tb_ranking_"
INCREMENTAL_FILE_PATTERN = re.compile(r"tb_incremental_ranking_(\d{4})\.csv$")

def read_object_text(s3_client, bucket: str, object_name: str) -> str:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_name)
        return response['Body'].read().decode('utf-8-sig')
    except ClientError as e:
        if e.response['Error']['Code'] in ('NoSuchKey', '404'):
            return ""
        raise

def is_date_already_processed(content: str, target_date: str) -> bool:
    if not content.strip():
        return False

    target_date_clean = str(target_date).strip()
    sample = content[:2048]
    delimiter = ";" if ";" in sample and "," not in sample else ","

    reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    for row in reader:
        val = row.get("date")
        if val and str(val).strip() == target_date_clean:
            return True
    return False

def previous_year_process(s3_client, bucket: str, current_year: int):
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=INCREMENTAL_PREFIX):
        for obj in page.get("Contents", []):
            old_file = obj["Key"]
            match = INCREMENTAL_FILE_PATTERN.search(old_file)
            if not match or int(match.group(1)) >= current_year:
                continue

            year = match.group(1)
            dest_file = f"{HISTORICAL_PREFIX}{year}.csv"
            try:
                s3_client.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': old_file},
                    Key=dest_file
                )
                copied_size = s3_client.head_object(Bucket=bucket, Key=dest_file)["ContentLength"]
                if copied_size != obj["Size"]:
                    raise RuntimeError(
                        f"Copy size mismatch for {dest_file}: expected {obj['Size']}, got {copied_size}"
                    )
                s3_client.delete_object(Bucket=bucket, Key=old_file)
                logger.info(f"Archived {old_file} to {dest_file}")
            except Exception as e:
                logger.error(f"Failed to archive previous year ranking file {old_file}: {e}")
                raise

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
        logger.info(f"Bucket '{bucket}' not found, creating it")
        s3_client.create_bucket(Bucket=bucket)

    today = datetime.now()
    current_monday = today - timedelta(days=today.weekday())
    current_year = current_monday.year
    week_str = current_monday.strftime("%Y-%m-%d")
    target_date = str(week_str).replace("-", "")

    previous_year_process(s3_client, bucket, current_year)

    object_name = f"{INCREMENTAL_PREFIX}{current_year}.csv"
    existing_content = read_object_text(s3_client, bucket, object_name)

    if is_date_already_processed(existing_content, target_date):
        logger.info(f"Date {target_date} already processed, skipping")
        return object_name

    logger.info(f"Scraping ATP rankings for week {target_date}...")
    url = "https://www.atptour.com/en/rankings/singles?rankRange=0-5000"
    response = requests.get(url, impersonate="chrome", timeout=60)

    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.select(".lower_row, .lower-row, tr.lower-row, tr.lower_row")

    ranking = []
    for row in rows:
        tds = row.find_all("td")
        cells = [td.get_text(strip=True) for td in tds]

        if len(cells) >= 8:
            player_anchor = tds[1].select_one(".player-cell a, a")
            player_id = ""

            if player_anchor:
                clean_player = player_anchor.get_text(strip=True)
                href = player_anchor.get("href", "")
                match = re.search(r"/players/[^/]+/([a-zA-Z0-9]+)/", href)
                if match:
                    player_id = match.group(1)
            else:
                raw_player = cells[1].split("\n")[-1].strip()
                clean_player = re.sub(r"^[+-]?\d+", "", raw_player).strip()

            ranking.append(
                {
                    "date": target_date,
                    "rank": cells[0],
                    "name": clean_player,
                    "id": player_id.upper(),
                    "points": cells[3],
                    "age": cells[2],
                    "lost_earned_points": cells[4],
                    "tourn_played": cells[5],
                    "dropping": cells[6],
                    "next_best": cells[7],
                }
            )

    if not ranking:
        logger.info("No ranking data scraped, nothing to save")
        return None

    out = io.StringIO()
    if existing_content.strip():
        out.write(existing_content)
        if not existing_content.endswith("\n"):
            out.write("\n")

    writer = csv.DictWriter(out, fieldnames=ranking[0].keys(), delimiter=",")
    if not existing_content.strip():
        writer.writeheader()
    writer.writerows(ranking)

    file_bytes = out.getvalue().encode('utf-8')
    s3_client.upload_fileobj(io.BytesIO(file_bytes), bucket, object_name)
    logger.info(f"Saved {len(ranking)} ranking rows to {object_name}")

def run_ingestion(conn_vars):
    logger.info("Ranking ingestion starting")
    extract_bot(conn_vars)
    logger.info("Ranking ingestion finished")

if __name__ == "__main__":
    extract_bot()