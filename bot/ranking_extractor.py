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

def is_date_already_processed(s3_client, bucket: str, object_name: str, target_date: str) -> bool:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_name)
        content = response['Body'].read().decode('utf-8-sig')
        if not content.strip():
            return False
            
        target_date_clean = str(target_date).strip()
        
        f = io.StringIO(content)
        sample = content[:2048]
        delimiter = ";" if ";" in sample and "," not in sample else ","

        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            val = row.get("date")
            if val:
                val_clean = str(val).strip()
                if val_clean == target_date_clean:
                    return True
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return False
        raise

def previous_year_process(s3_client, bucket: str, current_monday: datetime):
    is_first_monday_of_year = (current_monday.month == 1 and current_monday.day <= 7)

    if not is_first_monday_of_year:
        return

    prev_year = current_monday.year - 1
    old_file = f"raw/incremental/tb_incremental_ranking_{prev_year}.csv"
    dest_file = f"raw/historical/ranking/tb_ranking_{prev_year}.csv"

    try:
        s3_client.head_object(Bucket=bucket, Key=old_file)
        
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': old_file},
            Key=dest_file
        )
        
        s3_client.delete_object(Bucket=bucket, Key=old_file)
    except Exception as e:
        logger.error(f"Failed to archive previous year ranking file: {e}")
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

    previous_year_process(s3_client, bucket, current_monday)

    object_name = f"raw/incremental/tb_incremental_ranking_{current_year}.csv"

    if is_date_already_processed(s3_client, bucket, object_name, target_date):
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
            if player_anchor:
                clean_player = player_anchor.get_text(strip=True)
            else:
                raw_player = cells[1].split("\n")[-1].strip()
                clean_player = re.sub(r"^[+-]?\d+", "", raw_player).strip()

            ranking.append(
                {
                    "date": target_date,
                    "rank": cells[0],
                    "name": clean_player,
                    "age": cells[2],
                    "points": cells[3],
                    "lost_earned_points": cells[4],
                    "tourn_played": cells[5],
                    "dropping": cells[6],
                    "next_best": cells[7],
                }
            )

    if not ranking:
        logger.info("No ranking data scraped, nothing to save")
        return None

    existing_content = ""
    file_exists = False
    resp = s3_client.get_object(Bucket=bucket, Key=object_name)
    existing_content = resp['Body'].read().decode('utf-8')
    file_exists = True

    out = io.StringIO()
    if existing_content:
        out.write(existing_content)
        if not existing_content.endswith("\n"):
            out.write("\n")
            
    writer = csv.DictWriter(out, fieldnames=ranking[0].keys(), delimiter=",")
    if not file_exists or not existing_content.strip():
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