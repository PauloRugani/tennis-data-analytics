import os
import logging
from datetime import datetime
from curl_cffi import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_historical_matches():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    historical_dir = os.path.join(base_dir, "raw", "historical", "matches")
    incremental_dir = os.path.join(base_dir, "raw", "incremental")

    os.makedirs(historical_dir, exist_ok=True)
    os.makedirs(incremental_dir, exist_ok=True)

    current_year = datetime.now().year

    for year in range(1968, current_year + 1):
        file_name = f"atp_matches_{year}.csv"
        dest_path = os.path.join(historical_dir, file_name)
        
        if os.path.exists(dest_path):
            logger.info(f"{file_name} already exists locally, skipping")
            continue
        
        url = f"https://stats.tennismylife.org/data/{year}.csv"
        logger.info(f"Downloading {file_name} from {url}...")
        
        resp = requests.get(url, impersonate="chrome", timeout=60)
        if resp.status_code == 200:
            with open(dest_path, "wb") as f:
                f.write(resp.content)
            logger.info(f"Saved to {dest_path}")
        else:
            logger.error(f"Failed to download {year}.csv: HTTP {resp.status_code}")

    ongoing_name = "tb_ongoing_tourneys.csv"
    ongoing_dest = os.path.join(incremental_dir, ongoing_name)
    ongoing_url = "https://stats.tennismylife.org/data/ongoing_tourneys.csv"
    
    if os.path.exists(ongoing_dest):
        os.remove(ongoing_dest)
        
    logger.info(f"Downloading {ongoing_name} from {ongoing_url}...")
    resp_ongoing = requests.get(ongoing_url, impersonate="chrome", timeout=60)
    
    if resp_ongoing.status_code == 200:
        with open(ongoing_dest, "wb") as f:
            f.write(resp_ongoing.content)
        logger.info(f"Saved to {ongoing_dest}")
    else:
        logger.error("Failed to download ongoing_tourneys")

if __name__ == "__main__":
    logger.info("Starting historical matches extraction...")
    extract_historical_matches()
    logger.info("Finished")
