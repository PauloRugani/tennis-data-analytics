import csv
import os
import re
import shutil
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from curl_cffi import requests

AIRFLOW_TEMP_DIR = "/tmp/airflow_staging"
LOCAL_RAW_DATA_DIR = os.path.join("data", "raw")

def is_date_already_processed(csv_path: str, target_date: str) -> bool:
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return False

    target_date_clean = str(target_date).strip()
    found_dates = set()

    with open(csv_path, mode="r", encoding="utf-8-sig") as f:
        sample = f.read(2048)
        f.seek(0)
        delimiter = ";" if ";" in sample and "," not in sample else ","

        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            val = row.get("date")
            if val:
                val_clean = str(val).strip()
                found_dates.add(val_clean)
                if val_clean == target_date_clean:
                    return True
    return False

def previous_year_process(current_monday: datetime, save_folder: str):
    is_first_monday_of_year = (current_monday.month == 1 and current_monday.day <= 7)

    if not is_first_monday_of_year:
        return

    prev_year = current_monday.year - 1
    old_file = os.path.join(
        save_folder, f"incremental/tb_incremental_ranking_{prev_year}.csv"
    )

    if os.path.exists(old_file):
        os.makedirs(os.path.join(save_folder, "historical/ranking"), exist_ok=True)
        dest_file = os.path.join(
            save_folder, f"historical/ranking/tb_ranking_{prev_year}.csv"
        )
        shutil.move(old_file, dest_file)
        print(f"Moved: {old_file} -> {dest_file}")
        
def extract_bot(save_folder: str):
    today = datetime.now()
    current_monday = today - timedelta(days=today.weekday())
    current_year = current_monday.year
    week_str = current_monday.strftime("%Y-%m-%d")
    target_date = str(week_str).replace("-", "")

    previous_year_process(current_monday, save_folder)

    incremental_dir = os.path.join(save_folder, "incremental")
    os.makedirs(incremental_dir, exist_ok=True)

    filename = f"tb_incremental_ranking_{current_year}.csv"
    csv_path = os.path.join(incremental_dir, filename)

    if is_date_already_processed(csv_path, target_date):
        return csv_path

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
        print("No data")
        return None

    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0

    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ranking[0].keys(), delimiter=",")
        if not file_exists:
            writer.writeheader()
        writer.writerows(ranking)
    return csv_path

def run_ingestion():
    print(f"[Airflow] Download starts at: {AIRFLOW_TEMP_DIR}")
    extract_bot(save_folder=AIRFLOW_TEMP_DIR)

if __name__ == "__main__":
    created_file = extract_bot(save_folder=LOCAL_RAW_DATA_DIR)
    print(created_file)