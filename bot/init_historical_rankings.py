import os
import io
import re
import csv
import zipfile
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from curl_cffi import requests

def get_mondays_of_year(year: int):
    mondays = []
    d = datetime(year, 1, 1)
    while d.weekday() != 0:
        d += timedelta(days=1)
        
    today = datetime.now()
    while d <= today:
        mondays.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=7)
        
    return mondays

def extract_historical_rankings():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    historical_dir = os.path.join(base_dir, "raw", "historical", "ranking")
    incremental_dir = os.path.join(base_dir, "raw", "incremental")

    os.makedirs(historical_dir, exist_ok=True)
    os.makedirs(incremental_dir, exist_ok=True)

    current_year = datetime.now().year

    print("Downloading historical rankings from GitHub...")
    repo_url = "https://github.com/Tennismylife/TML-Rankings-Database/archive/refs/heads/main.zip"
    resp = requests.get(repo_url, timeout=120)
    
    if resp.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            for file_info in z.infolist():
                if "TML Rankings/" in file_info.filename and file_info.filename.endswith(".csv"):
                    match = re.search(r"(\d{4})\.csv$", file_info.filename)
                    if match:
                        year = int(match.group(1))
                        if year < current_year:
                            dest_filename = f"tb_ranking_{year}.csv"
                            dest_path = os.path.join(historical_dir, dest_filename)
                            
                            with z.open(file_info) as source, open(dest_path, "wb") as target:
                                target.write(source.read())
        print("Historical rankings downloaded and saved.")
    else:
        print(f"Failed to download repository")

    current_year_hist = os.path.join(historical_dir, f"tb_ranking_{current_year}.csv")
    if os.path.exists(current_year_hist):
        os.remove(current_year_hist)

    mondays = get_mondays_of_year(current_year)
    all_ranking_data = []

    print(f"Scraping current year rankings for {len(mondays)} Mondays...")
    for week_str in mondays:
        target_date = week_str.replace("-", "")
        print(f"Processing week {week_str}...")
        
        url = f"https://www.atptour.com/en/rankings/singles?rankRange=0-5000&dateWeek={week_str}"
        resp_atp = requests.get(url, impersonate="chrome", timeout=60)
        
        soup = BeautifulSoup(resp_atp.text, "html.parser")
        rows = soup.select(".lower_row, .lower-row, tr.lower-row, tr.lower_row")
        
        if not rows:
            print(f"No data found for {week_str}")
            continue

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

                all_ranking_data.append(
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

    if all_ranking_data:
        inc_file_name = f"tb_incremental_ranking_{current_year}.csv"
        inc_file_path = os.path.join(incremental_dir, inc_file_name)
        
        fieldnames = all_ranking_data[0].keys()
        
        with open(inc_file_path, "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=",")
            writer.writeheader()
            writer.writerows(all_ranking_data)
            
        print(f"Saved {len(all_ranking_data)} ranking records to {inc_file_path}")
    else:
        print("No incremental ranking data found.")

if __name__ == "__main__":
    print("Starting historical rankings extraction...")
    extract_historical_rankings()
    print("Finished.")
