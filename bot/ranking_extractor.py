import csv
import os
import shutil
from datetime import datetime, timedelta
from playwright.sync_api import Playwright, sync_playwright

RAW_DATA_DIR = os.path.join("data", "raw", "incremental")
HISTORICAL_DIR = os.path.join("data", "raw", "historical", "ranking")


def previous_year_process(current_year: int) -> None:
    prev_year = current_year - 1
    old_file = os.path.join(RAW_DATA_DIR, f"tb_incremental_ranking_{prev_year}.csv")

    if os.path.exists(old_file):
        os.makedirs(HISTORICAL_DIR, exist_ok=True)
        dest_file = os.path.join(HISTORICAL_DIR, f"tb_ranking_{prev_year}.csv")
        shutil.move(old_file, dest_file)


def is_date_already_processed(csv_path: str, target_date: str) -> bool:
    if not os.path.exists(csv_path):
        return False

    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("date") == target_date:
                return True
    return False


def run(playwright: Playwright) -> None:
    today = datetime.now()
    current_monday = today - timedelta(days=today.weekday())
    current_year = current_monday.year
    week_str = current_monday.strftime("%Y-%m-%d")

    previous_year_process(current_year)

    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    csv_path = os.path.join(RAW_DATA_DIR, f"tb_incremental_ranking_{current_year}.csv")

    if is_date_already_processed(csv_path, week_str):
        print(f"Data from {week_str} was already processed.")
        return

    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    try:
        page.goto("https://www.atptour.com/en/rankings/singles?rankRange=0-5000")

        cookie_btn = page.get_by_role("button", name="Accept All Cookies")
        if cookie_btn.is_visible(timeout=5000):
            cookie_btn.click()

        ranking = []
        raning_table = page.locator("xpath=/html/body/div[3]/div/div[2]/div[2]/div[1]/div/table[2]/tbody")
        raning_table.locator(".lower_row, .lower-row").first.wait_for(state="attached", timeout=15000)
        ranking_lines = raning_table.locator(".lower_row, .lower-row").all()

        for line in ranking_lines:
            cells = [c.strip() for c in line.locator("td").all_inner_texts()]
            if len(cells) >= 8:
                clean_player = cells[1].split("\n")[-1].strip()

                ranking.append({
                    "date": str(week_str).replace("-", ""),
                    "rank": cells[0],
                    "name": clean_player,
                    "age": cells[2],
                    "points": cells[3],
                    "lost_earned_points": cells[4],
                    "tourn_played": cells[5],
                    "dropping": cells[6],
                    "next_best": cells[7],
                })

        file_exists = os.path.exists(csv_path)

        if ranking:
            with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=ranking[0].keys(), delimiter=",")
                if not file_exists:
                    writer.writeheader()
                writer.writerows(ranking)

    except Exception as e:
        raise e
    finally:
        context.close()
        browser.close()


if __name__ == "__main__":
    with sync_playwright() as playwright:
        run(playwright)