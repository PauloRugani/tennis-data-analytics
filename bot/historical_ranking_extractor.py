import csv
import os
from datetime import datetime, timedelta
from playwright.sync_api import Playwright, sync_playwright

RAW_DATA_DIR = os.path.join("data", "raw", "incremental")


def run(playwright: Playwright) -> None:
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    csv_path = os.path.join(RAW_DATA_DIR, "tb_ranking_historical.csv")

    fieldnames = [
        "date_week",
        "rank",
        "player",
        "age",
        "official_points",
        "lost_earned_points",
        "tourn_played",
        "dropping",
        "next_best",
    ]

    file_exists = os.path.exists(csv_path)

    today = datetime.now()
    current_monday = today - timedelta(days=today.weekday())
    date_cursor = current_monday
    end_date = datetime(1973, 8, 23)

    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=",")
        if not file_exists:
            writer.writeheader()

        while date_cursor >= end_date:
            if date_cursor == current_monday:
                date_param = "Current+Week"
            else:
                date_param = date_cursor.strftime("%Y-%m-%d")

            week_str = date_cursor.strftime("%Y-%m-%d")
            url = f"https://www.atptour.com/en/rankings/singles?rankRange=0-5000&dateWeek={date_param}"

            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            try:
                page.goto(url, timeout=10000)

                cookie_btn = page.get_by_role("button", name="Accept All Cookies")
                if cookie_btn.is_visible(timeout=5000):
                    cookie_btn.click()

                ranking = []
                raning_table = page.locator("xpath=/html/body/div[3]/div/div[2]/div[2]/div[1]/div/table[2]/tbody")
                raning_table.locator(".lower_row, .lower-row").first.wait_for(state="attached", timeout=10000)
                ranking_lines = raning_table.locator(".lower_row, .lower-row").all()

                for line in ranking_lines[:200]:
                    cells = [c.strip() for c in line.locator("td").all_inner_texts()]
                    if len(cells) >= 8:
                        clean_player = cells[1].split("\n")[-1].strip()

                        contenct = {
                            "date_week": week_str,
                            "rank": cells[0],
                            "player": clean_player,
                            "age": cells[2],
                            "official_points": cells[3],
                            "lost_earned_points": cells[4],
                            "tourn_played": cells[5],
                            "dropping": cells[6],
                            "next_best": cells[7],
                        }
                        ranking.append(contenct)

                if ranking:
                    writer.writerows(ranking)
                    f.flush()

            except Exception as e:
                print(f"Sem dados ou erro para a semana {week_str}: {e}")

            finally:
                context.close()
                browser.close()

            date_cursor -= timedelta(weeks=1)


if __name__ == "__main__":
    with sync_playwright() as playwright:
        run(playwright)