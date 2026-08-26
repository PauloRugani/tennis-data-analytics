import csv
import os
from playwright.sync_api import Playwright, sync_playwright

RAW_DATA_DIR = os.path.join("data", "raw", "incremental")


def run(playwright: Playwright) -> None:
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

                contenct = {
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

        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        csv_path = os.path.join(RAW_DATA_DIR, "tb_ranking_weekly.csv")

        if ranking:
            with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=ranking[0].keys(), delimiter=",")
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