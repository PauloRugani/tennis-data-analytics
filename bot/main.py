import os
from datetime import datetime
from playwright.sync_api import Playwright, sync_playwright

RAW_DATA_DIR = os.path.join("data", "raw")

def download_file(page, role_name: str, file_path: str) -> None:
    final_path = os.path.join(RAW_DATA_DIR, file_path)
    
    with page.expect_download(timeout=60000) as download_info:  # Timeout de 60s
        page.get_by_role("link", name=role_name).click()
    
    download = download_info.value
    download.save_as(final_path)

def run(playwright: Playwright) -> None:
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    try:
        page.goto("https://stats.tennismylife.org/tennis-match-database", wait_until="networkidle")

        year = datetime.now().year
        atp_filename = f"atp_matches_{year}.csv"
        atp_filepath = os.path.join(RAW_DATA_DIR, "raw_separated", atp_filename)

        if not os.path.exists(atp_filepath):
            download_file(page, f"Download {year}.csv", f"raw_separated/{atp_filename}")
        if os.path.exists(atp_filepath):
            ongoing_file = f"ongoing_tourneys_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            download_file(page, "Download ongoing_tourneys.csv", ongoing_file)

    except Exception as e:
        raise e
    finally:
        context.close()
        browser.close()

with sync_playwright() as playwright:
    run(playwright)