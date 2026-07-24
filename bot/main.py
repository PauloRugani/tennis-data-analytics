import os
from datetime import datetime
from playwright.sync_api import Playwright, sync_playwright

RAW_DATA_DIR = os.path.join("data", "raw")

def download_file(page, role_name: str, relative_path: str) -> None:
    final_path = os.path.join(RAW_DATA_DIR, relative_path)
    
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    
    with page.expect_download(timeout=60000) as download_info:
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
        
        file_current_year = f"atp_matches_{year}.csv"
        file_last_year = f"atp_matches_{year - 1}.csv"
        
        path_current_year = os.path.join(RAW_DATA_DIR, "raw_separated", file_current_year)

        if not os.path.exists(path_current_year):
            download_file(page, f"Download {year - 1}.csv", f"raw_separated/{file_last_year}")
            
        download_file(page, f"Download {year}.csv", f"raw_separated/{file_current_year}")
        download_file(page, "Download ongoing_tourneys.csv", "tb_ongoing_tourneys.csv")

    except Exception as e:
        raise e
    finally:
        context.close()
        browser.close()

if __name__ == "__main__":
    with sync_playwright() as playwright:
        run(playwright)