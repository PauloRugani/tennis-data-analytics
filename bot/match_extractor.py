import os
import shutil
import tempfile
from datetime import datetime
from playwright.sync_api import Playwright, sync_playwright

AIRFLOW_TEMP_DIR = "/tmp/airflow_staging"
LOCAL_RAW_DATA_DIR = os.path.join("data", "raw")

def _download_file(page, role_name: str, base_dir: str, relative_path: str):
    final_path = os.path.join(base_dir, relative_path)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    with page.expect_download(timeout=60000) as download_info:
        page.get_by_role("link", name=role_name).click()

    download = download_info.value
    download.save_as(final_path)
    print(f"[Download] Saved: {final_path}")
    return final_path

def run(save_folder: str):
    os.makedirs(save_folder, exist_ok=True)
    downloaded_files = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto("https://stats.tennismylife.org/tennis-match-database", wait_until="networkidle")
            year = datetime.now().year

            file_curr = f"historical/matches/atp_matches_{year}.csv"
            file_prev = f"historical/matches/atp_matches_{year - 1}.csv"
            file_ongoing = "incremental/tb_ongoing_tourneys.csv"

            if not os.path.exists(os.path.join(save_folder, file_curr)):
                path_prev = _download_file(page, f"Download {year - 1}.csv", save_folder, file_prev)
                downloaded_files.append((path_prev, file_prev))

            path_curr = _download_file(page, f"Download {year}.csv", save_folder, file_curr)
            downloaded_files.append((path_curr, file_curr))

            path_ongoing = _download_file(page, "Download ongoing_tourneys.csv", save_folder, file_ongoing)
            downloaded_files.append((path_ongoing, file_ongoing))

        finally:
            context.close()
            browser.close()

    return downloaded_files


def run_airflow():
    print(f"[Airflow] Download starts at: {AIRFLOW_TEMP_DIR}")
    run(save_folder=AIRFLOW_TEMP_DIR)

def run_local():
    results = run(save_folder=LOCAL_RAW_DATA_DIR)