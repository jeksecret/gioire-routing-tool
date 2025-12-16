from dotenv import load_dotenv
import os
import re
from datetime import datetime
from playwright.sync_api import sync_playwright, expect, TimeoutError as PlaywrightTimeout

from app.supabase import get_supabase

# optimization_run helpers
from app.services.optimization_run import (
    get_existing_run,
    create_new_run,
    set_status_scraping,
    set_status_optimizing,
    set_status_scrape_error,
    set_meta_json,
)

# ==========================================
# Load environment variables
# ==========================================
load_dotenv()
USERNAME = os.getenv("HUG_USERNAME")
PASSWORD = os.getenv("HUG_PASSWORD")

SCRAPE_FACILITY = os.getenv("SCRAPE_FACILITY")
SCRAPE_YEAR = os.getenv("SCRAPE_YEAR")
SCRAPE_MONTH = os.getenv("SCRAPE_MONTH")
SCRAPE_DAY = os.getenv("SCRAPE_DAY")


# ==========================================
# Login flow (stable)
# ==========================================
def login_and_open_shuttle_page(page):
    print("Opening login page...")
    page.goto("https://www.hug-gioire.link/hug/wm/")

    print("Filling login form...")
    page.get_by_role("textbox", name="ログインID").fill(USERNAME)
    page.get_by_role("textbox", name="パスワード").fill(PASSWORD)

    page.get_by_role("button", name="ログインする").click()
    print("Login submitted...")

    page.locator("iframe").content_frame.get_by_role("heading", name="HUGからのお知らせ").click()
    page.locator("iframe").content_frame.get_by_text("お知らせ", exact=True).click()
    page.get_by_role("button", name=" 閉じる").click()
    page.get_by_role("link", name=" 今日の送迎").click()

    print("✅ Opened today’s pickup & drop-off page")


# ==========================================
# Date selection
# ==========================================
def select_date(page, year, month, day):
    print(f"Selecting date: {year}-{month}-{day}")

    page.get_by_role("listitem").filter(has_text="日付").click()

    # Convert month/day to single-digit numbers when needed
    year_str = str(int(year))
    month_str = str(int(month))   # ensures "03" -> "3"
    day_str = str(int(day))       # ensures "03" -> "3"

    # Select year
    page.locator("#ui-datepicker-div").get_by_role("combobox").first.select_option(year_str)

    # Select month
    page.locator("#ui-datepicker-div").get_by_role("combobox").nth(1).select_option(month_str)

    # Select day (EXACT match)
    page.get_by_role("link", name=day_str, exact=True).click()

    # Close datepicker if needed
    try:
        page.get_by_role("button", name="閉じる").click(timeout=500)
    except:
        pass

    expected = f"{year}/{month.zfill(2)}/{day.zfill(2)}"
    expect(page.get_by_role("textbox")).to_have_value(expected)

    print("✔ Date selected")
    page.get_by_role("button", name="表示変更").click()
    print("✔ Filter applied")


# ==========================================
# Clean name extraction
# ==========================================
def extract_clean_name(raw_text: str) -> str:
    if not raw_text:
        return ""

    lines = [line.strip() for line in raw_text.split("\n") if line.strip()]
    last_line = ""

    for line in lines:
        if re.fullmatch(r"[ぁ-ゖー\s]+", line):
            continue
        last_line = line

    last_line = re.sub(r"(さん|くん|ちゃん)\s*$", "", last_line)
    last_line = last_line.replace(" ", "").replace("　", "")
    return last_line


# ==========================================
# Scrape a single facility
# ==========================================
def scrape_single_facility(page, facility_name):
    print(f"\n🔎 Scraping facility: {facility_name}")

    page.get_by_role("link", name="すべて解除").click()
    page.locator(f'#facility_check input[value="{facility_name}"]').check()
    page.get_by_role("button", name="表示変更").click()

    page.locator("div.pickTableWrap").wait_for(timeout=10000)
    page.locator("div.sendTableWrap").wait_for(timeout=10000)

    rows_all = []

    def scrape_section(wrapper_class, pickup_flag):
        wrapper = page.locator(f"div.{wrapper_class}")
        if wrapper.locator("table").count() == 0:
            return

        for row in wrapper.locator("table tbody tr").all():

            if row.locator("div.nameBox").count() == 0:
                continue

            tcell = row.locator("td.greet_time_scheduled")
            time_val = None
            if tcell.count() > 0:
                raw = tcell.inner_text().strip()
                if raw and raw != "9999":
                    time_val = raw

            raw_name = row.locator("div.nameBox").inner_text().strip()
            user_name = extract_clean_name(raw_name)

            depot_cell = row.locator("td").nth(2)
            depot_name = depot_cell.inner_text().strip() if depot_cell.count() > 0 else None

            if row.locator("td.absence").count() > 0:
                place = "欠席"
            else:
                pcell = row.locator("td.place")
                place = pcell.inner_text().strip() if pcell.count() > 0 else "送迎なし"

            rows_all.append({
                "target_time": time_val,
                "user_name": user_name,
                "depot_name": depot_name,
                "place": place,
                "pickup_flag": pickup_flag,
            })

    scrape_section("pickTableWrap", "迎え")
    scrape_section("sendTableWrap", "送り")

    print(f"✔ Scraped {len(rows_all)} rows")
    return rows_all


# ==========================================
# Insert into Supabase
# ==========================================
def insert_scraped_data_to_supabase(rows, route_date):
    supabase = get_supabase()

    formatted = []
    for row in rows:
        target_dt = None
        if row["target_time"]:
            cleaned = row["target_time"].replace("：", ":")
            target_dt = datetime.strptime(f"{route_date} {cleaned}", "%Y-%m-%d %H:%M")

        formatted.append({
            "pickup_flag": row["pickup_flag"] == "迎え",
            "user_name": row["user_name"],
            "depot_name": row["depot_name"],
            "place": row["place"],
            "target_time": target_dt.isoformat() if target_dt else None,
            "payload": row,
        })

    supabase.schema("stg").from_("hug_raw_requests").insert(formatted).execute()


# ==========================================
# MAIN
# ==========================================
def main():
    facility = SCRAPE_FACILITY
    route_date = f"{SCRAPE_YEAR}-{SCRAPE_MONTH.zfill(2)}-{SCRAPE_DAY.zfill(2)}"

    existing = get_existing_run(facility, route_date)

    # If run exists → show summary and skip
    if existing:
        print("⚠ Existing run found for this facility + date.")
        print(f"ℹ Current status: {existing['status']}")

        meta = existing.get("meta_json") or {}
        row_count = meta.get("row_count", 0)
        print(f"ℹ Previous run imported {row_count} rows.")

        print("ℹ Already imported — skipping scrape.")
        return

    # Create new run
    run_id = create_new_run(facility, route_date, requested_by="system")
    print(f"🆔 optimization_run_id: {run_id}")
    set_status_scraping(run_id)
    print("✔ Run moved to scraping status")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-gpu", "--no-sandbox"]
            )
            page = browser.new_page()

            login_and_open_shuttle_page(page)
            select_date(page, SCRAPE_YEAR, SCRAPE_MONTH, SCRAPE_DAY)

            rows = scrape_single_facility(page, facility)

            browser.close()


        # Save meta_json snapshot
        set_meta_json(run_id, {
            "facility_name": facility,
            "route_date": route_date,
            "row_count": len(rows),
            "rows": rows,
        })
        print("ℹ Scraped rows recorded in meta_json")

        # Move to optimizing
        set_status_optimizing(run_id)
        print("✔ Run moved to optimizing status")

        # Insert rows only if not empty
        if len(rows) > 0:
            print("🚀 Inserting scraped data into Supabase...")
            insert_scraped_data_to_supabase(rows, route_date)
            print("✔ Saved rows to Supabase")

    except PlaywrightTimeout:
        print("❌ TIMEOUT — marking scrape_error")
        set_status_scrape_error(run_id)
        return

    except Exception as e:
        print("❌ ERROR:", e)
        set_status_scrape_error(run_id)
        return


if __name__ == "__main__":
    main()
