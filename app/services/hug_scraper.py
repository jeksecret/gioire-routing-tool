from dotenv import load_dotenv
import os
import re
from datetime import datetime
from playwright.sync_api import sync_playwright, expect
from app.supabase import get_supabase

# ==========================================
# 🔹 Load environment variables
# ==========================================
load_dotenv()
USERNAME = os.getenv("HUG_USERNAME")
PASSWORD = os.getenv("HUG_PASSWORD")

# ==========================================
# 🔹 Set scrape date (easy to edit / overrides allowed)
# ==========================================
SCRAPE_YEAR = os.getenv("SCRAPE_YEAR", "2025")
SCRAPE_MONTH = os.getenv("SCRAPE_MONTH", "10")
SCRAPE_DAY = os.getenv("SCRAPE_DAY", "10")

# ==========================================
# 🔹 Facility List
# ==========================================
FACILITIES = [
    "稲毛",
    "本千葉",
    "千葉大前",
    "アルト",
    "ジョイーレ石垣",
    "プリモいしがき",
    "ちぐさだい",
    "さくさべ",
    "牛久"
]

# ==========================================
# 🔹 Login Function (your updated version)
# ==========================================
def login(page):
    page.goto("https://www.hug-gioire.link/hug/wm/", wait_until="networkidle")

    page.get_by_role("textbox", name="ログインID").fill(USERNAME)
    page.get_by_role("textbox", name="パスワード").fill(PASSWORD)
    page.get_by_role("button", name="ログインする").click()

    # ===== YOUR SIMPLE ANNOUNCEMENT CLOSE =====
    page.wait_for_timeout(1500)
    page.get_by_role("button", name=" 閉じる").click()
    print("Announcement popup closed")

    expect(page.get_by_role("link", name=" 今日の送迎")).to_be_visible(timeout=10000)
    print("✅ Successfully logged in!")

# ==========================================
# 🔹 Date Selection
# ==========================================
def select_date(page, year: str, month: str, day: str):
    print(f"Selecting date: {year}-{month}-{day}")

    # Navigate
    page.get_by_role("link", name=" 今日の送迎").click()
    expect(page.locator("h1")).to_contain_text("の送迎管理")

    # Open datepicker
    page.get_by_role("listitem").filter(has_text="日付").click()

    # Select Year
    page.locator("#ui-datepicker-div").get_by_role("combobox").first.select_option(year)

    # Select Month
    page.locator("#ui-datepicker-div").get_by_role("combobox").nth(1).select_option(month)

    # Select Day
    page.get_by_role("link", name=day).click()

    # Auto-close → safe try
    try:
        page.get_by_role("button", name="閉じる").click(timeout=500)
    except:
        pass

    expected = f"{year}/{month.zfill(2)}/{day.zfill(2)}"
    expect(page.get_by_role("textbox")).to_have_value(expected)

    print(f"Date selected → {expected}")

    page.get_by_role("button", name="表示変更").click()
    print("Filter applied")

# ==========================================
# 🔹 Scrape ONE Facility
# ==========================================
def scrape_single_facility(page, facility_name):
    print(f"\n🔎 Scraping facility: {facility_name}")

    page.get_by_role("link", name="すべて解除").click()

    checkbox = page.locator(f'#facility_check input[value="{facility_name}"]')
    checkbox.check()

    page.get_by_role("button", name="表示変更").click()

    page.locator("div.pickTableWrap").wait_for(timeout=10000)
    page.locator("div.sendTableWrap").wait_for(timeout=10000)

    all_rows = []

    def scrape_section(wrapper_class, pickup_flag):
        wrapper = page.locator(f"div.{wrapper_class}")
        if wrapper.locator("table").count() == 0:
            return

        rows = wrapper.locator("table tbody tr").all()

        for row in rows:
            if row.locator("div.nameBox").count() == 0:
                continue

            # Time
            tcell = row.locator("td.greet_time_scheduled")
            time_val = None
            if tcell.count() > 0:
                raw_time = tcell.inner_text().strip()
                if raw_time and raw_time != "9999":
                    time_val = raw_time

            # Name
            raw_name = row.locator("div.nameBox").inner_text().replace("\n", " ").strip()
            user_name = re.sub(r"\s+", "　", raw_name)

            # Depot
            depot_cell = row.locator("td").nth(2)
            depot_name = depot_cell.inner_text().strip() if depot_cell.count() > 0 else None

            # Place
            if row.locator("td.absence").count() > 0:
                place = "欠席"
            else:
                place_cell = row.locator("td.place")
                ptext = place_cell.inner_text().strip() if place_cell.count() > 0 else ""
                place = ptext if ptext else "送迎なし"

            all_rows.append({
                "facility_name": facility_name,
                "target_time": time_val,
                "user_name": user_name,
                "depot_name": depot_name,
                "place": place,
                "pickup_flag": pickup_flag
            })

    scrape_section("pickTableWrap", "迎え")
    scrape_section("sendTableWrap", "送り")

    print(f"✔ {facility_name}: {len(all_rows)} rows scraped")
    return all_rows

# ==========================================
# 🔹 Scrape ALL facilities
# ==========================================
def scrape_all(page, year, month, day):
    login(page)
    select_date(page, year, month, day)

    print("\n🔍 Starting facility scraping...")

    all_data = []

    for f in FACILITIES:
        rows = scrape_single_facility(page, f)
        all_data.extend(rows)

    print(f"\n🎉 TOTAL SCRAPED ROWS = {len(all_data)}\n")
    return all_data

# ==========================================
# 🔹 Insert Into Supabase (UPDATED DATE FIX)
# ==========================================
def insert_scraped_data_to_supabase(rows):
    supabase = get_supabase()
    print("🚀 Inserting into Supabase...")

    formatted = []

    # Selected date (replaces datetime.now())
    selected_date = f"{SCRAPE_YEAR}-{SCRAPE_MONTH.zfill(2)}-{SCRAPE_DAY.zfill(2)}"

    for row in rows:
        time_raw = row["target_time"]
        target_dt = None

        if time_raw:
            dt_str = time_raw.replace("：", ":")
            try:
                target_dt = datetime.strptime(
                    f"{selected_date} {dt_str}",
                    "%Y-%m-%d %H:%M"
                )
            except:
                pass

        formatted.append({
            "pickup_flag": row["pickup_flag"] == "迎え",
            "facility_name": row["facility_name"],
            "user_name": row["user_name"],
            "depot_name": row["depot_name"],
            "place": row["place"],
            "target_time": target_dt.isoformat() if target_dt else None,
            "payload": row
        })

    supabase.schema("stg").from_("hug_raw_requests").insert(formatted).execute()
    print(f"✔ Inserted {len(formatted)} rows into Supabase.\n")

# ==========================================
# 🔹 Main Runner
# ==========================================
def main():
    year = SCRAPE_YEAR
    month = SCRAPE_MONTH
    day = SCRAPE_DAY

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=150)
        page = browser.new_page()

        rows = scrape_all(page, year, month, day)

        browser.close()

    # No deletion of old data
    insert_scraped_data_to_supabase(rows)


if __name__ == "__main__":
    main()
