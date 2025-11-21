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

SCRAPE_YEAR = os.getenv("SCRAPE_YEAR", "2025")
SCRAPE_MONTH = os.getenv("SCRAPE_MONTH", "10")
SCRAPE_DAY = os.getenv("SCRAPE_DAY", "10")

SCRAPE_FACILITY = "千葉大前"


# ==========================================
# 🔹 Name extraction (NEW CLEAN VERSION)
# ==========================================
def extract_clean_name(raw_text: str) -> str:
    """
    Extract the actual child name only.
    Removes:
    - furigana (hiragana-only lines)
    - trailing 'さん' / 'くん' / 'ちゃん'
    - whitespace & fullwidth whitespace
    """
    if not raw_text:
        return ""

    # Split into meaningful lines
    lines = [line.strip() for line in raw_text.split("\n") if line.strip()]
    last_line = ""

    # The actual name is always the non-hiragana line
    for line in lines:
        # Skip pure hiragana (furigana)
        if re.fullmatch(r"[ぁ-ゖー\s]+", line):
            continue
        last_line = line

    # Remove suffixes (さん, くん, ちゃん)
    last_line = re.sub(r"(さん|くん|ちゃん)\s*$", "", last_line)

    # Remove whitespace
    last_line = last_line.replace(" ", "").replace("　", "")

    return last_line


# ==========================================
# 🔹 Login
# ==========================================
def login(page):
    page.goto("https://www.hug-gioire.link/hug/wm/", wait_until="networkidle")

    page.get_by_role("textbox", name="ログインID").fill(USERNAME)
    page.get_by_role("textbox", name="パスワード").fill(PASSWORD)
    page.get_by_role("button", name="ログインする").click()

    # # Close announcement
    # page.wait_for_timeout(1500)
    # try:
    #     page.get_by_role("button", name=" 閉じる").click()
    #     print("Announcement popup closed")
    # except:
    #     pass

    # expect(page.get_by_role("link", name=" 今日の送迎")).to_be_visible(timeout=10000)
    print("✅ Logged in")


# ==========================================
# 🔹 Select date
# ==========================================
def select_date(page, year, month, day):
    print(f"Selecting date: {year}-{month}-{day}")

    page.get_by_role("link", name=" 今日の送迎").click()
    expect(page.locator("h1")).to_contain_text("の送迎管理")

    # Open datepicker
    page.get_by_role("listitem").filter(has_text="日付").click()

    # Year
    page.locator("#ui-datepicker-div").get_by_role("combobox").first.select_option(year)
    # Month
    page.locator("#ui-datepicker-div").get_by_role("combobox").nth(1).select_option(month)
    # Day
    page.get_by_role("link", name=day).click()

    try:
        page.get_by_role("button", name="閉じる").click(timeout=500)
    except:
        pass

    expected = f"{year}/{month.zfill(2)}/{day.zfill(2)}"
    expect(page.get_by_role("textbox")).to_have_value(expected)
    print("Date selected ✔")

    page.get_by_role("button", name="表示変更").click()
    print("Filter applied")


# ==========================================
# 🔹 Scrape ONE facility (with new name logic)
# ==========================================
def scrape_single_facility(page, facility_name):
    print(f"\n🔎 Scraping facility: {facility_name}")

    # Clear all
    page.get_by_role("link", name="すべて解除").click()

    # Select one
    checkbox = page.locator(f'#facility_check input[value="{facility_name}"]')
    checkbox.check()

    page.get_by_role("button", name="表示変更").click()

    # Ensure tables load
    page.locator("div.pickTableWrap").wait_for(timeout=10000)
    page.locator("div.sendTableWrap").wait_for(timeout=10000)

    rows_all = []

    def scrape_section(wrapper_class, pickup_flag):
        wrapper = page.locator(f"div.{wrapper_class}")
        if wrapper.locator("table").count() == 0:
            return

        rows = wrapper.locator("table tbody tr").all()

        for row in rows:
            # Name box missing → skip
            if row.locator("div.nameBox").count() == 0:
                continue

            # Time
            tcell = row.locator("td.greet_time_scheduled")
            time_val = None
            if tcell.count() > 0:
                val = tcell.inner_text().strip()
                if val and val != "9999":
                    time_val = val

            # Name (NEW CLEAN LOGIC)
            raw_name = row.locator("div.nameBox").inner_text().strip()
            user_name = extract_clean_name(raw_name)

            # Depot
            depot_cell = row.locator("td").nth(2)
            depot_name = depot_cell.inner_text().strip() if depot_cell.count() > 0 else None

            # Place
            if row.locator("td.absence").count() > 0:
                place = "欠席"
            else:
                pcell = row.locator("td.place")
                ptext = pcell.inner_text().strip() if pcell.count() > 0 else ""
                place = ptext if ptext else "送迎なし"

            rows_all.append({
                "facility_name": facility_name,
                "target_time": time_val,
                "user_name": user_name,
                "depot_name": depot_name,
                "place": place,
                "pickup_flag": pickup_flag,
            })

    scrape_section("pickTableWrap", "迎え")
    scrape_section("sendTableWrap", "送り")

    print(f"✔ Scraped: {len(rows_all)} rows")
    return rows_all


# ==========================================
# 🔹 Insert to Supabase
# ==========================================
def insert_scraped_data_to_supabase(rows):
    supabase = get_supabase()
    print("🚀 Inserting into Supabase...")

    selected_date = f"{SCRAPE_YEAR}-{SCRAPE_MONTH.zfill(2)}-{SCRAPE_DAY.zfill(2)}"
    formatted = []

    for row in rows:
        time_raw = row["target_time"]
        target_dt = None

        if time_raw:
            try:
                dt_str = time_raw.replace("：", ":")
                target_dt = datetime.strptime(f"{selected_date} {dt_str}", "%Y-%m-%d %H:%M")
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
    print(f"✔ Inserted {len(formatted)} rows")


# ==========================================
# 🔹 Main
# ==========================================
def main():
    year = SCRAPE_YEAR
    month = SCRAPE_MONTH
    day = SCRAPE_DAY

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=130)
        page = browser.new_page()

        login(page)
        select_date(page, year, month, day)
        rows = scrape_single_facility(page, SCRAPE_FACILITY)

        browser.close()

    insert_scraped_data_to_supabase(rows)


if __name__ == "__main__":
    main()
