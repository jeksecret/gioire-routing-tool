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
# 🔹 Login Function
# ==========================================
def login(page):
    """Logs in to the HUG website using the provided page."""
    page.goto("https://www.hug-gioire.link/hug/wm/", wait_until="networkidle")
    page.get_by_role("textbox", name="ログインID").fill(USERNAME)
    page.get_by_role("textbox", name="パスワード").fill(PASSWORD)
    page.get_by_role("button", name="ログインする").click()

    # Close the announcement popup if shown
    try:
        expect(page.locator("iframe").content_frame.get_by_role("heading", name="HUGからのお知らせ")).to_be_visible(timeout=5000)
        page.get_by_role("button", name=" 閉じる").click()
    except Exception:
        pass

    expect(page.get_by_role("link", name=" 今日の送迎")).to_be_visible(timeout=10000)
    print("✅ Successfully logged in!")


# ==========================================
# 🔹 Scraping Function
# ==========================================
def scrape_table(page):
    """Scrape rows from 今日の送迎 page."""
    login(page)
    print("🔍 Starting full table scraping test...")

    # Go to 今日の送迎
    page.get_by_role("link", name=" 今日の送迎").click()
    expect(page).to_have_url(re.compile(r"pickup\.php"))
    expect(page.locator("h1")).to_contain_text("の送迎管理")

    # Enable all facilities
    page.get_by_role("link", name="すべてチェック").click()
    page.get_by_role("button", name="表示変更").click()

    expect(page.locator("div.pickTableWrap")).to_be_visible(timeout=15000)
    expect(page.locator("div.sendTableWrap")).to_be_visible(timeout=15000)

    def wait_section_ready(wrapper_css: str, timeout_ms: int = 15000):
        wrapper = page.locator(wrapper_css)
        try:
            wrapper.locator("table").first.wait_for(state="attached", timeout=timeout_ms)
        except Exception:
            page.wait_for_timeout(1000)

    wait_section_ready("div.pickTableWrap")
    wait_section_ready("div.sendTableWrap")

    all_rows = []

    def scrape_section(wrapper_class, pickup_flag):
        wrapper = page.locator(f"div.{wrapper_class}")
        if wrapper.locator("table").count() == 0:
            return

        rows = wrapper.locator("table tbody tr").all()
        for row in rows:
            if row.locator("div.nameBox").count() == 0:
                continue

            # --- お迎え希望時間 ---
            time_cell = row.locator("td.greet_time_scheduled")
            target_time = None
            if time_cell.count() > 0:
                text = time_cell.inner_text().strip()
                if text and text != "9999":
                    target_time = text

            # --- 児童名 ---
            raw_name = row.locator("div.nameBox").inner_text().replace("\n", " ").strip()

            # Normalize multiple spaces → one full-width space
            user_name = re.sub(r"\s+", "　", raw_name)

            # --- 施設名 ---
            depot_cell = row.locator("td").nth(2)
            depot_name = depot_cell.inner_text().strip() if depot_cell.count() > 0 else None

            # --- 場所 (handle 欠席 + 送迎なし) ---
            if row.locator("td.absence").count() > 0:
                place = "欠席"
            else:
                place_cell = row.locator("td.place")
                if place_cell.count() > 0:
                    place_text = place_cell.inner_text().strip()
                    place = place_text if place_text else "送迎なし"
                else:
                    place = "送迎なし"

            all_rows.append({
                "target_time": target_time,
                "user_name": user_name,
                "depot_name": depot_name,
                "place": place,
                "pickup_flag": pickup_flag,
            })

    scrape_section("pickTableWrap", "迎え")
    scrape_section("sendTableWrap", "送り")

    print(f"✅ Finished scraping. Found {len(all_rows)} rows.\n")
    return all_rows


# ==========================================
# 🔹 Clear Previous Data
# ==========================================
def clear_previous_data():
    """Delete all existing data in stg.hug_raw_requests before inserting new data."""
    supabase = get_supabase()
    print("🧹 Clearing previous staging data...")
    try:
        response = supabase.schema("stg").from_("hug_raw_requests").delete().neq("id", 0).execute()
        print(f"✅ Cleared previous records: {len(response.data or [])}\n")
    except Exception as e:
        print("⚠️ Failed to clear previous data:", e)


# ==========================================
# 🔹 Supabase Insert Logic
# ==========================================
def insert_scraped_data_to_supabase(scraped_rows):
    """Insert scraped pickup/drop-off data into stg.hug_raw_requests."""
    supabase = get_supabase()
    print("🚀 Inserting scraped rows into Supabase...")

    formatted_rows = []
    for row in scraped_rows:
        try:
            time_str = row.get("target_time")
            user_name = row.get("user_name")
            depot_name = row.get("depot_name")
            place = row.get("place")
            pickup_text = row.get("pickup_flag")
            pickup_flag = pickup_text.strip() == "迎え"

            target_dt = None
            if time_str:
                try:
                    today = datetime.now().strftime("%Y-%m-%d")
                    dt_str = time_str.replace("：", ":")
                    target_dt = datetime.strptime(f"{today} {dt_str}", "%Y-%m-%d %H:%M")
                except Exception:
                    target_dt = None

            formatted_rows.append({
                "pickup_flag": pickup_flag,
                "user_name": user_name.strip(),
                "depot_name": depot_name.strip() if depot_name else None,
                "place": place.strip() if place else None,
                "target_time": target_dt.isoformat() if target_dt else None,
                "payload": {"raw_row": row}
            })
        except Exception:
            continue

    if not formatted_rows:
        print("❌ No valid rows to insert.\n")
        return

    try:
        supabase.schema("stg").from_("hug_raw_requests").insert(formatted_rows).execute()
        print(f"✅ Insert complete: {len(formatted_rows)} rows added.\n")
    except Exception as e:
        print("❌ Insert failed:", e)


# ==========================================
# 🔹 Main Runner
# ==========================================
def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=150)
        page = browser.new_page()
        all_rows = scrape_table(page)
        browser.close()

    clear_previous_data()
    insert_scraped_data_to_supabase(all_rows)


if __name__ == "__main__":
    main()
