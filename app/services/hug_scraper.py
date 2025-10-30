from dotenv import load_dotenv
import os

load_dotenv()

USERNAME = os.getenv("HUG_USERNAME")
PASSWORD = os.getenv("HUG_PASSWORD")

import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, expect
from app.supabase import get_supabase

# ==========================================
# 🔹 Login Function
# ==========================================
def login(page):
    """Logs in to the HUG website using the provided page."""
    page.goto("https://www.hug-gioire.link/hug/wm/", wait_until="networkidle")
    page.get_by_role("textbox", name="ログインID").fill(USERNAME)
    page.get_by_role("textbox", name="パスワード").fill(PASSWORD)
    page.get_by_role("button", name="ログインする").click()

    # Wait for dashboard link to confirm login success
    expect(page.get_by_role("link", name=" 今日の送迎")).to_be_visible(timeout=15000)

    # Close the announcement popup if visible
    expect(page.locator("iframe").content_frame.get_by_role("heading", name="HUGからのお知らせ")).to_be_visible(timeout=10000)
    page.get_by_role("button", name=" 閉じる").click()

    print("✅ Successfully logged in!")


# ==========================================
# 🔹 Scraping Function
# ==========================================
def scrape_table(page):
    """Scrape rows from 今日の送迎 page and print results."""
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

    # Helper: wait for table readiness
    def wait_section_ready(wrapper_css: str, timeout_ms: int = 15000):
        wrapper = page.locator(wrapper_css)
        try:
            wrapper.locator("table").first.wait_for(state="attached", timeout=timeout_ms)
        except Exception:
            print(f"⏳ Waiting longer for table inside {wrapper_css}...")
            page.wait_for_timeout(1000)

    wait_section_ready("div.pickTableWrap")
    wait_section_ready("div.sendTableWrap")

    all_rows = []

    # ------------------------
    # Helper to scrape a section
    # ------------------------
    def scrape_section(wrapper_class, pickup_flag):
        wrapper = page.locator(f"div.{wrapper_class}")

        if wrapper.locator("table").count() == 0:
            print(f"ℹ️ Section visible but no table inside {wrapper_class}.")
            return

        rows = wrapper.locator("table tbody tr").all()
        for row in rows:
            if row.locator("div.nameBox").count() == 0:
                continue  # skip rows with no child name

            # --- お迎え希望時間 ---
            time_cell = row.locator("td.greet_time_scheduled")
            target_time = None
            if time_cell.count() > 0:
                text = time_cell.inner_text().strip()
                if text and text != "9999":
                    target_time = text

            # --- 児童名 ---
            user_name = row.locator("div.nameBox").inner_text().replace("\n", " ").strip()

            # --- 施設名 ---
            depot_cell = row.locator("td").nth(2)
            depot_name = depot_cell.inner_text().strip() if depot_cell.count() > 0 else None

            # --- 場所 (欠席 / 送迎なし handling) ---
            place = None
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

    # --- Scrape Pickup and Drop-off ---
    print("\n--- 迎え (Pickup) ---")
    scrape_section("pickTableWrap", "迎え")

    print("\n--- 送り (Drop-off) ---")
    scrape_section("sendTableWrap", "送り")

    # --- Print results ---
    print("\n🧾 Extracted Data:")
    print("お迎え希望時間 | 児童名 | 施設名 | 場所 | 迎え/送り")
    print("------------------------------------------------------")
    for row in all_rows:
        print(f"{row['target_time'] or 'None'} | {row['user_name']} | "
            f"{row['depot_name'] or 'None'} | {row['place'] or 'None'} | {row['pickup_flag']}")

    print(f"\n✅ Finished scraping. Found {len(all_rows)} rows.")
    return all_rows


# ==========================================
# 🔹 Supabase Insert Logic
# ==========================================
def insert_scraped_data_to_supabase(scraped_rows):
    """Insert scraped pickup/drop-off data into stg.hug_raw_requests."""
    supabase = get_supabase()

    print("\n🚀 Inserting scraped rows into Supabase...")
    formatted_rows = []

    for row in scraped_rows:
        try:
            time_str = row.get("target_time")
            user_name = row.get("user_name")
            depot_name = row.get("depot_name")
            place = row.get("place")
            pickup_text = row.get("pickup_flag")

            pickup_flag = pickup_text.strip() == "迎え"

            # Combine today's date with scraped time for proper datetime
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
                "depot_name": depot_name.strip(),
                "place": place.strip(),
                "target_time": target_dt.isoformat() if target_dt else None,  # ISO string for timestamptz
                "payload": {"raw_row": row}
            })
        except Exception as e:
            print("⚠️ Skipping invalid row:", row, e)

    if not formatted_rows:
        print("❌ No valid rows to insert.")
        return

    try:
        response = supabase.schema("stg").from_("hug_raw_requests").insert(formatted_rows).execute()
        print(f"✅ Insert complete: {len(formatted_rows)} rows added.")
        print(response)
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

    insert_scraped_data_to_supabase(all_rows)


if __name__ == "__main__":
    main()
