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
# 🔹 Facility List (Based on HTML spec)
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
# 🔹 Login Function
# ==========================================
def login(page):
    page.goto("https://www.hug-gioire.link/hug/wm/", wait_until="networkidle")
    page.get_by_role("textbox", name="ログインID").fill(USERNAME)
    page.get_by_role("textbox", name="パスワード").fill(PASSWORD)
    page.get_by_role("button", name="ログインする").click()

    # Try to close popup
    try:
        iframe = page.locator("iframe").content_frame
        expect(iframe.get_by_role("heading", name="HUGからのお知らせ")).to_be_visible(timeout=5000)
        page.get_by_role("button", name=" 閉じる").click()
    except Exception:
        pass

    expect(page.get_by_role("link", name=" 今日の送迎")).to_be_visible(timeout=10000)
    print("✅ Successfully logged in!")


# ==========================================
# 🔹 Scrape ONE Facility
# ==========================================
def scrape_single_facility(page, facility_name):
    print(f"\n🔎 Scraping facility: {facility_name}")

    # 1. Reset all facilities
    page.get_by_role("link", name="すべて解除").click()

    # 2. Check ONLY the selected facility
    checkbox = page.locator(f'#facility_check input[value="{facility_name}"]')
    checkbox.check()

    # 3. Apply filter
    page.get_by_role("button", name="表示変更").click()

    # 4. Wait tables to refresh
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

            # --- 時間 ---
            tcell = row.locator("td.greet_time_scheduled")
            time_val = None
            if tcell.count() > 0:
                raw_time = tcell.inner_text().strip()
                if raw_time and raw_time != "9999":
                    time_val = raw_time

            # --- 名前 ---
            raw_name = row.locator("div.nameBox").inner_text().replace("\n", " ").strip()
            user_name = re.sub(r"\s+", "　", raw_name)  # Normalize → full-width space

            # --- Depot ---
            depot_cell = row.locator("td").nth(2)
            depot_name = depot_cell.inner_text().strip() if depot_cell.count() > 0 else None

            # --- Place ---
            if row.locator("td.absence").count() > 0:
                place = "欠席"
            else:
                place_cell = row.locator("td.place")
                if place_cell.count() > 0:
                    ptext = place_cell.inner_text().strip()
                    place = ptext if ptext else "送迎なし"
                else:
                    place = "送迎なし"

            all_rows.append({
                "facility_name": facility_name,
                "target_time": time_val,
                "user_name": user_name,
                "depot_name": depot_name,
                "place": place,
                "pickup_flag": pickup_flag
            })

    # Scrape both pickup + dropoff
    scrape_section("pickTableWrap", "迎え")
    scrape_section("sendTableWrap", "送り")

    print(f"✔ {facility_name}: {len(all_rows)} rows scraped")
    return all_rows


# ==========================================
# 🔹 Scrape ALL Facilities
# ==========================================
def scrape_all(page):
    login(page)

    print("\n🔍 Navigating to 今日の送迎 page...")
    page.get_by_role("link", name=" 今日の送迎").click()
    expect(page).to_have_url(re.compile(r"pickup\.php"))

    expect(page.locator("h1")).to_contain_text("の送迎管理")

    all_data = []

    # Run per-facility scraping
    for f in FACILITIES:
        rows = scrape_single_facility(page, f)
        all_data.extend(rows)

    print(f"\n🎉 TOTAL SCRAPED ROWS = {len(all_data)}\n")
    return all_data


# ==========================================
# 🔹 Clear Previous Data
# ==========================================
def clear_previous_data():
    supabase = get_supabase()
    print("🧹 Clearing previous staging data...")
    try:
        res = supabase.schema("stg").from_("hug_raw_requests").delete().neq("id", 0).execute()
        print(f"✔ Cleared: {len(res.data or [])} rows")
    except Exception as e:
        print("⚠️ Clear failed:", e)


# ==========================================
# 🔹 Insert into Supabase
# ==========================================
def insert_scraped_data_to_supabase(rows):
    supabase = get_supabase()
    print("🚀 Inserting into Supabase...")

    formatted = []

    for row in rows:
        try:
            time_raw = row["target_time"]
            target_dt = None
            if time_raw:
                today = datetime.now().strftime("%Y-%m-%d")
                dt_str = time_raw.replace("：", ":")
                try:
                    target_dt = datetime.strptime(f"{today} {dt_str}", "%Y-%m-%d %H:%M")
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
        except:
            continue

    if not formatted:
        print("❌ No valid rows to insert.")
        return

    try:
        supabase.schema("stg").from_("hug_raw_requests").insert(formatted).execute()
        print(f"✔ Inserted {len(formatted)} rows into Supabase.\n")
    except Exception as e:
        print("❌ Insert failed:", e)


# ==========================================
# 🔹 Main Runner
# ==========================================
def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=150)
        page = browser.new_page()
        rows = scrape_all(page)
        browser.close()

    clear_previous_data()
    insert_scraped_data_to_supabase(rows)


if __name__ == "__main__":
    main()
