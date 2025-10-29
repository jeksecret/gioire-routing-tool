from app.supabase import get_supabase
from app.notion import get_notion_client, query_database
from datetime import datetime
import pytz
import logging

logger = logging.getLogger(__name__)

supabase = get_supabase()
notion = get_notion_client()
JST = pytz.timezone("Asia/Tokyo")

def parse_notion_date(notion_date):
    if not notion_date:
        return None
    try:
        return datetime.fromisoformat(notion_date.replace("Z", "+00:00")).astimezone(JST)
    except Exception:
        return None

def json_safe(row: dict) -> dict:
    """Convert datetimes to ISO strings."""
    safe = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            safe[k] = v.isoformat()
        else:
            safe[k] = v
    return safe

def resolve_depot_id(notion_relation_id: str) -> int | None:
    """
    Stub function for mapping Notion relation IDs (事業所DB)
    to local core.depots.id in Supabase.

    Returns placeholder for now — replace once depot sync is implemented.
    """
    logger.debug(f"resolve_depot_id(): received relation id {notion_relation_id}, returning placeholder.")
    return 1

# ========== VEHICLES ==========
def sync_vehicles(notion_data_source_id: str):
    results = query_database(notion_data_source_id)
    pages = results.get("results", [])
    logger.info(f"Fetched {len(pages)} vehicle rows from Notion")

    rows = []
    for r in pages:
        props = r.get("properties", {})

        # Extract fields safely
        title_prop = props.get("車両名 / vehicle name", {}).get("title", [])
        name = title_prop[0]["plain_text"].strip() if title_prop else None
        seats = props.get("定員 / capacity", {}).get("number")
        notion_page_id = r.get("id")
        notion_last_edited = parse_notion_date(r.get("last_edited_time"))

        # Handle depot relation safely
        depot_relation = props.get("事業所DB", {}).get("relation", [])
        depot_id = None
        if depot_relation:
            related_page_id = depot_relation[0].get("id")
            depot_id = resolve_depot_id(related_page_id)

        # Skip incomplete rows
        if not name:
            logger.warning(f"Skipping page {notion_page_id} — no vehicle name found.")
            continue

        # Temporary fallback for missing depot
        if not depot_id:
            logger.warning(f"No depot_id for {name}, using placeholder 1 temporarily.")
            depot_id = 1  # Replace once depots sync exists

        # Build record
        row = {
            "depot_id": depot_id,
            "vehicle_name": name,
            "seats": seats,
            "notion_page_id": notion_page_id,
            "notion_last_edited": notion_last_edited,
        }
        rows.append(json_safe(row))

    # Upsert all valid records
    for row in rows:
        supabase.schema("core").from_("vehicles").upsert(row, on_conflict="notion_page_id").execute()
        logger.info(f"Upserted vehicle: {row['vehicle_name']}")

    return {"count": len(rows), "synced_rows": rows}
