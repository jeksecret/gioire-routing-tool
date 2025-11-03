from app.supabase import get_supabase
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

supabase = get_supabase()

def parse_iso_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
    except Exception:
        logger.warning(f"Failed to parse date: {date_str}")
        return None

def json_safe(row: dict) -> dict:
    """
    Convert datetimes to ISO strings.
    """
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
def upsert_vehicle(payload: str) -> dict:
    """
    Insert or update a vehicle record.
    """
    try:
        vehicle_name = payload.get("vehicle_name")
        seats = payload.get("seats")
        active = payload.get("active", True)
        notion_page_id = payload.get("notion_page_id")
        notion_last_edited = parse_iso_date(payload.get("notion_last_edited"))
        depot_relation_id = payload.get("depot_relation_id")

        depot_id = resolve_depot_id(depot_relation_id)

        if not vehicle_name or not notion_page_id:
            raise ValueError("Missing vehicle_name or notion_page_id")
        
        if seats is not None:
            try:
                seats = int(seats)
            except ValueError:
                raise ValueError("Seats must be an integer value")

        # Build row
        row = json_safe({
            "depot_id": depot_id,
            "vehicle_name": vehicle_name,
            "seats": seats,
            "active": active,
            "notion_page_id": notion_page_id,
            "notion_last_edited": notion_last_edited,
        })

        result = supabase.schema("core").from_("vehicles").upsert(row, on_conflict="notion_page_id").execute()
        logger.info(f"Upserted vehicle: {vehicle_name}")

        return {"status": "200", "vehicle": row, "result": result.data}

    except Exception as e:
        logger.error(f"upsert_vehicle() failed: {str(e)}")
        raise
