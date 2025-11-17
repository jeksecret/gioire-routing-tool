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

def _resolve_depot_node_id(depot_name: str) -> int | None:
    """
    Stub function for resolving or mapping depot_name to depot_node_id in core.nodes.

    Returns placeholder for now — replace once node sync is implemented.
    """
    logger.debug(f"_resolve_depot_node_id(): received depot_name '{depot_name}', returning placeholder 1.")
    return 1

def resolve_depot_id(notion_relation_id: str) -> int | None:
    """
    Stub function for mapping Notion relation IDs (事業所DB)
    to local core.depots.id in Supabase.

    Returns placeholder for now — replace once depot sync is implemented.
    """
    logger.debug(f"resolve_depot_id(): received relation id {notion_relation_id}, returning placeholder.")
    return 4

# ===============================
# VEHICLE
# ===============================
def upsert_vehicle(payload: str) -> dict:
    """
    Insert or update a vehicle record from Notion 車両DB.
    """
    try:
        vehicle_name = payload.get("vehicle_name")
        depot_relation_id = payload.get("depot_relation_id")
        seats = payload.get("seats")
        active = payload.get("active", True)
        notion_page_id = payload.get("notion_page_id")
        notion_last_edited = parse_iso_date(payload.get("notion_last_edited"))

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
            "vehicle_name": vehicle_name,
            "depot_id": depot_id,
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

# ===============================
# FACILITY / DEPOT
# ===============================
def upsert_depot(payload: dict) -> dict:
    """
    Insert or update a facility (depot) record from Notion 事業所DB.
    """
    try:
        depot_name = payload.get("depot_name")
        notion_page_id = payload.get("notion_page_id")
        notion_last_edited = parse_iso_date(payload.get("notion_last_edited"))
        active = payload.get("active", True)

        depot_node_id = _resolve_depot_node_id(depot_name)

        if not depot_name or not notion_page_id:
            raise ValueError("Missing depot_name or notion_page_id")

        # Build row
        row = json_safe({
            "depot_name": depot_name,
            "depot_node_id": depot_node_id,
            "active": active,
            "notion_page_id": notion_page_id,
            "notion_last_edited": notion_last_edited,
        })

        result = supabase.schema("core").from_("depots").upsert(row, on_conflict="notion_page_id").execute()
        logger.info(f"Upserted depot: {depot_name}")

        return {"status": "200", "depot": row, "result": result.data}

    except Exception as e:
        logger.error(f"upsert_depot() failed: {str(e)}")
        raise

# ===============================
# USER
# ===============================
def upsert_user(payload: dict) -> dict:
    """
    Insert or update a user record from Notion 利用者DB.
    """
    try:
        user_name = payload.get("user_name")
        reading_name = payload.get("reading_name")
        notion_page_id = payload.get("notion_page_id")
        notion_last_edited = parse_iso_date(payload.get("notion_last_edited"))
        active = payload.get("active", True)
        depot_relation_id = payload.get("depot_relation_id")

        if not user_name or not notion_page_id:
            raise ValueError("Missing user_name or notion_page_id")
        
        if user_name:
            if reading_name:
                user_name = f"{reading_name}　{user_name}"
            else:
                user_name = user_name

        depot_id = resolve_depot_id(depot_relation_id)

        # Build row
        row = json_safe({
            "user_name": user_name,
            "depot_id": depot_id,
            "active": active,
            "notion_page_id": notion_page_id,
            "notion_last_edited": notion_last_edited,
        })

        result = supabase.schema("core").from_("users").upsert(row, on_conflict="notion_page_id").execute()
        logger.info(f"Upserted user: {user_name}")

        return {"status": "200", "user": row, "result": result.data}

    except Exception as e:
        logger.error(f"upsert_user() failed: {str(e)}")
        raise

# ========== NODES ==========
# TODO: Insert or update a node record.
