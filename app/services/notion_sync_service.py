from app.supabase import get_supabase
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

supabase = get_supabase()

def parse_iso_date(date_str):
    """Convert ISO date string to standardized format."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
    except Exception:
        logger.warning(f"Failed to parse date: {date_str}")
        return None

def json_safe(row: dict) -> dict:
    """Convert datetime objects to JSON-serializable strings."""
    safe = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            safe[k] = v.isoformat()
        else:
            safe[k] = v
    return safe

def resolve_node_id(depot_name: str) -> int | None:
    """
    Stub function for resolving or mapping depot_name to depot_node_id in core.nodes.

    Returns placeholder for now — replace once node sync is implemented.
    """
    logger.debug(f"resolve_node_id(): received depot_name '{depot_name}', returning placeholder.")
    return 1

def resolve_depot_id(notion_relation_id: int | str | None) -> int | None:
    """
    Resolve Notion facility relation ID (numeric unique_id) to local depot_id.
    Queries core.depots where id matches the numeric relation.
    """
    if not notion_relation_id:
        logger.warning("resolve_depot_id(): No relation ID provided.")
        return None

    try:
        depot_id = int(notion_relation_id)
        result = (
            supabase.schema("core")
            .from_("depots")
            .select("id")
            .eq("id", depot_id)
            .limit(1)
            .execute()
        )
        if result.data and len(result.data) > 0:
            logger.debug(f"resolve_depot_id(): Matched numeric relation_id={notion_relation_id} → depot_id={depot_id}")
            return depot_id
        else:
            logger.warning(f"resolve_depot_id(): No depot found for relation_id={notion_relation_id}")
            return None
    except Exception as e:
        logger.error(f"resolve_depot_id() failed: {str(e)}")
        return None

# ===============================
# VEHICLE
# ===============================
def upsert_vehicle(payload: dict) -> dict:
    """Insert or update a vehicle record from Notion 車両DB."""
    try:
        vehicle_name = payload.get("vehicle_name")
        facility_relation_id = payload.get("facility_relation_id")
        seats = payload.get("seats")
        active = payload.get("active", True)
        notion_page_id = payload.get("notion_page_id")
        notion_last_edited = parse_iso_date(payload.get("notion_last_edited"))

        if not vehicle_name or not notion_page_id:
            raise ValueError("Missing vehicle_name or notion_page_id")
        
        if seats is not None:
            try:
                seats = int(seats)
            except ValueError:
                raise ValueError("Seats must be an integer")

        depot_id = resolve_depot_id(facility_relation_id)
        if not depot_id:
            logger.warning(f"No matching depot found for relation_id={facility_relation_id}.")

        # Build row
        row = json_safe({
            "vehicle_name": vehicle_name,
            "depot_id": depot_id,
            "seats": seats,
            "active": active,
            "notion_page_id": notion_page_id,
            "notion_last_edited": notion_last_edited,
        })

        result = (
            supabase.schema("core")
            .from_("vehicles")
            .upsert(row, on_conflict="notion_page_id")
            .execute()
        )

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
    Following nullable-FK design: depot_node_id is optional and can be linked later.
    """
    try:
        depot_name = payload.get("depot_name")
        notion_page_id = payload.get("notion_page_id")
        notion_last_edited = parse_iso_date(payload.get("notion_last_edited"))
        active = payload.get("active", True)

        if not depot_name or not notion_page_id:
            raise ValueError("Missing depot_name or notion_page_id")

        depot_node_id = resolve_node_id(depot_name)

        # Build row
        row = json_safe({
            "depot_name": depot_name,
            "depot_node_id": depot_node_id,
            "active": active,
            "notion_page_id": notion_page_id,
            "notion_last_edited": notion_last_edited,
        })

        result = (
            supabase.schema("core")
            .from_("depots")
            .upsert(row, on_conflict="notion_page_id")
            .execute()
        )

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
    Combines user's name and reading name using a full-width space.
    """
    try:
        user_name = payload.get("user_name")
        facility_relation_id = payload.get("facility_relation_id")
        notion_page_id = payload.get("notion_page_id")
        notion_last_edited = parse_iso_date(payload.get("notion_last_edited"))
        active = payload.get("active", True)

        if not user_name or not notion_page_id:
            raise ValueError("Missing user_name or notion_page_id")

        depot_id = resolve_depot_id(facility_relation_id)
        if not depot_id:
            logger.warning(f"No matching depot found for relation_id={facility_relation_id}.")

        # Build row
        row = json_safe({
            "user_name": user_name,
            "depot_id": 1,
            "active": active,
            "notion_page_id": notion_page_id,
            "notion_last_edited": notion_last_edited,
        })

        result = (
            supabase.schema("core")
            .from_("users")
            .upsert(row, on_conflict="notion_page_id")
            .execute()
        )

        logger.info(f"Upserted user: {user_name}")
        return {"status": "200", "user": row, "result": result.data}

    except Exception as e:
        logger.error(f"upsert_user() failed: {str(e)}")
        raise

# ========== NODES ==========
# def upsert_node(payload: dict) -> dict:
# TODO: Insert or update a node record.
