import logging
from typing import Dict, List, Any
from app.supabase import get_supabase
from datetime import datetime, timezone
import copy

logger = logging.getLogger(__name__)
supabase = get_supabase()

def unix_to_utc(ts: int | None):
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()

def process_ortools_result(payload: Dict[str, Any]) -> Dict[str, Any]:
    run_id = payload.get("run_id")
    routes = payload.get("routes")

    if not run_id or not routes:
        return {
            "status": "error",
            "message": "Invalid OR-Tools result payload (missing run_id or routes)"
        }

    # Idempotency: clear existing results
    supabase.schema("run").from_("routing_results").delete().eq("run_id", run_id).execute()

    insert_rows: List[dict] = []

    for route in routes:
        vehicle_id = route.get("vehicle_id")
        stops = route.get("stops", [])

        if not vehicle_id or not stops:
            logger.warning("Skipping route with missing vehicle_id or stops")
            continue

        for stop in stops:
            sequence = stop.get("sequence")
            event_type = stop.get("event_type")
            task_id = stop.get("task_id")
            arrival_at = stop.get("arrival_at")
            departure_at = stop.get("departure_at")
            passengers = stop.get("passengers", 0)

            if (
                sequence is None
                or not event_type
                or not task_id
                or arrival_at is None
                or departure_at is None
            ):
                logger.warning(f"Skipping invalid stop: {stop}")
                continue

            insert_rows.append({
                "run_id": run_id,
                "vehicle_id": vehicle_id,
                "task_id": task_id,
                "sequence": sequence,
                "arrival_at": unix_to_utc(arrival_at),
                "departure_at": unix_to_utc(departure_at),
                "passengers": passengers,
                "event_type": event_type,
                "meta_json": copy.deepcopy(stop)
            })

    if not insert_rows:
        return {
            "status": "error",
            "message": "No valid routing_results rows to insert"
        }

    # Insert into routing_results
    supabase.schema("run").from_("routing_results").insert(insert_rows).execute()

    logger.info(f"Inserted {len(insert_rows)} rows into run.routing_results")

    return {
        "status": "ok",
        "inserted": len(insert_rows)
    }
