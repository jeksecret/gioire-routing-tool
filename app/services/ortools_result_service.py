import logging
from typing import Dict, List, Any
from app.supabase import get_supabase

logger = logging.getLogger(__name__)
supabase = get_supabase()

def process_ortools_result(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    - Receive OR-Tools solver output
    - Insert routing results into run.routing_results
    """
    run_id = payload.get("run_id")
    routes = payload.get("routes")

    if not run_id or not routes:
        return {
            "status": "error",
            "message": "Invalid OR-Tools result payload (missing run_id or routes)"
        }

    insert_rows: List[dict] = []

    for route in routes:
        vehicle_id = route.get("vehicle_id")
        stops = route.get("stops", [])

        if not vehicle_id or not stops:
            logger.warning("Skipping route with missing vehicle_id or stops")
            continue

        # Collect TASK stops to anchor depot events
        task_stops = [s for s in stops if s.get("event_type") == "TASK" and s.get("task_id")]

        if not task_stops:
            logger.warning(f"No TASK stops found for vehicle_id={vehicle_id}, skipping route")
            continue

        for stop in stops:
            sequence = stop.get("sequence")
            event_type = stop.get("event_type")
            arrival_at = stop.get("arrival_at")
            departure_at = stop.get("departure_at")
            passengers = stop.get("passengers", 0)
            task_id = stop.get("task_id")

            if sequence is None or not event_type or not arrival_at:
                logger.warning(f"Skipping invalid stop: {stop}")
                continue

            if not task_id:
                logger.warning(f"task_id missing for stop, skipping: {stop}")
                continue

            insert_rows.append({
                "run_id": run_id,
                "vehicle_id": vehicle_id,
                "task_id": task_id,
                "sequence": sequence,
                "arrival_at": arrival_at,
                "departure_at": departure_at,
                "passengers": passengers,
                "event_type": event_type,
                "meta_json": stop,
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
