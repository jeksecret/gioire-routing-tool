import logging
from datetime import datetime
from app.supabase import get_supabase
from app.services.time_matrix_service import build_time_matrix 
from typing import Dict, List, Any

logger = logging.getLogger(__name__)
supabase = get_supabase()

def load_run(run_id: int) -> dict | None:
    """Load optimization_run entry."""
    run_q = (
        supabase.schema("run")
        .from_("optimization_run")
        .select("*")
        .eq("id", run_id)
        .single()
        .execute()
    )
    return run_q.data or None

def load_tasks(run_id: int) -> List[dict]:
    """Load routing tasks for this run."""
    task_q = (
        supabase.schema("run")
        .from_("routing_tasks")
        .select("id, task_type, user_id, node_id, depot_id, window_start, window_end")
        .eq("run_id", run_id)
        .order("id")
        .execute()
    )
    return task_q.data or []

def load_vehicles_for_facility(facility_name: str) -> List[dict]:
    """
    Load vehicles that belong to the depot matching optimization_run.facility_name.
    facility_name == depots.depot_name
    """
    depot_q = (
        supabase.schema("core")
        .from_("depots")
        .select("id")
        .eq("depot_name", facility_name)
        .single()
        .execute()
    )

    if not depot_q.data:
        logger.warning(f"[OR-Tools] No depot found for facility_name={facility_name}")
        return []

    depot_id = depot_q.data["id"]

    vehicle_q = (
        supabase.schema("core")
        .from_("vehicles")
        .select("id, vehicle_name, seats, depot_id")
        .eq("depot_id", depot_id)
        .eq("active", True)
        .execute()
    )

    return vehicle_q.data or []

def load_depots() -> Dict[int, dict]:
    """Load all depots indexed by depot_id."""
    depot_q = (
        supabase.schema("core")
        .from_("depots")
        .select("id, depot_name, depot_node_id")
        .execute()
    )
    return {d["id"]: d for d in (depot_q.data or [])}

# Build OR-Tools payload
def build_ortools_payload(run_id: int) -> Dict[str, Any]:
    """
    Compile all data required to send to OR-Tools.
    """
    logger.info(f"[OR-Tools] Starting payload build for run_id={run_id}")

    # Load optimization_run
    run = load_run(run_id)
    if not run:
        return {"status": "error", "message": "run_id not found"}

    route_date = run.get("route_date")
    facility_name = run.get("facility_name")

    # Load routing_tasks for this run
    tasks = load_tasks(run_id)
    if not tasks:
        return {"status": "error", "message": "no routing tasks"}

    # Load vehicles + depot mapping
    vehicles = load_vehicles_for_facility(facility_name)
    depot_map = load_depots()

    # Build time matrix
    tm_result = build_time_matrix(run_id)
    if tm_result["status"] not in ["ok", "miss"]:
        return {"status": "error", "message": "time matrix unavailable"}

    raw_matrix = tm_result["matrix"]
    node_ids = tm_result["node_ids"]
    buckets = tm_result["buckets"]

    # Create node → index mapping
    node_index = {nid: idx for idx, nid in enumerate(node_ids)}

    # Build compressed NxN matrix
    compressed_matrix = []
    for origin_id in node_ids:
        row = []
        for dest_id in node_ids:
            duration = raw_matrix[str(origin_id)][str(dest_id)]
            row.append(duration)
        compressed_matrix.append(row)

    # Format vehicles
    formatted_vehicles = []
    for v in vehicles:
        depot_id = v["depot_id"]
        depot = depot_map.get(depot_id)
        if not depot:
            logger.warning(f"[OR-Tools] No depot found for depot_id={depot_id}")
            continue

        depot_node = depot["depot_node_id"]
        if depot_node not in node_index:
            logger.warning(f"[OR-Tools] depot_node_id={depot_node} missing from matrix node_ids")
            continue

        formatted_vehicles.append({
            "vehicle_id": v["id"],
            "vehicle_name": v["vehicle_name"],
            "capacity": v["seats"],
            "start_index": node_index[depot_node],
            "end_index": node_index[depot_node],
        })

    if not formatted_vehicles:
        return {"status": "error", "message": "no vehicles available"}

    # Format tasks for OR-Tools
    formatted_tasks = []
    for t in tasks:
        node_id = t["node_id"]
        if node_id not in node_index:
            logger.warning(f"[OR-Tools] task node_id={node_id} missing from matrix node_ids")
            continue

        # Convert window_start/end → timestamps (int)
        window_start = int(datetime.fromisoformat(t["window_start"]).timestamp())
        window_end = int(datetime.fromisoformat(t["window_end"]).timestamp())

        formatted_tasks.append({
            "task_id": t["id"],
            "task_type": t["task_type"],
            "node_index": node_index[node_id],
            "user_id": t["user_id"],
            "window": [window_start, window_end],
        })

    # Final OR-Tools payload
    payload = {
        "date": route_date,
        "facility_name": facility_name,
        "node_ids": node_ids,
        "node_index": node_index,
        "time_matrix": compressed_matrix,
        "buckets": buckets,
        "vehicles": formatted_vehicles,
        "tasks": formatted_tasks,
    }

    logger.info(f"[OR-Tools] Payload build complete for run_id={run_id}")
    return {"status": "ok", "payload": payload}
