import logging
from datetime import datetime
from app.supabase import get_supabase
from app.services.travel_time_service import build_and_store_matrix

logger = logging.getLogger(__name__)
supabase = get_supabase()

def _parse_bucket(ts_str: str) -> int | None:
    """
    Parse an ISO timestamp string and convert it into an hourly departure bucket (epoch seconds).
    """
    if not ts_str:
        return None

    try:
        if ts_str.endswith("Z"):
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        else:
            ts = datetime.fromisoformat(ts_str)

        # Floor to the hour in epoch seconds
        epoch = ts.timestamp()
        return int(epoch // 3600) * 3600
    except Exception as e:
        logger.debug(f"[TimeMatrix] Failed to parse window_start '{ts_str}': {e}")
        return None

def build_time_matrix(run_id: int, profile: str = "driving") -> dict:
    """
    Build a filtered time matrix for selected nodes participating in a specific run.

    Logic:
    1. Retrieve routing_tasks for the given run_id.
    2. Derive departure_buckets from window_start (hourly integer buckets).
    3. Fetch cached travel_times (core.travel_times) that match node pairs and buckets.
    4. If cache miss, rebuild via build_and_store_matrix() using the earliest bucket.
    5. Return a structured matrix {origin_id: {dest_id: duration}}.
    """
    try:
        # Retrieve routing_tasks for this run
        task_query = (
            supabase.schema("run")
            .from_("routing_tasks")
            .select("node_id, window_start")
            .eq("run_id", run_id)
            .execute()
        )
        tasks = task_query.data or []
        if not tasks:
            logger.warning(f"[TimeMatrix] No routing_tasks found for run_id={run_id}")
            return {
                "status": "empty",
                "matrix": {},
                "node_ids": [],
                "buckets": []
            }

        # Collect node_ids and compute departure_buckets
        nodes = {t["node_id"] for t in tasks if t.get("node_id") is not None}
        buckets = set()
        
        for t in tasks:
            ws = t.get("window_start")
            bucket = _parse_bucket(ws) if ws else None
            if bucket is not None:
                buckets.add(bucket)

        if not nodes:
            logger.warning(f"[TimeMatrix] No valid node IDs for run_id={run_id}")
            return {
                "status": "empty",
                "matrix": {},
                "node_ids": [],
                "buckets": [],
            }
        
        if not buckets:
            logger.warning(f"[TimeMatrix] No valid departure buckets for run_id={run_id}")
            return {
                "status": "empty",
                "matrix": {},
                "node_ids": list(nodes),
                "buckets": [],
            }

        logger.info(
            f"[TimeMatrix] Selected node_ids={sorted(list(nodes))}, "
            f"departure_buckets={sorted(list(buckets))}"
        )

        # Query cached travel_times
        tt_query = (
            supabase.schema("core")
            .from_("travel_times")
            .select("origin_node_id, dest_node_id, duration, departure_bucket, profile")
            .in_("origin_node_id", list(nodes))
            .in_("dest_node_id", list(nodes))
            .in_("departure_bucket", list(buckets))
            .eq("profile", profile)
            .execute()
        )
        data = tt_query.data or []

        # Cache miss → rebuild
        if len(data) == 0:
            logger.warning(
                f"[TimeMatrix] Cache miss (no travel_times) for run_id={run_id}, "
                f"rebuilding matrix for selected nodes..."
            )
            earliest_bucket = min(buckets)

            # Retrieve node info for rebuild
            node_query = (
                supabase.schema("core")
                .from_("nodes")
                .select("id, address, latitude, longitude")
                .in_("id", list(nodes))
                .execute()
            )
            
            node_data = node_query.data or []
            if not node_data:
                logger.error(
                    f"[TimeMatrix] No node data found for selected nodes in run_id={run_id}"
                )
                return {
                    "status": "error",
                    "message": "no nodes available for rebuild",
                    "matrix": {},
                    "node_ids": sorted(list(nodes)),
                    "buckets": sorted(list(buckets)),
                }

            # Rebuild
            build_and_store_matrix(
                nodes=node_data,
                routing_preference="TRAFFIC_AWARE",
                departure_bucket=earliest_bucket,
                require_coords=True,
            )

            # Requery after rebuild
            tt_query = (
                supabase.schema("core")
                .from_("travel_times")
                .select("origin_node_id, dest_node_id, duration, departure_bucket, profile")
                .in_("origin_node_id", list(nodes))
                .in_("dest_node_id", list(nodes))
                .in_("departure_bucket", list(buckets))
                .eq("profile", profile)
                .execute()
            )
            data = tt_query.data or []

            if len(data) == 0:
                logger.warning(f"[TimeMatrix] No travel_times even after rebuild for run_id={run_id}")
                return {
                    "status": "miss",
                    "message": "no travel_times even after rebuild",
                    "matrix": {},
                    "node_ids": sorted(list(nodes)),
                    "buckets": sorted(list(buckets)),
                }

        # Build structured matrix
        matrix = {str(o): {} for o in nodes}
        
        # Fill known durations from travel_times
        for row in data:
            o = str(row["origin_node_id"])
            d = str(row["dest_node_id"])
            duration = row["duration"]
            if o not in matrix:
                matrix[o] = {}
            matrix[o][d] = duration

        # Ensure self-distance 0 and missing pairs as None
        for o in nodes:
            o_key = str(o)
            for d in nodes:
                d_key = str(d)
                if o == d:
                    matrix[o_key][d_key] = 0
                elif d_key not in matrix[o_key]:
                    matrix[o_key][d_key] = None

        logger.info(f"[TimeMatrix] Built matrix for run_id={run_id}")
        return {
            "status": "ok",
            "matrix": matrix,
            "node_ids": sorted(list(nodes)),
            "buckets": sorted(list(buckets)),
        }

    except Exception as e:
        logger.error(f"[TimeMatrix] build_time_matrix() failed for run_id={run_id}: {e}")
        raise
