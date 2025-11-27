import logging
from datetime import datetime
from app.supabase import get_supabase
from app.services.travel_time_service import build_and_store_matrix

logger = logging.getLogger(__name__)
supabase = get_supabase()


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
            return {"status": "empty", "matrix": {}}

        # Collect node_ids and compute departure_buckets
        nodes = {t["node_id"] for t in tasks if t.get("node_id")}
        buckets = set()
        for t in tasks:
            ws = t.get("window_start")
            if ws:
                try:
                    ts = datetime.fromisoformat(ws.replace("Z", "+00:00")).timestamp()
                    buckets.add(int(ts // 3600) * 3600)
                except Exception:
                    logger.debug(f"[TimeMatrix] Invalid window_start skipped: {ws}")

        if not nodes:
            logger.warning(f"[TimeMatrix] No valid node IDs for run_id={run_id}")
            return {"status": "empty", "matrix": {}}

        if not buckets:
            logger.warning(f"[TimeMatrix] No valid buckets derived for run_id={run_id}")
            return {"status": "empty", "matrix": {}}

        logger.info(f"[TimeMatrix] Found node_ids={nodes}, departure_buckets={buckets}")

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
            logger.warning(f"[TimeMatrix] Cache miss for run_id={run_id}, rebuilding matrix...")
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
                logger.error(f"[TimeMatrix] No node data found for run_id={run_id}")
                return {"status": "error", "message": "no nodes available for rebuild"}

            # Rebuild
            build_and_store_matrix(
                nodes=node_data,
                routing_preference="TRAFFIC_AWARE",
                departure_bucket=earliest_bucket,
                require_coords=True
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
                return {"status": "miss", "message": "no travel_times even after rebuild"}

        # Build structured matrix
        matrix = {str(o): {} for o in nodes}
        for row in data:
            o, d = str(row["origin_node_id"]), str(row["dest_node_id"])
            matrix[o][d] = row["duration"]

        # Fill self and missing entries
        for o in nodes:
            for d in nodes:
                if o == d:
                    matrix[str(o)][str(d)] = 0
                elif str(d) not in matrix[str(o)]:
                    matrix[str(o)][str(d)] = None

        logger.info(f"[TimeMatrix] Built matrix for run_id={run_id}")
        return {
            "status": "ok",
            "matrix": matrix,
            "node_ids": sorted(list(nodes)),
            "buckets": sorted(list(buckets)),
        }

    except Exception as e:
        logger.error(f"build_time_matrix() failed: {str(e)}")
        raise
