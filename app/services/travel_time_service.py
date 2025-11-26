import logging
from datetime import datetime, timezone
from app.supabase import get_supabase
from app.utils.routes_matrix_helper import build_matrix

logger = logging.getLogger(__name__)
supabase = get_supabase()

def build_and_store_matrix(
    nodes: list[dict],
    routing_preference: str = "TRAFFIC_AWARE",
    departure_bucket: int | None = None,
    require_coords: bool = False
):
    """
    Compute travel-time matrix between nodes using Google Routes API
    and upsert results into core.travel_times.
    If departure_bucket is provided, it is used directly (ensures consistency with routing_tasks).
    """

    # Determine departure bucket
    if departure_bucket is None:
        now_utc = datetime.now(timezone.utc)
        departure_bucket = (int(now_utc.timestamp()) // 3600) * 3600
        logger.info(
            f"[TravelTime] Using current UTC bucket: {departure_bucket} ({now_utc.strftime('%Y-%m-%d %H:00:%S')})"
        )
    else:
        dt_str = datetime.fromtimestamp(departure_bucket, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"[TravelTime] Using provided bucket: {departure_bucket} ({dt_str} UTC)")

    # Prepare point payloads
    points = []
    for n in nodes:
        points.append({
            "id": n["id"],
            "address": n.get("address"),
            "lat": n.get("latitude"),
            "lng": n.get("longitude"),
        })

    logger.info(f"[TravelTime] Building matrix for {len(points)} nodes via Google Routes API")

    # Call helper to build full matrix
    matrix_result = build_matrix(
        points,
        departure_time=None,
        routing_preference=routing_preference,
        require_coords=require_coords
    )

    ids = [int(x) for x in matrix_result["ids"]]
    minutes = matrix_result["minutes"]
    meters = matrix_result["meters"]

    # Prepare upsert rows
    rows = []
    for i, origin_id in enumerate(ids):
        for j, dest_id in enumerate(ids):
            if origin_id == dest_id:
                continue
            
            rows.append({
                "origin_node_id": origin_id,
                "dest_node_id": dest_id,
                "profile": "driving",
                "departure_bucket": departure_bucket,
                "options": {"routing_preference": routing_preference},
                "duration": int(minutes[i][j] * 60), # seconds
                "distance": int(meters[i][j]), # meters
                "raw_response": {
                    "status": "OK",
                    "duration": f"{int(minutes[i][j] * 60)}s", # seconds
                    "distanceMeters": int(meters[i][j]) # meters
                },
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })

    # Upsert into core.travel_times
    if rows:
        supabase.schema("core").from_("travel_times").upsert(rows).execute()
        logger.info(f"[TravelTime] Upserted {len(rows)} records into core.travel_times")

    return {
        "count": len(rows),
        "matrix_ids": ids,
        "departure_bucket": departure_bucket
    }
