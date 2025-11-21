import logging
from datetime import datetime, timedelta, timezone
from app.supabase import get_supabase

logger = logging.getLogger(__name__)
supabase = get_supabase()

def parse_time_jst_to_utc(target_time_str: str | None, date_base: datetime) -> datetime | None:
    """Convert target time like '09：30' JST → UTC datetime."""
    if not target_time_str or not str(target_time_str).strip():
        return None
    try:
        clean = str(target_time_str).replace("：", ":").strip()
        hour, minute = map(int, clean.split(":"))
        jst_time = datetime(date_base.year, date_base.month, date_base.day, hour, minute, tzinfo=timezone(timedelta(hours=9)))
        return jst_time.astimezone(timezone.utc)
    except Exception as e:
        logger.warning(f"Failed to parse target_time '{target_time_str}': {e}")
        return None

def resolve_node_ids(depot_name: str, place: str):
    """Return node IDs for depot_name (kind=depot) and place (kind=place)."""
    depot_node_id, place_node_id = None, None

    depot_query = (supabase.schema("core")
        .from_("nodes")
        .select("id")
        .eq("place", depot_name)
        .eq("kind", "depot")
        .execute())
    if depot_query.data:
        depot_node_id = depot_query.data[0]["id"]

    place_query = (supabase.schema("core")
        .from_("nodes")
        .select("id")
        .eq("place", place)
        .eq("kind", "place")
        .execute())
    if place_query.data:
        place_node_id = place_query.data[0]["id"]

    return depot_node_id, place_node_id

def resolve_fk_ids(depot_name: str, user_name: str):
    """Return FK IDs for depot_name and user_name."""
    depot_id, user_id = None, None

    depot_q = supabase.schema("core").from_("depots").select("id").eq("depot_name", depot_name).execute()
    if depot_q.data:
        depot_id = depot_q.data[0]["id"]

    user_q = supabase.schema("core").from_("users").select("id").eq("user_name", user_name).execute()
    if user_q.data:
        user_id = user_q.data[0]["id"]

    return depot_id, user_id

def get_travel_minutes(origin_node_id: int, dest_node_id: int) -> int:
    """
    Fetch travel time (minutes) from core.travel_times.
    Converts duration (seconds → minutes).
    """
    q = (
        supabase.schema("core")
        .from_("travel_times")
        .select("duration")
        .eq("origin_node_id", origin_node_id)
        .eq("dest_node_id", dest_node_id)
        .execute()
    )

    if not q.data or len(q.data) == 0:
        logger.warning(f"[TravelTime] No record found for origin={origin_node_id} → dest={dest_node_id}")
        raise ValueError(f"Missing travel time for {origin_node_id} → {dest_node_id}")

    seconds = int(q.data[0]["duration"])
    if seconds <= 0:
        logger.warning(f"[TravelTime] Invalid duration ({seconds}s) for origin={origin_node_id} → dest={dest_node_id}")
        raise ValueError(f"Invalid travel duration ({seconds}s) for {origin_node_id} → {dest_node_id}")

    minutes = max(1, seconds // 60)
    logger.warning(f"[TravelTime] Matched travel time {origin_node_id} → {dest_node_id}: {seconds}s ≈ {minutes}min")
    return minutes

def split_and_create_tasks(run_id: int = 1):
    """Split stg.hug_raw_requests into paired PICK/DROP tasks."""
    rows = supabase.schema("stg").from_("hug_raw_requests").select("id, payload").execute().data
    if not rows:
        logger.info("No records found in stg.hug_raw_requests.")
        return {"created": 0, "details": []}

    created_tasks = []
    base_date = datetime.now(timezone(timedelta(hours=9))) # JST today

    for r in rows:
        raw = r.get("payload", {})
        if not raw:
            logger.warning(f"Skipping record {r['id']}: empty payload")
            continue

        user_name = raw.get("user_name")
        depot_name = raw.get("depot_name")
        place = raw.get("place")
        pickup_flag_raw = raw.get("pickup_flag")
        target_time_str = raw.get("target_time")

        # pickup_flag判定
        pickup_flag = True if "迎" in str(pickup_flag_raw) else False

        target_time_utc = parse_time_jst_to_utc(target_time_str, base_date)
        if not target_time_utc:
            logger.warning(f"Skipping record {r['id']} ({user_name}): invalid target_time {target_time_str}")
            continue

        depot_id, user_id = resolve_fk_ids(depot_name, user_name)
        depot_node_id, place_node_id = resolve_node_ids(depot_name, place)
        if not depot_id or not user_id or not depot_node_id or not place_node_id:
            logger.warning(f"Skipping record {r['id']}: missing depot/user/node mapping.")
            continue

        # travel time lookup
        travel_min = get_travel_minutes(depot_node_id, place_node_id)
        if not travel_min:
            logger.warning(f"No travel time found between nodes {depot_node_id}→{place_node_id}. Using default 30 min.")
            travel_min = 30

        pair_key = f"user_{user_id}_{base_date.strftime('%Y%m%d')}"

        # pickup_flag = True
        if pickup_flag:
            # PICK: place → depot
            pick_start = target_time_utc - timedelta(minutes=10)
            pick_end = target_time_utc + timedelta(minutes=10)

            # DROP: travel time + 30 min buffer
            drop_start = target_time_utc + timedelta(minutes=travel_min)
            drop_end = drop_start + timedelta(minutes=30)

            created_tasks += [
                {
                    "run_id": run_id,
                    "task_type": "PICK",
                    "user_id": user_id,
                    "depot_id": depot_id,
                    "node_id": place_node_id,
                    "window_start": pick_start.isoformat(),
                    "window_end": pick_end.isoformat(),
                    "pair_key": pair_key,
                },
                {
                    "run_id": run_id,
                    "task_type": "DROP",
                    "user_id": user_id,
                    "depot_id": depot_id,
                    "node_id": depot_node_id,
                    "window_start": drop_start.isoformat(),
                    "window_end": drop_end.isoformat(),
                    "pair_key": pair_key,
                }
            ]

        # pickup_flag = False
        else:
            # DROP: place as destination
            drop_start = target_time_utc - timedelta(minutes=10)
            drop_end = target_time_utc + timedelta(minutes=60)

            # PICK: add travel time window
            pick_end = target_time_utc
            pick_start = pick_end - timedelta(minutes=travel_min)

            created_tasks += [
                {
                    "run_id": run_id,
                    "task_type": "PICK",
                    "user_id": user_id,
                    "depot_id": depot_id,
                    "node_id": depot_node_id,
                    "window_start": pick_start.isoformat(),
                    "window_end": pick_end.isoformat(),
                    "pair_key": pair_key,
                },
                {
                    "run_id": run_id,
                    "task_type": "DROP",
                    "user_id": user_id,
                    "depot_id": depot_id,
                    "node_id": place_node_id,
                    "window_start": drop_start.isoformat(),
                    "window_end": drop_end.isoformat(),
                    "pair_key": pair_key,
                }
            ]

    # bulk insert
    if created_tasks:
        supabase.schema("run").from_("routing_tasks").insert(created_tasks).execute()
        logger.info(f"Inserted {len(created_tasks)} tasks into run.routing_tasks.")

    return {"created": len(created_tasks), "details": created_tasks[:4]}
