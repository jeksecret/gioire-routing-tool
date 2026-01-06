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
        jst_time = datetime(
            date_base.year, date_base.month, date_base.day,
            hour, minute, tzinfo=timezone(timedelta(hours=9))
        )
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

    depot_query = (supabase.schema("core")
        .from_("depots")
        .select("id")
        .eq("depot_name", depot_name)
        .execute())
    if depot_query.data:
        depot_id = depot_query.data[0]["id"]

    user_query = (supabase.schema("core")
        .from_("users")
        .select("id")
        .eq("user_name", user_name)
        .execute())
    if user_query.data:
        user_id = user_query.data[0]["id"]

    return depot_id, user_id

def get_travel_minutes(origin_node_id: int, dest_node_id: int) -> int:
    """
    Fetch travel time (minutes) from core.travel_times.
    Converts duration (seconds → minutes).
    """
    tt_query = (
        supabase.schema("core")
        .from_("travel_times")
        .select("duration")
        .eq("origin_node_id", origin_node_id)
        .eq("dest_node_id", dest_node_id)
        .execute()
    )

    if not tt_query.data:
        logger.warning(f"[TravelTime] No record found for origin={origin_node_id} → dest={dest_node_id}")
        raise ValueError(f"Missing travel time for {origin_node_id} → {dest_node_id}")

    seconds = int(tt_query.data[0]["duration"])
    if seconds <= 0:
        logger.warning(f"[TravelTime] Invalid duration ({seconds}s) for origin={origin_node_id} → dest={dest_node_id}")
        raise ValueError(f"Invalid travel duration ({seconds}s) for {origin_node_id} → {dest_node_id}")

    minutes = max(1, seconds // 60)
    logger.warning(f"[TravelTime] Matched travel time {origin_node_id} → {dest_node_id}: {seconds}s ≈ {minutes}min")
    return minutes

def split_and_create_tasks(run_id: int):
    """
    Read ONLY optimization_run.meta_json.rows for this run_id
    Create PICK/DROP tasks exclusively for this run_id
    """
    created_count = 0
    updated_count = 0

    inserts = []
    updates = []

    # Load optimization_run entry
    run_query = (
        supabase.schema("run")
        .from_("optimization_run")
        .select("meta_json")
        .eq("id", run_id)
        .single()
        .execute()
    )

    if not run_query.data:
        return {
            "created": 0,
            "updated": 0,
            "error": f"run_id={run_id} not found"
        }

    meta = run_query.data["meta_json"]
    rows = meta.get("rows", [])
    route_date = meta.get("route_date")

    if not rows:
        return {
            "created": 0,
            "updated": 0,
            "error": "No rows in meta_json",
        }

    # Convert route_date into JST base date
    try:
        year, month, day = map(int, route_date.split("-"))
        base_date = datetime(year, month, day, tzinfo=timezone(timedelta(hours=9)))
    except:
        base_date = datetime.now(timezone(timedelta(hours=9)))

    # Only process runs for TODAY in JST
    today_jst = datetime.now(timezone(timedelta(hours=9))).date()
    run_date_jst = base_date.date()

    if run_date_jst != today_jst:
        logger.info(
            f"[TaskSplit] run_id={run_id} is for {run_date_jst}, "
            f"but today is {today_jst}. Skipping updates/inserts."
        )
        return {
            "created": created_count,
            "updated": updated_count,
            "skipped": "Run date is not today — no updates or inserts applied"
        }

    # Load existing task
    existing_query = (
        supabase.schema("run")
        .from_("routing_tasks")
        .select("id, user_id, task_type")
        .eq("run_id", run_id)
        .execute()
    )

    existing_map = {} # (user_id, task_type) → id
    for t in existing_query.data or []:
        existing_map[(t["user_id"], t["task_type"])] = t["id"]

    trip_seq = 0
    # Generate tasks for each row
    for r in rows:
        user_name = r.get("user_name")
        depot_name = r.get("depot_name")
        place = r.get("place")
        pickup_flag_raw = r.get("pickup_flag")
        target_time_str = r.get("target_time")

        # skip invalid rows
        if place in ["欠席", None]:
            logger.info(f"Skipping absent user {user_name}")
            continue

        target_time_utc = parse_time_jst_to_utc(target_time_str, base_date)
        if not target_time_utc:
            logger.warning(f"Skipping: invalid target_time '{target_time_str}'")
            continue

        depot_id, user_id = resolve_fk_ids(depot_name, user_name)
        depot_node_id, place_node_id = resolve_node_ids(depot_name, place)

        if not all([depot_id, user_id, depot_node_id, place_node_id]):
            logger.warning(f"Skipping due to missing mapping: {user_name}")
            continue

        # travel time
        try:
            travel_min = get_travel_minutes(depot_node_id, place_node_id)
        except Exception:
            travel_min = 30 # fallback

        trip_seq += 1
        pair_key = f"user_{user_id}_{base_date.strftime('%Y%m%d')}_{trip_seq}"
        is_pickup = "迎" in str(pickup_flag_raw)

        # Build PICK & DROP windows
        if is_pickup:
            # PICK: place → depot
            pick_start = target_time_utc - timedelta(minutes=10)
            pick_end = target_time_utc + timedelta(minutes=10)

            # DROP: depot ← place
            drop_start = target_time_utc + timedelta(minutes=travel_min)
            drop_end = drop_start + timedelta(minutes=30)

            pick_task = {
                "run_id": run_id,
                "task_type": "PICK",
                "user_id": user_id,
                "depot_id": depot_id,
                "node_id": place_node_id,
                "window_start": pick_start.isoformat(),
                "window_end": pick_end.isoformat(),
                "pair_key": pair_key,
            }

            drop_task = {
                "run_id": run_id,
                "task_type": "DROP",
                "user_id": user_id,
                "depot_id": depot_id,
                "node_id": depot_node_id,
                "window_start": drop_start.isoformat(),
                "window_end": drop_end.isoformat(),
                "pair_key": pair_key,
            }

        else:
            # DROP: place is final destination
            drop_start = target_time_utc - timedelta(minutes=10)
            drop_end = target_time_utc + timedelta(minutes=60)

            # PICK: depot → place
            pick_end = target_time_utc
            pick_start = pick_end - timedelta(minutes=travel_min)

            pick_task = {
                "run_id": run_id,
                "task_type": "PICK",
                "user_id": user_id,
                "depot_id": depot_id,
                "node_id": depot_node_id,
                "window_start": pick_start.isoformat(),
                "window_end": pick_end.isoformat(),
                "pair_key": pair_key,
            }

            drop_task = {
                "run_id": run_id,
                "task_type": "DROP",
                "user_id": user_id,
                "depot_id": depot_id,
                "node_id": place_node_id,
                "window_start": drop_start.isoformat(),
                "window_end": drop_end.isoformat(),
                "pair_key": pair_key,
            }

        # If user + task_type exists → UPDATE instead of INSERT
        for task in [pick_task, drop_task]:
            key = (task["user_id"], task["task_type"])

            if key in existing_map:
                task_id = existing_map[key]
                updates.append((task_id, task))
                updated_count += 1
            else:
                inserts.append(task)
                created_count += 1

    # Insert into routing_tasks
    if inserts:
        supabase.schema("run").from_("routing_tasks").insert(inserts).execute()
        logger.info(f"Inserted {len(inserts)} new tasks into run.routing_tasks.")

    for task_id, row in updates:
        supabase.schema("run").from_("routing_tasks").update(row).eq("id", task_id).execute()

    if updates:
        logger.info(f"Updated {len(updates)} existing tasks in run.routing_tasks.")

    return {
        "created": created_count,
        "updated": updated_count,
    }
