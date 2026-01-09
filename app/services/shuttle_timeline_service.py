from typing import Dict, List
from app.supabase import get_supabase

supabase = get_supabase()

def load_shuttle_timelines(run_id: int) -> Dict[int, List[dict]]:
    """
    Reconstruct shuttle timeline per vehicle from routing_results
    """
    q = (
        supabase.schema("run")
        .from_("routing_results")
        .select("*")
        .eq("run_id", run_id)
        .order("vehicle_id")
        .order("sequence")
        .execute()
    )

    rows = q.data or []

    timelines: Dict[int, List[dict]] = {}

    for r in rows:
        vid = r["vehicle_id"]
        timelines.setdefault(vid, []).append(r)

    return timelines
