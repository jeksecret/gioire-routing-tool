from datetime import datetime
from app.supabase import get_supabase


# ============================================================
# 1) Fetch existing run (facility + date)
# ============================================================
def get_existing_run(facility_name: str, route_date: str):
    supabase = get_supabase()
    res = (
        supabase.schema("run")
        .from_("optimization_run")
        .select("*")
        .eq("facility_name", facility_name)
        .eq("route_date", route_date)
        .execute()
    )
    return res.data[0] if res.data else None


# ============================================================
# 2) Create new run
# ============================================================
def create_new_run(facility_name: str, route_date: str, requested_by="system"):
    supabase = get_supabase()
    payload = {
        "facility_name": facility_name,
        "route_date": route_date,
        "status": "pending",
        "requested_by": requested_by,
        # use DB defaults for created_at
    }

    res = (
        supabase.schema("run")
        .from_("optimization_run")
        .insert(payload)
        .execute()
    )

    # Return full object because scraper expects it
    return res.data[0]


# ============================================================
# 3) Status updates
# ============================================================
def set_status_scraping(run_id: int):
    supabase = get_supabase()
    supabase.schema("run").from_("optimization_run").update(
        {
            "status": "scraping",
            "started_at": "now()",   # let Postgres handle timestamp
        }
    ).eq("id", run_id).execute()


def set_status_optimizing(run_id: int):
    supabase = get_supabase()
    supabase.schema("run").from_("optimization_run").update(
        {"status": "optimizing"}
    ).eq("id", run_id).execute()


def set_status_success(run_id: int):
    supabase = get_supabase()
    supabase.schema("run").from_("optimization_run").update(
        {
            "status": "success",
            "finished_at": "now()",  # let Postgres handle timestamp
        }
    ).eq("id", run_id).execute()


def set_status_failed(run_id: int):
    supabase = get_supabase()
    supabase.schema("run").from_("optimization_run").update(
        {
            "status": "failed",
            "finished_at": "now()",
        }
    ).eq("id", run_id).execute()
