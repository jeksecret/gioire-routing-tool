from datetime import datetime
from app.supabase import get_supabase


# ============================================================
# Fetch existing run (facility + date)
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
# Create new run
# ============================================================
def create_new_run(facility_name: str, route_date: str, requested_by="system"):
    supabase = get_supabase()
    payload = {
        "facility_name": facility_name,
        "route_date": route_date,
        "status": "pending",
        "requested_by": requested_by,
        "created_at": datetime.utcnow().isoformat(),
    }

    res = (
        supabase.schema("run")
        .from_("optimization_run")
        .insert(payload)
        .execute()
    )

    return res.data[0]["id"]


# ============================================================
# Update status: scraping
# ============================================================
def set_status_scraping(run_id: int):
    supabase = get_supabase()
    supabase.schema("run").from_("optimization_run").update(
        {
            "status": "scraping",
            "started_at": datetime.utcnow().isoformat(),
        }
    ).eq("id", run_id).execute()


# ============================================================
# Update status: optimizing
# ============================================================
def set_status_optimizing(run_id: int):
    supabase = get_supabase()
    supabase.schema("run").from_("optimization_run").update(
        {
            "status": "optimizing",
            "finished_at": datetime.utcnow().isoformat(),
        }
    ).eq("id", run_id).execute()


# ============================================================
# Update status: scrape_error
# ============================================================
def set_status_scrape_error(run_id: int):
    supabase = get_supabase()
    supabase.schema("run").from_("optimization_run").update(
        {
            "status": "scrape_error",
            "finished_at": datetime.utcnow().isoformat(),
        }
    ).eq("id", run_id).execute()


# ============================================================
# Save meta_json snapshot
# ============================================================
def set_meta_json(run_id: int, meta: dict):
    supabase = get_supabase()
    supabase.schema("run").from_("optimization_run").update(
        {"meta_json": meta}
    ).eq("id", run_id).execute()
