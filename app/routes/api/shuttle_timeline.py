import logging
from fastapi import APIRouter, Query
from app.services.shuttle_timeline_service import load_shuttle_timelines

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/timeline")
def get_shuttle_timeline(run_id: int = Query(..., description="optimization_run.id")):
    """
    Returns authoritative shuttle timelines reconstructed from
    run.routing_results (ordered by vehicle_id, sequence).
    """
    logger.info(f"[ShuttleTimeline] Loading timeline for run_id={run_id}")

    timelines = load_shuttle_timelines(run_id)

    if not timelines:
        return {
            "status": "error",
            "message": f"No routing_results found for run_id={run_id}"
        }

    return {
        "status": "ok",
        "run_id": run_id,
        "vehicles": timelines
    }
