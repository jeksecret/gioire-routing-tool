from fastapi import APIRouter, HTTPException, Query
from app.supabase import get_supabase
from app.services import travel_time_service

router = APIRouter()

@router.post("/build")
def build_time_matrix(
    routing_preference: str = Query("TRAFFIC_AWARE"),
    require_coords: bool = Query(False)
):
    """
    Build travel-time matrix using Google Routes API and store in core.travel_times.
    """
    supabase = get_supabase()

    try:
        nodes = supabase.schema("core").from_("nodes").select(
            "id, address, latitude, longitude"
        ).execute().data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch nodes: {e}")

    if not nodes:
        raise HTTPException(status_code=400, detail="No nodes found in core.nodes")

    try:
        result = travel_time_service.build_and_store_matrix(
            nodes,
            routing_preference=routing_preference,
            require_coords=require_coords
        )
        return {"status": "success", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Matrix build failed: {e}")
