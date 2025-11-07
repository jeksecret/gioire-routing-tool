from fastapi import APIRouter, HTTPException, Query
from app.services import task_split_service

router = APIRouter()

@router.post("/split")
def split_pick_drop_tasks(run_id: int = Query(1, description="Optimization run ID")):
    """
    Convert records from stg.hug_raw_requests into PICK/DROP tasks
    and insert them into run.routing_tasks.
    """
    try:
        result = task_split_service.split_and_create_tasks(run_id=run_id)
        return {"status": "success", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Task split failed: {e}")
