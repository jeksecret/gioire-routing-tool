from fastapi import APIRouter, HTTPException
from app.services.time_matrix_service import build_time_matrix

router = APIRouter()

@router.post("/matrix")
async def generate_matrix(run_id: int):
    try:
        result = build_time_matrix(run_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
