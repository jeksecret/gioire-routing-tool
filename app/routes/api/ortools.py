from fastapi import APIRouter
from app.services.ortools_request_service import build_ortools_payload

router = APIRouter()

@router.post("/build-ortools")
async def build_ortools(run_id: int):
    """Return OR-Tools formatted payload."""
    return build_ortools_payload(run_id)
