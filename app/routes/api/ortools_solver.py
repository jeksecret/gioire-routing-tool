from fastapi import APIRouter
from app.services.ortools_request_service import build_ortools_payload
from app.services.ortools_solver_service import solve_ortools

router = APIRouter()

@router.post("/solve")
def solve_by_run_id(run_id: int):
    built = build_ortools_payload(run_id)
    if built.get("status") != "ok":
        return built

    payload = built["payload"]
    return solve_ortools(payload, run_id=run_id)
