from fastapi import APIRouter
from app.services.ortools_request_service import build_ortools_payload
from app.services.ortools_solver_service import (solve_ortools, post_solver_result_to_make)

router = APIRouter()

@router.post("/solve")
def solve_by_run_id(run_id: int):
    built = build_ortools_payload(run_id)
    if built.get("status") != "ok":
        return built

    payload = built["payload"]
    result = solve_ortools(payload, run_id=run_id)

    if result.get("status") == "ok":
        try:
            post_solver_result_to_make(result)
        except Exception as e:
            result["make_webhook_warning"] = str(e)

    return result
