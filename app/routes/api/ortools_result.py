import logging
from fastapi import APIRouter, Request
from app.services.ortools_result_service import process_ortools_result

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/result")
async def receive_ortools_result(request: Request):
    """
    Receive OR-Tools solver output and store routing results.
    """
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f"Invalid JSON received: {e}")
        return {
            "status": "error",
            "message": "Invalid JSON payload"
        }

    logger.info("Received OR-Tools result payload")

    return process_ortools_result(payload)
