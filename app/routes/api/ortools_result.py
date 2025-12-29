import logging
from fastapi import APIRouter, Request
from app.services.ortools_result_service import process_ortools_result

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/result")
async def receive_ortools_result(request: Request):
    """
    Receive OR-Tools solver output and persist routing results.
    """
    payload = await request.json()

    logger.info("Received OR-Tools result payload")

    result = process_ortools_result(payload)

    return result
