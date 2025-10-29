from fastapi import APIRouter
from app.services import notion_sync_service as notion_sync
import os

router = APIRouter()

@router.post("/vehicles")
def sync_vehicles():
    data_source_id = os.getenv("NOTION_VEHICLES_DATA_SOURCE_ID")
    result = notion_sync.sync_vehicles(data_source_id)
    return {"status": 200, **result}
