from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from app.services.notion_sync_service import upsert_vehicle

router = APIRouter()

class VehicleSyncPayload(BaseModel):
    vehicle_name: str = Field(..., description="Vehicle name from Notion")
    seats: int | None = Field(None, description="Vehicle seat capacity")
    active: bool | None = Field(True, description="Active status")
    depot_relation_id: str | None = Field(None, description="Notion relation ID for depot")
    notion_page_id: str = Field(..., description="Notion page ID")
    notion_last_edited: str | None = Field(None, description="ISO last edited time")

@router.post("/vehicles")
async def sync_vehicles(payload: VehicleSyncPayload):
    """
    Sync notion vehicle data into the database.
    """
    try:
        result = upsert_vehicle(payload.model_dump())
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
