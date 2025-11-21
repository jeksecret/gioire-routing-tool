from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from app.services.notion_sync_service import upsert_vehicle, upsert_depot, upsert_user

router = APIRouter()

# ===============================
# VEHICLE SYNC
# ===============================
class VehicleSyncPayload(BaseModel):
    vehicle_name: str = Field(..., description="Vehicle name from Notion")
    depot_relation_id: str | None = Field(None, description="Notion relation ID for depot")
    seats: int | None = Field(None, description="Vehicle seat capacity")
    active: bool | None = Field(True, description="Active status")
    notion_page_id: str = Field(..., description="Notion page ID")
    notion_last_edited: str | None = Field(None, description="ISO last edited time")

@router.post("/vehicles")
async def sync_vehicles(payload: VehicleSyncPayload):
    """
    Sync vehicle data into the database.
    """
    try:
        result = upsert_vehicle(payload.model_dump())
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# ===============================
# DEPOT SYNC
# ===============================
class DepotSyncPayload(BaseModel):
    depot_name: str = Field(..., description="Facility/Depot name from Notion")
    active: bool | None = Field(True, description="Active flag")
    notion_page_id: str = Field(..., description="Notion page ID")
    notion_last_edited: str | None = Field(None, description="ISO timestamp of last edit")

@router.post("/depots")
async def sync_depots(payload: DepotSyncPayload):
    """
    Sync facility (depot) data into the database.
    """
    try:
        result = upsert_depot(payload.model_dump())
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# ===============================
# USER SYNC
# ===============================
class UserSyncPayload(BaseModel):
    user_name: str = Field(..., description="User name  from Notion")
    depot_relation_id: str | None = Field(None, description="Notion relation ID for related depot")
    active: bool | None = Field(True, description="Active status")
    notion_page_id: str = Field(..., description="Notion page ID")
    notion_last_edited: str | None = Field(None, description="ISO last edited time")

@router.post("/users")
async def sync_users(payload: UserSyncPayload):
    """
    Sync user data into the database.
    """
    try:
        result = upsert_user(payload.model_dump())
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
