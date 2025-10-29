from fastapi import FastAPI
from dotenv import load_dotenv
from app.routes.api.notion_sync import router as notion_sync_router

load_dotenv()

app = FastAPI()

@app.get("/")
def read_root():
    """Root endpoint for API status."""
    return {"message": "API is running"}

app.include_router(notion_sync_router, prefix="/api/sync")
