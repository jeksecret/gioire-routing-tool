from fastapi import FastAPI
from dotenv import load_dotenv
from app.routes.api.notion_sync import router as notion_sync_router
from app.routes.api.travel_times import router as travel_times_router
from app.routes.api.task_split import router as task_split_router
from app.routes.api.time_matrix import router as time_matrix_router
from app.routes.api.scraper_router import router as scraper_router
from app.routes.api.ortools import router as ortools_router
from app.routes.api.ortools_result import router as ortools_result_router

load_dotenv()

app = FastAPI()

@app.get("/")
def read_root():
    """Root endpoint for API status."""
    return {"message": "API is running"}

@app.get("/status")
def status_check():
    """Health check endpoint."""
    return {"status": "200"}

app.include_router(notion_sync_router, prefix="/api/sync")
app.include_router(travel_times_router, prefix="/api/travel-times")
app.include_router(task_split_router, prefix="/api/task")
app.include_router(time_matrix_router, prefix="/api/time-matrix")
app.include_router(scraper_router, prefix="/api/hug")
app.include_router(ortools_router, prefix="/api/ortools")
app.include_router(ortools_result_router, prefix="/api/ortools")
