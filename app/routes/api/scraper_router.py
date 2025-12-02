from fastapi import APIRouter, HTTPException
import subprocess

router = APIRouter()

@router.post("/hug-scraper/run")
def run_hug_scraper(facility_name: str, route_date: str):
    """
    Trigger the HUG scraper from the API.
    Runs the local hug_scraper.py script.
    """
    try:
        result = subprocess.run(
            ["python", "-m", "app.services.hug_scraper"],
            capture_output=True,
            text=True
        )

        return {
            "status": "ok",
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
