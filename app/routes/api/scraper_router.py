from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import os
import subprocess

router = APIRouter()

class HugScrapeRequest(BaseModel):
    SCRAPE_FACILITY: str
    SCRAPE_YEAR: int
    SCRAPE_MONTH: int
    SCRAPE_DAY: int

@router.post("/hug-scraper/run")
def run_hug_scraper(req: HugScrapeRequest):
    """
    Trigger the HUG scraper from Make.com.
    Sets environment variables dynamically and runs hug_scraper.py
    """

    # Set env vars for scraper
    os.environ["SCRAPE_FACILITY"] = req.SCRAPE_FACILITY
    os.environ["SCRAPE_YEAR"] = str(req.SCRAPE_YEAR)
    os.environ["SCRAPE_MONTH"] = str(req.SCRAPE_MONTH)
    os.environ["SCRAPE_DAY"] = str(req.SCRAPE_DAY)

    try:
        # Run scraper
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
