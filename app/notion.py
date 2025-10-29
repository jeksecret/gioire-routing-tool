from notion_client import Client
from functools import lru_cache
import os
from dotenv import load_dotenv

load_dotenv()

@lru_cache(maxsize=1)
def get_notion_client() -> Client:
    """Return a cached Notion client instance."""
    notion_token = os.getenv("NOTION_TOKEN")
    if not notion_token:
        raise RuntimeError("Environment variable NOTION_TOKEN is not set.")
    return Client(auth=notion_token)

def query_database(data_source_id: str) -> dict:
    """Fetch all rows (pages) from a Notion database."""
    notion = get_notion_client()
    try:
        results = notion.data_sources.query(data_source_id=data_source_id)
        return results
    except Exception as e:
        raise RuntimeError(f"Failed to query Notion data source {data_source_id}: {str(e)}")
