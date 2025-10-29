import os
from functools import lru_cache
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

@lru_cache(maxsize=1)
def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if url is None:
        raise RuntimeError("Environment variable SUPABASE_URL is not set.")
    if key is None:
        raise RuntimeError("Neither SUPABASE_SERVICE_ROLE_KEY nor SUPABASE_ANON_KEY is set.")
    return create_client(url, key)
