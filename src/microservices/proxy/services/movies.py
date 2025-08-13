import os
import random
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"
MOVIES_MIGRATION_PERCENT = max(
    0, min(100, int(os.getenv("MOVIES_MIGRATION_PERCENT", "50")))
)

MONOLITH_SERVICE_API = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_API = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")


def _choose_backend_random() -> str:
    if not GRADUAL_MIGRATION or MOVIES_MIGRATION_PERCENT == 0:
        # Когда фича выключена — можно направлять всё на монолит
        return MONOLITH_SERVICE_API
    r = random.random()  # 0.0 <= r < 1.0
    return (
        MONOLITH_SERVICE_API
        if r < (MOVIES_MIGRATION_PERCENT / 100.0)
        else MOVIES_SERVICE_API
    )


def get_movies():
    url = f"{_choose_backend_random()}/api/movies"
    logger.info(f"Fetching movies from: {url}")
    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching movies: {e}")
        return {"error": f"Failed to fetch movies: {e}"}
