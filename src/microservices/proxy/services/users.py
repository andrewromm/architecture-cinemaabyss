import os

import requests

MONOLITH_SERVICE_API = os.getenv("MONOLITH_URL", "http://monolith:8080")


def get_users():
    try:
        r = requests.get(f"{MONOLITH_SERVICE_API}/api/users", timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        return {"error": f"Failed to fetch users: {e}"}
