# ingestion/shopify_inventory.py
# Fetch Shopify locations, then fetch inventory levels per location
# Requires: read_inventory

import os
import json
from pathlib import Path
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"

# --- load .env
if ENV_PATH.exists():
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")

OUT_DIR = BASE_DIR / "outputs"
OUT_DIR.mkdir(exist_ok=True)
OUT_PATH = OUT_DIR / "inventory_levels.json"


def _session():
    s = requests.Session()
    s.headers.update({
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
        "Content-Type": "application/json",
    })
    return s


def fetch_locations():
    if not SHOPIFY_STORE_DOMAIN or not SHOPIFY_ADMIN_TOKEN:
        raise RuntimeError("Missing SHOPIFY_STORE_DOMAIN or SHOPIFY_ADMIN_TOKEN in .env")

    url = (
        f"https://{SHOPIFY_STORE_DOMAIN}"
        f"/admin/api/{SHOPIFY_API_VERSION}/locations.json"
    )
    resp = _session().get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("locations", [])


def fetch_inventory_for_location(location_id: int):
    """
    GET /inventory_levels.json?location_ids=...
    paginated via Link header
    """
    levels = []
    url = (
        f"https://{SHOPIFY_STORE_DOMAIN}"
        f"/admin/api/{SHOPIFY_API_VERSION}/inventory_levels.json"
        f"?location_ids={location_id}&limit=250"
    )
    sess = _session()

    while url:
        resp = sess.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        levels.extend(payload.get("inventory_levels", []))

        # pagination
        link_header = resp.headers.get("Link")
        next_url = None
        if link_header:
            parts = [p.strip() for p in link_header.split(",")]
            for part in parts:
                if 'rel="next"' in part:
                    start = part.find("<") + 1
                    end = part.find(">")
                    next_url = part[start:end]
                    break
        url = next_url

    return levels


def main():
    locations = fetch_locations()
    if not locations:
      raise RuntimeError("No Shopify locations found — cannot fetch inventory levels.")

    all_levels = []
    for loc in locations:
        loc_id = loc["id"]
        print(f"[inventory] fetching levels for location {loc_id} ({loc.get('name')}) …")
        loc_levels = fetch_inventory_for_location(loc_id)
        all_levels.extend(loc_levels)

    OUT_PATH.write_text(
        json.dumps({"inventory_levels": all_levels}, indent=2),
        encoding="utf-8"
    )
    print(f"[inventory] ✅ Saved {len(all_levels)} inventory levels to {OUT_PATH}")


if __name__ == "__main__":
    main()
