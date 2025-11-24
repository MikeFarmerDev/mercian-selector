# ingestion/shopify_inventory.py
# Fetch Shopify locations, then fetch inventory levels per location
# Requires: read_inventory

import os
import json
from pathlib import Path
from typing import Optional, Dict
import requests
import argparse

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

SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")

VALID_STORES = {"global", "eu", "au"}

OUT_DIR = BASE_DIR / "outputs"
OUT_DIR.mkdir(exist_ok=True)


def _get_store_credentials(store: str) -> Dict[str, str]:
    """
    Resolve the Shopify domain and admin token for the given store key.

    Store keys:
      - "global" -> SHOPIFY_STORE_DOMAIN / SHOPIFY_ADMIN_TOKEN
      - "eu"     -> SHOPIFY_EU_STORE_DOMAIN / SHOPIFY_EU_ADMIN_TOKEN
      - "au"     -> SHOPIFY_AU_STORE_DOMAIN / SHOPIFY_AU_ADMIN_TOKEN
    """
    store = store.lower().strip()
    if store not in VALID_STORES:
        raise ValueError(f"Unsupported store '{store}'. Expected one of {sorted(VALID_STORES)}.")

    if store == "global":
        domain = os.getenv("SHOPIFY_STORE_DOMAIN")
        token = os.getenv("SHOPIFY_ADMIN_TOKEN")
    elif store == "eu":
        domain = os.getenv("SHOPIFY_EU_STORE_DOMAIN")
        token = os.getenv("SHOPIFY_EU_ADMIN_TOKEN")
    elif store == "au":
        domain = os.getenv("SHOPIFY_AU_STORE_DOMAIN")
        token = os.getenv("SHOPIFY_AU_ADMIN_TOKEN")
    else:
        # Should never hit this because of VALID_STORES check above.
        raise ValueError(f"Unhandled store '{store}'.")

    if not domain or not token:
        raise RuntimeError(f"Missing Shopify credentials for store '{store}' in .env")

    return {"domain": domain, "token": token}


def _session(token: str):
    s = requests.Session()
    s.headers.update(
        {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        }
    )
    return s


def fetch_locations(store: str):
    creds = _get_store_credentials(store)
    url = (
        f"https://{creds['domain']}"
        f"/admin/api/{SHOPIFY_API_VERSION}/locations.json"
    )
    sess = _session(creds["token"])
    resp = sess.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("locations", [])


def fetch_inventory_for_location(store: str, location_id: int):
    """
    GET /inventory_levels.json?location_ids=...
    paginated via Link header
    """
    creds = _get_store_credentials(store)
    levels = []
    url = (
        f"https://{creds['domain']}"
        f"/admin/api/{SHOPIFY_API_VERSION}/inventory_levels.json"
        f"?location_ids={location_id}&limit=250"
    )
    sess = _session(creds["token"])

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

def main(store: Optional[str] = None):
    """
    Entry point for fetching inventory levels for a specific store.

    When called from the CLI, --store is mandatory and must be one of:
      - global
      - eu
      - au
    """
    if store is None:
        parser = argparse.ArgumentParser(description="Fetch Shopify inventory levels for a specific store.")
        parser.add_argument(
            "--store",
            required=True,
            choices=sorted(VALID_STORES),
            help="Which store to fetch inventory for (e.g. 'global', 'eu', 'au').",
        )
        args = parser.parse_args()
        store = args.store

    locations = fetch_locations(store)
    if not locations:
        raise RuntimeError(f"No Shopify locations found for store '{store}' — cannot fetch inventory levels.")

    all_levels = []
    for loc in locations:
        loc_id = loc["id"]
        print(f"[inventory] fetching levels for store {store} at location {loc_id} ({loc.get('name')}) …")
        loc_levels = fetch_inventory_for_location(store, loc_id)
        all_levels.extend(loc_levels)

    out_path = OUT_DIR / f"inventory_levels_{store}.json"
    out_path.write_text(
        json.dumps({"inventory_levels": all_levels}, indent=2),
        encoding="utf-8",
    )
    print(f"[inventory] ✅ Saved {len(all_levels)} inventory levels for store '{store}' to {out_path}")


if __name__ == "__main__":
    main()
