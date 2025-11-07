# ingestion/flatten_inventory.py
# Flatten inventory_levels.json → outputs/inventory_flat.csv (+ summary)
# Requires: SHOPIFY creds in .env (to map location_id → location name)

import csv
import json
import os
from pathlib import Path
from datetime import datetime
import requests

BASE_DIR = Path(__file__).resolve().parent.parent  # repo root
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

INGESTION_DIR = BASE_DIR / "outputs"
IN_PATH = INGESTION_DIR / "inventory_levels.json"  # produced by ingestion/shopify_inventory.py
OUT_CSV = INGESTION_DIR / "inventory_flat.csv"
OUT_SUMMARY = INGESTION_DIR / "inventory_summary.txt"

def _session():
    s = requests.Session()
    s.headers.update({
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN or "",
        "Content-Type": "application/json",
    })
    return s

def _fetch_locations():
    """Return {location_id:int -> name:str}. If no creds, fall back to ID as str."""
    mapping = {}
    if not SHOPIFY_STORE_DOMAIN or not SHOPIFY_ADMIN_TOKEN:
        return mapping  # no enrichment, still flatten
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/locations.json"
    resp = _session().get(url, timeout=30)
    resp.raise_for_status()
    for loc in resp.json().get("locations", []):
        mapping[int(loc["id"])] = loc.get("name") or str(loc["id"])
    return mapping

def _parse_iso(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def main():
    if not IN_PATH.exists():
        raise FileNotFoundError(f"Missing input: {IN_PATH} (run ingestion/shopify_inventory.py first)")

    payload = json.loads(IN_PATH.read_text(encoding="utf-8"))
    levels = payload.get("inventory_levels", [])

    # Enrich location names if possible
    loc_map = _fetch_locations()

    # --- Excel-safe text forcing to preserve long IDs ---
    def _excel_text(s):
        if s is None:
            return ""
        s = str(s)
        return f'="{s}"'  # keeps full numeric string

    PURE_CSV = False  # flip True for raw numeric output

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["inventory_item_id","location_id","location_name","available","updated_at","admin_graphql_api_id"])
        for row in levels:
            inv_item_id = row.get("inventory_item_id")
            loc_id = row.get("location_id")
            name = loc_map.get(int(loc_id)) if isinstance(loc_id, int) and loc_map else None

            inv_out = str(inv_item_id) if PURE_CSV else _excel_text(inv_item_id)
            loc_out = str(loc_id)       if PURE_CSV else _excel_text(loc_id)

            w.writerow([
                inv_out,
                loc_out,
                name or (str(loc_id) if loc_id is not None else ""),
                row.get("available"),
                row.get("updated_at"),
                row.get("admin_graphql_api_id"),
            ])

    # Simple summary
    total = len(levels)
    by_loc = {}
    dates = []
    for r in levels:
        lid = r.get("location_id")
        by_loc[lid] = by_loc.get(lid, 0) + 1
        dt = _parse_iso(r.get("updated_at") or "")
        if dt: dates.append(dt)

    dates_sorted = sorted(dates) if dates else []
    oldest = dates_sorted[0].isoformat() if dates_sorted else "n/a"
    newest = dates_sorted[-1].isoformat() if dates_sorted else "n/a"

    with OUT_SUMMARY.open("w", encoding="utf-8") as f:
        f.write(f"Flattened rows: {total}\n")
        f.write(f"Updated_at range: {oldest} → {newest}\n")
        f.write("Rows by location_id (name):\n")
        for lid, count in sorted(by_loc.items(), key=lambda x: x[0] or 0):
            name = None
            try:
                name = loc_map.get(int(lid)) if loc_map and lid is not None else None
            except Exception:
                pass
            label = f"{lid} ({name})" if name else f"{lid}"
            f.write(f"  - {label}: {count}\n")

    print(f"[flatten] ✅ Wrote {OUT_CSV} and {OUT_SUMMARY}")

if __name__ == "__main__":
    main()
