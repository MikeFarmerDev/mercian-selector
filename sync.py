"""
sync.py
Run the full data refresh for the Mercian Stick Selector:

1. Pull latest products from Shopify (ingestion/shopify_discover.py)
2. Flatten to CSV for the selector (ingestion/flatten_and_report.py)
3. Merge into StickSelection.xlsx with guards (tools/merge_excel.py)
"""

from pathlib import Path
import sys

# make sure we can import from the project
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# 1) Shopify → products_full.json
from ingestion import shopify_discover
# 2) products_full.json → outputs/shopify_update.csv
from ingestion import flatten_and_report
# 3) outputs/shopify_update.csv → data/StickSelection.xlsx
from tools import merge_excel


def main():
    print("[sync] Step 1/4: Discover from Shopify…")
    shopify_discover.main()

    print("[sync] Step 2/4: Fetch inventory levels…")
    from ingestion import shopify_inventory
    shopify_inventory.main()

    print("[sync] Step 3/4: Flatten to CSV…")
    flatten_and_report.main()


    print("[sync] Step 4/4: Merge into Excel…")
    merge_excel.main()

    print("[sync] ✅ All steps complete.")


if __name__ == "__main__":
    main()
