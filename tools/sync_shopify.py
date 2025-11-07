import os
import json
import csv
from datetime import datetime

# project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "sync_map.json")


def load_config(path=CONFIG_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def flatten_shopify_products(shopify_data):
    """
    Take the JSON you ALREADY create from Shopify (products_full.json)
    and turn it into a flat CSV the merge step can consume.
    """
    rows = []
    for product in shopify_data:
        handle = product.get("handle") or ""
        title = product.get("title") or ""
        status = product.get("status") or "active"
        images = product.get("images") or []
        image_url = images[0]["src"] if images else ""
        active = status == "active"

        variants = product.get("variants") or []
        if not variants:
            rows.append({
                "product_code": "",
                "title": title,
                "price_ex_vat": "",
                "image_url": image_url,
                "product_url": f"/products/{handle}" if handle else "",
                "active": active,
                "colour": "",
                "short_description": product.get("body_html") or ""
            })
            continue

        # primary variant = first variant
        v = variants[0]
        product_code = v.get("sku") or product.get("handle") or ""
        price_ex_vat = v.get("price") or ""

        # try to get a colour if present on variant
        colour = (
            v.get("option1")
            or (product.get("options") or [{}])[0].get("name", "")
            or ""
        )

        rows.append({
            "product_code": product_code,
            "title": title,
            "price_ex_vat": price_ex_vat,
            "image_url": image_url,
            "product_url": f"/products/{handle}" if handle else "",
            "active": active,
            "colour": colour,
            "short_description": product.get("body_html") or ""
        })
    return rows


def main():
    config = load_config()
    source_json = os.path.join(BASE_DIR, config["shopify"]["source_json"])
    output_csv = os.path.join(BASE_DIR, config["shopify"]["flattened_csv"])

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    with open(source_json, "r", encoding="utf-8") as f:
        shopify_data = json.load(f)

    rows = flatten_shopify_products(shopify_data)

    fieldnames = [
        "product_code",
        "title",
        "price_ex_vat",
        "image_url",
        "product_url",
        "active",
        "colour",
        "short_description"
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[{datetime.now()}] Shopify flattened → {output_csv} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
