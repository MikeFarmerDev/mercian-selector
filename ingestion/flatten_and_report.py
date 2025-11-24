# flatten_and_report.py
# Purpose: Read products_full.json from Shopify discovery, flatten to one-row-per-variant CSV,
#          auto-expand product/variant metafields into columns, and write a quick health report.
# Usage:   python ingestion/flatten_and_report.py
# Outputs: outputs/shopify_update.csv, outputs/flatten_summary.txt

import os
import json
import csv
import re
import statistics
from pathlib import Path
from collections import Counter
import argparse
from typing import Optional

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
# Domain is store-specific now; STORE_DOMAIN is kept only for fallback URL construction.
GLOBAL_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "shop.mercianhockey.com")

VALID_STORES = {"global", "eu", "au"}

def get_store_domain(store: str):
    store = store.lower().strip()
    if store not in VALID_STORES:
        raise ValueError(f"Unsupported store '{store}'. Expected one of {sorted(VALID_STORES)}.")
    if store == "global":
        return os.getenv("SHOPIFY_STORE_DOMAIN") or GLOBAL_STORE_DOMAIN
    if store == "eu":
        return os.getenv("SHOPIFY_EU_STORE_DOMAIN") or GLOBAL_STORE_DOMAIN
    if store == "au":
        return os.getenv("SHOPIFY_AU_STORE_DOMAIN") or GLOBAL_STORE_DOMAIN


# project root = mercian-selector
BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = BASE_DIR / "outputs"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Inputs & Outputs ----------
def store_paths(store: str):
    # per-store input & output
    # products_full_<store>.json is written at the repo root by shopify_discover.py
    src = BASE_DIR / f"products_full_{store}.json"
    out_csv = OUTPUTS_DIR / f"shopify_update_{store}.csv"
    out_summary = OUTPUTS_DIR / f"flatten_summary_{store}.txt"
    return src, out_csv, out_summary

# ------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------
def strip_html(s: str) -> str:
    return re.sub("<[^>]+>", "", s or "").strip()


def _nodes_or_edges(container):
    """Return list of nodes whether container is {'nodes': [...]} or {'edges': [{'node': {...}}]}."""
    if not isinstance(container, dict):
        return []
    if isinstance(container.get("nodes"), list):
        return container["nodes"]
    if isinstance(container.get("edges"), list):
        out = []
        for e in container["edges"]:
            if isinstance(e, dict) and isinstance(e.get("node"), dict):
                out.append(e["node"])
        return out
    return []


def load_products(path: Path):
    """
    Load products from products_full.json produced by your discovery script.
    Handles:
      - raw list of products
      - {'products': [...]}
      - GraphQL: {'data': {'products': {'nodes':[...]} or {'edges':[{'node':...}]}}}
      - Attempts deep search for any list of product-like dicts.
    """
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)

    # 1) Already a list of products
    if isinstance(data, list) and (not data or isinstance(data[0], dict)):
        return data

    # 2) Simple wrappers
    for key in ("products", "items", "nodes"):
        if isinstance(data, dict) and isinstance(data.get(key), list):
            return data[key]

    # 3) GraphQL canonical shapes
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        d = data["data"]

        # data.products.{nodes|edges}
        if isinstance(d.get("products"), dict):
            prods = _nodes_or_edges(d["products"])
            if prods:
                return prods

        # Fallback: any child dict that looks like a products connection
        for v in d.values():
            if isinstance(v, dict):
                cand = _nodes_or_edges(v)
                if cand:
                    return cand
            if isinstance(v, list) and v and isinstance(v[0], dict) and ("title" in v[0] or "handle" in v[0]):
                return v

    # 4) Deep search anywhere for a list of product-like dicts
    def _find_products(obj):
        if isinstance(obj, list) and obj and isinstance(obj[0], dict) and ("title" in obj[0] or "handle" in obj[0]):
            return obj
        if isinstance(obj, dict):
            for val in obj.values():
                found = _find_products(val)
                if found:
                    return found
        return None

    found = _find_products(data)
    if found:
        return found

    top = list(data.keys()) if isinstance(data, dict) else type(data)
    raise RuntimeError(f"Unrecognised products structure. Top-level: {top}")


def extract_variants(product):
    """
    Return a list of variant dicts regardless of REST or GraphQL shape.
    """
    raw = product.get("variants")
    if isinstance(raw, list):
        nodes = raw
    elif isinstance(raw, dict):
        nodes = _nodes_or_edges(raw)
    else:
        nodes = []

    norm = []
    for v in (nodes or [{}]):
        if not isinstance(v, dict):
            continue
        vv = dict(v)

        # price fields: if moneyV2 present, lift amount
        if isinstance(vv.get("price"), dict) and "amount" in vv["price"]:
            vv["price"] = vv["price"]["amount"]
        if isinstance(vv.get("compareAtPrice"), dict) and "amount" in vv["compareAtPrice"]:
            vv["compareAtPrice"] = vv["compareAtPrice"]["amount"]

        # inventory and availability normalisation
        if "inventoryQuantity" not in vv and "quantityAvailable" in vv:
            vv["inventoryQuantity"] = vv["quantityAvailable"]
        if "availableForSale" not in vv and "available" in vv:
            vv["availableForSale"] = vv["available"]

        norm.append(vv)

    return norm or [{}]


def kv_metafields(obj):
    """
    Returns dict of {"namespace.key": value} for metafields on product or variant.
    """
    out = {}
    if not obj:
        return out
    mfs = obj.get("metafields") or []
    # GraphQL nodes/edges
    if isinstance(mfs, dict):
        nodes = _nodes_or_edges(mfs)
        for n in nodes:
            if not isinstance(n, dict):
                continue
            ns = n.get("namespace") or n.get("namespaceName")
            key = n.get("key")
            val = n.get("value")
            if ns and key:
                out[f"{ns}.{key}"] = val
        return out
    # REST list
    if isinstance(mfs, list):
        for m in mfs:
            if not isinstance(m, dict):
                continue
            ns = m.get("namespace")
            key = m.get("key")
            val = m.get("value")
            if ns and key:
                out[f"{ns}.{key}"] = val
    return out


def first_image(prod):
    # 1) GraphQL featuredImage
    fi = prod.get("featuredImage")
    if isinstance(fi, dict):
        url = fi.get("url") or fi.get("src") or fi.get("originalSrc")
        if url:
            return url

    # 2) images can be a plain list OR a GraphQL connection (nodes/edges)
    imgs = prod.get("images") or []
    if isinstance(imgs, dict):  # GraphQL connection
        nodes = _nodes_or_edges(imgs)
        if nodes:
            i0 = nodes[0] or {}
            return i0.get("url") or i0.get("src") or i0.get("originalSrc")
    elif isinstance(imgs, list) and imgs:
        i0 = imgs[0] or {}
        return i0.get("url") or i0.get("src") or i0.get("originalSrc")

    return None


def first_variant_image(variant):
    # --- line before (context) ---
    for key in ("featuredImage", "image", "featuredMedia", "media"):
        node = variant.get(key)
        if isinstance(node, dict):
            url = node.get("url") or node.get("src") or node.get("originalSrc")
            if not url:
                preview = node.get("preview") or {}
                image = preview.get("image") or {}
                url = image.get("url") or image.get("src") or image.get("originalSrc")
            if url:
                return url
        elif isinstance(node, list) and node:
            # --- new code handling GraphQL-style media lists ---
            media0 = node[0]
            if isinstance(media0, dict):
                url = (
                    media0.get("url")
                    or media0.get("src")
                    or media0.get("originalSrc")
                    or ((media0.get("preview") or {}).get("image") or {}).get("url")
                )
                if url:
                    return url
    return None
    # --- line after (context) ---

def get_collections(prod):
    cols = prod.get("collections") or []
    titles = []
    if isinstance(cols, list):
        for c in cols:
            t = (c or {}).get("title")
            if t:
                titles.append(t)
    return ",".join(titles)


def boolish(x):
    if isinstance(x, bool):
        return x
    if x is None:
        return None
    s = str(x).strip().lower()
    if s in ("true", "yes", "1"):
        return True
    if s in ("false", "no", "0"):
        return False
    return None


def num(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except:
        return None


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main(store: Optional[str] = None):
    if store is None:
        parser = argparse.ArgumentParser(description="Flatten product + variant data for a specific store.")
        parser.add_argument(
            "--store",
            required=True,
            choices=sorted(VALID_STORES),
            help="Which store to flatten (global, eu, au)."
        )
        args = parser.parse_args()
        store = args.store

    SRC, OUT_CSV, OUT_SUMMARY = store_paths(store)

    if not SRC.exists():
        raise FileNotFoundError(f"Missing input: {SRC} (run discovery for store '{store}' first)")

    products = load_products(SRC)

    # discover metafield columns dynamically
    product_mf_cols, variant_mf_cols = set(), set()
    for p in products:
        product_mf_cols.update(kv_metafields(p).keys())
        for v in extract_variants(p):
            variant_mf_cols.update(kv_metafields(v).keys())

    base_cols = [
        # Product-level
        "product_id", "handle", "title", "vendor", "product_status", "published_at",
        "product_type", "tags", "collections", "product_url", "product_image",
        # Variant-level
        "variant_id", "inventory_item_id", "variant_title", "sku", "option1", "option2", "option3",
        "price", "compare_at_price", "available_for_sale", "inventory_quantity", "barcode",
        "variant_image", "display_image",
    ]

    cols = base_cols + sorted(product_mf_cols) + sorted(variant_mf_cols)

    # ensure our extra columns exist
    for extra in [
        "Description Narrative",
        "Meta Description",
        "Image URL",
        "Product URL",
        "price_ex_vat",
    ]:
        if extra not in cols:
            cols.append(extra)

    rows = []

    for p in products:
        # ---------------- product-level data ----------------
        prod = {
            "product_id": p.get("id"),
            "handle": p.get("handle"),
            "title": p.get("title"),
            "vendor": p.get("vendor"),
            "product_status": p.get("status") or p.get("publishedStatus") or p.get("published_scope"),
            "published_at": p.get("publishedAt") or p.get("published_at"),
            "product_type": p.get("productType") or p.get("product_type"),
            "tags": ",".join(p.get("tags") or []),
            "collections": get_collections(p),
            "product_url": p.get("onlineStoreUrl") or p.get("online_store_url"),
            "product_image": first_image(p),
        }

        # description: long (HTML) → plain text
        raw_desc = (
            p.get("descriptionHtml")
            or p.get("body_html")
            or p.get("body")
            or p.get("description")
            or ""
        )
        clean_desc = strip_html(raw_desc)

        # meta description
        meta_desc = ""
        if isinstance(p.get("seo"), dict):
            meta_desc = p["seo"].get("description") or ""
        if not meta_desc:
            # fallback to the metafield we know exists on your site
            meta_desc = kv_metafields(p).get("global.description_tag", "")

        prod["Description Narrative"] = clean_desc
        prod["Meta Description"] = meta_desc

        # product metafields (after we used them as fallback)
        prod_mf = kv_metafields(p)

        # ---------------- variant loop ----------------
        for v in extract_variants(p):
            row = dict.fromkeys(cols, "")

            # carry product fields
            for k, val in prod.items():
                row[k] = "" if val is None else val

            # variant fields
            row["variant_id"] = v.get("id", "")
            inv_obj = v.get("inventoryItem") or {}
            if isinstance(inv_obj, dict):
                inv_gid = inv_obj.get("id")
                if inv_gid:
                    # extract numeric part from gid://shopify/InventoryItem/…
                    row["inventory_item_id"] = inv_gid.split("/")[-1]
            row["variant_title"] = v.get("title", "")
            row["sku"] = v.get("sku", "")
            row["option1"] = v.get("option1") or ""
            row["option2"] = v.get("option2") or ""
            row["option3"] = v.get("option3") or ""
            row["price"] = v.get("price", "")
            row["compare_at_price"] = v.get("compareAtPrice") or v.get("compare_at_price") or ""
            row["available_for_sale"] = v.get("availableForSale") if "availableForSale" in v else v.get("available") or ""
            row["inventory_quantity"] = v.get("inventoryQuantity") or v.get("inventory_quantity") or ""
            row["barcode"] = v.get("barcode") or ""

            # image priority: variant first, then product
            row["variant_image"] = first_variant_image(v) or ""
            row["display_image"] = row["variant_image"] or row["product_image"]

            # ---- Excel-friendly columns for Stick Selector sync ----
            # Image URL
            row["Image URL"] = row["display_image"] or ""

            # Product URL: prefer Shopify onlineStoreUrl, else build from handle
            product_url = prod.get("product_url") or ""
            if not product_url:
                handle = prod.get("handle") or ""
                if handle:
                    domain = get_store_domain(store)
                    product_url = f"https://{domain}/products/{handle}"

            row["Product URL"] = product_url

            # Description Narrative already on prod → row already has it

            # price_ex_vat: just pass the raw Shopify price; Excel/merge will calc Full Price
            row["price_ex_vat"] = row.get("price") or ""

            # metafields (product first, then variant overwrite)
            for k, val in prod_mf.items():
                row[k] = "" if val is None else val
            for k, val in kv_metafields(v).items():
                row[k] = "" if val is None else val

            rows.append(row)

    # ------------------------------------------------------------------
    # WRITE CSV
    # ------------------------------------------------------------------
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            # Excel-safe coercion to preserve long numeric IDs (no scientific notation)
            inv_id = r.get("inventory_item_id")
            if inv_id is not None and inv_id != "":
                r["inventory_item_id"] = f'="{str(inv_id)}"'
            w.writerow(r)

    # ------------------------------------------------------------------
    # SUMMARY
    # ------------------------------------------------------------------
    product_count = len(products)
    variant_count = len(rows)
    by_status = Counter((p.get("status") or p.get("publishedStatus") or "unknown") for p in products)
    prices = [num(r["price"]) for r in rows if num(r["price"]) is not None]
    inv_q = [num(r["inventory_quantity"]) for r in rows if num(r["inventory_quantity"]) is not None]
    available_true = sum(1 for r in rows if boolish(r["available_for_sale"]) is True)
    available_false = sum(1 for r in rows if boolish(r["available_for_sale"]) is False)

    def safe_stats(nums):
        if not nums:
            return "n/a"
        return f"min={min(nums):.2f}, median={statistics.median(nums):.2f}, max={max(nums):.2f}"

    missing_price = sum(1 for r in rows if r["price"] in ("", None))
    missing_variant_id = sum(1 for r in rows if r["variant_id"] in ("", None))

    lines = []
    lines.append("=== Flatten Summary ===")
    lines.append(f"Products: {product_count}")
    lines.append(f"Variants (rows): {variant_count}")
    lines.append("")
    lines.append("Product status counts:")
    for k, v in by_status.items():
        lines.append(f"  - {k}: {v}")
    lines.append("")
    lines.append(
        f"Variants available_for_sale: true={available_true}, false={available_false}, "
        f"unknown={variant_count - available_true - available_false}"
    )
    lines.append(f"Price stats: {safe_stats(prices)}")
    lines.append(f"Total inventory qty (sum of known): {sum(inv_q) if inv_q else 'n/a'}")
    lines.append(f"Missing price rows: {missing_price}")
    lines.append(f"Missing variant_id rows: {missing_variant_id}")

    OUT_SUMMARY.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_CSV} and {OUT_SUMMARY}")

if __name__ == "__main__":
    main()
