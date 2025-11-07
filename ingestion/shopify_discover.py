import os, json, requests, sys
from typing import Optional, Dict
from dotenv import load_dotenv

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

load_dotenv()

STORE = os.getenv("SHOPIFY_STORE_DOMAIN")
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")
COLLECTION_HANDLE = os.getenv("STICK_COLLECTION_HANDLE")  # optional

assert STORE and TOKEN, "Missing SHOPIFY_STORE_DOMAIN or SHOPIFY_ADMIN_TOKEN in .env"

URL = f"https://{STORE}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {"Content-Type": "application/json", "X-Shopify-Access-Token": TOKEN}

def gql(query: str, variables: Optional[Dict] = None) -> dict:
    r = requests.post(URL, headers=HEADERS, json={"query": query, "variables": variables or {}}, timeout=30)
    try:
        data = r.json()
    except Exception as e:
        print("Non-JSON response:", r.status_code, r.text[:500])
        raise
    if r.status_code != 200 or "errors" in data:
        print("GraphQL error/status:", r.status_code, json.dumps(data.get("errors"), indent=2))
    return data

def save(name: str, obj: dict):
    out_path = BASE_DIR / name
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    print(f"→ wrote {out_path}")

# ----------------------------
# A) Metafield DEFINITION scan
# ----------------------------
defs_product_q = """
query ProductMetafieldDefs($first:Int!){
  metafieldDefinitions(ownerType: PRODUCT, first: $first) {
    edges {
      node {
        namespace
        key
        name
        type { name }
        description
      }
    }
  }
}
"""

defs_variant_q = """
query VariantMetafieldDefs($first:Int!){
  metafieldDefinitions(ownerType: PRODUCTVARIANT, first: $first) {
    edges {
      node {
        namespace
        key
        name
        type { name }
        description
      }
    }
  }
}
"""

print("Scanning metafield DEFINITIONS…")
defs_prod = gql(defs_product_q, {"first": 200})
defs_var  = gql(defs_variant_q, {"first": 200})
schema_definitions = {"product": defs_prod, "variant": defs_var}
save("schema_definitions.json", schema_definitions)

# ----------------------------------------
# B) Broad PRODUCT snapshot with pagination
# ----------------------------------------
if COLLECTION_HANDLE:
    print(f"Fetching ALL products from collection handle: {COLLECTION_HANDLE} (paginated)")
    products_q = """
    query ProductsFromCollection($handle:String!, $pageSize:Int!, $cursor:String){
      collectionByHandle(handle: $handle) {
        title
        handle
        products(first: $pageSize, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              id
              title
              handle
              status
              publishedAt
              productType
              tags
              onlineStoreUrl

              # ✅ ADD THESE
              descriptionHtml
              seo {
                title
                description
              }

              featuredImage { url altText }
              images(first: 10) { edges { node { url altText } } }
              collections(first: 5) { edges { node { handle title } } }
              options { id name values }
              metafields(first: 50) {
                edges { node { namespace key type value } }
              }
              variants(first: 50) {
                edges {
                  node {
                    id
                    title
                    sku
                    inventoryItem { id }   # <— Add this line to expose inventory_item_id
                    availableForSale
                    price
                    compareAtPrice
                    selectedOptions { name value }
                    metafields(first: 50) {
                      edges { node { namespace key type value } }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    """


    all_nodes = []
    cursor = None
    page = 0
    while True:
        resp = gql(products_q, {"handle": COLLECTION_HANDLE, "pageSize": 50, "cursor": cursor})
        col = resp.get("data", {}).get("collectionByHandle") or {}
        products = col.get("products", {})
        edges = products.get("edges", []) or []
        all_nodes.extend([e["node"] for e in edges])

        pageInfo = products.get("pageInfo", {}) or {}
        hasNext = pageInfo.get("hasNextPage")
        cursor = pageInfo.get("endCursor")
        page += 1
        print(f"  page {page}: +{len(edges)} (total {len(all_nodes)})")
        if not hasNext:
            break

    products_data = {"data": {"collectionByHandle": {"handle": COLLECTION_HANDLE, "nodes": all_nodes}}}

    # ✅ Save and print summary here
    save("ingestion/products_full.json", products_data)
    print(f"Products fetched: {len(products_data['data']['collectionByHandle']['nodes'])}")

else:
    # Fallback: non-collection fetch (you can leave this as-is or comment out)
    ...
# --- Wrap everything in a callable function ---

def run_discovery():
    """Perform full Shopify discovery and save schema + product data."""
    load_dotenv()

    # Save schema first
    schema_definitions = {"product": defs_prod, "variant": defs_var}
    save("schema_definitions.json", schema_definitions)

    if COLLECTION_HANDLE:
        print(f"Fetching ALL products from collection handle: {COLLECTION_HANDLE} (paginated)")
        products_q = """
        query ProductsFromCollection($handle:String!, $pageSize:Int!, $cursor:String){
          collectionByHandle(handle: $handle) {
            products(first: $pageSize, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              edges {
                node {
                  id
                  title
                  handle
                  status
                  publishedAt

                  # ✅ NEW: pull long + meta descriptions
                  descriptionHtml
                  seo {
                    title
                    description
                  }

                  images(first: 10) {
                    edges {
                      node {
                        src: originalSrc
                        altText
                      }
                    }
                  }
                  variants(first: 50) {
                    edges {
                      node {
                        id
                        sku
                        inventoryItem { id }   # <— Add this line to expose inventory_item_id
                        price
                        compareAtPrice
                        selectedOptions { name value }
                        metafields(first: 30) {
                          edges {
                            node {
                              key
                              namespace
                              type
                              value
                            }
                          }
                        }
                      }
                    }
                  }
                  metafields(first: 30) {
                    edges {
                      node {
                        key
                        namespace
                        type
                        value
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """.strip()

        all_nodes = []
        cursor = None
        page = 0
        while True:
            resp = gql(products_q, {"handle": COLLECTION_HANDLE, "pageSize": 50, "cursor": cursor})
            col = resp.get("data", {}).get("collectionByHandle") or {}
            products = col.get("products", {}) or {}
            edges = products.get("edges", []) or []
            all_nodes.extend([e["node"] for e in edges])

            pageInfo = products.get("pageInfo", {}) or {}
            hasNext = pageInfo.get("hasNextPage")
            cursor = pageInfo.get("endCursor")
            page += 1
            print(f"  page {page}: +{len(edges)} (total {len(all_nodes)})")
            if not hasNext:
                break

        products_data = {"data": {"collectionByHandle": {"handle": COLLECTION_HANDLE, "nodes": all_nodes}}}
        save("products_full.json", products_data)
        print(f"Products fetched: {len(products_data['data']['collectionByHandle']['nodes'])}")
    else:
        print("No collection handle specified — skipping product fetch.")


# --- Entry point for both CLI and sync.py ---

def main():
    """Programmatic entrypoint so sync.py can run discovery."""
    run_discovery()


if __name__ == "__main__":
    main()

