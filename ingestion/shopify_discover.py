import os, json, requests, sys, argparse
from typing import Optional, Dict
from dotenv import load_dotenv

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

load_dotenv()

API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")
COLLECTION_HANDLE = os.getenv("STICK_COLLECTION_HANDLE")  # optional

VALID_STORES = {"global", "eu", "au"}


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
        # Should never hit this because of the VALID_STORES guard above.
        raise ValueError(f"Unhandled store '{store}'.")

    if not domain or not token:
        raise RuntimeError(f"Missing Shopify credentials for store '{store}'.")

    return {"domain": domain, "token": token}


def make_gql_client(store: str):
    """
    Build a GraphQL caller bound to the correct Shopify store for this run.
    """
    creds = _get_store_credentials(store)
    url = f"https://{creds['domain']}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": creds["token"],
    }

    def _gql(query: str, variables: Optional[Dict] = None) -> dict:
        r = requests.post(
            url,
            headers=headers,
            json={"query": query, "variables": variables or {}},
            timeout=30,
        )
        try:
            data = r.json()
        except Exception:
            print("Non-JSON response:", r.status_code, r.text[:500])
            raise
        if r.status_code != 200 or "errors" in data:
            print("GraphQL error/status:", r.status_code, json.dumps(data.get("errors"), indent=2))
        return data

    return _gql

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

# --- Wrap everything in a callable function ---

def run_discovery(store: str):
    """Perform full Shopify discovery for a specific store and save schema + product data."""
    store = store.lower().strip()
    if store not in VALID_STORES:
        raise ValueError(f"Unsupported store '{store}'. Expected one of {sorted(VALID_STORES)}.")

    print(f"=== Shopify discovery starting for store: {store} ===")
    gql = make_gql_client(store)

    # --- A) Metafield DEFINITION scan (per store) ---
    print("Scanning metafield DEFINITIONS…")
    defs_prod = gql(defs_product_q, {"first": 200})
    defs_var = gql(defs_variant_q, {"first": 200})
    schema_definitions = {"product": defs_prod, "variant": defs_var}
    save(f"schema_definitions_{store}.json", schema_definitions)

    # --- B) Broad PRODUCT snapshot with pagination ---
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
                  productType
                  tags
                  onlineStoreUrl

                  # ✅ ADD THESE
                  descriptionHtml
                  seo { title description }
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
        save(f"products_full_{store}.json", products_data)
        print(f"Products fetched for store '{store}': {len(products_data['data']['collectionByHandle']['nodes'])}")
    else:
        print(f"No collection handle specified — skipping product fetch for store '{store}'.")

    print(f"=== Shopify discovery complete for store: {store} ===")


# --- Entry point for both CLI and sync.py ---

def main(store: Optional[str] = None):
    """Programmatic entrypoint so sync.py or the CLI can run discovery."""
    # If called directly (CLI), parse the --store argument.
    if store is None:
        parser = argparse.ArgumentParser(description="Run Shopify discovery for a specific store.")
        parser.add_argument(
            "--store",
            required=True,
            choices=sorted(VALID_STORES),
            help="Which store to run discovery for (e.g. 'global', 'eu', 'au').",
        )
        args = parser.parse_args()
        store = args.store

    run_discovery(store)

if __name__ == "__main__":
    main()


