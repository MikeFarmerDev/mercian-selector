"""
Hybrid Capsule Loader for Mercian Selector (v1.0)
- Loads static text capsules from /capsules/*.txt
- Builds dynamic Product Facts capsule from Excel dataset (StickSelection.xlsx)
- Provides a unified API for adapters/app to assemble AI payloads
"""

from __future__ import annotations
import json
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent  # repo root under /mnt/data
CAPSULES_DIR = BASE_DIR / "capsules"
# Config search: support root and /config/sync_map.json
CONFIG_CANDIDATES = [
    BASE_DIR / "sync_map.json",
    BASE_DIR / "config" / "sync_map.json",
]

# --------------------------
# Utility: config + hashing
# --------------------------

def _load_config() -> dict:
    for p in CONFIG_CANDIDATES:
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    raise FileNotFoundError(
        "Config file not found. Tried: "
        + " | ".join(str(p) for p in CONFIG_CANDIDATES)
    )

def hash_capsule(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# --------------------------
# Capsule I/O
# --------------------------

def load_capsule(name: str) -> str:
    """
    Reads a text capsule from /capsules/{name}.txt
    """
    path = CAPSULES_DIR / f"{name}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Capsule not found: {path}")
    return path.read_text(encoding="utf-8")


# --------------------------
# Dataset access
# --------------------------

@dataclass
class DatasetInfo:
    path: Path
    sheet: str
    primary_key: str


def _dataset_info() -> DatasetInfo:
    cfg = _load_config()
    excel = cfg.get("excel", {})
    path = excel.get("path", "data/StickSelection.xlsx")
    sheet = excel.get("sheet", "Sheet1")
    pk = excel.get("primary_key", "Product Code")
    return DatasetInfo(path=BASE_DIR / path, sheet=sheet, primary_key=pk)


def _load_dataset() -> 'pd.DataFrame':
    import pandas as pd
    info = _dataset_info()
    if not info.path.exists():
        raise FileNotFoundError(f"Excel dataset not found: {info.path}")
    df = pd.read_excel(info.path, sheet_name=info.sheet)
    # Normalize columns for robust access
    df.columns = [str(c).strip() for c in df.columns]
    return df


# --------------------------
# Product Facts builder
# --------------------------

PF_FIELDS = [
    # (display label, column name fallback list in order)
    ("Product Code", ["Product Code", "SKU", "Code"]),
    ("Title", ["Title", "Name"]),
    ("Player Type", ["Player Type"]),
    ("Playing Level", ["Playing Level", "Level"]),
    ("Carbon", ["Carbon", "Carbon %", "Carbon%"]),
    ("Bow", ["Bow"]),
    ("Length", ["Length"]),
    ("RRP", ["Full Price", "RRP", "Price (including VAT)"]),
    ("Available", ["available", "Available"]),
    ("Updated", ["updated_at_latest", "Updated At"]),
    # --- Rich play attributes for narrative comparisons ---
    ("Drag Flicking", ["Drag Flicking", "DragFlick", "DF", "Drag Flick"]),
    ("Aerial", ["Aerial", "Aerials", "Lifts"]),
    ("Reverse Stick Hitting", ["Reverse Stick Hitting", "Reverse", "Reverse Hitting"]),
    ("Power", ["Power"]),
    ("Touch and Control", ["Touch and Control", "Touch", "Control"]),
    ("3D", ["3D", "3D Skills"]),
    ("Hitting & Passing", ["Hitting & Passing", "Hitting and Passing", "Hitting", "Passing"]),
    ("Playing Level", ["Playing Level", "Level"]),
]
DESC_FIELDS = ["Description", "description_full", "Long Description", "Description Narrative"]


def _get_first(df_row, names, default: str = "") -> str:
    import pandas as pd
    for n in names:
        if n in df_row and pd.notna(df_row[n]):
            return str(df_row[n]).strip()
    return default


def build_product_facts(product_codes: List[str], top_n: Optional[int] = None) -> str:
    """
    Builds the Product Facts capsule text for a given ordered list of Product Codes.
    Reads StickSelection.xlsx and formats a deterministic, audit-friendly block.
    """
    if top_n is not None:
        product_codes = product_codes[:top_n]

    df = _load_dataset()
    pk = _dataset_info().primary_key

    # Preserve requested order
    df_idx = {str(code): i for i, code in enumerate(product_codes)}
    subset = df[df[pk].astype(str).isin(product_codes)].copy()
    subset["__order"] = subset[pk].astype(str).map(df_idx)
    subset = subset.sort_values("__order", kind="stable")

    sections = []
    for i, (_, row) in enumerate(subset.iterrows(), start=1):
        # header line parts
        title = _get_first(row, ["Title", "Name"], default="(No Title)")
        length = _get_first(row, ["Length"], default="")
        header = f"#{i} {title}" + (f" ({length})" if length else "")

        # meta line
        meta_parts = []
        for label, cols in PF_FIELDS:
            val = _get_first(row, cols, default="")
            if val != "":
                meta_parts.append(f"{label} {val}")
        meta_line = " · ".join(meta_parts)

        # description
        desc = _get_first(row, DESC_FIELDS, default="")

        block = []
        block.append(header)
        if meta_line:
            block.append(meta_line)
        if desc:
            block.append(f"Description (verbatim): {desc}")
        sections.append("\n".join(block))

    header = "Product Facts Capsule — Source: StickSelection.xlsx (verbatim descriptions)\n"
    body = "\n\n".join(sections) if sections else "(No matching products)"
    text = header + body
    return text


# --------------------------
# Capsule set assembly
# --------------------------

def get_capsules_for_skus(product_codes: List[str], requirement_text: Optional[str] = None, top_n: Optional[int] = 3) -> Dict[str, str]:
    """
    Returns a dict of all capsule texts needed for AI payload assembly.
    - product_codes: ordered list from ranked selection
    - requirement_text: optional override; if None, reads /capsules/requirement.txt
    - top_n: limit number of product sections (default 3)
    """
    capsules = {}
    capsules["brand"] = load_capsule("brand")
    capsules["business_logic"] = load_capsule("business_logic")
    capsules["bow"] = load_capsule("bow")
    capsules["brief"] = load_capsule("brief")
    capsules["requirement"] = requirement_text if requirement_text is not None else load_capsule("requirement")
    capsules["product_facts"] = build_product_facts(product_codes, top_n=top_n)
    return capsules


# --------------------------
# Convenience: payload stub
# --------------------------

def build_ai_payload_from_capsules(capsules: Dict[str, str]) -> dict:
    """
    Constructs an OpenAI Chat Completions payload from capsule texts.
    (Adapters/app may further modify model params as needed.)
    """
    system_msg = capsules["brief"]
    user_msg = (
        "Brand Capsule:\n" + capsules["brand"] + "\n\n" +
        "Requirement Capsule:\n" + capsules["requirement"] + "\n\n" +
        "Business Logic Capsule:\n" + capsules["business_logic"] + "\n\n" +
        "Product Facts Capsule:\n" + capsules["product_facts"] + "\n\n" +
        "Bow Capsule:\n" + capsules["bow"] + "\n\n" +
        "Please generate the player-facing recommendation text."
    )
    return {
        "model": "gpt-5",
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ]
    }
