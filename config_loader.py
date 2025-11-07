"""
config_loader.py
Reads the Mercian Stick Selector configuration workbook (Excel) and returns a structured dict.
"""

import os
import pandas as pd
from typing import Any, Dict

# Default path (local dev)
BASE_DIR = os.path.dirname(__file__)
DEFAULT_PATH = os.path.join(BASE_DIR, "data", "Selector_Config_Template_v1.0.xlsx")

def load_excel_config(path: str = None) -> Dict[str, Any]:
    """
    Loads the Excel configuration and converts each sheet to a dict or DataFrame.
    """
    path = path or os.environ.get("SELECTOR_CONFIG_PATH", DEFAULT_PATH)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Selector config not found at: {path}")

    try:
        # Read all sheets
        xl = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    except Exception as e:
        raise RuntimeError(f"Failed to read Excel config: {e}")

    # --- Basic validation ---
    required = ["Meta", "Traits", "Context", "CapsAndBlend"]
    missing = [r for r in required if r not in xl]
    if missing:
        raise KeyError(f"Missing required sheets: {missing}")

    # --- Extract into usable format ---
    config = {
        "meta": xl["Meta"].iloc[0].to_dict(),
        "traits": xl["Traits"].to_dict(orient="records"),
        "context": xl["Context"].to_dict(orient="records"),
        "bands": xl.get("Bands", pd.DataFrame()).to_dict(orient="records"),
        "caps": xl["CapsAndBlend"].set_index("key")["value"].to_dict(),
        "bow_rules": xl.get("BowRules", pd.DataFrame()).to_dict(orient="records"),
        "contrast_rules": xl.get("ContrastRules", pd.DataFrame()).to_dict(orient="records"),
        "eligibility": xl.get("Eligibility", pd.DataFrame()).to_dict(orient="records"),
        "rationale": xl.get("Rationale", pd.DataFrame()).to_dict(orient="records"),
        "colmap": xl.get("ColumnMap", pd.DataFrame()).to_dict(orient="records"),
    }

    config["meta"]["loaded_from"] = path
    return config


def print_summary(cfg: Dict[str, Any]) -> None:
    """
    Nicely prints a high-level summary of loaded configuration for diagnostics.
    """
    meta = cfg.get("meta", {})
    print("\n=== Selector Configuration Summary ===")
    print(f"Version: {meta.get('config_version')}")
    print(f"Last Updated: {meta.get('last_updated')}")
    print(f"Author: {meta.get('author')}")
    print(f"Loaded from: {meta.get('loaded_from')}")
    print(f"Traits loaded: {len(cfg.get('traits', []))}")
    print(f"Context rules: {len(cfg.get('context', []))}")
    print(f"Bow rules: {len(cfg.get('bow_rules', []))}")
    print("=====================================\n")


if __name__ == "__main__":
    cfg = load_excel_config()
    print_summary(cfg)

