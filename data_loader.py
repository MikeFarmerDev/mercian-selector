import os
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[0]

EXCEL_PATH = BASE_DIR / "data" / "StickSelection.xlsx"
EU_EXCEL_PATH = BASE_DIR / "data" / "StickSelection_EU.xlsx"
AU_EXCEL_PATH = BASE_DIR / "data" / "StickSelection_AU.xlsx"

REGION_ENV_VAR = "SELECTOR_REGION"
DEFAULT_REGION = "GLOBAL"

SHEET_NAME = "Sheet1"


# ---------------------------------------------------------------------------
# Region-aware Excel loader
# ---------------------------------------------------------------------------

def load_excel_config(path=None, sheet=SHEET_NAME):
    """
    Load the stick selection Excel file.

    If `path` is not provided, the function will:
    - Read SELECTOR_REGION from environment
    - Default to GLOBAL
    - Map region -> matching Excel file
    - Fall back safely to GLOBAL if missing
    """

    # If no explicit path was given, region decides
    if path is None:
        region = os.getenv(REGION_ENV_VAR, DEFAULT_REGION)
        region = (region or DEFAULT_REGION).strip().upper() or DEFAULT_REGION

        if region == "EU":
            path = EU_EXCEL_PATH
        elif region == "AU":
            # If AU file isn’t present yet, fall back to GLOBAL safely
            if AU_EXCEL_PATH.exists():
                path = AU_EXCEL_PATH
            else:
                path = EXCEL_PATH
        else:
            path = EXCEL_PATH

    df = pd.read_excel(path, sheet_name=sheet)
    return df


# ---------------------------------------------------------------------------
# Explicit wrappers (unchanged)
# ---------------------------------------------------------------------------

def load_global_config():
    """Load the Global stick selection sheet explicitly."""
    return load_excel_config(path=EXCEL_PATH, sheet=SHEET_NAME)


def load_eu_config():
    """Load the EU stick selection sheet explicitly."""
    return load_excel_config(path=EU_EXCEL_PATH, sheet=SHEET_NAME)


def load_au_config():
    """Load the AU stick selection sheet explicitly (if present)."""
    if AU_EXCEL_PATH.exists():
        return load_excel_config(path=AU_EXCEL_PATH, sheet=SHEET_NAME)
    else:
        return load_excel_config(path=EXCEL_PATH, sheet=SHEET_NAME)
