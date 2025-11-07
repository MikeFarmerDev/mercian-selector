import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[0]
EXCEL_PATH = BASE_DIR / "data" / "StickSelection.xlsx"
SHEET_NAME = "Sheet1"

def load_excel_config(path=EXCEL_PATH, sheet=SHEET_NAME):
    df = pd.read_excel(path, sheet_name=sheet)

    # --- Shopify is master for visibility ---
    def _true(v):
        try:
            # handle numeric or boolean 1s
            if pd.notna(v) and float(v) == 1.0:
                return True
        except Exception:
            pass
        return str(v).strip().upper() in ("TRUE", "1", "YES", "Y")

    # Preferred boolean column
    if "Shopify Active" in df.columns:
        active_mask = df["Shopify Active"].apply(_true)
        if active_mask.any():
            df = df[active_mask]
            return df

    # Fallback to raw status text
    if "Shopify Status" in df.columns:
        df = df[df["Shopify Status"].astype(str).str.upper() == "ACTIVE"]

    return df
