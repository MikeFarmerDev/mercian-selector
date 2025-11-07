from . import Profile, Row, Frame
import pandas as pd
from pandas import Series

# --- helpers -----------------------------------------------------------------

def _num(row: Row, col: str) -> float:
    return float(row[col]) if col in row and pd.notna(row[col]) else 0.0

def _norm01(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


# --- core scoring -------------------------------------------------------------

def score_row(row: Series, profile: Profile, prefer_bow: bool, preferred_bow: str, config=None) -> float:
    """
    Returns a higher-is-better score for a single stick row against the user's profile.
    Soft preferences only (hard gates are handled in filters/fallbacks).
    """
    score = 0.0

    bow_raw = str(row.get("Bow", "")).strip().lower()
    # normalise family names used in bias logic
    if bow_raw in {"standard", "standard bend"}:
        bow_fam = "mid"
    elif bow_raw == "dsh":
        bow_fam = "xtreme"      # behaves like extreme-low for lift/specialist head
    else:
        bow_fam = bow_raw

    carbon = _num(row, "Carbon")
    price  = _num(row, "Full Price")
    power_attr = _num(row, "Power")
    touch_attr = _num(row, "Touch and Control")
    aerial_attr = _num(row, "Aerial")
    # Dragflick attribute may not exist in the sheet; treat missing as 0.
    df_attr = _num(row, "Drag Flicking") if "Drag Flicking" in row else 0.0

    # --- 1) Affordability shaping: prefer near-ceiling (not the cheapest)
    budget = float(profile.get("budget", 0) or 0)
    if budget and price:
        u = max(0.0, min(1.0, price / budget))   # 0=free, 1=at ceiling
        score += 0.35 * (u ** 1.5)               # concave preference near ceiling

    # --- 2) Minimum spec by journey (keep low-end out at higher journeys)
    journey = str(profile.get("journey", "")).lower()
    if journey == "elite" and carbon < 70:
        score -= 0.6
    elif journey == "evolution" and carbon < 40:
        score -= 0.3

    # --- 3) User-declared preferred bow (very soft)
    if prefer_bow and preferred_bow:
        if bow_raw == preferred_bow:
            score += 0.15

    # --- 4) Aerial bias (Phase 1): late-bend family + aerial attribute
    # order: ultimate v2 > ultimate > xtreme > pro > mid
    aerial_intensity = max(0.0, (float(profile.get("aerials", 0)) - 7.0) / 3.0)  # only kicks in from 8–10
    if aerial_intensity > 0:
        order = ["ultimate v2", "ultimate", "xtreme", "pro", "mid"]
        try:
            rank_bias = (len(order) - order.index(bow_fam)) / len(order)  # 1.0 .. 0.2
        except ValueError:
            rank_bias = 0.0

        # combine model-family preference and sheet attribute
        score += 0.12 * aerial_intensity * rank_bias
        score += 0.12 * aerial_intensity * _norm01(aerial_attr, 0.0, 10.0)

    # --- 5) Dragflick bias (NEW): extreme/concave preference + DF attribute
    # order: xtreme > ultimate v2 > ultimate > pro > mid
    df_intensity = max(0.0, (float(profile.get("dragflick", 0)) - 7.0) / 3.0)  # only from 8–10
    if df_intensity > 0:
        order_df = ["xtreme", "ultimate v2", "ultimate", "pro", "mid"]
        try:
            rank_bias_df = (len(order_df) - order_df.index(bow_fam)) / len(order_df)
        except ValueError:
            rank_bias_df = 0.0
        score += 0.12 * df_intensity * rank_bias_df
        score += 0.12 * df_intensity * _norm01(df_attr, 0.0, 10.0)

    # --- 6) Light balance: power vs touch (kept gentle; main logic in filters/priorities)
    priority = str(profile.get("priority", "Both")).lower()
    if priority == "power":
        score += 0.08 * _norm01(power_attr, 0.0, 10.0)
    elif priority == "touch and control" or priority == "touch":
        score += 0.08 * _norm01(touch_attr, 0.0, 10.0)
    else:
        # Both: tiny blended nudge
        score += 0.04 * _norm01(power_attr, 0.0, 10.0)
        score += 0.04 * _norm01(touch_attr, 0.0, 10.0)

    return score


def rank(results: Frame, profile: Profile, config=None) -> Frame:
    """
    Scores and orders the candidate set (descending by Score, then by price asc).
    """
    preferred_bow = str(profile.get("preferred_bow", "") or "").lower()
    prefer_bow = preferred_bow in {"standard", "pro", "ultimate", "ultimate v2", "xtreme", "dsh", "znake", "mid"}

    scored = results.copy()
    if scored.empty:
        # ensure downstream doesn’t crash
        scored["Score"] = []
        return scored

    scored["Score"] = scored.apply(
        lambda r: score_row(r, profile, prefer_bow, preferred_bow, config=config),
        axis=1
    )
    ranked = scored.sort_values(["Score", "Full Price"], ascending=[False, True])
    return ranked
