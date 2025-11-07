from . import Frame, Profile
import numpy as np
import pandas as pd

TIER_ORDER = ["Genesis", "Evolution", "Intermediate", "Performance", "Elite"]

def allowed_tiers_for(journey) -> list[str]:
    """
    Accepts either a brand label ('Genesis'/'Evolution'/'Elite') or a numeric skill (0–10).
    Returns the allowed tier list used for gating.
    """
    # Brand labels
    if isinstance(journey, str):
        j = journey.strip().lower()
        if j == "genesis":
            return ["Genesis"]
        if j == "evolution":
            return ["Genesis", "Evolution"]
        if j in {"intermediate", "performance"}:
            return ["Evolution", "Intermediate", "Performance"]
        if j == "elite":
            return TIER_ORDER[:]
    # Numeric fallback (skill 0–10)
    try:
        s = float(journey)
    except Exception:
        s = 0.0
    if s <= 3:
        return ["Genesis"]
    if s <= 6:
        return ["Genesis", "Evolution"]
    if s <= 8:
        return ["Evolution", "Intermediate", "Performance"]
    return TIER_ORDER[:]


def tier_sanity(df: Frame) -> Frame:
    if "Playing Level" in df.columns:
        return df[df["Playing Level"].isin(TIER_ORDER)]
    return df

def apply_tier_gate(df: Frame, allowed: list[str]) -> Frame:
    return df[df["Playing Level"].isin(allowed)]

def apply_hard_filters(df: Frame, profile: Profile, allowed: list[str]) -> Frame:
    results = df.copy()
    results = tier_sanity(results)
    results = apply_tier_gate(results, allowed)

    # Tier gate (match allowed journey tiers)
    if "Playing Level" in results.columns:
        results = results[results["Playing Level"].isin(allowed)]

    # Price ceiling with 5% grace (hard filter)
    budget = float(profile.get("budget") or 0)
    if budget and "Full Price" in results.columns:
        results = results[results["Full Price"].astype(float) <= 1.05 * budget]

    # Length ±0.5"
    if profile.get("length") is not None and "Length" in results.columns:
        results = results[np.isclose(results["Length"].astype(float), profile["length"], atol=0.5)]

    # Player Type exact (case-insensitive)
    if "Player Type" in results.columns and profile.get("player_type"):
        results = results[results["Player Type"].str.lower() == str(profile["player_type"]).lower()]

    return results


