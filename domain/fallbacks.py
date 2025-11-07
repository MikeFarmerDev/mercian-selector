from . import Frame, Profile
import numpy as np
import pandas as pd
from .filters import TIER_ORDER

def _cap_budget(df: Frame, profile: Profile, allowed_tiers: list[str], cap: float, length_atol: float) -> Frame:
    q = (df["Player Type"].str.lower() == profile["player_type"].lower()) & \
        (df["Playing Level"].isin(allowed_tiers))
    if profile["length"] is not None:
        q = q & (np.isclose(df["Length"].astype(float), profile["length"], atol=length_atol))
    tmp = df[q]
    if profile["budget"]:
        tmp = tmp[tmp["Full Price"].astype(float) <= cap]
    return tmp

def apply_fallbacks(df: Frame, initial: Frame, profile: Profile, allowed_tiers: list[str]) -> tuple[Frame, dict]:
    """
    Returns (results, info) where info includes counters and reason list.
    """
    counters = {"budget_relax": 0, "length_relax": 0, "tier_relax": 0, "final_cheapest": 0}
    reasons: list[str] = []
    results = initial

    if results.empty:
        results = _cap_budget(df, profile, allowed_tiers, profile["budget"] * 1.10 if profile["budget"] else 0, 0.5)
        if not results.empty:
            counters["budget_relax"] += 1
            reasons.append("budget(+10%)")

    if results.empty:
        results = _cap_budget(df, profile, allowed_tiers, profile["budget"] * 1.20 if profile["budget"] else 0, 0.5)
        if not results.empty:
            counters["budget_relax"] += 1
            reasons.append("budget(+20%)")

    if results.empty:
        # relax length to ±1.0"
        tmp = _cap_budget(df, profile, allowed_tiers, profile["budget"] * 1.20 if profile["budget"] else 0, 1.0)
        if not tmp.empty:
            results = tmp
            counters["length_relax"] += 1
            reasons.append("length(±1.0)")

    if results.empty:
        # open tiers upward one step
        allowed_idx = max([TIER_ORDER.index(t) for t in allowed_tiers]) if allowed_tiers else 0
        higher_tiers = TIER_ORDER[:min(len(TIER_ORDER), allowed_idx + 2)]
        tmp = _cap_budget(df, profile, higher_tiers, profile["budget"] * 1.20 if profile["budget"] else 0, 1.0)
        if not tmp.empty:
            results = tmp
            counters["tier_relax"] += 1
            reasons.append("tiers(↑1 step)")

    if results.empty:
        # Final fallback: cheapest 3 of the player's type
        results = df[df["Player Type"].str.lower() == profile["player_type"].lower()].sort_values("Full Price").head(3)
        counters["final_cheapest"] = len(results)
        reasons.append("final_cheapest(3)")

    info = {"relaxed": reasons, "counters": counters} if reasons else {}
    return results, info
