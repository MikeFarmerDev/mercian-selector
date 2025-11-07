
"""
domain.adapters
----------------
Adapters translate core selection data into context-specific outputs.

Two main classes:
1. WildcardAdapter – decides which secondary product to highlight.
2. RationaleAdapter – produces a short narrative explaining the results.
"""

from typing import Any, Dict, List, Optional
import pandas as pd
import time
import os

# Optional OpenAI import (deferred for flexibility)
try:
    from rationale import generate_rationale
except ImportError:
    generate_rationale = None

# --- Capsule loader imports (capsule-driven AI payload) ---
from domain.capsules_loader import (
    get_capsules_for_skus,
    build_ai_payload_from_capsules,
    hash_capsule,
)

# --------------------------------------------------------------------
# Base class
# --------------------------------------------------------------------

class BaseAdapter:
    """Common adapter interface with timing + metadata logging."""
    name: str = "BaseAdapter"

    def __init__(self):
        self.metadata: Dict[str, Any] = {}

    def get(self, *args, **kwargs) -> Any:
        raise NotImplementedError("Subclasses must implement get()")

    def _start(self):
        self._t0 = time.perf_counter()

    def _stop(self):
        if hasattr(self, "_t0"):
            self.metadata["latency_ms"] = round((time.perf_counter() - self._t0) * 1000.0, 2)


# --------------------------------------------------------------------
# WildcardAdapter
# --------------------------------------------------------------------

class WildcardAdapter(BaseAdapter):
    """
    Selects a single 'wildcard' product from a ranked DataFrame.

    Inputs:
        - ranked_df: pd.DataFrame (scored and sorted)
        - primaries: list of selected primary rows (dicts)
        - profile: player profile dict
    Output:
        - dict representing chosen wildcard (or None)
    """
    name = "WildcardAdapter"

    def get(self, ranked_df: pd.DataFrame, primaries: List[Dict], profile: Dict) -> Optional[Dict]:
        self._start()
        wildcard = None

        # Defensive copy
        df = ranked_df.copy()
        primary_codes = [p["Product Code"] for p in primaries if "Product Code" in p]

        for _, row in df.iterrows():
            code = row.get("Product Code")
            if code in primary_codes:
                continue

            bow = str(row.get("Bow", "")).strip()
            carbon = float(row.get("Carbon", 0))
            price = float(row.get("Full Price", 0))

            price_ok = True
            if profile.get("budget"):
                price_ok = price <= (float(profile["budget"]) * 1.20)

            # Select a wildcard that differs from primaries in Bow or Carbon
            if price_ok and (
                not primaries
                or bow != str(primaries[0].get("Bow"))
                or carbon != float(primaries[0].get("Carbon", 0))
            ):
                wildcard = row.to_dict()
                break

        self._stop()
        self.metadata["found"] = bool(wildcard)
        self.metadata["primary_count"] = len(primaries)
        return wildcard


# --------------------------------------------------------------------
# RationaleAdapter
# --------------------------------------------------------------------

class RationaleAdapter(BaseAdapter):
    """
    Generates or retrieves rationale text for recommendations.

    Inputs:
        - profile: player profile dict
        - primaries: list of primary sticks
        - wildcard: optional wildcard stick
    Output:
        - dict: { "summary": str, "bullets": list[str] }
    """
    name = "RationaleAdapter"

    def get(self, profile: Dict, primaries: List[Dict], wildcard: Optional[Dict]) -> Dict[str, Any]:
        self._start()

        result: Dict[str, Any] = {"summary": "", "bullets": []}
        fallback_reason = None

        # --- Inject actual bow context for accuracy in the AI prompt ---
        p1_bow = (primaries[0].get("Bow","") if primaries else "").strip()
        p2_bow = (primaries[1].get("Bow","") if len(primaries) > 1 else "").strip()
        profile_with_bows = dict(profile)
        profile_with_bows["_p1_bow"] = p1_bow
        profile_with_bows["_p2_bow"] = p2_bow


        # Try OpenAI rationale if available and enabled
        # Restrict narrative vocabulary to bows actually present in the selected sticks
        bows_present = []
        for s in (primaries or [])[:2]:
            b = (s.get("Bow") or "").strip()
            if b and b not in bows_present:
                bows_present.append(b)
        if wildcard:
            b = (wildcard.get("Bow") or "").strip()
            if b and b not in bows_present:
                bows_present.append(b)

        if generate_rationale:
            try:
                result = generate_rationale(profile_with_bows, primaries, wildcard, allowed_bows=bows_present) or result


            except Exception as e:
                fallback_reason = f"AI call failed: {str(e)}"
        else:
            fallback_reason = "OpenAI rationale generator unavailable."

        # Fallback: local rule-based rationale
        if not result.get("summary"):
            summary = self._rule_based_summary(profile, primaries, wildcard)
            result = {"summary": summary, "bullets": []}
            if fallback_reason:
                result["bullets"] = [fallback_reason]

        self._stop()
        result["adapter_meta"] = self.metadata
        return result

    # ----------------------------------------------------------------
    # Simple local rule-based rationale (when AI unavailable)
    # ----------------------------------------------------------------
    def _rule_based_summary(
        self, profile: Dict, primaries: List[Dict], wildcard: Optional[Dict]
    ) -> str:
        level = profile.get("journey", 0)
        pri_names = [p.get("Description", "a stick") for p in primaries]
        bow = (primaries[0].get("Bow") if primaries else "Standard")

        summary = f"Recommended {', '.join(pri_names)} for a level {level} player preferring the {bow} bow."
        if wildcard:
            summary += f" The wildcard offers an alternative {wildcard.get('Bow','')} bow."
        return summary


# --------------------------------------------------------------------
# Adapter registry / factory
# --------------------------------------------------------------------

def assemble_capsule_payload(
    profile: Dict,
    primaries: List[Dict],
    wildcard: Optional[Dict] = None,
    top_n: int = 3,
    requirement_text: Optional[str] = None,
) -> Dict:
    """
    Build the OpenAI payload using the capsule system.
    - Uses primaries (ranked selections) to generate dynamic Product Facts.
    - requirement_text: pass explicit text if you want to override requirement.txt.
    Returns: dict ready for openai.chat.completions.create(**payload)
    """
    # Ordered product codes from selected primaries
    product_codes: List[str] = []
    for p in primaries:
        code = (p.get("Product Code") or "").strip()
        if code:
            product_codes.append(code)

    # Get capsule texts (brand, logic, bow, brief, requirement, product_facts)
    capsules = get_capsules_for_skus(
        product_codes=product_codes,
        requirement_text=requirement_text,
        top_n=top_n,
    )

    # Optional: attach capsule hashes for audit (caller can log these)
    capsule_hashes = {k: hash_capsule(v) for k, v in capsules.items()}

    payload = build_ai_payload_from_capsules(capsules)
    # Stash audit meta so caller can persist in logs if desired
    payload["_capsule_hashes"] = capsule_hashes
    payload["_capsule_meta"] = {
        "top_n": top_n,
        "product_codes": product_codes,
        "has_wildcard": bool(wildcard),
    }
    return payload


ADAPTERS = {
    "wildcard": WildcardAdapter,
    "rationale": RationaleAdapter,
}

def get_adapter(name: str) -> BaseAdapter:
    """Return adapter instance by name ('wildcard' or 'rationale')."""
    cls = ADAPTERS.get(name.lower())
    if not cls:
        raise ValueError(f"Unknown adapter: {name}")
    return cls()
