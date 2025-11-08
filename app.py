from config import should_generate_rationale, settings
from rationale import generate_rationale

# Capsule payload assembly (AI Core v1.0)
from domain.adapters import assemble_capsule_payload

from flask import Flask, request, jsonify, g
from flask_cors import CORS
import pandas as pd
import numpy as np
import os, time
import re

from config_loader import load_excel_config
CONFIG = load_excel_config()  # reads data/Selector_Config_*.xlsx

from data_loader import load_excel_config as load_dataset  # add this
df = load_dataset()  # add this

# [MOD] domain modules
from domain.filters import apply_hard_filters, allowed_tiers_for, TIER_ORDER
from domain.scoring import rank
from domain.fallbacks import apply_fallbacks

app = Flask(__name__)

# CORS: allow local dev + local WordPress
ALLOWED_ORIGINS = [
    "http://127.0.0.1",
    "http://127.0.0.1:8000",
    "http://localhost",
    "http://localhost:8000",
    "http://aissess-test.local",
    "http://aissess-test.local:5000",
    "http://192.168.0.19",
]

CORS(app, resources={
    r"/api/*": {
        "origins": ALLOWED_ORIGINS,
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
    }
})

print(">>> Dataset Loaded:", df.shape)

PLAYER_TYPE_MAP = {
    "outdoor player": "Outdoor Player",
    "outdoor goalkeeper": "Outdoor Goalkeeper",
    "indoor player": "Indoor Player",
    "indoor goalkeeper": "Indoor Goalkeeper",
}
PRIORITY_MAP = {
    "touch": "Touch and Control",
    "power": "Power",
    "both": "Both",
    "unknown": "Not Sure"
}

def parse_float(v, default=None):
    try:
        return float(str(v).replace("£", "").strip())
    except:
        return default

# [MOD] latency logging middleware
@app.before_request
def _start_timer():
    g._t0 = time.perf_counter()

@app.after_request
def _finish_timer(resp):
    try:
        dt = (time.perf_counter() - getattr(g, "_t0", time.perf_counter())) * 1000.0
        resp.headers["X-Response-Time-ms"] = f"{dt:.1f}"
    except Exception:
        pass
    return resp

@app.get("/")
def home():
    from flask import render_template
    return render_template("index.html")


# [MOD] healthz for probes
@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok", "dataset_rows": int(df.shape[0])}), 200

# Back-compat for your earlier check
@app.get("/health")
def health():
    return "ok", 200


@app.route("/api/recommend", methods=["POST", "OPTIONS"])
def recommend():
    global df
    # Handle browser preflight CORS request
    if request.method == "OPTIONS":
        return "", 200  # 200 OK works best for preflight

    payload = request.get_json(silent=True) or {}

    required = ["skill", "attack", "midfield", "defence", "budget",
                "dragflick", "aerials", "category", "priority", "bow", "length"]

    print("DEBUG incoming payload:", payload)
    missing = [k for k in required if k != "length" and payload.get(k) in (None, "", [])]

    if missing:
        return jsonify({
            "ok": False,
            "error": "Missing fields",
            "missing": missing
        }), 400

    profile = {
        "skill": float(payload.get("skill", 0)),
        "attack": float(payload.get("attack", 0)),
        "midfield": float(payload.get("midfield", 0)),
        "defence": float(payload.get("defence", 0)),
        "budget": float(payload.get("budget", 0)),
        "dragflick": float(payload.get("dragflick", 0)),
        "aerials": float(payload.get("aerials", 0)),
        "category": (payload.get("category") or "").strip().lower(),
        "priority": (payload.get("priority") or "").strip(),
        "bow": (payload.get("bow") or "").strip().lower(),
        "length": str(payload.get("length", "")).strip(),
    }

    # Run your main selection logic
    # (first ranking uses current df – final ranking uses reloaded df below)

    # Run filters & scoring
    ranked = rank(df, profile, config=CONFIG)

    # Build capsule payload for AI using the ranked primaries (top 3 by default)
    primaries = []
    if ranked is not None and not ranked.empty:
        primaries = ranked.head(3).to_dict(orient="records")

    # If you want to override requirement.txt (optional), set requirement_text here.
    requirement_text = None

    # Capsule payload — ready for openai.chat.completions.create(**payload)
    capsule_payload = assemble_capsule_payload(
        profile=profile,
        primaries=primaries,
        top_n=3,
        requirement_text=requirement_text,
    )

    # NOTE:
    # - capsule_payload contains:
    #   - model/messages for OpenAI (system=brief; user=brand/requirement/logic/product_facts/bow)
    #   - _capsule_hashes (SHA256 per capsule) for audit
    #   - _capsule_meta (product_codes, top_n, etc.)
    #
    # You can now:
    #   response = openai.chat.completions.create(**capsule_payload)
    #   narrative = response.choices[0].message["content"]
    #   ...then pass `narrative` to your renderer / JSON response


    # make sure we have results
    if ranked is None or ranked.empty:
        return jsonify({"ok": False, "error": "No sticks matched your criteria"}), 200

    # take the top 3 rows (initial view)
    top3 = ranked.head(3).to_dict(orient="records")

    # safe access
    primary = top3[0] if len(top3) > 0 else None
    secondary = top3[1] if len(top3) > 1 else None
    wildcard = top3[2] if len(top3) > 2 else None

    # --- Derivations / normalisation ---
    # Brand-aligned journey tiers
    s = profile["skill"]
    if s <= 3:
        profile["journey"] = "Genesis"
    elif s <= 7:
        profile["journey"] = "Evolution"
    else:
        profile["journey"] = "Elite"

    # Player type from category (matches dataset)
    profile["player_type"] = PLAYER_TYPE_MAP.get(profile["category"], "Outdoor Player")

    # Map priority and bow for internal consistency
    profile["preferred_bow"] = profile["bow"]
    profile["priority"] = PRIORITY_MAP.get(profile["priority"].lower(), "Both")

    # Normalise length → float or None
    try:
        profile["length"] = float(profile["length"])
    except:
        profile["length"] = None

    # always reload latest dataset so new Image URL / Product URL are used
    df = load_dataset()

    # [MOD] tier gating via domain
    allowed_tiers = allowed_tiers_for(profile["journey"])


    # [MOD] apply hard filters
    hard = apply_hard_filters(df, profile, allowed_tiers)
    hard_empty = hard.empty

    # [MOD] fallbacks with counters + reasons
    results, fallback_info = apply_fallbacks(df, hard, profile, allowed_tiers)

    # [MOD] scoring via domain
    ranked = rank(results, profile, config=CONFIG).reset_index(drop=True)
    print("DEBUG length in profile:", profile.get("length"), type(profile.get("length")))
    # NEW: if user picked a length, keep only rows with that exact length (fallback = keep all)
    if profile.get("length") and "Length" in ranked.columns:
        exact = ranked[ranked["Length"].astype(float) == float(profile["length"])]
        if not exact.empty:
            ranked = exact.reset_index(drop=True)
    # --- Phase 1: Suitability curve (0–1) + Peak selector with DF/Aerial nudges

    primaries_list, wildcard = [], None
    if not ranked.empty:
        smin = float(ranked["Score"].min())
        smax = float(ranked["Score"].max())
        rng = (smax - smin) if (smax > smin) else 1.0
        ranked["Prob"] = (ranked["Score"] - smin) / rng

        def bow_fam(bow_raw: str) -> str:
            b = (bow_raw or "").strip().lower()
            if b in {"standard", "standard bend"}: return "mid"
            if b == "dsh": return "xtreme"
            return b

        def is_solid_core(row) -> bool:
            desc = str(row.get("Description","")).upper()
            code = str(row.get("Product Code","")).upper()
            return (" SC" in desc) or ("SC " in desc) or (" CK" in code) or ("CK" in code and "SC" in code)

        # Intensities (match scoring.py thresholds)
        df_intensity = max(0.0, (float(profile.get("dragflick", 0)) - 7.0) / 3.0)
        ar_intensity = max(0.0, (float(profile.get("aerials",   0)) - 7.0) / 3.0)
        budget = float(profile.get("budget") or 0)

        # Helpers to pick best by predicate
        def pick_best(pred):
            for _, r in ranked.iterrows():
                if pred(r): return r.to_dict()
            return None

        # Default P1: top row
        p1 = ranked.iloc[0].to_dict()

        # Dragflick-first override: prefer Xtreme as P1 when DF is high (>=8)
        if df_intensity > 0.0:
            p1_df = pick_best(lambda r: bow_fam(r.get("Bow","")) == "xtreme")
            if p1_df: p1 = p1_df

        # Aerials-first override: prefer Ultimate V2 as P1 when Aerials is high (>=8) and DF not high
        if ar_intensity > 0.0 and df_intensity == 0.0:
            p1_ar = pick_best(lambda r: bow_fam(r.get("Bow","")) == "ultimate v2")
            if p1_ar: p1 = p1_ar

        # Primary 2: near-top prob AND contrasts p1 (bow or ≥10 carbon)
        p2 = None
        top_prob   = float(p1.get("Prob", 1.0))
        top_bow    = bow_fam(p1.get("Bow",""))
        top_carbon = float(p1.get("Carbon", 0))

        for _, r in ranked.iterrows():
            if r.to_dict() == p1:
                continue
            prob = float(r["Prob"])
            bow_diff = bow_fam(r.get("Bow","")) != top_bow
            carbon_diff = abs(float(r.get("Carbon",0)) - top_carbon) >= 10.0
            near = prob >= max(0.0, top_prob - 0.10)

            # With DF high, try to prefer a Solid Core Ultimate V2 as P2 within budget
            if df_intensity > 0.0 and budget:
                if bow_fam(r.get("Bow","")) == "ultimate v2" and is_solid_core(r) and near:
                    p2 = r.to_dict()
                    break

            if near and (bow_diff or carbon_diff):
                p2 = r.to_dict()
                break

        if p2 is None and len(ranked) > 1:
            # fallback: the next best that isn't p1
            for _, r in ranked.iloc[1:].iterrows():
                if r.to_dict() != p1:
                    p2 = r.to_dict()
                    break

        # Wildcard: within ~15% of top, different bow to P1/P2, and NOT Solid Core
        if len(ranked) > 2:
            used_bows = {bow_fam(p1.get("Bow",""))}
            if p2: used_bows.add(bow_fam(p2.get("Bow","")))
            for _, r in ranked.iterrows():
                rd = r.to_dict()
                if rd == p1 or (p2 and rd == p2): 
                    continue
                prob = float(r.get("Prob", 0.0))
                bow_ok = bow_fam(r.get("Bow","")) not in used_bows
                if prob >= max(0.0, top_prob - 0.15) and bow_ok and not is_solid_core(r):
                    wildcard = rd
                    break

        primaries_list = [p1] + ([p2] if p2 else [])


    # Convert helper
    def as_dict(row_or_dict):
        d = row_or_dict if isinstance(row_or_dict, dict) else row_or_dict.to_dict()

        # Normalise numeric format
        if "Full Price" in d and isinstance(d["Full Price"], (int, float)):
            d["Full Price"] = f"{float(d['Full Price']):.2f}"

        # Map image + product URLs for HTML use
        if "Image URL" in d:
            d["image_url"] = d["Image URL"]
        if "Product URL" in d:
            d["product_url"] = d["Product URL"]

        return d


    primaries_out = [as_dict(p) for p in primaries_list] if primaries_list else []

    # --- Adapters (keep existing), but prefer our wildcard if we found one
    from domain.adapters import get_adapter

    wildcard_adapter = get_adapter("wildcard")
    wildcard_src = wildcard if isinstance(wildcard, dict) else wildcard_adapter.get(ranked, primaries_out, profile)
    wildcard_out = as_dict(wildcard_src) if isinstance(wildcard_src, dict) else None

    # ------------------------------------------------------------
    # RATIONALE (force single-paragraph, 120–180 words if too short)

    # ------------------------------------------------------------
    rationale_adapter = get_adapter("rationale")
    rationale = rationale_adapter.get(profile, primaries_out, wildcard_out)

    # --- ENHANCED OPENAI MERCIAN NARRATIVE ---
    def word_count(t):
        return len(str(t).split())

    adapter_text = ""
    adapter_had_bullets = False
    if isinstance(rationale, dict):
        adapter_text = rationale.get("summary") or rationale.get("text") or ""
        adapter_had_bullets = "bullets" in rationale

    needs_openai = (word_count(adapter_text) < 120) or adapter_had_bullets

    if needs_openai:
        # 1) Build rich context
        # Load brief + build Product Facts from Excel for the 3 selected sticks
        from domain.capsules_loader import load_capsule, build_product_facts
        _codes = [p.get("Product Code") for p in primaries_out if p.get("Product Code")]
        pf_text = build_product_facts(_codes, top_n=3)
        brief_text = load_capsule("brief")
        bow_text = load_capsule("bow")

        stick_blocks = []
        from rationale import PROMPT_SPEC_V1_0

        # Build player context string (you already collect this info earlier in the route)
        player_context = (
            f"journey={profile.get('journey')}, "
            f"attack={profile.get('attack')}, midfield={profile.get('midfield')}, "
            f"defence={profile.get('defence')}, aerials={profile.get('aerials')}, "
            f"dragflick={profile.get('dragflick')}, budget=£{profile.get('budget')} "
            f"({profile.get('category')}, priority={profile.get('priority')}, bow={profile.get('bow')})"
        )

        # Combine the master prompt spec with context and stick data
        stick_descriptions = "\n\n".join(stick_blocks)
        combined_prompt = f"{PROMPT_SPEC_V1_0}\n\nPLAYER PROFILE:\n{player_context}\n\nSTICKS:\n{stick_descriptions}"

        # Attach to profile so generate_rationale() can use it
        profile["_custom_prompt"] = combined_prompt


        context = (
            f"PLAYER_PROFILE: journey={profile.get('journey')}, "
            f"focus=attack:{profile.get('attack')}, aerials:{profile.get('aerials')}, "
            f"dragflick:{profile.get('dragflick')}, budget=£{profile.get('budget')}.\n\n"
            "STICKS:\n" + pf_text + "\n\n"
            "BOW DEFINITIONS:\n" + bow_text
        )

        prompt = (
            brief_text
        )

        combined_prompt = f"{prompt}\n\nCONTEXT:\n{context}"
        profile["_custom_prompt"] = combined_prompt
        raw_rationale = generate_rationale(profile, primaries_out, wildcard_out)

        # Normalise to string
        if isinstance(raw_rationale, dict):
            full_text = (
                raw_rationale.get("summary")
                or raw_rationale.get("text")
                or raw_rationale.get("message")
                or ""
            )
        else:
            full_text = str(raw_rationale or "")

        words = full_text.split()
        if len(words) > 500:
            full_text = " ".join(words[:500])

        # --- preserve punctuation and paragraph breaks for frontend display ---
        full_text = re.sub(r"[ \t]+", " ", full_text)  # tidy spaces, keep \n

        # build HTML paragraphs from \n
        paras = [p.strip() for p in full_text.split("\n") if p.strip()]
        full_text_html = "".join(f"<p>{p}</p>" for p in paras)

        # make sure it ends properly
        if full_text and not full_text.endswith((".", "!", "?")):
            full_text += "."

        rationale = {"summary": full_text, "summary_html": full_text_html}


    payload = {
        "ok": True,
        "profile": profile,
        "primaries": primaries_out,
        "wildcard": wildcard_out,
        "fallback": fallback_info,
        "rationale": rationale,
    }

    # --- LOGGING: record this event ---
    from domain.logger import log_event

    rationale_text = ""
    if isinstance(rationale, dict) and rationale.get("summary"):
        rationale_text = rationale["summary"][:200].replace("\n", " ")

    primaries_summary = ", ".join([p.get("Description", "Unknown") for p in primaries_out])

    wildcard_summary = (
        wildcard_out.get("Description", "None") if isinstance(wildcard_out, dict) else "None"
    )

    log_event({
        "journey": profile.get("journey"),
        "player_type": profile.get("player_type"),
        "budget": profile.get("budget"),
        "fallbacks": fallback_info,
        "adapter_latency_ms": rationale.get("adapter_meta", {}).get("latency_ms") if isinstance(rationale, dict) else None,
        "response_time_ms": request.headers.get("X-Response-Time-ms"),
        "status": "ok" if payload.get("ok") else "error",
        "rationale_summary": rationale_text,
        "primaries": primaries_summary,
        "wildcard": wildcard_summary,
    })

    import json
    from flask import Response
    import numpy as np

    # --- CLEAN NaN, inf, -inf from the payload recursively ---
    def clean_nan(obj):
        if isinstance(obj, dict):
            return {k: clean_nan(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_nan(v) for v in obj]
        elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
            return None
        else:
            return obj

    clean_payload = clean_nan(payload)

    safe_json = json.dumps(clean_payload, allow_nan=False)
    return Response(safe_json, status=200, mimetype="application/json")

@app.route("/demo")
def demo():
    from flask import send_file
    return send_file("demo.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
