# rationale.py

from openai import OpenAI
from config import settings
import json
import re

# --- L2 diagnostics (enabled only when DIAGNOSTICS=1) ---
import os, time, hashlib

def _diag_log(label: str, content: str):
    if os.getenv("DIAGNOSTICS") != "1":
        return
    ts = time.strftime("%Y%m%d_%H%M%S")
    h  = hashlib.sha1(content.encode("utf-8")).hexdigest()[:6]
    os.makedirs("logs", exist_ok=True)
    path = f"logs/L2_{label}_{ts}_{h}.txt"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception:
        pass  # never block app flow

PROMPT_SPEC_V1_0 = """
### MERCIAN STICK SELECTOR – EXPERT NARRATIVE SPECIFICATION ###

Purpose:
Generate a single 120–180-word paragraph that sounds unmistakably Mercian — expert, composed, and emotionally intelligent — explaining why the recommended sticks fit the player’s profile.

Tone & Voice:
- Intelligent, British-engineered precision.
- Confident but never boastful; technical mastery expressed through calm authority.
- Write for players, not engineers — translate technology into what it feels like on the pitch.
- Use rhythm and vocabulary consistent with Mercian’s brand line: 'Helping every player play their best game.'

Core Brief:
You are Mercian’s lead design engineer explaining to a player why these sticks are right for them.
Describe how the materials, bow shapes, and construction choices express Mercian’s philosophy: power through feel, control through design, performance through understanding.

Each output must:
1. Begin with a bridge from the player’s stated focus (attack, aerials, drag-flicking, etc.) to the stick design.
2. Interpret R&D and materials in sensory terms — e.g. 'the polymeric core dampens vibration so control feels effortless even under pressure.'
3. Highlight innovation as deliberate craftsmanship — three years of development, vacuum-filled core, Toray carbon precision weave.
4. Reference balance, feel, and touch as the ultimate goal — not just power metrics.
5. Close with an emotionally resonant line linking performance to confidence: 'Because when every detail is tuned to your touch, you can win before you play.'

Data Inputs:
Stick title, bow type, carbon %, price, full description, key features, and player profile summary.

Prompt Template:
You are Mercian’s equipment expert.
Write ONE paragraph of 120–180 words in Mercian’s confident, UK-English tone.

Use the following data to explain why these sticks perfectly match the player’s profile.

PLAYER PROFILE:
{player_context}

STICKS:
{full_descriptions_with_bow_carbon_features}

Tone: intelligent, British-engineered, quietly confident.
Focus on how technology translates into feel, control, and confidence.
End with a single sentence beginning: 'Because when every detail is tuned to your touch…'
"""

# Compressed bow-knowledge stanza (static context)
BOW_KNOWLEDGE = """
Ultimate: aggressive low bow (~24.5mm @ lowest legal), enhances drag flicks, aerials, and 3D lifts via toe thinning and head sculpting.
Ultimate V2: same low bend as Ultimate but stiffened profile and slightly rebalanced head for stability and turnover accuracy.
Xtreme: same bend as Ultimate with 2mm concave shaft (sling-shot flicking/slapping), 4° face twist for reverse control, thinned handle feel.
Pro: ~23.5mm @ ~230mm (mid-low), thicker head + balance to sweet spot -> hitting/slapping power, cleaner turnover.
DSH: small concave face (within FIH 4mm) cushions ball, aids 3D control & lifted passes, head thicker -> slap power.
Mid: ~22mm @ ~300mm, straighter profile aligns hands to face -> accuracy for hitting/slapping, defensive distribution focus.
"""

s = settings()
client = OpenAI(api_key=s.openai_api_key)


def generate_rationale(profile, primaries, wildcard, allowed_bows=None):
    try:
        if not isinstance(profile, dict):
            raise TypeError(f"profile is {type(profile).__name__}, expected dict")
        print("L2_DIAG: ENTERED generate_rationale()")
        import sys; sys.stdout.flush()
        print("L2_DIAG profile_keys:", sorted(list(profile.keys())))
    except Exception as _e:
        print("L2_DIAG: ENTRY ERROR in generate_rationale ->", repr(_e))
        import sys; sys.stdout.flush()
        # fail safe into deterministic minimal result instead of crashing
        return {
            "summary": "System note: unable to read profile cleanly; returning deterministic rationale.",
            "bullets": [],
            "source": "deterministic",
            "meta": {"error": repr(_e)}
        }

    # --- If app.py provided a full custom prompt, use it directly ---
    if profile.get("_custom_prompt"):
        try:
            import json as _json
            messages = [
                {"role": "system", "content": "You are Mercian’s equipment expert."},
                {"role": "user", "content": profile["_custom_prompt"]},
            ]
            _diag_log("pre_ai", _json.dumps(messages, ensure_ascii=False, indent=2))
            print("L2_MARK reached OpenAI call (_custom_prompt path)")
        except Exception as _e:
            print("L2_DIAG custom_prompt prep error:", repr(_e))
            import sys; sys.stdout.flush()
            messages = [
                {"role": "system", "content": "You are Mercian’s equipment expert."},
                {"role": "user", "content": str(profile.get("_custom_prompt", ""))},
            ]

        import time  # local import is fine; keeps this change self-contained

        # Robust OpenAI call with retries + longer timeout + precise logs
        for _try in range(1, 4):
            try:
                t0 = time.time()
                resp = client.chat.completions.create(
                    model=s.model,
                    messages=messages,
                    max_tokens=s.max_tokens,
                    temperature=min(0.3, s.temperature),
                    timeout=max(s.request_timeout, 30),  # ensure ≥30s
                )
                dt_ms = int((time.time() - t0) * 1000)
                content = resp.choices[0].message.content.strip() if resp.choices else ""
                _diag_log("post_ai", f"ms={dt_ms} chars={len(content)} :: {content[:4000]}")
                _diag_log("rationale_source", "openai")
                return {
                    "summary": content,
                    "bullets": [],
                    "source": "openai",
                    "meta": {"ms": dt_ms, "chars": len(content)}
                }
            except Exception as e:
                _diag_log("post_ai_retry", f"try={_try} error={repr(e)}")
                time.sleep(0.8 * _try)

        # If we get here, OpenAI failed all tries; surface error explicitly
        return {"summary": "", "bullets": [], "source": "openai_error", "meta": {"error": "openai_call_failed"}}

        # If we get here, OpenAI failed all tries; fall through to deterministic path
        _diag_log("rationale_source", "deterministic_fallback")

        # fall through to legacy short-form prompt


    # --- Pull true bow context from injected fields (added by adapters) ---
    bow1 = profile.get("_p1_bow") or (primaries[0].get("Bow", "") if primaries else "")
    bow2 = profile.get("_p2_bow") or (primaries[1].get("Bow", "") if len(primaries) > 1 else "")
    wildcard_bow = wildcard.get("Bow", "") if wildcard else ""

    # Gather actual bow families from the selected items (ignore "", "None", "none")
    families = [b.strip() for b in [bow1, bow2, wildcard_bow] if b and str(b).strip().lower() != "none"]

    # If adapter passed allowed_bows, use the intersection; otherwise fall back to families
    if allowed_bows:
        allowed = [b.strip() for b in allowed_bows if b and str(b).strip().lower() != "none"]
        allowed = [b for b in allowed if b in families] or families
    else:
        allowed = families

    allowed_bows_line = ", ".join(dict.fromkeys(allowed))

    # Stick lengths present (use the requested length if provided)
    lengths_present = []
    if profile.get("length"):
        lengths_present = [str(profile.get("length"))]
    lengths_line = ", ".join(lengths_present) if lengths_present else "unspecified"

    # Family/length cardinality for strict constraints (derive from actual families, not 'allowed')
    same_family = len(dict.fromkeys(families)) == 1 and len(families) >= 1
    family_name = families[0] if same_family else ""
    same_length = len(lengths_present) <= 1  # treat single or UNSPECIFIED length as 'same'

    # Only show knowledge for the bows we actually selected
    bow_knowledge_subset = "\n".join(
        ln for ln in BOW_KNOWLEDGE.strip().splitlines()
        if ln.strip() and ln.split(":", 1)[0].strip() in dict.fromkeys(allowed)
    )
    print(f"RATIONALE MODE? same_family={same_family}, same_length={same_length}, families={families}")

    # Even if family/length align, we still want an AI narrative; do not return early.
    if same_family and same_length:
        print("L2_NOTE: identical family & length detected — proceeding to OpenAI narrative")
    # (no return here; flow continues into prompt building)

    prompt = f"""
CONSTRAINTS:
- Only use these bow names: {allowed_bows_line}. Do not use any other bow names or generic terms like 'mid-bow' or 'low bow'.
- Lengths present: {lengths_line}. If only one length is present, do NOT state or imply that one stick is longer/shorter.
- If all selected sticks share one bow family (same_family={same_family}, name='{family_name}'), do NOT claim that one has a more/less aggressive bend than another; describe the family characteristics only.
- If multiple bow families are present, any comparison must be limited to those families explicitly and be supported by the bow_knowledge_subset.

FORBIDDEN PHRASES (unless explicitly supported by the facts above):
- "longer length", "shorter length", "more aggressive bow", "more aggressive bend", "more concave", "less concave"

IF same_family == True:
- Do NOT use comparative or contrastive language between sticks (no "they differ", "one stick", "another", "more aggressive", "less aggressive", "more/less concave").
- Write a single, collective family description (use the bow family name: '{allowed_bows_line}').

IF same_length == True:
- Do NOT mention length or imply any difference in reach (no "longer", "shorter", "slightly longer", "added reach").

Player context:
- Attack: {profile.get('attack')}
- Midfield: {profile.get('midfield')}
- Defence: {profile.get('defence')}
- Dragflick importance: {profile.get('dragflick')}
- Aerial importance: {profile.get('aerials')}
- Budget: £{profile.get('budget')}
- Preferred bow (input): {profile.get('preferred_bow')}
- Primary bow: {bow1}
- Secondary bow: {bow2}
- Wildcard bow: {wildcard_bow}

Relevant bow trade-offs:
{bow_knowledge_subset}

Write ~60 words:
- Focus on play-style fit (e.g., attacking, defensive, flicking, aerial, control)
- Mention bow effects on lifts/flicks/turnover/hitting where relevant
- Brand-safe, no guarantees, no price promises

Return a JSON object ONLY:
{{
  "summary": "<~60 word explanation>",
  "bullets": ["point 1", "point 2"]
}}
"""

    _diag_log("pre_ai", prompt)
    # L2_DIAG: full OpenAI input log (model + token count + prompt snippet)
    try:
        _log_in = {
            "model": s.model,
            "max_tokens": s.max_tokens,
            "temperature": s.temperature,
            "timeout": s.request_timeout,
            "prompt_len": len(prompt),
            "prompt_preview": prompt[:600]
        }
        print("L2_DIAG OPENAI_INPUT:", _log_in)
    except Exception as _e_in:
        print("L2_DIAG OPENAI_INPUT_LOG_ERROR:", repr(_e_in))

    # Prepare timing/error holders for the OpenAI call
    t0 = time.time()
    last_err = None


# line after (unchanged)
    try:
        resp = client.chat.completions.create(
            model=s.model,
            messages=[
                {"role": "system", "content": "Follow the user's CONSTRAINTS exactly. Do not contradict them. Reply with a single JSON object only."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=s.max_tokens,
            temperature=min(0.2, s.temperature),
            timeout=s.request_timeout,
            response_format={"type": "json_object"},
        )

# change (new or modified lines)
        dt_ms = int((time.time() - t0) * 1000)

        raw = resp.choices[0].message.content.strip()

        # Parse response safely
        try:
            parsed = json.loads(raw)
        except Exception:
            cleaned = raw.strip().strip("`")
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:].lstrip()
            m = re.search(r"\{[\s\S]*\}", cleaned)
            parsed = json.loads(m.group(0)) if m else {"summary": cleaned}

        return {
            "summary": parsed.get("summary", "").strip(),
            "bullets": [
                b for b in parsed.get("bullets", [])
                if isinstance(b, str) and b.strip()
            ][:4],
            "source": "openai",
            "meta": {"ms": dt_ms, "chars": len(parsed.get("summary", ""))}
        }

    except Exception as e:
        print("RATIONALE ERROR:", repr(e))
        return {"summary": "", "bullets": [], "source": "openai_error", "meta": {"error": repr(e)}}

