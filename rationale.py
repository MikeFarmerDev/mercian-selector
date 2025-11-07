# rationale.py

from openai import OpenAI
from config import settings
import json
import re

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
    # --- If app.py provided a full custom prompt, use it directly ---
    if profile.get("_custom_prompt"):
        try:
            resp = client.chat.completions.create(
                model=s.model,
                messages=[
                    {"role": "system", "content": "You are Mercian’s equipment expert."},
                    {"role": "user", "content": profile["_custom_prompt"]},
                ],
                max_tokens=s.max_tokens,
                temperature=min(0.3, s.temperature),
                timeout=s.request_timeout,
            )
            content = resp.choices[0].message.content.strip() if resp.choices else ""
            return {"summary": content, "bullets": []}
        except Exception as e:
            print("RATIONALE ERROR (custom prompt):", repr(e))
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

    # Deterministic non-comparative path: skip the model when there's nothing valid to compare
    if same_family and same_length:
        print("RATIONALE MODE: deterministic return")
        items = []
        for s_ in (primaries or [])[:2]:
            if s_:
                items.append({
                    "title": s_.get("Title") or s_.get("Description") or "",
                    "code": s_.get("Product Code") or "",
                    "carbon": s_.get("Carbon") or "",
                    "solid_core": "SC" in (s_.get("Title") or "") or "Solid Core" in (s_.get("Description") or "")
                })
        if wildcard:
            items.append({
                "title": wildcard.get("Title") or wildcard.get("Description") or "",
                "code": wildcard.get("Product Code") or "",
                "carbon": wildcard.get("Carbon") or "",
                "solid_core": "SC" in (wildcard.get("Title") or "") or "Solid Core" in (wildcard.get("Description") or "")
            })

        any_sc = any(x["solid_core"] for x in items)
        highest_carbon = max(
            (int(x["carbon"]) or 0)
            for x in items
            if str(x["carbon"]).isdigit()
        ) if items else 0

        lead = (
            f"Because you prioritise attacking play and aerial skills, we've selected three sticks that match your style "
            f"and sit comfortably within your £{profile.get('budget')} budget."
        )
        family_line = (
            f" Each model uses Mercian’s {family_name} profile, optimised for fast lifts, clean 3D execution and quick release."
        )
        power_touch = (
            " Their high-carbon lay-ups provide the power you expect while preserving the responsive feel for close control."
            if highest_carbon >= 90 else
            " Their balanced lay-ups keep touch and feel at the forefront while still delivering confident hitting power."
        )
        sc_line = " Solid Core construction features in this range to enhance touch and feedback under pressure." if any_sc else ""
        close = " The result is confident, repeatable performance that lets you play on the front foot with precision."

        summary_text = f"Why these?\n{lead}{family_line} {power_touch}{sc_line}{close}"
        return {"summary": summary_text.strip(), "bullets": []}

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
        }

    except Exception as e:
        print("RATIONALE ERROR:", repr(e))
        return None
