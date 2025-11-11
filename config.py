"""
Mercian Stick Selector – configuration
Step 1: Centralised settings & feature flags.

Usage in app.py:
    from config import settings, should_generate_rationale

Put a `.env` file alongside app.py.
"""

import os
from dataclasses import dataclass
from functools import lru_cache

# Optional: load .env if present (does nothing if python-dotenv isn't installed)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

print("DEBUG OPENAI_KEY loaded:", bool(os.getenv("OPENAI_API_KEY")))
print("DEBUG ENABLE_RATIONALE:", os.getenv("ENABLE_RATIONALE"))
print("DEBUG RATIONALE_DEFAULT:", os.getenv("RATIONALE_DEFAULT"))

@dataclass(frozen=True)
class Settings:
    # OpenAI
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    request_timeout: float = float(os.getenv("OPENAI_TIMEOUT", "12.0"))
    max_tokens: int = int(os.getenv("OPENAI_MAX_TOKENS", "1000"))
    temperature: float = float(os.getenv("OPENAI_TEMPERATURE", "0.2"))

    # Feature flags
    enable_rationale: bool = os.getenv("ENABLE_RATIONALE", "1") == "1"
    rationale_default: int = int(os.getenv("RATIONALE_DEFAULT", "1"))  # 1=on, 0=off

    # A/B query param name
    ab_param: str = "rationale"


@lru_cache(maxsize=1)
def settings() -> Settings:
    return Settings()


def should_generate_rationale(qparam_value: str | None) -> bool:
    """
    Decide whether to call OpenAI:
    - Master flag can disable globally
    - Query param (?rationale=0|1) overrides per-request
    """
    s = settings()
    if not s.enable_rationale:
        return False
    if qparam_value is None or str(qparam_value).strip() == "":
        return bool(s.rationale_default)
    val = str(qparam_value).strip()
    return val == "1"
