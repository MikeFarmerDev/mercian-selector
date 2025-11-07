
"""
domain.logger
--------------
Lightweight logging module for Mercian Stick Selector insights.
Creates /logs/selector_log.csv and appends one row per API call.
"""

import os
import csv
from datetime import datetime
from typing import Dict, Any

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "selector_log.csv")

# Ensure logs directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Define header for CSV
FIELDNAMES = [
    "timestamp",
    "journey",
    "player_type",
    "budget",
    "fallbacks",
    "adapter_latency_ms",
    "response_time_ms",
    "status",
    "rationale_summary",
    "primaries",
    "wildcard",
]


def log_event(event: Dict[str, Any]) -> None:
    """Append a single event row to the CSV log."""
    try:
        # ensure file exists with headers
        file_exists = os.path.isfile(LOG_FILE)
        with open(LOG_FILE, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            if not file_exists:
                writer.writeheader()

                writer.writerow({
                    "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "journey": event.get("journey"),
                    "player_type": event.get("player_type"),
                    "budget": event.get("budget"),
                    "fallbacks": event.get("fallbacks"),
                    "adapter_latency_ms": event.get("adapter_latency_ms"),
                    "response_time_ms": event.get("response_time_ms"),
                    "status": event.get("status", "ok"),
                    "rationale_summary": event.get("rationale_summary", ""),
                    "primaries": event.get("primaries", ""),
                    "wildcard": event.get("wildcard", ""),
                })

    except Exception as e:
        print(f"[LOGGER] Failed to write event: {e}")
