"""
Shared Spotify API budget tracker.

Spotify Development Mode has aggressive rate limits. This module
tracks API calls across all discovery scripts in a single cron run,
keeping total usage under a safe ceiling.

Budget file resets if it's older than 1 hour (i.e. from a previous run).
"""

import json
import os
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUDGET_PATH = os.path.join(ROOT, ".api_budget.json")

# Safe ceiling for Development Mode — stay well under the limit
MAX_CALLS_PER_RUN = 120


def _load():
    if os.path.exists(BUDGET_PATH):
        try:
            with open(BUDGET_PATH) as f:
                data = json.load(f)
            # Reset if from a previous run (older than 1 hour)
            if time.time() - data.get("started", 0) > 3600:
                return None
            return data
        except:
            pass
    return None


def _save(data):
    with open(BUDGET_PATH, "w") as f:
        json.dump(data, f)


def get_remaining():
    """How many API calls are left in this run's budget."""
    data = _load()
    if not data:
        return MAX_CALLS_PER_RUN
    return max(0, MAX_CALLS_PER_RUN - data.get("used", 0))


def record_call():
    """Record one API call. Returns False if budget is exhausted."""
    data = _load()
    if not data:
        data = {"started": time.time(), "used": 0}
    data["used"] = data.get("used", 0) + 1
    _save(data)
    return data["used"] <= MAX_CALLS_PER_RUN


def is_exhausted():
    """True if the shared budget is used up."""
    return get_remaining() <= 0


def reset():
    """Force reset the budget (called at start of cron run)."""
    _save({"started": time.time(), "used": 0})


def get_used():
    """How many calls have been used this run."""
    data = _load()
    return data.get("used", 0) if data else 0
