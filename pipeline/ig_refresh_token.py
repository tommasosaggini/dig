#!/usr/bin/env python3
"""
Keep IG_GRAPH_TOKEN alive.

Instagram long-lived tokens last 60 days and can be exchanged for a fresh 60
days at any point after they are 24 hours old. Nothing warns you when one dies:
the API answers `{"error":{"message":"API access blocked.","code":200}}`, which
reads like a sanction on the app rather than an expired credential — we spent a
while hunting a Meta enforcement action that did not exist.

So: refresh well before expiry, on every cron pass, and rewrite .env in place.
Refreshing is idempotent and free, and re-refreshing an already-fresh token just
resets the clock, so there is no reason to be clever about scheduling.

    python3 pipeline/ig_refresh_token.py [--force]

Exits 0 and prints a line either way — this runs inside ig_cron.sh, and a token
that cannot be refreshed must not take the rest of the pipeline down with it.
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env

ENV_PATH = os.path.join(ROOT, ".env")
# Refresh when the stored token is older than this. The API cannot tell us how
# long the CURRENT token has left — `expires_in` describes the new one it just
# minted, so it always reads ~60 days — which means age has to be tracked here.
# The stamp file's mtime is that record. 30 days leaves a month of slack for a
# laptop that was closed, travelling, or off.
REFRESH_AFTER_DAYS = 30
STAMP = os.path.join(ROOT, ".ig_token_refreshed")


def _get(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    load_env()
    tok = os.environ.get("IG_GRAPH_TOKEN", "")
    if not tok:
        print("  no IG_GRAPH_TOKEN set — nothing to refresh.")
        return

    if not args.force and os.path.exists(STAMP):
        age_days = (time.time() - os.path.getmtime(STAMP)) / 86400.0
        if age_days < REFRESH_AFTER_DAYS:
            print(f"  token healthy (refreshed {age_days:.0f}d ago).")
            return

    try:
        res = _get("https://graph.instagram.com/refresh_access_token"
                   "?grant_type=ig_refresh_token&access_token=" + tok)
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200]
        # Do not fail the cron run. A dead token needs a human in the Meta
        # dashboard; the render/analysis stages are unaffected and should go on.
        print(f"  token refresh FAILED ({e.code}): {body}")
        print("  → regenerate at: App Dashboard → Instagram → "
              "API setup with Instagram business login → Generate token")
        return
    except Exception as e:
        print(f"  token refresh failed: {e}")
        return

    days = res.get("expires_in", 0) / 86400.0
    new = res.get("access_token", "")
    if not new.startswith("IGAA"):
        print(f"  refresh returned no usable token: {str(res)[:120]}")
        return

    src = open(ENV_PATH).read()
    out, n = re.subn(r"^IG_GRAPH_TOKEN=.*$", "IG_GRAPH_TOKEN=" + new,
                     src, count=1, flags=re.M)
    if not n:
        print("  WARNING: no IG_GRAPH_TOKEN line in .env — not written.")
        return
    # Write via a temp file in the same directory: a half-written .env would
    # take down every other pipeline that sources it, not just Instagram.
    tmp = ENV_PATH + ".tmp"
    with open(tmp, "w") as f:
        f.write(out)
    os.replace(tmp, ENV_PATH)
    with open(STAMP, "w") as f:
        f.write(f"{days:.0f}\n")
    print(f"  token refreshed — {days:.0f} days remaining.")


if __name__ == "__main__":
    main()
