#!/usr/bin/env python3
"""Resolve Unknown-region artists against MusicBrainz — opportunistic drip.

5,754 pool tracks (2,877 distinct primary artists) sit under region
'Unknown': Bandcamp artists with no declared location plus Spotify rows whose
artist never went through MB. lib/mb_resolve already answers exactly this
question with the hardened name-matching bar (namesake policy, >=2-token
agreement), so this walks Unknown artists through it and stamps
origin_region on ALL their tracks when MB is confident. No hint exists here,
so only confident matches land — a miss stays Unknown, visibly.

MB's request budget on this box is mostly consumed by the hourly genre
crawler, so this drip is OPPORTUNISTIC: it does what it can and exits
cleanly the moment MB pushes back (MBRateLimited), keeping whatever it
resolved. Misses are recorded with a timestamp and retried after 60 days —
MB grows, so a miss is not forever.

Usage:
    python3 scripts/backfill_unknown_regions.py --limit 60
"""
import argparse
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.db import fetchall, execute                 # noqa: E402
from lib.mb_resolve import resolve_artist, MBRateLimited  # noqa: E402
from lib.region_norm import canonical_region         # noqa: E402

STATE_PATH = os.path.join(ROOT, "scripts", ".unknown_region_misses.json")
RETRY_S = 60 * 24 * 3600

UNKNOWN = ("COALESCE(NULLIF(origin_region,''), NULLIF(region,''), 'Unknown')"
           " = 'Unknown'")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=60)
    args = ap.parse_args()

    misses = {}
    if os.path.exists(STATE_PATH):
        try:
            misses = json.load(open(STATE_PATH))
        except Exception:
            pass
    now = time.time()

    rows = fetchall(
        f"""SELECT trim(split_part(artist, ',', 1)) AS a, count(*) AS n
            FROM tracks WHERE {UNKNOWN}
            GROUP BY 1 ORDER BY count(*) DESC""")
    todo = [r for r in rows
            if r["a"] and now - misses.get(r["a"], 0) > RETRY_S][:args.limit]
    print(f"{len(rows)} unknown artists, trying {len(todo)} this run")

    resolved = tracks_fixed = missed = 0
    for r in todo:
        name = r["a"]
        try:
            res = resolve_artist(name)
        except MBRateLimited:
            print("MB rate-limited — stopping run, keeping progress")
            break
        except Exception as e:
            print(f"  ! {name[:30]}: {type(e).__name__}")
            continue
        country = canonical_region((res or {}).get("country") or "")
        if not country:
            misses[name] = now
            missed += 1
            continue
        execute(
            f"""UPDATE tracks SET origin_region = %s
                WHERE trim(split_part(artist, ',', 1)) = %s AND {UNKNOWN}""",
            (country, name))
        resolved += 1
        tracks_fixed += r["n"]
        print(f"  ✓ {name[:32]:32s} → {country} ({r['n']} tracks)")

    json.dump(misses, open(STATE_PATH, "w"))
    print(f"resolved={resolved} artists ({tracks_fixed} tracks) "
          f"missed={missed} (retry in 60d)")


if __name__ == "__main__":
    main()
