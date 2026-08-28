#!/usr/bin/env python3
"""Drip-backfill release years for Bandcamp tracks.

All 32k Bandcamp rows were ingested without a year (the discover feed items
don't carry one), which is most of why 33k pool tracks had no decade. The
tralbum payload has the release date; play-time resolves now backfill it for
free, but plays only touch what gets played — this script walks the backlog
at Bandcamp-polite pace, a bounded batch per run, from cron.

Tracks whose tralbum lookup fails terminally (deleted from Bandcamp, 404,
no_tracks) are recorded in a state file so the drip never re-spends calls on
them; transient network errors are NOT recorded and retry next run.

Usage:
    python3 scripts/backfill_bc_years.py --limit 300
"""
import argparse
import json
import os
import sys
import time
import urllib.error

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.db import fetchall, execute       # noqa: E402
from lib import bandcamp                   # noqa: E402

STATE_PATH = os.path.join(ROOT, "scripts", ".bc_year_dead.json")
TERMINAL = {"http_404", "http_403", "api_error", "no_tracks"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--pace", type=float, default=1.2,
                    help="seconds between tralbum calls")
    args = ap.parse_args()

    dead = set()
    if os.path.exists(STATE_PATH):
        try:
            dead = set(json.load(open(STATE_PATH)))
        except Exception:
            pass

    rows = fetchall(
        "SELECT id FROM tracks WHERE source = 'bandcamp' "
        "AND coalesce(year, '') = '' ORDER BY id LIMIT %s",
        (args.limit + len(dead),))
    todo = [r["id"] for r in rows if r["id"] not in dead][:args.limit]
    print(f"backlog batch: {len(todo)} tracks ({len(dead)} known-dead skipped)")

    filled = errors = newly_dead = 0
    for tid in todo:
        band, track = bandcamp.parse_id(tid)
        if not band:
            dead.add(tid)
            newly_dead += 1
            continue
        try:
            d = bandcamp.tralbum_details(band, track, tralbum_type="t")
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                dead.add(tid)
                newly_dead += 1
            else:
                errors += 1
            time.sleep(args.pace)
            continue
        except Exception:
            errors += 1          # transient — retry next run
            time.sleep(args.pace)
            continue
        time.sleep(args.pace)
        if d.get("error"):
            dead.add(tid)
            newly_dead += 1
            continue
        year = bandcamp.release_year(d)
        if not year:
            dead.add(tid)        # payload has no date — asking again won't help
            newly_dead += 1
            continue
        execute(
            "UPDATE tracks SET year = %s, decade = %s "
            "WHERE id = %s AND coalesce(year, '') = ''",
            (year, year[:3] + "0s", tid))
        filled += 1

    json.dump(sorted(dead), open(STATE_PATH, "w"))
    # `remaining - len(dead)` went negative (-2, logged for days): a row can
    # leave the no-year set without leaving `dead`, because the play-time
    # backfill in server.py fills years too. Subtracting a set that is not a
    # subset produces a count that cannot exist, and a negative backlog reads
    # as "done" when it is really "this arithmetic is wrong". Ask the database
    # for the answer instead of deriving it.
    remaining = fetchall(
        "SELECT count(*) AS n FROM tracks WHERE source = 'bandcamp' "
        "AND coalesce(year, '') = '' AND NOT (id = ANY(%s))",
        (sorted(dead),))[0]["n"]
    print(f"filled={filled} dead+={newly_dead} transient_errors={errors} "
          f"remaining_backlog={remaining}")


if __name__ == "__main__":
    main()
