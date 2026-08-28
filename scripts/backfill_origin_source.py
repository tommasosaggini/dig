#!/usr/bin/env python3
"""Stamp `tracks.origin_source` — where each row's country claim came from.

Reads only evidence already in the row (origin_region, source, query, region);
makes no network calls. Safe to re-run: it recomputes every row from scratch,
so a later resolver upgrade can simply re-run it.

Precedence is "strongest evidence wins", and the important line is between the
tiers that are ABOUT THE ARTIST and the tiers that are about how we happened to
find the track. `market` is the latter: pipeline/discover.py wrote the Spotify
search storefront into `region`, so 'Singapore' on a Réunion maloya record only
ever meant "the SG storefront returned it".

Usage:  python3 scripts/backfill_origin_source.py [--dry-run]
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env          # noqa: E402
load_env()
from lib.db import get_conn           # noqa: E402
from lib.origin import classify, TRUSTED  # noqa: E402

TRUSTED_SET = set(TRUSTED)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = get_conn()
    import psycopg2.extras
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, source, region, origin_region, origin_source, query FROM tracks")
        rows = cur.fetchall()
    print(f"classifying {len(rows)} rows")

    from collections import Counter
    tally = Counter()
    updates = []
    for r in rows:
        src, country = classify(r)
        tally[src] += 1
        # A trusted tier must land its answer in origin_region — served_region
        # reads that one field and nothing else. Bandcamp rows historically
        # kept their location only in `region`, so this is where they move.
        updates.append((src, country or r["origin_region"], r["id"]))

    total = len(rows)
    trusted = sum(n for k, n in tally.items() if k in TRUSTED_SET)
    print(f"\n{'origin_source':<22}{'rows':>8}{'share':>9}")
    for k, n in tally.most_common():
        mark = "  trusted" if k in TRUSTED_SET else ""
        print(f"  {k:<20}{n:>8}{n/total:>8.1%}{mark}")
    print(f"\ntrusted country claim: {trusted}/{total} = {trusted/total:.1%}")
    print(f"needs resolving      : {total-trusted}/{total} = {(total-trusted)/total:.1%}")

    if args.dry_run:
        print("\nDRY RUN — nothing written.")
        return

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur, "UPDATE tracks SET origin_source = %s, origin_region = %s "
                 "WHERE id = %s",
            updates, page_size=2000)
    conn.commit()
    conn.close()
    print("\nwritten.")


if __name__ == "__main__":
    main()
