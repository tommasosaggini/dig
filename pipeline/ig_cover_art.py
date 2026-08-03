#!/usr/bin/env python3
"""
Replace YouTube video thumbnails on queued posts with the real cover art.

Runs before render in the cron. Anything already pointing at a genuine sleeve is
left alone; only ytimg URLs (and empty ones) are looked up. A found cover is
written back to `tracks.art_url` too, so the pool keeps it and the lookup is
paid once per track rather than once per post.

Clearing rendered_at is deliberate: changing the artwork must rebuild the card,
otherwise the queue shows an old image and the mismatch is invisible.

    python3 pipeline/ig_cover_art.py [--force] [--limit N]
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import cover_art
from lib.db import execute, fetchall


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true",
                    help="re-look-up even items that already have a cover")
    ap.add_argument("--limit", type=int, default=200)
    args = ap.parse_args()

    # %% — psycopg2 reads a lone % as a parameter placeholder and dies on the
    # LIKE pattern before the query ever reaches Postgres.
    where = "" if args.force else \
        "AND (q.artwork_url IS NULL OR q.artwork_url LIKE '%%ytimg%%')"
    rows = fetchall(f"""
        SELECT q.id, q.track_id, q.track_name, q.artist, q.artwork_url
        FROM ig_post_queue q
        WHERE q.status NOT IN ('published', 'skipped', 'failed') {where}
        ORDER BY q.queue_order LIMIT %s
    """, (args.limit,))
    if not rows:
        print("  every queued post already has real cover art.")
        return

    found = 0
    for r in rows:
        url = cover_art.find(r["artist"], r["track_name"])
        if not url:
            print(f"  ·  #{r['id']} {r['track_name'][:34]} — no cover found, "
                  f"keeping thumbnail")
            continue
        execute("UPDATE ig_post_queue SET artwork_url = %s, rendered_at = NULL "
                "WHERE id = %s", (url, r["id"]))
        if r["track_id"]:
            execute("UPDATE tracks SET art_url = %s WHERE id = %s AND "
                    "art_url IS NULL", (url, r["track_id"]))
        found += 1
        print(f"  ✓  #{r['id']} {r['track_name'][:34]}")
    print(f"  {found}/{len(rows)} got real cover art.")


if __name__ == "__main__":
    main()
