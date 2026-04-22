#!/usr/bin/env python3
"""
DIG — one-shot junk purge.

Removes tracks from the live `tracks` table where `is_trash(name, artist)` matches.
Run after deploying an updated lib/track_filter.py to clean out historical pollution.

Usage:  python3 scripts/purge_junk.py [--dry-run]
"""
import os
import sys
import argparse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.db import get_conn, fetchall  # noqa: E402
from lib.track_filter import is_trash  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Report only, don't delete")
    parser.add_argument("--limit", type=int, default=0, help="Sample limit (0 = all)")
    args = parser.parse_args()

    sql = "SELECT id, name, artist, album, region FROM tracks"
    if args.limit:
        sql += f" LIMIT {args.limit}"
    rows = fetchall(sql)
    print(f"Loaded {len(rows)} tracks from DB")

    junk_ids = []
    samples = []
    for r in rows:
        if is_trash(r["name"] or "", r["artist"] or "", r.get("album") or ""):
            junk_ids.append(r["id"])
            if len(samples) < 30:
                samples.append((r["region"], r["artist"], r["name"]))

    pct = 100 * len(junk_ids) / max(len(rows), 1)
    print(f"Flagged {len(junk_ids)} junk tracks ({pct:.2f}%)")
    print()
    print("Sample (first 30):")
    for region, artist, name in samples:
        print(f"  [{region:18}] {(artist or '?')[:35]:35} -- {(name or '?')[:50]}")

    if args.dry_run:
        print("\n--dry-run set; no deletions.")
        return

    if not junk_ids:
        print("Nothing to purge.")
        return

    print(f"\nDeleting {len(junk_ids)} rows...")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Delete in batches of 500 to keep statements small
            BATCH = 500
            for i in range(0, len(junk_ids), BATCH):
                chunk = junk_ids[i:i + BATCH]
                cur.execute("DELETE FROM tracks WHERE id = ANY(%s)", (chunk,))
            conn.commit()
        print(f"Deleted {len(junk_ids)} junk tracks.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
