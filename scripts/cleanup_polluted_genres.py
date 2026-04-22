#!/usr/bin/env python3
"""
scripts/cleanup_polluted_genres.py

Strip AI-query strings accidentally stored in tracks.genres[]. Any entry that:
  - is longer than 30 characters, or
  - contains a decade marker like "1970s" or "2010s", or
  - contains 4+ space-separated words
is a polluted query-string, not a canonical genre. Remove it.

Tracks whose genres array becomes empty after cleanup will be picked up by
label_discovery.py on the next cron run and assigned 1-3 proper genres.

Usage:
    python scripts/cleanup_polluted_genres.py [--dry-run]
"""

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.db import fetchall, get_conn


DECADE_PATTERN = re.compile(r"\b(1\d|20)\d{2}s\b", re.IGNORECASE)


def is_polluted(g: str) -> bool:
    g = (g or "").strip()
    if not g:
        return True
    if len(g) > 30:
        return True
    if DECADE_PATTERN.search(g):
        return True
    if len(g.split()) >= 4:
        return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove query-string pollution from tracks.genres")
    parser.add_argument("--dry-run", action="store_true", help="Report only, don't write")
    args = parser.parse_args()

    print("🧹 DIG — CLEANING POLLUTED GENRES\n")

    rows = fetchall(
        "SELECT id, genres FROM tracks WHERE array_length(genres, 1) > 0"
    )
    print(f"  Scanned {len(rows):,} tracks with non-empty genres\n")

    to_update: list[tuple[list[str], str]] = []
    cleaned_counts: dict[str, int] = {"only_pollution": 0, "mixed": 0}
    sample_pollution: set[str] = set()

    for r in rows:
        orig = list(r["genres"] or [])
        kept = [g for g in orig if not is_polluted(g)]
        removed = [g for g in orig if is_polluted(g)]
        if not removed:
            continue
        for g in removed[:1]:
            if len(sample_pollution) < 20:
                sample_pollution.add(g)
        if kept:
            cleaned_counts["mixed"] += 1
        else:
            cleaned_counts["only_pollution"] += 1
        to_update.append((kept, r["id"]))

    print(f"  Tracks needing cleanup:        {len(to_update):,}")
    print(f"    Fully polluted (→ empty):    {cleaned_counts['only_pollution']:,}")
    print(f"    Mixed (keep valid entries):  {cleaned_counts['mixed']:,}")
    print(f"\n  Sample of pollution removed:")
    for g in sorted(sample_pollution)[:15]:
        print(f"    · {g!r}")

    if args.dry_run:
        print("\nDRY RUN — no changes. Re-run without --dry-run to apply.")
        return
    if not to_update:
        print("\nNothing to do.")
        return

    import psycopg2.extras
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                "UPDATE tracks SET genres = %s::TEXT[] WHERE id = %s",
                to_update,
                page_size=500,
            )
        conn.commit()
        print(f"\n✓ Cleaned {len(to_update):,} tracks.")
        print(f"  {cleaned_counts['only_pollution']:,} tracks now have empty genres — label_discovery.py will re-populate them on the next cron run.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
