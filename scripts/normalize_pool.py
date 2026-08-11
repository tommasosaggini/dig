#!/usr/bin/env python3
"""One-shot pool data-quality pass: canonicalize regions, report the rest.

The audit (2026-08-11) found the region axis at 442 distinct values for ~200
real places — 'USA'/'US'/'United States', bare ISO codes from the MB drain,
cities from MB areas and Bandcamp locations, provinces from Bandcamp's
last-token fallthrough. Write-time canonicalization now lives in
lib/region_norm (wired into _upsert_track, ingest_mb_artists,
location_to_country, ingest_curator); this script folds the EXISTING rows
onto the same canon.

It also clears impossible decades ('0000s') and prints a report on the
things it deliberately does NOT touch — duplicate (artist, name) pairs
across sources and rows with empty names — so drift is seen, not silently
"fixed" by a script guessing.

Usage:
    python3 scripts/normalize_pool.py            # dry run: show what would change
    python3 scripts/normalize_pool.py --apply    # do it
"""
import argparse
import os
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.db import get_conn, fetchall          # noqa: E402
from lib.region_norm import canonical_region   # noqa: E402


def plan_column(col: str):
    """[(old, new, count)] for every value the canon would change."""
    rows = fetchall(
        f"SELECT {col} AS v, count(*) AS n FROM tracks "
        f"WHERE {col} IS NOT NULL AND {col} != '' GROUP BY 1")
    changes = []
    for r in rows:
        new = canonical_region(r["v"])
        if new != r["v"]:
            changes.append((r["v"], new, r["n"]))
    return sorted(changes, key=lambda c: -c[2])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    total_rows = 0
    plans = {}
    for col in ("region", "origin_region"):
        plan = plan_column(col)
        plans[col] = plan
        n = sum(c for _, _, c in plan)
        total_rows += n
        print(f"\n=== {col}: {len(plan)} values → {n} rows ===")
        merged = defaultdict(int)
        for old, new, cnt in plan:
            merged[new] += cnt
        for old, new, cnt in plan[:25]:
            print(f"  {old[:34]:34s} → {new[:26]:26s} {cnt:6d}")
        if len(plan) > 25:
            print(f"  … and {len(plan) - 25} more values")
        top = sorted(merged.items(), key=lambda x: -x[1])[:6]
        print("  biggest merges:", ", ".join(f"{k} +{v}" for k, v in top))

    bad_decades = fetchall(
        "SELECT decade, count(*) AS n FROM tracks "
        "WHERE decade IS NOT NULL AND decade !~ '^(19|20)[0-9]0s$' AND decade != '' "
        "GROUP BY 1")
    print(f"\n=== impossible decades ===")
    for r in bad_decades:
        print(f"  {r['decade']!r}: {r['n']} rows → cleared to ''")

    print(f"\nTOTAL rows to update: {total_rows + sum(r['n'] for r in bad_decades)}")

    # ── Report-only: visible weirdness this script refuses to auto-fix ──────
    dupes = fetchall(
        """
        SELECT count(*) AS pairs, sum(n - 1) AS extra_rows FROM (
            SELECT lower(artist || ' - ' || name) AS k, count(*) AS n
            FROM tracks GROUP BY 1 HAVING count(*) > 1
        ) d
        """)[0]
    empties = fetchall(
        "SELECT count(*) AS n FROM tracks "
        "WHERE coalesce(trim(name),'') = '' OR coalesce(trim(artist),'') = ''")[0]
    no_genres = fetchall(
        "SELECT count(*) AS n FROM tracks "
        "WHERE genres IS NULL OR array_length(genres, 1) IS NULL")[0]
    print("\n=== report only (not touched) ===")
    print(f"  duplicate (artist, name) pairs: {dupes['pairs']} "
          f"({dupes['extra_rows']} extra rows — often legit: reissues on "
          f"another source, and dedup-at-serve already collapses them)")
    print(f"  rows with empty artist or name: {empties['n']}")
    print(f"  rows with no genres yet:        {no_genres['n']} "
          f"(labelling backfills these)")

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for col, plan in plans.items():
                for old, new, _ in plan:
                    if new:
                        cur.execute(
                            f"UPDATE tracks SET {col} = %s WHERE {col} = %s",
                            (new, old))
                    else:
                        cur.execute(
                            f"UPDATE tracks SET {col} = NULL WHERE {col} = %s",
                            (old,))
            for r in bad_decades:
                cur.execute("UPDATE tracks SET decade = '' WHERE decade = %s",
                            (r["decade"],))
        conn.commit()
        print("\nAPPLIED.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    after = fetchall("SELECT count(DISTINCT region) AS n FROM tracks")[0]["n"]
    print(f"distinct region values now: {after}")


if __name__ == "__main__":
    main()
