#!/usr/bin/env python3
"""
scripts/migrate_emergent_catalog.py

One-time migration from a priori Cartesian catalog to emergent catalog.

The current catalog_cells table is a pre-computed Cartesian product of every
(region × genre × decade). Most of those combinations are nonsense (Japan ×
maskandi × 1930s) and will never yield a track. The table bloats to 700k+ rows
that pollute the scan queue and make "% explored" meaningless.

This script:
  1. Deletes cells that have never been searched AND have never yielded tracks.
  2. Rebuilds cells for every (region, derived_genre, decade) that exists in
     the current tracks table — so the catalog reflects real, observed shapes.
  3. Prints before/after counts.

Run once:
    python scripts/migrate_emergent_catalog.py [--dry-run]
"""

import argparse
import os
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.db import fetchone, fetchall, execute, get_conn
from lib.cell_accounting import cell_coord


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate catalog_cells to emergent model")
    parser.add_argument("--dry-run", action="store_true", help="Show counts only, don't modify DB")
    args = parser.parse_args()

    print("📐 DIG — EMERGENT CATALOG MIGRATION\n")

    before = fetchone("SELECT COUNT(*) AS n FROM catalog_cells")
    before_n = before["n"] if before else 0
    touched = fetchone(
        "SELECT COUNT(*) AS n FROM catalog_cells "
        "WHERE NOT (explored = 0 AND fetched = 0 AND last_scanned IS NULL)"
    )
    touched_n = touched["n"] if touched else 0
    to_drop = before_n - touched_n

    print(f"  Current catalog_cells: {before_n:,}")
    print(f"    Touched (searched or has tracks): {touched_n:,}")
    print(f"    A priori Cartesian junk to drop:  {to_drop:,}")

    # ── Compute emergent cells from tracks ──────────────────────────────
    print("\n  Deriving emergent cells from tracks table …")
    tracks = fetchall(
        "SELECT region, origin_region, decade, genres, query, artist, album "
        "FROM tracks"
    )
    print(f"    Scanned {len(tracks):,} tracks")

    emergent: set[tuple[str, str, str]] = set()
    cell_track_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for t in tracks:
        cell = cell_coord(dict(t))
        emergent.add(cell)
        cell_track_counts[cell] += 1

    print(f"    Emergent cells (unique shapes): {len(emergent):,}")

    # Sample a few for visual check
    top = sorted(cell_track_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
    print("\n  Top emergent cells (region | genre | decade — track count):")
    for (r, g, d), n in top:
        print(f"    {r} | {g} | {d} — {n}")

    if args.dry_run:
        print("\nDRY RUN — no changes. Re-run without --dry-run to apply.")
        return

    # ── Apply migration ─────────────────────────────────────────────────
    print("\n  Applying …")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 1. Prune Cartesian junk — cells never searched and with no tracks
            cur.execute(
                "DELETE FROM catalog_cells "
                "WHERE explored = 0 AND fetched = 0 AND last_scanned IS NULL"
            )
            pruned = cur.rowcount
            print(f"    Deleted {pruned:,} untouched cells")

            # 2. Ensure every emergent cell exists
            inserted = 0
            for (r, g, d) in emergent:
                cell_id = f"{r}|{g}|{d}"
                cur.execute(
                    """
                    INSERT INTO catalog_cells (cell_id, region, genre, decade, explored, fetched)
                    VALUES (%s, %s, %s, %s, 0, 0)
                    ON CONFLICT (cell_id) DO NOTHING
                    """,
                    (cell_id, r, g, d),
                )
                inserted += cur.rowcount
            print(f"    Inserted {inserted:,} new emergent cells (pre-existing left untouched)")

            # 3. Clear the stale scan queue — it referenced deleted cells
            cur.execute(
                "DELETE FROM catalog_scan_queue "
                "WHERE cell_id NOT IN (SELECT cell_id FROM catalog_cells)"
            )
            queue_pruned = cur.rowcount
            print(f"    Pruned {queue_pruned:,} stale scan queue entries")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # ── Report ──────────────────────────────────────────────────────────
    after = fetchone("SELECT COUNT(*) AS n FROM catalog_cells")
    after_n = after["n"] if after else 0
    print(f"\n  catalog_cells: {before_n:,} → {after_n:,} (Δ {after_n - before_n:+,})")
    print("\n✓ Migration complete. The catalog now reflects observed shapes only.")


if __name__ == "__main__":
    main()
