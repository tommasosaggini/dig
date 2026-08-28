#!/usr/bin/env python3
"""
DIG — retro-trim artists that got past the cap before it counted names.

`lib/artist_cap.py` now gates on `lower(btrim(artist))` for every source.
Until 2026-08-17 it gated on `artist_ids[1]`, which is empty on every
Bandcamp, YouTube and SoundCloud row — so 65% of the pool was ingested
with no cap at all. The stock left behind, measured the day the gate was
fixed: 3,429 artists over the cap holding 24,412 tracks, 24% of the pool.
AQVARIA alone had 38, off a single `bandcamp-tag:cayman-islands` scrape.

WHAT THIS IS AND IS NOT FOR. The repetition the listener actually feels is
already handled upstream: `_bootstrap_sample` gives every artist exactly
one slot in the working set, so an artist with 38 tracks and one with 3
now draw identically. This script is about the pool, not the player:

  * surplus tracks crowd the relaxed tail of a thin cell, so once a small
    region's fresh artists are spent, what is left is one act's back
    catalogue;
  * they inflate every count the discovery loop steers by. Cayman Islands
    reads as 114 tracks and is 36 artists, 9 of them one act. analyze_pool
    then scores the cell as covered and stops sending probes at it.

WHICH THREE SURVIVE. Ranked by how much of the track we actually know —
audio analysis, then AI labels, then genres, then a release year, then
quality_score, then the earliest ingest as a stable tie-break. A trimmed
artist keeps its best-described tracks, so trimming makes the pool's
average metadata richer rather than poorer.

WHAT IS NEVER DELETED. Any track in `user_history` or `user_ledger`. Those
rows are somebody's listening record — the ledger is the one thing in DIG
that is supposed to be permanent — and deleting them would tear a hole in
a diary to tidy a pool. They are kept and they COUNT toward the cap, so a
listener's saves can leave an artist legitimately above it.

Dry-run by default. Nothing is written without --apply.

  python3 scripts/trim_over_cap_artists.py                 # report
  python3 scripts/trim_over_cap_artists.py --top 40        # worst offenders
  python3 scripts/trim_over_cap_artists.py --apply         # do it
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

BACKUP_DIR = os.path.join(ROOT, "backups")

from lib.artist_cap import ARTIST_CAP          # noqa: E402
from lib.db import fetchall, get_conn          # noqa: E402

# Ranked best-first: the survivors are the tracks we know most about. Kept as
# one SQL fragment because the SELECT that reports and the DELETE that acts
# must order identically — two orderings would report one set and delete
# another, which is the kind of divergence that only shows up as loss.
KEEP_ORDER = """
    (t.audio_analyzed_at IS NOT NULL)::int DESC,
    (t.label_energy IS NOT NULL)::int DESC,
    (t.genres IS NOT NULL AND cardinality(t.genres) > 0)::int DESC,
    (t.year IS NOT NULL)::int DESC,
    COALESCE(t.quality_score, 0) DESC,
    t.added_at ASC,
    t.id ASC
"""

# One CTE, used by both the report and the delete. `protected` is evaluated
# per artist BEFORE the ranking so a saved track never competes for a slot —
# it is kept outright and the cap is spent against it.
SURPLUS_CTE = f"""
WITH protected AS (
    SELECT track_id AS id FROM user_history
    UNION
    -- The ledger keys on a display string, not an id, and the join must stay
    -- byte-identical to _track_key() in lib/discovery_lock.py — a ledger
    -- entry we fail to match is a saved track we silently delete.
    SELECT t.id FROM tracks t JOIN user_ledger l
      ON l.track_key = lower(COALESCE(t.artist, '') || ' - ' || COALESCE(t.name, ''))
),
ranked AS (
    SELECT t.id,
           lower(btrim(t.artist)) AS akey,
           t.artist,
           (p.id IS NOT NULL) AS is_protected,
           row_number() OVER (
               PARTITION BY lower(btrim(t.artist))
               ORDER BY (p.id IS NOT NULL)::int DESC, {KEEP_ORDER}
           ) AS rn
    FROM tracks t
    LEFT JOIN protected p ON p.id = t.id
    WHERE t.artist IS NOT NULL AND btrim(t.artist) <> ''
),
surplus AS (
    SELECT * FROM ranked WHERE rn > %s AND NOT is_protected
)
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cap", type=int, default=ARTIST_CAP)
    ap.add_argument("--top", type=int, default=25, help="offenders to list")
    ap.add_argument("--apply", action="store_true", help="actually delete")
    args = ap.parse_args()
    args.stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    totals = fetchall(SURPLUS_CTE + """
        SELECT count(*) AS surplus,
               count(DISTINCT akey) AS artists
        FROM surplus""", (args.cap,))[0]
    pool = fetchall("SELECT count(*) AS n FROM tracks")[0]["n"]

    if not totals["surplus"]:
        print(f"nothing over the cap of {args.cap} — pool is {pool:,} tracks")
        return 0

    print(f"pool {pool:,} tracks")
    print(f"over cap {args.cap}: {totals['artists']:,} artists, "
          f"{totals['surplus']:,} surplus tracks "
          f"({100.0 * totals['surplus'] / pool:.1f}% of the pool)\n")

    offenders = fetchall(SURPLUS_CTE + """
        SELECT max(artist) AS artist, count(*) AS surplus
        FROM surplus GROUP BY akey ORDER BY surplus DESC LIMIT %s""",
        (args.cap, args.top))
    print(f"worst {len(offenders)}:")
    for o in offenders:
        print(f"  -{o['surplus']:4}  {o['artist'][:70]}")

    # Protected rows are the reason a trimmed artist can still sit above the
    # cap afterwards. Say so rather than let the post-trim audit look wrong.
    kept = fetchall(SURPLUS_CTE.replace("AND NOT is_protected", "") + """
        SELECT count(*) AS n FROM surplus WHERE is_protected""", (args.cap,))[0]["n"]
    if kept:
        print(f"\n{kept:,} surplus tracks are in your history or ledger "
              f"and are kept regardless")

    if not args.apply:
        print(f"\ndry run — nothing deleted. Re-run with --apply.")
        return 0

    # Dump the full rows first. Re-running discovery would not bring these
    # back — they came from tag scrapes and search phases that are not
    # replayable — so the only honest way to make this reversible is to hold
    # the rows. Written and fsynced BEFORE the DELETE, so a crash between the
    # two leaves a superset on disk rather than a hole.
    os.makedirs(BACKUP_DIR, exist_ok=True)
    path = os.path.join(BACKUP_DIR, f"over_cap_trim_{args.stamp}.json")
    doomed = fetchall(SURPLUS_CTE + """
        SELECT t.* FROM tracks t JOIN surplus s ON s.id = t.id""", (args.cap,))
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doomed, fh, ensure_ascii=False, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    print(f"\nbacked up {len(doomed):,} rows -> {path}")

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(SURPLUS_CTE + """
                DELETE FROM tracks WHERE id IN (SELECT id FROM surplus)""",
                (args.cap,))
            deleted = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    print(f"deleted {deleted:,} tracks")
    if deleted != len(doomed):
        print(f"  NOTE: backup holds {len(doomed):,}, delete removed "
              f"{deleted:,} — the pool changed under the two statements")

    after = fetchall("""
        SELECT count(*) AS tracks, count(DISTINCT lower(btrim(artist))) AS artists
        FROM tracks""")[0]
    print(f"pool now {after['tracks']:,} tracks / {after['artists']:,} artists")
    return 0


if __name__ == "__main__":
    sys.exit(main())
