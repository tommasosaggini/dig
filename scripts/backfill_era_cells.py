#!/usr/bin/env python3
"""DIG — create the catalog cells the starved decades never got.

THE BUG
-------
`_ensure_cell` (lib/discovery_lock.py) creates a catalog cell for exactly the
(region, genre, decade) of the track that arrived — and nothing else. So a genre
first learned from a 2024 track gets a 2020s cell and is never once searched in
the 1980s, however plainly that genre existed then. The pool feeds itself its
own skew: recent tracks make recent cells, recent cells get scanned, and they
return more recent tracks.

Measured 2026-08-18, distinct genres holding at least one cell per decade:

    1980s   462        2000s  5,363
    1990s   665        2010s  5,468
                       2020s  5,460

That is not a fact about music. `dub`, `ebm`, `bebop`, `americana` and 21 other
genres had cells in the 1970s AND the 1990s with nothing in between — EBM is a
quintessentially eighties genre with no eighties cell to search.

The consequence is a hard ceiling: pipeline/discover.py can only scan cells that
exist, so no amount of steering it toward the 1980s could find more than the 321
unscanned eighties cells that happened to exist.

WHAT THIS DOES
--------------
For every (region, genre) pair DIG ALREADY searches, ensure a cell exists in
each starved decade — but only where the pool holds real evidence that the genre
existed in that decade.

EVIDENCE, NOT SPECULATION. The gate is `MIN_TRACKS` real tracks already in
`tracks` tagged with that genre and dated in that decade. Without it this would
happily create "greek trap 1980s", and every nonsense cell costs a live Spotify
search to disprove — the picker only demotes a cell as barren AFTER it has come
back empty once. At ~35 cells scanned a day that is a budget worth protecting.

Idempotent (ON CONFLICT DO NOTHING), so it is safe on a schedule: new genres
keep arriving from the 2020s end, and each run extends the ones that have since
earned it.

  scripts/backfill_era_cells.py              # dry run — counts only
  scripts/backfill_era_cells.py --apply
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env  # noqa: E402
load_env()

from lib.db import fetchall, get_conn  # noqa: E402
from lib.era import STARVED_FROM, STARVED_TO, starved_decades  # noqa: E402

# How many real tracks of a genre must already sit in a decade before DIG will
# spend searches looking for more there. Two rather than one: a single track can
# be a mis-tagged year or a reissue dated to its repress.
MIN_TRACKS = 2

# A cap so one run can never dump a hundred thousand rows into the scan queue.
# Not a policy — the evidence gate above is the policy — just a blast radius.
MAX_NEW_CELLS = 20000

CANDIDATE_SQL = """
WITH evidence AS (
    -- Genre x decade combinations the pool can PROVE exist.
    SELECT unnest(genres) AS genre, left(year, 3) || '0s' AS decade
    FROM tracks
    WHERE year ~ '^[0-9]{4}$'
      AND year::int BETWEEN %s AND %s
    GROUP BY 1, 2
    HAVING count(*) >= %s
),
searched AS (
    -- The (region, genre) pairs DIG already searches, in any decade.
    SELECT genre, region FROM catalog_cells GROUP BY 1, 2
)
SELECT s.region, s.genre, e.decade
FROM evidence e
JOIN searched s ON s.genre = e.genre
WHERE NOT EXISTS (
    SELECT 1 FROM catalog_cells c
    WHERE c.region = s.region AND c.genre = s.genre AND c.decade = e.decade
)
"""


def candidates():
    """The (region, genre, decade) cells that are missing and earned. Pure read."""
    return [(r["region"], r["genre"], r["decade"]) for r in fetchall(
        CANDIDATE_SQL, (STARVED_FROM, STARVED_TO, MIN_TRACKS))]


# Rows per INSERT. Batched because this is a scheduled job reaching a database
# that may be on the other side of an SSH tunnel: the first run sent 7,139
# single-row statements and spent eleven minutes doing it, essentially all of it
# waiting on round trips. One statement per thousand rows makes the same work a
# few seconds.
INSERT_BATCH = 1000


def insert_cells(rows):
    """Create the cells. ON CONFLICT DO NOTHING, so re-running is free."""
    from psycopg2.extras import execute_values

    conn = get_conn()
    made = 0
    try:
        with conn.cursor() as cur:
            for i in range(0, len(rows), INSERT_BATCH):
                chunk = [(f"{r}|{g}|{d}", r, g, d, 0, 0)
                         for r, g, d in rows[i:i + INSERT_BATCH]]
                # RETURNING + fetch, not cur.rowcount. execute_values pages
                # internally (page_size defaults to 100), so rowcount reflects
                # only the LAST page it sent — the first run of this reported
                # "Inserted 742 new cells (6400 already existed)" while all
                # 7,142 had in fact landed. A success line that under-reports by
                # 90% is how a genuine failure goes unnoticed later.
                made += len(execute_values(
                    cur,
                    "INSERT INTO catalog_cells"
                    " (cell_id, region, genre, decade, explored, fetched)"
                    " VALUES %s ON CONFLICT (cell_id) DO NOTHING"
                    " RETURNING cell_id",
                    chunk,
                    fetch=True,
                ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return made


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true",
                   help="actually write the cells (default is a dry run)")
    p.add_argument("--max", type=int, default=MAX_NEW_CELLS)
    args = p.parse_args()

    rows = candidates()
    by_decade = {}
    for _, _, d in rows:
        by_decade[d] = by_decade.get(d, 0) + 1

    print(f"Starved decades: {', '.join(starved_decades())}")
    print(f"Evidence gate:   >= {MIN_TRACKS} real tracks in the pool\n")
    for d in sorted(by_decade):
        print(f"  {d}  {by_decade[d]:>6} cells missing")
    print(f"  {'TOTAL':6} {len(rows):>6}")

    if len(rows) > args.max:
        print(f"\ncapped at {args.max} (of {len(rows)}); re-run for the rest")
        rows = rows[:args.max]

    if not args.apply:
        print("\nDRY RUN — pass --apply to write them.")
        return

    made = insert_cells(rows)
    print(f"\nInserted {made} new cells "
          f"({len(rows) - made} already existed).")


if __name__ == "__main__":
    main()
