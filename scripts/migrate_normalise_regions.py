#!/usr/bin/env python3
"""
DIG — collapse duplicate spellings of the same region.

WHY. `tracks.region` is one axis of the (region, genre, decade) cell that both
halves of the diversity machinery run on: cell_accounting caps ingest per cell,
and explore.py serves from the cells a listener has seen least. Two spellings of
one place therefore split into two cells, and the listener gets served the same
country twice as often as intended while the ingest cap lets in twice as many
tracks. Measured 2026-08-07:

    United States  8442  +  USA  6387  +  US  1
    UK             2886  +  United Kingdom  2339
    Russia          611  +  Russian Federation  5
    Netherlands     521  +  The Netherlands    14
    Czechia          40  +  Czech Republic     10
    UAE               4  +  United Arab Emirates 1

8,757 tracks, and the two big pairs are half the pool's largest regions.

WHAT THIS DELIBERATELY DOES NOT DO. The field mixes granularities — alongside
countries it carries Caribbean (1218), Nordic (837), West Africa (765), Eastern
Europe (597), East Africa (549). Rolling Nigeria up into West Africa, or
splitting Nordic into five countries, is a decision about what the axis MEANS
and belongs to a person, not to a migration. Only exact synonyms are touched.

CATALOG_CELLS MOVES TOO, in the same transaction. cell_id is the composite
'region|genre|decade' and it is the primary key, so renaming a region collides
ids: 'USA|house|1990s' meets 'United States|house|1990s'. Colliding cells are
MERGED — counters summed, timestamps kept at the later value — because those
counters are the exploration history and halving them would make the explorer
re-walk ground it has already covered. Renaming without merging would violate
the primary key; deleting the loser would lose that history silently.

    python3 scripts/migrate_normalise_regions.py            # dry run
    python3 scripts/migrate_normalise_regions.py --apply
"""
import argparse
import os
import re
import sys
import unicodedata

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib.db import fetchall, get_conn

# Exact synonyms only. The canonical side is the full formal name, chosen for
# consistency rather than for current row count — "UK" is the more common
# spelling in the data today but sits beside "United States", and a mixed
# convention is how the next duplicate gets introduced.
ALIASES = {
    "usa": "United States",
    "us": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "united states of america": "United States",
    "america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "great britain": "United Kingdom",
    "britain": "United Kingdom",
    "russian federation": "Russia",
    "the netherlands": "Netherlands",
    "holland": "Netherlands",
    "czech republic": "Czechia",
    "uae": "United Arab Emirates",
    "korea, south": "South Korea",
    "republic of korea": "South Korea",
    "drc": "DR Congo",
    "democratic republic of the congo": "DR Congo",
    "ivory coast": "Côte d’Ivoire",
    "cote divoire": "Côte d’Ivoire",
}


def norm_key(s):
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9 ]", "", s.lower()).strip()


def canonical(region):
    """The spelling this region should have, or None if it is already right."""
    want = ALIASES.get(norm_key(region))
    return want if want and want != region else None


def plan():
    """[(current, canonical, n_tracks)] for every value that needs changing."""
    rows = fetchall(
        "SELECT btrim(region) AS r, count(*) AS c FROM tracks "
        "WHERE coalesce(btrim(region), '') <> '' GROUP BY 1")
    out = []
    for r in rows:
        want = canonical(r["r"])
        if want:
            out.append((r["r"], want, r["c"]))
    return sorted(out, key=lambda x: -x[2])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    moves = plan()
    if not moves:
        print("  every region is already canonical — nothing to do.")
        return
    total = sum(n for _, _, n in moves)
    print(f"  {len(moves)} spelling(s) to fold, {total} tracks affected\n")
    for cur, want, n in moves:
        print(f"    {cur[:28]:30} -> {want[:24]:26} {n:>6} tracks")

    cells = fetchall("SELECT cell_id, region, genre, decade FROM catalog_cells")
    renames = [c for c in cells if canonical(c["region"])]
    ids = {c["cell_id"] for c in cells}
    collisions = sum(
        1 for c in renames
        if f"{canonical(c['region'])}|{c['genre']}|{c['decade']}" in ids)
    print(f"\n  catalog_cells: {len(renames)} cell(s) move, "
          f"{collisions} collide with an existing cell and will be merged")

    if not args.apply:
        print("\n  DRY RUN — pass --apply to write.")
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # One transaction: tracks and cells must never disagree about how a
            # region is spelled, or the explorer reads a split history.
            for old, want, _ in moves:
                cur.execute("UPDATE tracks SET region = %s WHERE btrim(region) = %s",
                            (want, old))

            # Merge colliding cells first (summing the exploration history),
            # then rename the survivors that have nothing to collide with.
            for c in renames:
                want = canonical(c["region"])
                new_id = f"{want}|{c['genre']}|{c['decade']}"
                if new_id == c["cell_id"]:
                    continue
                cur.execute("SELECT 1 FROM catalog_cells WHERE cell_id = %s", (new_id,))
                if cur.fetchone():
                    cur.execute(
                        """
                        UPDATE catalog_cells t SET
                          explored = coalesce(t.explored,0) + coalesce(s.explored,0),
                          fetched  = coalesce(t.fetched,0)  + coalesce(s.fetched,0),
                          pool_size = greatest(coalesce(t.pool_size,0),
                                               coalesce(s.pool_size,0)),
                          last_scanned = greatest(t.last_scanned, s.last_scanned),
                          last_fetched = greatest(t.last_fetched, s.last_fetched)
                        FROM catalog_cells s
                        WHERE t.cell_id = %s AND s.cell_id = %s
                        """, (new_id, c["cell_id"]))
                    cur.execute("DELETE FROM catalog_cells WHERE cell_id = %s",
                                (c["cell_id"],))
                else:
                    cur.execute(
                        "UPDATE catalog_cells SET cell_id = %s, region = %s "
                        "WHERE cell_id = %s", (new_id, want, c["cell_id"]))
        conn.commit()
    finally:
        conn.close()

    left = plan()
    print(f"\n  applied. regions still needing a fold: {len(left)}")
    print("  distinct regions now:",
          fetchall("SELECT count(DISTINCT btrim(region)) c FROM tracks")[0]["c"])
    print("  catalog_cells now:",
          fetchall("SELECT count(*) c FROM catalog_cells")[0]["c"])


if __name__ == "__main__":
    main()
