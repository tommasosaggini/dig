#!/usr/bin/env python3
"""Geographic Bandcamp digging — water-filled by country, via tag pages.

The classic discover feed has no location filter, so the Bandcamp side of the
pool mirrored Bandcamp's user base: 52% US/UK/CA (measured 2026-08-11). This
script is the same least-fed-first rule as the MB drain and the working-set
sampler, applied to Bandcamp: each run picks the countries with the FEWEST
Bandcamp tracks in the pool and digs their tag pages
(lib/bandcamp.discover_by_tag). No quotas, no target ratios — a country with
a thin tag page contributes what it has and drops out (state file marks it
exhausted); long-run imbalance can only reflect genuine supply.

The artist's own location decides the row's region (a Stockholm band tagging
'cambodia' files under Sweden), so tag noise cannot mislabel origins.

State (scripts/.bc_tag_state.json): per-tag cursor + slice progression
(top → new → exhausted). Exhausted tags re-open after 30 days — new releases
accrete.

Usage:
    python3 scripts/ingest_bandcamp_tags.py --countries 8 --pages 2 [--dry-run]
"""
import argparse
import json
import os
import re
import sys
import time
import unicodedata

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env               # noqa: E402
load_env()
from lib import bandcamp                   # noqa: E402
from lib.db import fetchall                # noqa: E402
from lib.discovery_lock import locked_update  # noqa: E402
from lib.artist_db import register_tracks  # noqa: E402
from lib.track_filter import is_trash      # noqa: E402
from lib.region_norm import ISO2           # noqa: E402

STATE_PATH = os.path.join(ROOT, "scripts", ".bc_tag_state.json")
REOPEN_S = 30 * 24 * 3600

# Countries whose Bandcamp presence the classic feed already over-serves —
# digging their tags would spend the budget where the skew came from.
SKIP = {"United States", "United Kingdom", "Canada", "Australia", "Germany",
        "France", "Netherlands", "Sweden", "Norway", "Denmark", "Belgium"}


def tag_slug(country: str) -> str:
    """'Sri Lanka' -> 'sri-lanka' — Bandcamp's tag_norm_names shape."""
    s = "".join(c for c in unicodedata.normalize("NFD", country.lower())
                if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def least_fed_countries(n: int, state: dict) -> list:
    """Candidate countries by ascending Bandcamp pool count (water-filling)."""
    counts = {r["c"]: r["n"] for r in fetchall("""
        SELECT COALESCE(NULLIF(origin_region,''), NULLIF(region,''), 'Unknown') AS c,
               count(*) AS n
        FROM tracks WHERE source = 'bandcamp' GROUP BY 1""")}
    now = time.time()
    out = []
    for country in ISO2.values():
        if country in SKIP or country in ("Soviet Union",):
            continue
        st = state.get(tag_slug(country), {})
        if st.get("exhausted_at") and now - st["exhausted_at"] < REOPEN_S:
            continue
        out.append((counts.get(country, 0), country))
    out.sort()
    return [c for _, c in out[:n]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--countries", type=int, default=8)
    ap.add_argument("--pages", type=int, default=2, help="pages per country per run")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    state = {}
    if os.path.exists(STATE_PATH):
        try:
            state = json.load(open(STATE_PATH))
        except Exception:
            pass

    countries = least_fed_countries(args.countries, state)
    print(f"digging (least-fed first): {countries}")

    existing = set(r["id"] for r in fetchall("SELECT id FROM tracks"))
    to_insert, matched, dup, trash = {}, 0, 0, 0

    for country in countries:
        tag = tag_slug(country)
        st = state.setdefault(tag, {"slice": "top", "cursor": None})
        for _ in range(args.pages):
            try:
                rows, cursor, total = bandcamp.discover_by_tag(
                    tag, cursor=st.get("cursor"), slice=st.get("slice", "top"))
            except bandcamp.BandcampBlocked as e:
                print(f"  ! blocked — stopping run: {e}")
                json.dump(state, open(STATE_PATH, "w"))
                return
            except Exception as e:
                print(f"  ! {tag}: {type(e).__name__} {str(e)[:60]}")
                break
            fresh = 0
            for tr in rows:
                if tr["id"] in existing:
                    dup += 1
                    continue
                if not tr["name"] or not tr["artist"]:
                    continue
                if is_trash(tr["name"], tr["artist"], tr.get("album", "")):
                    trash += 1
                    continue
                tr["query"] = f"bandcamp-tag:{tag}"
                existing.add(tr["id"])
                to_insert.setdefault(tr["region"] or "Unknown", []).append(tr)
                matched += 1
                fresh += 1
            print(f"  {tag:18s} [{st.get('slice','top'):3s}] +{fresh:3d} new "
                  f"(page had {len(rows)}, tag total {total})")
            if cursor and rows:
                st["cursor"] = cursor
            else:
                # This slice is walked out: top → new → exhausted (reopens
                # after 30 days — releases accrete).
                if st.get("slice", "top") == "top":
                    st["slice"], st["cursor"] = "new", None
                else:
                    st["exhausted_at"] = time.time()
                break

    print(f"\nNEW={matched} dup={dup} trash={trash} "
          f"across {len(to_insert)} region buckets")
    if args.dry_run:
        print("DRY RUN — nothing written.")
        return

    if to_insert:
        def _merge(disk):
            for region, rows in to_insert.items():
                ex = disk.get(region, [])
                ex_ids = set(t["id"] for t in ex)
                for t in rows:
                    if t["id"] not in ex_ids:
                        ex.append(t)
                        ex_ids.add(t["id"])
                disk[region] = ex
        locked_update(_merge)
        for region, rows in to_insert.items():
            register_tracks(rows, region=region, source="bandcamp")
        print(f"inserted {matched} tracks")
    json.dump(state, open(STATE_PATH, "w"))


if __name__ == "__main__":
    main()
