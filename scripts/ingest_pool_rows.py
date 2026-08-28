#!/usr/bin/env python3
"""
DIG — ingest already-resolved pool rows from a JSON file.

THE MISSING LAST STEP. Two lanes resolve real tracks and then stop:

    scrape_ig_curator.py  ─┐
    scrape_nts_show.py    ─┴─> resolve_curator_bandcamp.py --out rows.json
                                                                    ???

`resolve_curator_bandcamp.py` writes pool rows in exactly the shape
`bandcamp.discover()` produces and nothing consumes them — every other
ingest script crawls its own source, so none of them takes a rows file.
Checked rather than assumed on 2026-08-17: `nts_rotational_resolved.json`
had been sitting resolved on disk with 0 of its rows in the pool. Work
that resolves correctly and lands nowhere reads exactly like work that
was never run, which is the failure this script exists to close.

Everything goes through `locked_update`, so these rows get the same
treatment as any other ingest and nothing here is a second code path:
the advisory lock, the artist cap (by NAME, so a curator naming one act
twelve times still lands three), region canonicalisation, and the
same-source name-key dedup all apply for free.

Rows are expected to carry at least {id, name, artist, source}. `region`
is whatever the resolver knew — often the curator's own words — and is
canonicalised downstream by _upsert_track, not here, so there is one
region vocabulary rather than two.

Dry-run by default.

  python3 scripts/ingest_pool_rows.py --in rows.json                 # report
  python3 scripts/ingest_pool_rows.py --in rows.json --apply
  python3 scripts/ingest_pool_rows.py --in rows.json --query curator:doubleudiego --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.env import load_env      # noqa: E402
load_env()

from lib.artist_db import register_tracks       # noqa: E402
from lib.db import fetchall                     # noqa: E402
from lib.discovery_lock import locked_update    # noqa: E402
from lib.region_norm import canonical_region    # noqa: E402
from lib.track_filter import is_trash           # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--query", help="override the `query` tag on every row "
                                    "(provenance — how this got into the pool)")
    ap.add_argument("--apply", action="store_true", help="actually write")
    args = ap.parse_args()

    with open(args.inp, encoding="utf-8") as fh:
        rows = json.load(fh)
    if not isinstance(rows, list):
        print("expected a JSON list of pool rows")
        return 1
    print(f"{len(rows)} rows in {os.path.basename(args.inp)}")

    ids = [r["id"] for r in rows if r.get("id")]
    existing = {r["id"] for r in fetchall(
        "SELECT id FROM tracks WHERE id = ANY(%s)", (ids,))} if ids else set()

    keep, skipped = [], {"no_id": 0, "already_in_pool": 0, "trash": 0,
                         "dup_in_file": 0}
    seen = set()
    for r in rows:
        tid = r.get("id")
        if not tid:
            skipped["no_id"] += 1
            continue
        if tid in existing:
            skipped["already_in_pool"] += 1
            continue
        if tid in seen:
            skipped["dup_in_file"] += 1
            continue
        if is_trash(r.get("name") or "", r.get("artist") or "",
                    r.get("album") or ""):
            skipped["trash"] += 1
            continue
        seen.add(tid)
        row = dict(r)
        if args.query:
            row["query"] = args.query
        row.setdefault("query", "pool_rows")
        keep.append(row)

    print(f"  to insert: {len(keep)}")
    for k, v in skipped.items():
        if v:
            print(f"  skipped {k}: {v}")
    for r in keep[:12]:
        print(f"    {(r.get('region') or '—')[:16]:18} "
              f"{(r.get('artist') or '')[:28]:30} {(r.get('name') or '')[:34]}")
    if len(keep) > 12:
        print(f"    … and {len(keep) - 12} more")

    if not keep:
        print("\nnothing to do")
        return 0
    if not args.apply:
        print("\ndry run — nothing written. Re-run with --apply.")
        return 0

    # Group by the region the row claims, because that is the shape
    # locked_update's modify_fn hands back. Canonicalise the KEY here so two
    # spellings of one place don't become two buckets; _upsert_track
    # canonicalises the stored column itself.
    by_region = {}
    for r in keep:
        key = canonical_region(r.get("region") or "") or "Unknown"
        by_region.setdefault(key, []).append(r)

    def _merge(data):
        for region, ts in by_region.items():
            data.setdefault(region, []).extend(ts)

    locked_update(_merge)

    # Confirm from the DB rather than from what we asked for: the artist cap
    # inside locked_update silently drops rows on purpose, so "we sent 40"
    # is not "40 landed" and printing the former would be a false success.
    landed = {r["id"] for r in fetchall(
        "SELECT id FROM tracks WHERE id = ANY(%s)", ([r["id"] for r in keep],))}
    print(f"\nsent {len(keep)} | landed {len(landed)} | "
          f"dropped {len(keep) - len(landed)} (artist cap / dedup)")

    try:
        register_tracks([r for r in keep if r["id"] in landed])
    except Exception as e:
        print(f"  (artist registry not updated: {type(e).__name__}: {e})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
