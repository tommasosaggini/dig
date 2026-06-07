#!/usr/bin/env python3
"""
DIG — Bandcamp ingest.

Fills the pool with fully-streamable Bandcamp tracks by enumerating Bandcamp's
own discover taxonomy (genre x page x sort) and taking ONE signature (featured)
track per release. This is the non-hardcoded diversity engine: we walk every
genre instead of hand-writing artist/genre search strings, and Bandcamp leans
experimental, so it broadens the pool well beyond Spotify's shape.

Costs ZERO Spotify quota — entirely free of the drip-lane (lib/spotify_gate).
Stream URLs are resolved fresh at PLAY time (see lib/bandcamp.resolve_stream),
so we store only the stable identity 'bc:<band_id>:<track_id>' plus art/genre/
location metadata.

Usage:
  python scripts/ingest_bandcamp.py [--genres ambient,jazz,...] [--pages 2]
      [--sorts top,new] [--limit 500] [--pace 1.0] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

ENV_PATH = os.path.join(ROOT, ".env")
if os.path.exists(ENV_PATH):
    for _line in open(ENV_PATH):
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

from lib import bandcamp
from lib.track_filter import is_trash
from lib.discovery_lock import locked_update
from lib.artist_db import register_tracks
from lib.db import fetchall

STATE_PATH = os.path.join(ROOT, ".bandcamp_ingest_state.json")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--genres", default="", help="comma list; default = all top-level genres")
    ap.add_argument("--pages", type=int, default=2, help="pages per genre/sort (~48 releases each)")
    ap.add_argument("--sorts", default="top,new", help="comma list: top,new,rec")
    ap.add_argument("--limit", type=int, default=500, help="max NEW tracks to ingest this run")
    ap.add_argument("--max-calls", type=int, default=0, help="hard cap on discover HTTP calls this run (0=unlimited); keep low for cron trickle")
    ap.add_argument("--pace", type=float, default=1.0, help="legacy; pacing is now enforced in lib.bandcamp (the gate)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    genres = [g.strip() for g in args.genres.split(",") if g.strip()] or bandcamp.GENRES
    sorts = [s.strip() for s in args.sorts.split(",") if s.strip()]

    # Resume: remember which (genre,sort,page) cells we've already swept.
    state = {"swept": []}
    if os.path.exists(STATE_PATH):
        try:
            state = json.load(open(STATE_PATH))
        except Exception:
            pass
    swept = set(tuple(x) for x in state.get("swept", []))

    # Back off cleanly if Bandcamp recently pushed back.
    rem = bandcamp.cooldown_remaining()
    if rem > 0:
        print(f"  Bandcamp in cooldown for {rem}s — skipping run (backing off).")
        return

    existing_ids = set(r["id"] for r in fetchall("SELECT id FROM tracks"))
    print(f"  pool has {len(existing_ids)} tracks | genres={len(genres)} sorts={sorts} pages={args.pages}\n")

    to_insert = {}        # region -> [track dicts]
    seen_ids = set()
    matched = dup = trash = nostream = 0
    calls = 0

    stop = False
    for g in genres:
        if stop:
            break
        for s in sorts:
            if stop:
                break
            for p in range(args.pages):
                cell = (g, s, p)
                if cell in swept:
                    continue
                try:
                    # Pacing/jitter is enforced inside lib.bandcamp (the gate);
                    # no extra sleep here.
                    rows = bandcamp.discover(g, page=p, sort=s)
                    calls += 1
                except bandcamp.BandcampBlocked as e:
                    # Bandcamp pushed back — STOP the whole run immediately. The
                    # gate has recorded a cooldown so later runs no-op until it
                    # clears. Never keep hammering.
                    print(f"  !! Bandcamp pushed back ({e}) — STOPPING run, backing off. !!")
                    stop = True
                    break
                except Exception as e:
                    print(f"  ! discover {g}/{s}/p{p} failed: {type(e).__name__} {str(e)[:80]}")
                    continue
                swept.add(cell)
                for tr in rows:
                    tid = tr["id"]
                    if tid in existing_ids or tid in seen_ids:
                        dup += 1
                        continue
                    if not tr.get("name") or not tr.get("artist"):
                        nostream += 1
                        continue
                    if is_trash(tr["name"], tr["artist"], tr.get("album", "")):
                        trash += 1
                        continue
                    tr["query"] = f"bandcamp:{g}"
                    seen_ids.add(tid)
                    to_insert.setdefault(tr["region"] or "Unknown", []).append(tr)
                    matched += 1
                    if args.dry_run and matched <= 40:
                        print(f"  ✓ {tr['artist'][:26]:28} - {tr['name'][:30]:32} | {g:12} {tr['region']}")
                print(f"  {g:12} {s:4} p{p}: +{len(rows)} seen (run total new={matched}, calls={calls})")
                if matched >= args.limit or (args.max_calls and calls >= args.max_calls):
                    stop = True
                    break

    total_new = sum(len(v) for v in to_insert.values())
    print(f"\n  calls={calls} | NEW={matched} dup={dup} trash={trash} skip={nostream}")
    print(f"  NEW tracks to insert: {total_new} across {len(to_insert)} region buckets")

    if args.dry_run:
        print("\n  DRY RUN — nothing written.")
        return

    if to_insert:
        def _merge(disk):
            for region, rows in to_insert.items():
                ex = disk.get(region, [])
                ex_ids = set(t["id"] for t in ex)
                for t in rows:
                    if t["id"] not in ex_ids:
                        ex.append(t); ex_ids.add(t["id"])
                disk[region] = ex
        locked_update(_merge)
        for region, rows in to_insert.items():
            register_tracks(rows, region=region, source="bandcamp")
        print(f"  inserted {total_new} tracks")

    state["swept"] = [list(x) for x in swept]
    json.dump(state, open(STATE_PATH, "w"))
    print(f"  state saved ({len(swept)} cells swept)")


if __name__ == "__main__":
    main()
