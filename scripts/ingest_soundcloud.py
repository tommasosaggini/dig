#!/usr/bin/env python3
"""
DIG — SoundCloud ingest.

Fills the pool with streamable SoundCloud tracks by enumerating genres and
searching (access=playable). SoundCloud hides a lot of niche electro/techno that
exists nowhere else, so this broadens the pool well beyond Spotify/Bandcamp.

Costs ZERO Spotify quota. Stream URLs are resolved fresh at PLAY time
(lib/soundcloud.get_stream) — SoundCloud ToS forbids downloading, so we store
ONLY the stable identity 'sc:<track_id>' plus art/genre metadata. This is why
SoundCloud never feeds the Instagram clip pipeline.

Requires SOUNDCLOUD_CLIENT_ID / SOUNDCLOUD_CLIENT_SECRET in .env.

Usage:
  python scripts/ingest_soundcloud.py [--genres techno,house,...] [--limit 400]
      [--per-genre 50] [--pace 1.0] [--dry-run]
"""
from __future__ import annotations

import argparse
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

from lib import soundcloud
from lib.track_filter import is_trash
from lib.discovery_lock import locked_update
from lib.artist_db import register_tracks
from lib.db import fetchall


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--genres", default="", help="comma list; default = lib.soundcloud.GENRES")
    ap.add_argument("--per-genre", type=int, default=50, help="tracks fetched per genre (max 200)")
    ap.add_argument("--limit", type=int, default=400, help="max NEW tracks to ingest this run")
    ap.add_argument("--pace", type=float, default=1.0, help="seconds between genre searches")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    try:
        soundcloud._creds()
    except soundcloud.SoundCloudError as e:
        print(f"  {e}")
        print("  Register an app at https://developers.soundcloud.com/docs/api/register-app")
        return

    genres = [g.strip() for g in args.genres.split(",") if g.strip()] or soundcloud.GENRES
    existing_ids = set(r["id"] for r in fetchall("SELECT id FROM tracks"))
    print(f"  pool has {len(existing_ids)} tracks | genres={len(genres)} per-genre={args.per_genre}\n")

    to_insert = {}
    seen_ids = set()
    matched = dup = trash = empty = 0
    stop = False

    for g in genres:
        if stop:
            break
        try:
            rows = soundcloud.discover(g, limit=args.per_genre)
        except soundcloud.SoundCloudError as e:
            print(f"  ! search {g} failed: {e}")
            if "rate limited" in str(e):
                print("  backing off — stopping run.")
                break
            continue
        for tr in rows:
            tid = tr["id"]
            if tid in existing_ids or tid in seen_ids:
                dup += 1
                continue
            if not tr.get("name") or not tr.get("artist"):
                empty += 1
                continue
            if is_trash(tr["name"], tr["artist"], tr.get("album", "")):
                trash += 1
                continue
            tr["query"] = f"soundcloud:{g}"
            seen_ids.add(tid)
            to_insert.setdefault(tr.get("region") or "Unknown", []).append(tr)
            matched += 1
            if args.dry_run and matched <= 40:
                print(f"  ✓ {tr['artist'][:26]:28} - {tr['name'][:30]:32} | {g}")
        print(f"  {g:14}: +{len(rows)} seen (run total new={matched})")
        if matched >= args.limit:
            stop = True
        time.sleep(max(0, args.pace))

    total_new = sum(len(v) for v in to_insert.values())
    print(f"\n  NEW={matched} dup={dup} trash={trash} empty={empty}")
    print(f"  NEW tracks to insert: {total_new}")

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
            register_tracks(rows, region=region, source="soundcloud")
        print(f"  inserted {total_new} tracks")


if __name__ == "__main__":
    main()
