#!/usr/bin/env python3
"""
DIG — rebuild pool rows for curator tracks that resolved and never landed.

`resolve_curator_bandcamp.py` records every hit in `.curator_bandcamp_state.json`
as {caption_line: "bc:<band>:<track>"} and writes the full rows to whatever
`--out` the run was given. For most of its history nothing consumed those
files (see scripts/ingest_pool_rows.py) and they were not kept, so the state
file is the only surviving record: **523 of 538 resolved ids were not in the
pool** on 2026-08-17. The ids are enough — `tralbum_details` sells the rest
back.

WHY THIS IS BETTER THAN RE-RESOLVING. Re-running the resolver would re-ask
Bandcamp's search by name and re-roll the same match lottery. Going by id
asks about the exact track that was already matched, and the tralbum payload
carries MORE than search results do:

  * `location` → region, via location_to_country (search returns none)
  * `tags` → genres, via normalize_genres (search returns none)
  * `release_date` → year (search returns none)
  * a streaming URL, which PROVES the row is playable — the one thing a
    stranded id cannot tell us and the thing that makes a pool row worthless
    if it is false. Non-streamable hits are dropped here rather than parked
    in the pool to fail at play time.

RE-GATED, NOT TRUSTED. These ids were matched by the OLD loose artist rule,
which accepted a remixer's account as the artist ("David Bowie - Heroes
(Mindsodt-D ReWork)" for "David Bowie") — 8 of 23 hits in the doubleudiego
batch were re-uploads. So every row is re-checked against the caption it came
from with the current gates before it is offered for ingest. Rehydrating
without re-gating would launder the old bug into the pool.

The asked artist is recovered by splitting the caption line on its FIRST
" - ", which is how resolve_curator_bandcamp built it (f"{artist} - {track}").
An artist whose own name contains " - " is read wrong; that costs a skipped
row, never a wrong one, because a mis-split artist fails the gate.

PACING. tralbum_details rides lib.bandcamp's RESOLVE lane (0.3s), which is
sized for a listener waiting on a play. This is a bulk sweep, so it paces
itself well above that — a block here costs 2h for everyone (see
BLOCK_COOLDOWN_SECONDS).

Resumable and dry-run by default. Output feeds ingest_pool_rows.py.

  python3 scripts/rehydrate_stranded_curator_rows.py --limit 20
  python3 scripts/rehydrate_stranded_curator_rows.py --out rows.json --limit 600
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

from lib.env import load_env      # noqa: E402
load_env()

from lib import bandcamp                     # noqa: E402
from lib.db import fetchall                  # noqa: E402
from lib.track_filter import is_trash        # noqa: E402

STATE_PATH = os.path.join(ROOT, ".curator_bandcamp_state.json")
DONE_PATH = os.path.join(ROOT, ".curator_rehydrate_state.json")


def _split_raw(raw):
    """"Artist - Track" -> (artist, track). See the module note on the split."""
    for sep in (" - ", " – ", " — "):
        if sep in raw:
            a, t = raw.split(sep, 1)
            return a.strip(), t.strip()
    return raw.strip(), ""


def _load_done():
    try:
        with open(DONE_PATH) as fh:
            return set(json.load(fh).get("done") or [])
    except Exception:
        return set()


def _save_done(done):
    tmp = DONE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"done": sorted(done)}, fh)
    os.replace(tmp, DONE_PATH)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", help="write pool rows here (otherwise dry-run)")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--pace", type=float, default=1.8,
                    help="seconds between calls; well above the resolve lane's "
                         "0.3s because this is a bulk sweep")
    ap.add_argument("--fresh", action="store_true", help="ignore resume state")
    args = ap.parse_args()

    with open(STATE_PATH) as fh:
        hits = json.load(fh).get("hits") or {}
    ids = list(hits.values())
    inpool = {r["id"] for r in fetchall(
        "SELECT id FROM tracks WHERE id = ANY(%s)", (ids,))} if ids else set()
    done = set() if args.fresh else _load_done()

    todo = [(raw, bid) for raw, bid in hits.items()
            if bid not in inpool and bid not in done]
    print(f"{len(hits)} resolved | {len(inpool)} already in pool | "
          f"{len(done)} already attempted | {len(todo)} stranded "
          f"| this run: {min(args.limit, len(todo))}\n")

    rem = bandcamp.cooldown_remaining()
    if rem:
        print(f"Bandcamp cooldown active — {rem}s left. Nothing attempted.")
        return 0

    rows = []
    counts = {"not_streamable": 0, "regated_out": 0, "trash": 0, "error": 0}
    for i, (raw, bid) in enumerate(todo[:args.limit]):
        band_id, track_id = bandcamp.parse_id(bid)
        if not band_id:
            counts["error"] += 1
            done.add(bid)
            continue
        if i:
            time.sleep(args.pace)
        try:
            d = bandcamp.resolve_stream(band_id, track_id)
        except bandcamp.BandcampBlocked as e:
            print(f"\n  !! Bandcamp pushed back ({e}) — stopping, state saved. !!")
            break
        except Exception as e:
            counts["error"] += 1
            done.add(bid)
            print(f"  ERR  {raw[:44]:46} {type(e).__name__}")
            continue
        done.add(bid)
        if not d.get("ok"):
            counts["not_streamable"] += 1
            continue

        asked_a, asked_t = _split_raw(raw)
        got_a, got_t = d.get("artist") or "", d.get("title") or ""
        # The old loose rule is why these need re-checking at all.
        if not bandcamp.is_same_artist(asked_a, got_a) \
                or bandcamp._gained_a_bootleg_mark(asked_t, got_t) \
                or bandcamp._title_restates_the_artist(asked_a, got_t):
            counts["regated_out"] += 1
            print(f"  gate {asked_a[:22]:24} — {asked_t[:22]:24} -> {got_a[:24]}")
            continue
        if is_trash(got_t, got_a, ""):
            counts["trash"] += 1
            continue

        loc = d.get("location") or ""
        year = d.get("release_year") or ""
        rows.append({
            "id": bid,
            "name": got_t,
            "artist": got_a,
            "album": "",
            "art": d.get("art") or "",
            "genres": bandcamp.normalize_genres(
                d.get("tags") or [], bandcamp.location_tokens(loc)),
            "region": bandcamp.location_to_country(loc) or "",
            "location": loc,
            "year": year,
            "decade": (year[:3] + "0s") if year else "",
            "duration": d.get("duration") or 0,
            "source": "bandcamp",
        })
        print(f"  OK   {got_a[:26]:28} {got_t[:28]:30} "
              f"{(bandcamp.location_to_country(loc) or '—')[:16]:18} {year}")

    _save_done(done)
    attempted = len(rows) + sum(counts.values())
    print(f"\n  attempted {attempted} | rebuilt {len(rows)} | " +
          " | ".join(f"{k} {v}" for k, v in counts.items() if v))
    print(f"  remaining stranded: {max(0, len(todo) - attempted)}")

    if args.out and rows:
        prev = []
        if os.path.exists(args.out):
            try:
                with open(args.out, encoding="utf-8") as fh:
                    prev = json.load(fh)
            except Exception:
                prev = []
        seen = {r["id"] for r in prev}
        prev.extend(r for r in rows if r["id"] not in seen)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(prev, fh, ensure_ascii=False, indent=1)
        print(f"  wrote {len(prev)} pool rows -> {args.out}")
    elif not args.out:
        print("  (dry run — pass --out to save)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
