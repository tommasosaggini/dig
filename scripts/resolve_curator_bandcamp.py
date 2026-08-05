#!/usr/bin/env python3
"""DIG — resolve scraped curator candidates to real Bandcamp tracks.

Input is what scripts/scrape_ig_curator.py writes: free-text
`{"artist": ..., "track": ..., "country": ..., "style": ...}` rows lifted from
a curator's captions. This turns each one into a pool row identical in shape to
what `bandcamp.discover()` produces, so nothing downstream can tell the two
apart.

WHY BANDCAMP AND NOT SPOTIFY. Spotify's /search is rate-limited into ~16-22h
bans in Dev Mode, and the id-keyed walk that replaced it needs an id we do not
have. MusicBrainz resolves the NAME reliably — 6/6 on a hand sample — but only
about one artist in six carries a Spotify link in its url-relations. Bandcamp
answers by name, and is the source Dig prefers everywhere anyway.

PRECISION IS THE POINT, not yield. `bandcamp.resolve_track` requires the band
name to agree with the artist we asked for, which is what rejects a Sinatra
bootleg credited to a label and a Tokischa re-upload credited to another. A
wrong match is worse than no match: it puts someone else's music in the pool
under a curator's name and nothing downstream can notice.

WHAT THE SEARCH DOES NOT CARRY. discover() rows come with region, genres and
duration; search results do not. Region and genres are exactly what Dig
stratifies on, so they are filled from the caption where the curator stated
them — dublysm writes `Localisation :` and `Style :` on nearly every post — and
left empty otherwise for the MusicBrainz backfill to pick up later.

Paced by lib.bandcamp's shared gate (4-8s between calls, cross-process) and
resumable, so a run can be stopped and restarted without re-asking anything.
Dry-run by default.

  python3 scripts/resolve_curator_bandcamp.py --in curators_merged.json --limit 60
"""
from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()

from lib import bandcamp
from lib.track_filter import is_trash

STATE_PATH = os.path.join(ROOT, ".curator_bandcamp_state.json")


def _load_state():
    try:
        with open(STATE_PATH) as fh:
            d = json.load(fh)
        return set(d.get("done") or []), {k: v for k, v in (d.get("hits") or {}).items()}
    except Exception:
        return set(), {}


def _save_state(done, hits):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"done": sorted(done), "hits": hits}, fh, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)


def _region_from(cand):
    """The curator's own words for where this is from.

    Left as the caption's string rather than mapped to Dig's region vocabulary
    here: `location_to_country` expects a Bandcamp location line, and inventing
    a second mapping is how two disagreeing region taxonomies start.
    """
    return (cand.get("country") or "").strip()


def _genres_from(cand):
    style = (cand.get("style") or "").strip()
    if not style:
        return []
    # dublysm writes "Dub Techno, Ambient, Minimal <then prose about the act>".
    # Splitting on commas and keeping the short leading items takes the tags and
    # drops the essay that follows them.
    out = []
    for part in style.split(","):
        p = " ".join(part.split())
        if p and len(p.split()) <= 3 and len(p) <= 28:
            out.append(p.lower())
        else:
            break
    return out[:5]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", help="write resolved pool rows here")
    ap.add_argument("--limit", type=int, default=50, help="candidates to attempt this run")
    ap.add_argument("--fresh", action="store_true", help="ignore the resume state")
    args = ap.parse_args()

    with open(args.inp, encoding="utf-8") as fh:
        cands = json.load(fh)
    done, hits = (set(), {}) if args.fresh else _load_state()

    rem = bandcamp.cooldown_remaining()
    if rem:
        print(f"Bandcamp cooldown active — {rem}s left. Nothing attempted.")
        return 0

    todo = [c for c in cands if c.get("raw") not in done]
    print(f"{len(cands)} candidates | {len(done)} already attempted | "
          f"{len(todo)} remaining | this run: {min(args.limit, len(todo))}\n")

    resolved, rejected, trashed, errors = [], 0, 0, 0
    for cand in todo[:args.limit]:
        key = cand.get("raw")
        artist, title = cand.get("artist") or "", cand.get("track") or ""
        try:
            row = bandcamp.resolve_track(artist, title)
        except bandcamp.BandcampBlocked as e:
            # Stop the whole run: hammering during a block only prolongs it,
            # and the state file means nothing attempted so far is lost.
            print(f"\n  !! Bandcamp pushed back ({e}) — stopping, state saved. !!")
            break
        except Exception as e:
            errors += 1
            print(f"  ERR   {artist[:24]:26} — {title[:26]:28} {type(e).__name__}")
            done.add(key)
            continue
        done.add(key)
        if not row:
            rejected += 1
            print(f"  ---   {artist[:24]:26} — {title[:26]}")
            continue
        if is_trash(row["name"], row["artist"], row.get("album", "")):
            trashed += 1
            continue
        row["region"] = _region_from(cand) or row.get("region") or ""
        row["genres"] = _genres_from(cand) or row.get("genres") or []
        row["query"] = cand.get("source") or "curator"
        resolved.append(row)
        hits[key] = row["id"]
        print(f"  HIT   {artist[:24]:26} — {title[:26]:28} -> {row['artist'][:22]:24} / {row['name'][:24]}")

    _save_state(done, hits)
    attempted = len(resolved) + rejected + trashed + errors
    pct = (100.0 * len(resolved) / attempted) if attempted else 0
    print(f"\n  attempted {attempted} | resolved {len(resolved)} ({pct:.0f}%) | "
          f"no-match {rejected} | trash {trashed} | errors {errors}")
    print(f"  cumulative: {len(hits)} resolved of {len(done)} attempted")

    if args.out and resolved:
        prev = []
        if os.path.exists(args.out):
            try:
                with open(args.out, encoding="utf-8") as fh:
                    prev = json.load(fh)
            except Exception:
                prev = []
        seen = {r["id"] for r in prev}
        prev.extend(r for r in resolved if r["id"] not in seen)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(prev, fh, ensure_ascii=False, indent=1)
        print(f"  wrote {len(prev)} pool rows -> {args.out}")
    elif not args.out:
        print("  (dry run — pass --out to write pool rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
