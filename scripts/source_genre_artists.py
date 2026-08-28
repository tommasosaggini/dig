#!/usr/bin/env python3
"""
DIG — find out who plays the genres Dig cannot serve.

Walks genre_vocabulary worst-coverage-first and asks MusicBrainz, Wikidata and
Discogs who plays each one, caching every answer. Produces no tracks; it
produces the artist NAMES that the artist-anchored ingest then resolves. The
split matters: names are cheap, free and un-metered, while resolving them costs
platform requests, so we buy all the names first and spend the expensive calls
knowing what we are aiming at.

RESUMABLE AND BOUNDED, because this is a 2,000-genre job against three shared
services. Every (genre, source) attempt is recorded, so a second run continues
rather than repeats. --max-minutes stops cleanly on a wall clock so it can be
handed a window. A source that is DOWN is retried next run; a source that
answered "nobody" is not.

    python3 scripts/source_genre_artists.py --limit 50
    python3 scripts/source_genre_artists.py --max-minutes 90
    python3 scripts/source_genre_artists.py --genre kuduro
"""
import argparse
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import genre_artists, genre_vocab


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100,
                    help="max genres this run")
    ap.add_argument("--max-minutes", type=float, default=0,
                    help="stop cleanly after this long (0 = no limit)")
    ap.add_argument("--max-tracks", type=int, default=0,
                    help="treat genres with <= this many tracks as underserved")
    ap.add_argument("--genre", help="source one named genre and exit")
    ap.add_argument("--redo", action="store_true",
                    help="re-ask sources that already answered")
    args = ap.parse_args()

    genre_vocab.ensure_genre_schema()
    genre_artists.ensure_artist_schema()

    if args.genre:
        r = genre_artists.source_genre(args.genre, skip_done=not args.redo)
        print(f"  {args.genre}: {r}")
        return

    gaps = genre_vocab.underserved(limit=args.limit * 4, max_tracks=args.max_tracks)
    if not gaps:
        print("  no underserved genres — every genre in the vocabulary has tracks.")
        return

    # Skip genres every source has already answered for, without asking the
    # network. Over-fetching the candidate list above and filtering here keeps
    # the run moving forward instead of re-walking finished ground.
    done_counts = genre_artists.sourced_counts()      # one query, not one per genre
    todo = []
    for g in gaps:
        if args.redo or done_counts.get(g["genre"], 0) < 3:
            todo.append(g)
        if len(todo) >= args.limit:
            break
    if not todo:
        print(f"  the worst {len(gaps)} genres have all been sourced already; "
              f"raise --limit or --max-tracks to go further down the list.")
        return

    print(f"  sourcing {len(todo)} genres (of {len(gaps)} underserved seen)\n")
    t0 = time.time()
    tot_added = 0
    outages = {}
    for i, g in enumerate(todo, 1):
        if args.max_minutes and (time.time() - t0) / 60.0 >= args.max_minutes:
            print(f"\n  time budget reached after {i - 1} genres — stopping "
                  f"cleanly, state saved.")
            break
        r = genre_artists.source_genre(g["genre"], skip_done=not args.redo)
        tot_added += r["added"]
        for s in r["unavailable"]:
            outages[s] = outages.get(s, 0) + 1
        by = " ".join(f"{k}={v}" for k, v in sorted(r["by_source"].items()))
        flag = "  !" + ",".join(r["unavailable"]) if r["unavailable"] else ""
        print(f"  [{i:>4}/{len(todo)}] {g['genre'][:34]:36} +{r['added']:<3} {by}{flag}")

    s = genre_artists.sourcing_summary()
    mins = (time.time() - t0) / 60.0
    print(f"\n  {tot_added} artist rows added in {mins:.1f} min")
    print(f"  cumulative: {s['artists']} artists across {s['genres_with_artists']} "
          f"genres | {s['genres_asked']} genres asked | {s['awaiting']} awaiting ingest")
    if outages:
        print("  sources that went unavailable (will be retried next run): "
              + ", ".join(f"{k}×{v}" for k, v in outages.items()))


if __name__ == "__main__":
    main()
