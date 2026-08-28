#!/usr/bin/env python3
"""
DIG — refresh the world genre vocabulary and recount Dig's coverage of it.

Answers the only question that matters about the discovery engine: of every
genre humans are known to make, how many can Dig actually serve? Before this
existed the answer was unknown, and the unknown was hiding a 20%.

Run it after any ingest. Cheap: one MusicBrainz request plus one pass over the
pool's genre labels — no per-genre lookups, nothing metered.

    python3 scripts/sync_genre_vocab.py
    python3 scripts/sync_genre_vocab.py --gaps 60     # print the work queue
    python3 scripts/sync_genre_vocab.py --no-fetch    # recount only, offline
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import genre_vocab


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-fetch", action="store_true",
                    help="skip MusicBrainz; recount coverage of what is stored")
    ap.add_argument("--gaps", type=int, default=30,
                    help="how many zero-coverage genres to print")
    args = ap.parse_args()

    genre_vocab.ensure_genre_schema()

    if args.no_fetch:
        print("  (--no-fetch: keeping the stored vocabulary)")
    else:
        try:
            genres = genre_vocab.fetch_world_genres()
        except Exception as e:
            print(f"  ! MusicBrainz unreachable ({type(e).__name__}) — "
                  f"recounting the stored vocabulary instead.")
            genres = None
        if genres:
            added, total = genre_vocab.sync_vocabulary(genres)
            print(f"  vocabulary: {total} genres ({added:+d} new this run)")

    genre_vocab.refresh_coverage()
    s = genre_vocab.coverage_summary()
    total = s["total"] or 1
    print()
    print(f"  world genres          {s['total']:>6}")
    print(f"  served (10+ tracks)   {s['covered']:>6}  "
          f"({100 * s['covered'] // total}%)   <- the number to move")
    print(f"  thin (1-9 tracks)     {s['thin']:>6}")
    print(f"  ZERO tracks           {s['zero']:>6}  "
          f"({100 * s['zero'] // total}%)")

    gaps = genre_vocab.underserved(limit=args.gaps)
    if gaps:
        print(f"\n  next {len(gaps)} gaps (the discovery work queue):")
        for g in gaps:
            print(f"    {g['genre']}")


if __name__ == "__main__":
    main()
