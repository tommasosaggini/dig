#!/usr/bin/env python3
"""
DIG — turn scraped curator captions into artists the pool will actually ingest.

Takes the JSON from scrape_ig_curator.py and resolves every named artist
against MusicBrainz. Two jobs, one pass:

  1. SETTLE THE ORIENTATION. A caption reading "A – B" does not say which side
     is the artist, and curators are not consistent even with themselves:
     lyon__beatsonandon writes "Colourful Environment – Gboyega Adelaja"
     (track first) and "Gino Paoli – La Gatta" (artist first) in the same feed.
     The scraper marks those rows `assumed`; this asks MusicBrainz which side
     is a real artist and flips the row when the evidence says so. Rows the
     caption itself settled (`stated`, via "X by Y") are trusted, not re-litigated.

  2. ENRICH. lib.mb_resolve.resolve_artist writes each confident match into
     `mb_artists` — the same staging table the country enumerator fills — so
     scripts/ingest_mb_artists.py drains it into `tracks` on its hourly cron
     like any other artist. That path costs ZERO Spotify calls, which matters:
     ingest_curator.py resolves through Spotify's /search, and dev-mode quota
     has been answering 429 with Retry-After measured in hours.

NOTE: resolving IS enriching. mb_resolve.resolve_artist stages every confident
match in `mb_artists` itself — that is what it is for — so there is no version
of this that looks things up without writing them. `--dry-run` therefore does
no lookups at all: it prints the rows and says which ones are still guesses.

    python3 scripts/resolve_curator_artists.py --in cands.json --dry-run
    python3 scripts/resolve_curator_artists.py --in cands.json
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
from lib import mb_resolve


def _resolve(name, cache, country=None):
    """resolve_artist with a per-run memo — a curator names an artist twice.

    The country comes from the caption's flag emoji and is what separates the
    Latvian Zodiac from the three others MusicBrainz knows, so it is part of
    the memo key: the same name under a different flag is a different question.
    """
    key = ((name or "").strip().lower(), (country or "").upper())
    if key not in cache:
        try:
            cache[key] = mb_resolve.resolve_artist(name, country=country)
        except Exception as e:
            print(f"    (lookup failed for {name!r}: {e})")
            cache[key] = None
    return cache[key]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True)
    ap.add_argument("--dry-run", action="store_true",
                    help="no lookups at all — list the rows and their confidence")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    rows = json.load(open(args.infile, encoding="utf-8"))
    if args.limit:
        rows = rows[:args.limit]
    print(f"{len(rows)} candidates from {args.infile}\n")

    if args.dry_run:
        by_orient = {}
        for r in rows:
            by_orient.setdefault(r.get("orient", "n/a"), []).append(r)
        for k, v in sorted(by_orient.items()):
            print(f"  {k}: {len(v)}")
            for r in v[:4]:
                print(f"      {r['artist'][:36]:36}  {r['track'][:28]}")
        print("\n  (dry run — no lookups made, nothing staged)")
        return

    cache = {}
    flipped = confirmed = unresolved = 0
    out = []
    for r in rows:
        artist, track = r.get("artist") or "", r.get("track") or ""
        cc = r.get("country")
        hit = _resolve(artist, cache, cc)
        # A row the caption did not settle gets BOTH sides looked up and the
        # better-evidenced one wins. Asking only "did this side resolve?" is
        # not enough: MusicBrainz has an artist called "La Gatta", so the
        # scraper's guess at "Gino Paoli – La Gatta" resolved, the flip never
        # fired, and the song was filed as the performer.
        #
        # The caption's own flag breaks the tie, which is why it is worth
        # parsing: "Uhuru" and "Sankomota" are both real artists, but only
        # Sankomota is in Lesotho and the caption says 🇱🇸.
        if r.get("orient") == "assumed":
            other = _resolve(track, cache, cc)

            def _agrees(h):
                return bool(h) and bool(cc) and \
                    (h.get("country") or "").upper() == cc.upper()

            # ONE way to earn a flip: the other side matches the flag the
            # curator wrote and this side does not. Corroboration or nothing.
            #
            # "This side didn't resolve, the other did" reads like evidence and
            # is not, because failing to resolve is the normal state for the
            # artists this account posts about. On "Disco Hindú – Lichy y Los
            # Ángeles" the real artist is in no database and the TITLE matched
            # a Ukrainian act, so that rule confidently filed the song as the
            # performer. Same for "T.P. Orchestre Poly-Rythmo – Aihe Ni Kpe
            # We", where the title matched someone Japanese.
            #
            # Without a flag there is nothing to corroborate with, so the
            # curator's own convention — learned from his "by" posts — stands.
            if other and _agrees(other) and not _agrees(hit):
                artist, track = track, artist
                hit = other
                flipped += 1
                r["orient"] = "flipped-by-musicbrainz"
        r["artist"], r["track"] = artist, track
        if hit:
            confirmed += 1
            r["mbid"] = hit.get("mbid")
            r["mb_country"] = hit.get("country")
            r["spotify_id"] = hit.get("spotify_id")
            mark = "✓" if hit.get("spotify_id") else "·"
            print(f"  {mark} {artist[:38]:38}  {track[:30]:30}  "
                  f"{hit.get('country') or r.get('country') or '--'}"
                  f"{'  [no spotify link]' if not hit.get('spotify_id') else ''}")
        else:
            unresolved += 1
            print(f"  ✗ {artist[:38]:38}  {track[:30]:30}  not in MusicBrainz")
        out.append(r)

    print(f"\n  {confirmed} artist(s) resolved, {flipped} row(s) flipped on "
          f"evidence, {unresolved} not found")
    with_sp = sum(1 for r in out if r.get("spotify_id"))
    print(f"  {with_sp} carry a Spotify link — those are the ones "
          f"ingest_mb_artists.py can drain into the pool")
    path = args.infile.replace(".json", "") + ".resolved.json"
    json.dump(out, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"  staged in mb_artists; wrote {path}")


if __name__ == "__main__":
    sys.exit(main() or 0)
