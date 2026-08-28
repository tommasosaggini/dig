#!/usr/bin/env python3
"""
DIG — feed a curator's vocabulary back into the discovery seed system.

Curators know things the databases do not. Bubbling is the proof: MusicBrainz
has 3 artists for it, Wikidata has the wrong entity entirely, and English
Wikipedia has no article — while one NTS residency yielded twenty bubbling
tracks by name. Where the formal sources are thinnest, curators are richest, so
a curator scrape should not end as a one-off import. Its artists become genre
seeds like any other, and its genre NAMES extend the vocabulary.

Takes the candidates JSON that scrape_nts_show.py and scrape_ig_curator.py
already write — `{"artist": ..., "track": ..., "style": ...}` — and writes:

  * genre_artists rows, source='curator', so ingest_genre_artists looks the
    artist up on Bandcamp like any MusicBrainz or Discogs seed
  * genre_vocabulary rows for styles the world list does not carry, source
    'curator', so coverage starts being tracked for them at all

Genres are matched against the existing vocabulary by normalised key first, so
"Tamil Film Music" lands on MusicBrainz's spelling rather than creating a
near-duplicate row beside it.

    python3 scripts/curator_to_genre_seeds.py --in nts_rotational.json
    python3 scripts/curator_to_genre_seeds.py --in nts_rotational.json --dry-run
"""
import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import genre_artists, genre_vocab
from lib.db import fetchall, get_conn


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--source", default="curator")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    genre_vocab.ensure_genre_schema()
    genre_artists.ensure_artist_schema()

    with open(args.inp, encoding="utf-8") as f:
        cands = json.load(f)

    # Existing vocabulary, keyed, so a curator's spelling attaches to the
    # canonical genre instead of forking it.
    vocab = {r["genre_key"]: r["genre"] for r in
             fetchall("SELECT genre, genre_key FROM genre_vocabulary")}

    pairs, new_genres = set(), {}
    for c in cands:
        artist = genre_artists._usable(c.get("artist") or "")
        style = (c.get("style") or "").strip()
        if not artist or not style:
            continue
        # A curator's style field can carry several ("Dancehall, Jungle,
        # Bubbling") — each is a genre in its own right, and the artist plays
        # all of them as far as this episode is concerned.
        for part in [p.strip() for p in style.split(",") if p.strip()]:
            key = genre_vocab.genre_key(part)
            if not key:
                continue
            canonical = vocab.get(key)
            if not canonical:
                canonical = part
                new_genres[key] = part
            pairs.add((canonical, artist))

    print(f"  {len(cands)} candidates -> {len(pairs)} (genre, artist) seeds")
    if new_genres:
        print(f"  {len(new_genres)} genre(s) not in the world vocabulary, "
              f"adding as '{args.source}': {', '.join(sorted(new_genres.values()))}")
    by_genre = {}
    for g, a in sorted(pairs):
        by_genre.setdefault(g, []).append(a)
    for g, arts in sorted(by_genre.items()):
        print(f"    {g[:26]:28} {len(arts):>3} artists  {', '.join(arts[:3])[:52]}")

    if args.dry_run:
        print("\n  DRY RUN — nothing written.")
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            if new_genres:
                execute_values(
                    cur,
                    "INSERT INTO genre_vocabulary (genre, genre_key, source) "
                    "VALUES %s ON CONFLICT (genre) DO NOTHING",
                    [(v, k, args.source) for k, v in new_genres.items()])
            execute_values(
                cur,
                "INSERT INTO genre_artists (genre, artist, source) VALUES %s "
                "ON CONFLICT (genre, artist, source) DO NOTHING",
                [(g, a, args.source) for g, a in sorted(pairs)], page_size=200)
        conn.commit()
    finally:
        conn.close()
    print(f"\n  wrote {len(pairs)} seeds; ingest_genre_artists will pick them up.")
    genre_vocab.refresh_coverage()


if __name__ == "__main__":
    main()
