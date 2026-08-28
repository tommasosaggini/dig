#!/usr/bin/env python3
"""
DIG — turn sourced artist names into pool tracks, for the genres Dig cannot serve.

The second half of the coverage backfill. source_genre_artists.py answers "who
plays kuduro" from MusicBrainz, Wikidata and Discogs; this asks Bandcamp what
those specific people have, and files it under the genre that led us to them.

ARTIST-ANCHORED, ALWAYS. Every query is a person's name. No genre word is ever
typed into a platform search here — "mongolian throat singing" as a query
returns whatever ranks for the phrase, and the results are indistinguishable
from discovery while being mostly stereotype compilations. The band-name
agreement rule inside bandcamp.tracks_by_artist is what keeps the anchor
honest: a hit whose band name does not agree with the artist we asked about is
discarded, however well it matches otherwise.

TRUST IS PER (GENRE, SOURCE), NOT PER SOURCE. Ranking the sources globally is
wrong in both directions. MusicBrainz is excellent on acid trance (thirteen
correct acts) and useless on abhang, where its only seed was a German breakcore
act; a lone tag on an obscure genre is one editable field with nothing to
contradict it. So trust_seed() asks how much a source knows about THAT genre
rather than which company holds the data.

The genre written onto a track is the genre that led us to the artist, so a
wrong seed becomes a mislabelled track — and a mislabelled genre is worse than
an empty one, because it pollutes the pool AND makes the coverage number lie.
That is why free-text seeds ended up excluded by default despite often being
RIGHT: see MIN_GENRE_LEN_FOR_TEXT for the measurement that settled it.

WHY BANDCAMP AND NOT YOUTUBE. YouTube's Data API charges 100 quota units per
search against a 10,000/day cap — 100 searches, which is not a backfill. And
bulk yt-dlp scraping risks getting this IP throttled, which would break the
Instagram pipeline's audio resolver, a working thing traded for a speculative
one. Bandcamp is free, self-paced and already trusted here.

    python3 scripts/ingest_genre_artists.py --limit 200
    python3 scripts/ingest_genre_artists.py --max-minutes 120
    python3 scripts/ingest_genre_artists.py --include-weak     # discogs-text too
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
from lib import bandcamp, genre_artists, genre_vocab
from lib.artist_db import register_tracks
from lib.db import fetchall
from lib.discovery_lock import locked_update
from lib.track_filter import is_trash

# A database tag is one editable field. When a source knows only one or two
# artists for a whole genre, that field IS the genre as far as we are concerned,
# and there is nothing to contradict it if it is wrong. Measured: MusicBrainz's
# lone seed for `aak` is Linkin Park and its lone seed for `abhang` is a German
# breakcore act, while for genres where it found 3+ the seeds are right.
MIN_SEEDS_TO_TRUST = 3

# Free-text Discogs seeds are NOT ingested by default any more. Measured on the
# first 352 backfilled tracks: the corroborated sources were ~13/14 correct
# (acousmatic pulled Francis Dhomont, Michel Chion, Kassel Jaeger), while the
# free-text slice was mostly mislabelled —
#
#     balani show -> Cheb Zahouani            (Algerian rai, not Malian)
#     agbadza     -> Amadinda Percussion Grp  (a Cage piece, Hungarian ensemble)
#     3-step      -> Adamski, "3 Steps Ahead" (matched the words "3 steps")
#
# The seeds themselves are often RIGHT — abhang gave Bhimsen Joshi and Kishori
# Amonkar — but Indian classical masters are not on Bandcamp, so the good seeds
# convert to nothing and only the bad ones become tracks. A mislabelled genre
# is worse than an empty one: it pollutes the pool AND makes coverage lie.
# The rows stay in genre_artists for a source that can actually resolve them;
# --include-weak still ingests them for anyone who wants to look.
MIN_GENRE_LEN_FOR_TEXT = 5


def trust_seed(source, genre, seeds_from_source, genre_has_strong_seed,
               include_weak=False):
    """Is this (genre, artist) seed worth a Bandcamp lookup?

    Pure, so the calibration is testable without a database — and it needed
    testing, because the obvious ranking is wrong. See the constants above:
    MusicBrainz beats Discogs free-text on acid trance and loses to it on
    abhang, so the decision is per (genre, source), never per source.
    """
    if source in ("curator", "discogs"):
        return True                     # a DJ played it / a curated style
    if source in ("musicbrainz", "wikidata"):
        return seeds_from_source >= MIN_SEEDS_TO_TRUST
    if source == "discogs-text":
        return include_weak and len(genre) >= MIN_GENRE_LEN_FOR_TEXT
    return False


def candidates(limit, include_weak):
    """Artists worth spending a Bandcamp lookup on, worst-covered genre first.

    Trust is decided per (genre, source) rather than by ranking the sources
    globally, because the ranking inverts on exactly the genres that matter.
    For acid trance MusicBrainz is excellent and Discogs free-text is noise;
    for abhang it is the other way round. What separates them is not which
    company holds the data, it is how much that source knows about THAT genre.

    Filtering happens in Python: the pending set is a few thousand rows, and
    the rule is three conditions that read as English here and as a thicket of
    CTEs in SQL.
    """
    rows = fetchall("""
        SELECT * FROM (
          SELECT DISTINCT ON (a.artist)
                 a.genre, a.artist, a.source, v.track_count
          FROM genre_artists a
          JOIN genre_vocabulary v ON v.genre = a.genre
          WHERE a.ingested_at IS NULL
          ORDER BY a.artist, a.source
        ) c
        ORDER BY c.track_count ASC, c.genre ASC, c.artist ASC
    """)
    # How many artists each source found per genre, and which genres have any
    # corroborated (non-free-text) seed at all.
    found = {(r["genre"], r["source"]): r["found"] for r in fetchall(
        "SELECT genre, source, found FROM genre_sourcing_state")}
    has_strong = {r["genre"] for r in fetchall(
        "SELECT DISTINCT genre FROM genre_artists WHERE source <> 'discogs-text'")}

    trusted = [r for r in rows
               if trust_seed(r["source"], r["genre"],
                             found.get((r["genre"], r["source"]), 0),
                             r["genre"] in has_strong, include_weak)]

    # ROUND-ROBIN ACROSS GENRES, not straight down the list. Every gap genre
    # has exactly zero tracks, so they all tie on coverage and the tie-break is
    # the name — which meant a run spent itself alphabetically: 257 tracks
    # across five genres, all of them beginning "a", while kuduro and bubbling
    # sat untouched with 51 and 26 seeds waiting. Taking a few artists from
    # each genre in turn buys breadth first and depth later, which is the
    # direction the coverage number actually needs to move.
    by_genre = {}
    for r in trusted:
        by_genre.setdefault(r["genre"], []).append(r)
    keep = []
    while len(keep) < limit:
        progressed = False
        for genre in list(by_genre):
            bucket = by_genre[genre]
            if not bucket:
                continue
            keep.append(bucket.pop(0))
            progressed = True
            if len(keep) >= limit:
                break
        if not progressed:
            break
    return keep


# How many artists to buffer before writing. Small enough that an interrupted
# run loses at most this much work, large enough that the pool is not rewritten
# once per artist.
FLUSH_EVERY = 20


def flush(batch, dry_run=False):
    """Write a batch's tracks to the pool, THEN mark its artists ingested.

    That order is the whole point. The first version marked each artist done
    inside the loop and inserted the tracks only after the loop finished, so
    stopping the run early — a timeout, a Bandcamp block, anything — lost every
    track it had found AND left the artists flagged as done, meaning they would
    never be retried. Measured on the first run: 128 artists marked, 0 tracks
    in the pool.
    """
    rows = [t for _, kept in batch for t in kept]
    if rows and not dry_run:
        by_region = {}
        for t in rows:
            by_region.setdefault(t.get("region") or "Unknown", []).append(t)

        def _merge(disk):
            for region, rs in by_region.items():
                ex = disk.get(region, [])
                ex_ids = {t["id"] for t in ex}
                for t in rs:
                    if t["id"] not in ex_ids:
                        ex.append(t)
                        ex_ids.add(t["id"])
                disk[region] = ex
        locked_update(_merge)
        for region, rs in by_region.items():
            register_tracks(rs, region=region, source="bandcamp")
    if not dry_run:
        for c, kept in batch:
            genre_artists.mark_ingested(c["genre"], c["artist"], len(kept))
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=150)
    ap.add_argument("--max-minutes", type=float, default=0)
    ap.add_argument("--include-weak", action="store_true",
                    help="also ingest artists found only by Discogs free-text")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    genre_vocab.ensure_genre_schema()
    genre_artists.ensure_artist_schema()

    rem = bandcamp.cooldown_remaining()
    if rem:
        print(f"  Bandcamp cooldown active — {rem}s left. Nothing attempted.")
        return

    rows = candidates(args.limit, args.include_weak)
    if not rows:
        print("  nothing awaiting ingest (try --include-weak, or source more genres).")
        return
    print(f"  {len(rows)} artists to look up on Bandcamp\n")

    existing = {r["id"] for r in fetchall("SELECT id FROM tracks")}
    t0 = time.time()
    found_rows, hit_artists, empty, trashed = [], 0, 0, 0
    batch, written = [], 0

    for i, c in enumerate(rows, 1):
        if args.max_minutes and (time.time() - t0) / 60.0 >= args.max_minutes:
            print(f"\n  time budget reached after {i - 1} artists — stopping cleanly.")
            break
        try:
            tracks = bandcamp.tracks_by_artist(c["artist"], limit=8)
        except bandcamp.BandcampBlocked as e:
            # Stop the whole run. Hammering through a block only extends it,
            # and nothing attempted so far is lost.
            print(f"\n  !! Bandcamp pushed back ({e}) — stopping, progress saved. !!")
            break
        except Exception as e:
            print(f"  ERR   {c['artist'][:30]:32} {type(e).__name__}")
            continue

        kept = []
        for t in tracks:
            if t["id"] in existing:
                continue
            if is_trash(t["name"], t["artist"], t.get("album", "")):
                trashed += 1
                continue
            # The genre that led us here is the genre this track is filed
            # under — that is the entire point of the backfill, and it is why
            # the artist had to be right.
            t["genres"] = [c["genre"]]
            t["query"] = f"genre-backfill:{c['genre']}"
            existing.add(t["id"])
            kept.append(t)

        if kept:
            hit_artists += 1
            found_rows.extend(kept)
            print(f"  [{i:>4}/{len(rows)}] {c['genre'][:20]:22} {c['artist'][:26]:28} "
                  f"+{len(kept)}")
        else:
            empty += 1
        batch.append((c, kept))
        if len(batch) >= FLUSH_EVERY:
            written += flush(batch, args.dry_run)
            batch = []

    if batch:
        written += flush(batch, args.dry_run)

    print(f"\n  {len(found_rows)} tracks from {hit_artists} artists "
          f"({empty} had nothing on Bandcamp, {trashed} filtered)")
    if args.dry_run:
        print("  DRY RUN — nothing written.")
        return
    if not written:
        return
    print(f"  inserted {written} tracks into the pool")

    genre_vocab.refresh_coverage()
    s = genre_vocab.coverage_summary()
    print(f"  coverage now: {s['covered']} served / {s['thin']} thin / "
          f"{s['zero']} zero  (of {s['total']})")


if __name__ == "__main__":
    main()
