#!/usr/bin/env python3
"""
DIG — resolve genre seed artists through Spotify, for what Bandcamp cannot serve.

THE GAP THIS CLOSES. ingest_genre_artists.py asks Bandcamp what a seeded artist
has, and for 97 genres Bandcamp came back empty — not a bug, just what Bandcamp
is: an indie self-release platform. Pandit Bhimsen Joshi, Kishori Amonkar and
Alim Qasimov are not on it. They ARE on Spotify, properly credited and licensed,
and MusicBrainz carries the link that gets us there.

THE ROUTE, and why it does not touch /search. Spotify removed /search for
Dev Mode apps in November 2024 (see lib/mb_resolve for the measurements), so the
artist id has to come from somewhere else:

    artist name -> MusicBrainz url-relations -> spotify_id   (free, cached)
    spotify_id  -> /artists/{id}/albums -> /albums/{id}/tracks   (still 200)

NO NAME CHECK ON THIS PATH, deliberately — the opposite of the Bandcamp
resolver. There we search by text and the band-name agreement rule is the only
thing stopping a re-uploader answering for the artist. Here we arrive by
Spotify's own artist id, which is a far stronger anchor than any string
comparison, and a name check would actively do harm: Bhimsen Joshi's Spotify
catalogue includes a Japanese release credited "パンディット・ビームセン・ジョーシー",
which is exactly right and which no string match would accept.

QUOTA IS THE SCARCE RESOURCE. Spotify's is shared with the live crons and a
burst costs everyone ~16 hours, so this runs on a small call budget, paces
itself, and stops the moment it is rate-limited rather than pushing through.
MusicBrainz lookups are cached in mb_artists, so a repeated name is free.

    python3 scripts/ingest_genre_artists_spotify.py --limit 40 --dry-run
    python3 scripts/ingest_genre_artists_spotify.py --limit 60 --call-budget 90
"""
import argparse
import importlib.util
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import genre_artists, genre_vocab, mb_resolve
from lib.artist_db import register_tracks
from lib.db import fetchall
from lib.discovery_lock import locked_update
from lib.track_filter import is_trash

# ingest_mb_artists is a script, not a module, but fetch_top_track is exactly
# the id-keyed walk this needs and duplicating it would mean two copies of the
# knowledge about which Spotify endpoints still answer.
_spec = importlib.util.spec_from_file_location(
    "ingest_mb_artists", os.path.join(ROOT, "scripts", "ingest_mb_artists.py"))
_mba = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mba)


def candidates(limit, retry_failed):
    """Seeded artists worth a Spotify lookup, worst-covered genre first.

    Defaults to the ones Bandcamp already tried and found nothing for — that
    is precisely the population this exists to rescue — and round-robins over
    genres so a short run spreads instead of draining one scene.
    """
    where = ("a.ingested_at IS NOT NULL AND a.track_count = 0" if retry_failed
             else "a.ingested_at IS NULL")
    rows = fetchall(f"""
        SELECT * FROM (
          SELECT DISTINCT ON (a.artist) a.genre, a.artist, a.source, v.track_count
          FROM genre_artists a
          JOIN genre_vocabulary v ON v.genre = a.genre
          WHERE {where} AND a.source <> 'discogs-text'
          ORDER BY a.artist, a.source
        ) c
        ORDER BY c.track_count ASC, c.genre ASC, c.artist ASC
    """)
    by_genre = {}
    for r in rows:
        by_genre.setdefault(r["genre"], []).append(r)
    keep = []
    while len(keep) < limit:
        progressed = False
        for g in list(by_genre):
            if by_genre[g]:
                keep.append(by_genre[g].pop(0))
                progressed = True
                if len(keep) >= limit:
                    break
        if not progressed:
            break
    return keep


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--call-budget", type=int, default=80,
                    help="max Spotify calls this run (shared dev-mode quota)")
    ap.add_argument("--pace", type=float, default=1.0)
    ap.add_argument("--max-minutes", type=float, default=0)
    ap.add_argument("--fresh", action="store_true",
                    help="target artists never tried, instead of Bandcamp's misses")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    genre_vocab.ensure_genre_schema()
    genre_artists.ensure_artist_schema()

    rows = candidates(args.limit, retry_failed=not args.fresh)
    if not rows:
        print("  nothing to resolve.")
        return
    print(f"  {len(rows)} artists to look up via MusicBrainz -> Spotify\n")

    existing = {r["id"] for r in fetchall("SELECT id FROM tracks")}
    t0, calls, found, no_link, no_track = time.time(), 0, [], 0, 0

    for i, c in enumerate(rows, 1):
        if args.max_minutes and (time.time() - t0) / 60.0 >= args.max_minutes:
            print(f"\n  time budget reached after {i - 1} artists.")
            break
        if calls >= args.call_budget:
            print(f"\n  Spotify call budget ({args.call_budget}) spent — stopping.")
            break
        try:
            r = mb_resolve.resolve_artist(c["artist"])
        except mb_resolve.MBRateLimited:
            print("\n  MusicBrainz asked us to back off — stopping, state saved.")
            break
        except Exception:
            r = None
        if not r or not r.get("spotify_id"):
            no_link += 1
            continue

        try:
            t = _mba.fetch_top_track(r["spotify_id"], c["artist"], None)
            calls += 1
        except Exception as e:
            # A 429 here is the shared quota; do not push through it.
            print(f"  !! Spotify pushed back ({type(e).__name__}) — stopping. !!")
            break
        time.sleep(args.pace)
        if not t or not t.get("id"):
            no_track += 1
            continue
        if t["id"] in existing:
            continue
        if is_trash(t.get("name", ""), t.get("artist", ""), t.get("album", "")):
            continue

        # No name check: we arrived by Spotify's own artist id. See the module
        # docstring — the katakana credit is the case that proves it.
        existing.add(t["id"])
        t["genres"] = [c["genre"]]
        t["query"] = f"genre-backfill-spotify:{c['genre']}"
        t["region"] = t.get("region") or r.get("country") or ""
        found.append((c, t))
        print(f"  [{i:>4}/{len(rows)}] {c['genre'][:20]:22} {c['artist'][:24]:26} "
              f"-> {str(t.get('name'))[:30]}")

    print(f"\n  {len(found)} tracks | {no_link} artists MusicBrainz has no Spotify "
          f"link for | {no_track} with no fetchable track | {calls} Spotify calls")
    if args.dry_run:
        print("  DRY RUN — nothing written.")
        return
    if not found:
        return

    by_region = {}
    for _, t in found:
        by_region.setdefault(t.get("region") or "Unknown", []).append(t)

    def _merge(disk):
        for region, rs in by_region.items():
            ex = disk.get(region, [])
            ex_ids = {x["id"] for x in ex}
            for t in rs:
                if t["id"] not in ex_ids:
                    ex.append(t)
                    ex_ids.add(t["id"])
            disk[region] = ex
    locked_update(_merge)
    for region, rs in by_region.items():
        register_tracks(rs, region=region, source="spotify")
    for c, t in found:
        genre_artists.mark_ingested(c["genre"], c["artist"], 1)
    print(f"  inserted {len(found)} tracks into the pool")

    genre_vocab.refresh_coverage()
    s = genre_vocab.coverage_summary()
    print(f"  coverage: {s['covered']} served / {s['thin']} thin / {s['zero']} zero")


if __name__ == "__main__":
    main()
