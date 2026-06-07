#!/usr/bin/env python3
"""
DIG — Drain mb_artists into the main `tracks` pool.

For each MusicBrainz artist with a known Spotify ID and no prior ingest,
fetch their top tracks, apply our `is_trash` filter, insert ONE
representative track. The point is artist breadth — one track per artist
is enough to surface them in discovery; subsequent listening can
naturally pull more if the user engages.

The Spotify resolution is pre-done by enumerate_mb_artists (via MB's
streaming-url relation), so this script is just `artist_top_tracks`
calls — no name-search guesswork. That makes it idempotent and high-
quality.

Usage:
  scripts/ingest_mb_artists.py [--limit 200] [--country BR] [--dry-run]

Rate limit: 0.5s/call to Spotify, ~7200 artists/hour theoretical.
Realistic: 3000-5000/hour after backoffs.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from lib.db import fetchall, get_conn
from lib.track_filter import is_trash
from lib.artist_cap import is_over_cap

from lib.spotify_gate import make_client
sp = make_client()  # gated: cooldown-guarded + globally paced (lib/spotify_gate.py)

# Country code → Spotify market for top-tracks lookup. MB country is
# usually a country code already, but some artists come from regions
# without a market — fall back to global.
MARKET_FALLBACK = "US"


def fetch_top_track(spotify_id: str, artist_name: str | None,
                    market: str | None) -> dict | None:
    """Get a representative track for a Spotify artist ID.

    /artists/{id}/top-tracks is blocked in Spotify Dev Mode. Workaround:
    search by `artist:"<name>"`, then filter to tracks where our target
    spotify_id is the PRIMARY (first-listed) artist — skipping remixes /
    features where the artist is just a guest. Returns the first non-trash
    track as a dict ready to insert into `tracks`."""
    if not artist_name:
        return {"_error": "no_artist_name"}
    q = f'artist:"{artist_name}"'
    try:
        # Dev Mode caps search limit ≤ 10
        s = sp.search(q=q, type="track", limit=10,
                      market=(market or MARKET_FALLBACK))
    except spotipy.SpotifyException as e:
        if e.http_status == 429:
            wait = int(e.headers.get("Retry-After", 0)) if hasattr(e, 'headers') and e.headers else 0
            # If the cooldown is anything more than a brief blip, ABORT the
            # whole run. Recursing here was a bug — every retry reset
            # Spotify's cooldown counter, keeping us perpetually rate
            # limited and burning thousands of failed calls in cron logs.
            if wait > 60:
                # Persist the cooldown so the next cron tick (and any other
                # script in the next minute) short-circuits via the shared
                # pre-flight check before making a single call.
                from lib.spotify_health import record_429 as _record_429
                _record_429(wait)
                print(f"  RATE LIMITED for {wait}s — aborting run "
                      f"(cron will pick up after cooldown)")
                raise SystemExit(0)
            print(f"  rate limited, waiting {wait}s once")
            time.sleep(wait)
            return {"_error": "spotify_429_brief"}
        return {"_error": f"spotify_{e.http_status}"}
    except Exception as e:
        return {"_error": f"network: {str(e)[:80]}"}

    items = (s.get("tracks") or {}).get("items") or []
    if not items:
        return {"_error": "search_no_results"}

    # Filter: primary artist must match our spotify_id (skip features)
    primary_hits = [t for t in items
                    if t.get("artists") and t["artists"][0].get("id") == spotify_id]
    if not primary_hits:
        # Soft fallback: artist appears anywhere in artist list (some
        # collaborations don't have a clear primary). Useful for groups.
        primary_hits = [t for t in items
                        if any(a.get("id") == spotify_id for a in t.get("artists", []))]
    if not primary_hits:
        return {"_error": "search_no_match_for_id"}

    for t in primary_hits:
        name = t.get("name") or ""
        artists = ", ".join(a["name"] for a in t.get("artists", []))
        album = (t.get("album") or {}).get("name") or ""
        if is_trash(name, artists, album):
            continue
        if not t.get("id"):
            continue
        return _spotify_track_to_row(t)
    return {"_error": "all_search_hits_were_trash"}


def _spotify_track_to_row(t: dict) -> dict:
    artists = t.get("artists") or []
    artist = ", ".join(a["name"] for a in artists)
    artist_ids = [a["id"] for a in artists if a.get("id")]
    album = t.get("album") or {}
    rel = album.get("release_date") or ""
    year = rel[:4] if len(rel) >= 4 else ""
    decade = (year[:3] + "0s") if year else None
    return {
        "id": t["id"],
        "name": t.get("name", ""),
        "artist": artist,
        "artist_ids": artist_ids,
        "album": album.get("name") or "",
        "popularity": t.get("popularity", 0),  # dead for Dev Mode but consistent
        "source": "spotify",
        "query": "mb-enumerated",
        "decade": decade,
        "year": year,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=200,
                   help="Max artists to drain in this run")
    p.add_argument("--country", help="Restrict to one MB country code")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    # Bail before touching Spotify if our app key is in cooldown.
    from lib.spotify_health import pre_flight_or_exit
    pre_flight_or_exit("ingest_mb_artists")

    where = "WHERE spotify_id IS NOT NULL AND ingested_at IS NULL"
    params = []
    if args.country:
        where += " AND country = %s"
        params.append(args.country.upper())

    rows = fetchall(
        f"SELECT mbid, name, country, area, spotify_id, mb_tags FROM mb_artists "
        f"{where} ORDER BY enumerated_at LIMIT %s",
        (*params, args.limit))

    if not rows:
        print("Nothing to ingest. Run enumerate_mb_artists.py first.")
        return

    print(f"Draining {len(rows)} mb_artists → tracks…")
    ingested = 0
    skipped = 0
    errors = {}
    started = datetime.now(timezone.utc)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for i, r in enumerate(rows, 1):
                # Use country code if it's a 2-letter, else fall back to US
                market = r["country"] if r["country"] and len(r["country"]) == 2 else None

                if args.dry_run:
                    print(f"  [{i}] would query: {r['name']} ({r['spotify_id']}, market={market})")
                    continue

                track = fetch_top_track(r["spotify_id"], r["name"], market)
                time.sleep(0.5)

                if not track:
                    errors["unknown"] = errors.get("unknown", 0) + 1
                    continue
                if "_error" in track:
                    err = track["_error"]
                    errors[err] = errors.get(err, 0) + 1
                    cur.execute(
                        "UPDATE mb_artists SET ingest_error = %s WHERE mbid = %s",
                        (err, r["mbid"]))
                    continue

                # Region: prefer MB area name (matches our region taxonomy
                # better than ISO codes), fall back to country.
                region = r["area"] or r["country"] or ""
                origin_region = r["country"] or r["area"] or None

                # Cap gate: skip artists already at the per-artist limit.
                # Mark the mb_artists row so the ingest cron won't keep
                # retrying it indefinitely.
                primary = (track.get("artist_ids") or [None])[0]
                if is_over_cap(cur, primary):
                    cur.execute(
                        "UPDATE mb_artists SET ingest_error = 'artist_at_cap' "
                        "WHERE mbid = %s",
                        (r["mbid"],))
                    errors["artist_at_cap"] = errors.get("artist_at_cap", 0) + 1
                    continue

                # Insert into tracks (idempotent via ON CONFLICT). Don't
                # clobber existing rows that may have richer labels already.
                cur.execute(
                    """
                    INSERT INTO tracks (
                      id, name, artist, artist_ids, album, popularity, source,
                      region, origin_region, decade, year, query, genres
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                      origin_region = COALESCE(tracks.origin_region, EXCLUDED.origin_region)
                    """,
                    (track["id"], track["name"], track["artist"],
                     track["artist_ids"], track["album"], track["popularity"],
                     track["source"], region, origin_region,
                     track.get("decade"), track.get("year"), "mb-enumerated",
                     r["mb_tags"] or []))

                cur.execute(
                    """
                    UPDATE mb_artists
                       SET ingested_at = NOW(),
                           ingested_track_id = %s,
                           ingest_error = NULL
                     WHERE mbid = %s
                    """, (track["id"], r["mbid"]))
                ingested += 1

                if i % 25 == 0:
                    conn.commit()
                    print(f"  [{i}/{len(rows)}] {ingested} ingested, errors={errors}")
        conn.commit()
    finally:
        conn.close()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\n=== DONE ===  ingested={ingested}, "
          f"errors={errors}, elapsed={elapsed:.1f}s "
          f"(rate={ingested / max(elapsed, 1):.1f}/s)")


if __name__ == "__main__":
    main()
