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

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from lib.env import load_env
load_env()
from lib.db import fetchall, get_conn
from lib.track_filter import is_trash
from lib.artist_cap import is_over_cap

from lib.spotify_gate import make_client
sp = make_client()  # gated: cooldown-guarded + globally paced (lib/spotify_gate.py)

# Country code → Spotify market for top-tracks lookup. MB country is
# usually a country code already, but some artists come from regions
# without a market — fall back to global.
MARKET_FALLBACK = "US"


# Verdicts, not accidents: re-running the identical search returns the identical
# answer, so a row carrying one of these is finished and must leave the queue.
# Transient failures — "network: …", "spotify_429_brief", "spotify_5xx" — are
# deliberately NOT here. Those rows stay selectable and get retried, which is
# the whole reason this is a list of names rather than `ingest_error IS NULL`:
# one network blip would otherwise retire an artist permanently.
TERMINAL_ERRORS = (
    "no_artist_name",
    "no_spotify_id",
    "search_no_results",
    "search_no_match_for_id",
    "all_search_hits_were_trash",
    "artist_has_no_albums",
    "all_album_tracks_were_trash",
    "artist_at_cap",
)

# How much of an artist's catalogue to walk before giving up. Bounded because
# this replaced ONE search call per artist and must not become twenty: at most
# 1 + ALBUM_SCAN calls, so 4. The old search path cost 1 call and resolved ~80%;
# this costs up to 4 and cannot mis-resolve, and it uses endpoints that are not
# banned — which is the whole point.
ALBUM_PAGE = 10      # albums fetched in the single artist_albums call
ALBUM_SCAN = 3       # albums actually opened, newest first
TRACK_PAGE = 50      # tracks per album (one call covers almost every album)


def _abort_if_locked_out(e) -> dict:
    """Shared 429 handling. A cooldown longer than a blip ends the RUN.

    Recursing/retrying here was a bug once: every retry reset Spotify's counter
    and kept us perpetually locked out. Persist it instead, so the next cron
    tick and every other script short-circuit in pre-flight without a call.
    """
    wait = int(e.headers.get("Retry-After", 0)) if getattr(e, "headers", None) else 0
    if wait > 60:
        from lib.spotify_health import record_429 as _record_429
        _record_429(wait)
        print(f"  RATE LIMITED for {wait}s — aborting run "
              f"(cron will pick up after cooldown)")
        raise SystemExit(0)
    print(f"  rate limited, waiting {wait}s once")
    time.sleep(wait)
    return {"_error": "spotify_429_brief"}


def _title_matches(want: str, got: str) -> bool:
    """Is `got` the track the curator named? Token containment either way —
    captions abbreviate ("Ngày Xưa Anh Nói" for a longer official title) and
    Spotify pads ("… - Remastered 2019"). Deliberately loose: the cost of a
    false positive is a neighbouring track by the RIGHT artist, which is a
    fine outcome, while the cost of being strict is never matching at all."""
    import re as _re
    import unicodedata as _ud

    def toks(s):
        s = _ud.normalize("NFKD", str(s or ""))
        s = "".join(c for c in s if not _ud.combining(c))
        s = _re.sub(r"[^\w\s]", " ", s, flags=_re.UNICODE)
        return {t for t in s.lower().split() if len(t) > 1}

    w, g = toks(want), toks(got)
    if not w or not g:
        return False
    return w.issubset(g) or g.issubset(w)


def fetch_top_track(spotify_id: str, artist_name: str | None,
                    market: str | None, prefer_title: str | None = None) -> dict | None:
    """A representative track for a Spotify artist ID — WITHOUT /search.

    THE ENDPOINT THIS USED IS GONE.

    It went: artist name → `search?q=artist:"<name>"` → filter hits whose
    primary artist id matches. That was itself a workaround, because
    /artists/{id}/top-tracks is 403 in Dev Mode. Spotify's November 2024
    restrictions then took /search too — measured 2026-08-04 on three separate
    machines, minutes apart, all returning the SAME countdown:

        /search              429  Retry-After 57737 / 57775 / 57797  (~16h)
        /artists/{id}        200
        /artists/{id}/albums 200
        /albums/{id}/tracks  200
        top-tracks, artists?ids=, tracks?ids=, related-artists,
        audio-features       403      recommendations, playlists  404

    Identical countdown from a Tokyo laptop, a Milan Mac mini and the Hetzner
    server proves ONE app-wide counter — not an IP block, and not bursting.
    Pacing could never have fixed this: the dial was not connected to anything.

    So walk the two endpoints that still answer: albums → tracks. This is also
    strictly more accurate than the search it replaces, which matched by NAME
    and produced this run's entire error histogram (search_no_match_for_id 323,
    search_no_results 121, all_search_hits_were_trash 8). An id-keyed album
    walk cannot match the wrong artist.

    `artist_name` is now used only for trash-filtering, and `market` is unused
    — kept in the signature so callers do not change.
    """
    if not spotify_id:
        return {"_error": "no_spotify_id"}
    try:
        # Newest first is what `albums` returns by default; ask for albums AND
        # singles because a great many enumerated artists (the long tail this
        # queue exists for) have released only singles.
        albums = sp.artist_albums(spotify_id, album_type="album,single",
                                  limit=ALBUM_PAGE)
    except spotipy.SpotifyException as e:
        if e.http_status == 429:
            return _abort_if_locked_out(e)
        return {"_error": f"spotify_{e.http_status}"}
    except Exception as e:
        return {"_error": f"network: {str(e)[:80]}"}

    items = (albums or {}).get("items") or []
    if not items:
        return {"_error": "artist_has_no_albums"}

    saw_any_track = False
    # First acceptable track, held back only when a title was asked for. With
    # no `prefer_title` this stays None and the loop returns immediately, so
    # the call count and the answer are exactly what they were before.
    fallback = None
    for al in items[:ALBUM_SCAN]:
        if not al.get("id"):
            continue
        try:
            tr = sp.album_tracks(al["id"], limit=TRACK_PAGE)
        except spotipy.SpotifyException as e:
            if e.http_status == 429:
                return _abort_if_locked_out(e)
            continue          # one bad album must not retire the artist
        except Exception:
            continue
        for t in (tr or {}).get("items") or []:
            if not t.get("id"):
                continue
            saw_any_track = True
            # The album walk can still surface a compilation cut where our
            # artist is a guest, so keep the primary-artist rule the search
            # path had. `artists` here is the SIMPLIFIED object — no album
            # field — which is why the album is threaded through below.
            arts = t.get("artists") or []
            if not arts or arts[0].get("id") != spotify_id:
                if not any(a.get("id") == spotify_id for a in arts):
                    continue
            names = ", ".join(a.get("name", "") for a in arts)
            if is_trash(t.get("name") or "", names, al.get("name") or ""):
                continue
            # album/{id}/tracks returns SimplifiedTrackObject: no `album`, no
            # `popularity`. Graft the album we already hold so the row keeps
            # its release date, and therefore its year/decade.
            row = _spotify_track_to_row(dict(t, album=al))
            if not prefer_title:
                return row
            # A curator named a specific track. We are already holding this
            # artist's tracklists, so honouring that costs nothing — but only
            # the tracklists we were going to fetch anyway. Not finding it is
            # not a failure: a different track by the right artist is still
            # the discovery the ingest is for.
            if _title_matches(prefer_title, t.get("name") or ""):
                row["_title_matched"] = True
                return row
            if fallback is None:
                fallback = row
    if fallback is not None:
        return fallback
    return {"_error": "all_album_tracks_were_trash" if saw_any_track
                      else "artist_has_no_albums"}


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

    # A row that failed for a TERMINAL reason must not be selected again.
    # Without this the queue never advances: failures set ingest_error but
    # leave ingested_at NULL, and nothing filtered on ingest_error, so the 200
    # oldest rows were re-selected every 30 minutes forever. Measured
    # 2026-07-31: the same 200 artists, the same error histogram every run
    # (141/42/12/5), ingested=0, ~9,600 Spotify searches a day spent on them —
    # which is what kept tripping the 24h app-wide lockout — while 26,335
    # never-tried artists queued behind them.
    where = ("WHERE spotify_id IS NOT NULL AND ingested_at IS NULL "
             "AND (ingest_error IS NULL OR ingest_error <> ALL(%s))")
    params = [list(TERMINAL_ERRORS)]
    if args.country:
        where += " AND country = %s"
        params.append(args.country.upper())

    # Water-filling drain order: every slot goes to the country with the
    # fewest artists ingested so far. Until 2026-08-11 this was
    # ORDER BY enumerated_at — plain FIFO — which made pool composition an
    # accident of crawl order: everything ingested came from the countries
    # enumerated on day one (GH 213, NG 204, UA 197 …) while KR/IN/PH/TH sat
    # queued at ZERO behind a ~15-month backlog. Rank each candidate by the
    # count its country would reach if picked (already-ingested + its position
    # in the country's queue) and drain ascending: the least-fed country is
    # always served next, one artist per round. Self-limiting — a country with
    # nothing queued drops out on its own — so long-run imbalance can only
    # reflect genuine supply, never queue order. No quotas, no target ratios.
    rows = fetchall(
        f"""
        WITH queue AS (
            SELECT mbid, name, country, area, spotify_id, mb_tags,
                   row_number() OVER (
                       PARTITION BY country ORDER BY enumerated_at
                   ) AS pos
            FROM mb_artists {where}
        ),
        fed AS (
            SELECT country, count(*) AS n FROM mb_artists
            WHERE ingested_at IS NOT NULL GROUP BY country
        )
        SELECT q.mbid, q.name, q.country, q.area, q.spotify_id, q.mb_tags
        FROM queue q
        LEFT JOIN fed ON fed.country IS NOT DISTINCT FROM q.country
        ORDER BY COALESCE(fed.n, 0) + q.pos, q.pos, q.country
        LIMIT %s
        """,
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

                # Region: the COUNTRY, canonicalized. This used to prefer the
                # MB area name, but an MB area is as often a city or oblast
                # ('Tbilisi', "Kharkivs'ka Oblast'") as a country — that plus
                # raw ISO codes from the country field is where most of the
                # pool's 442-distinct-regions drift came from. canonical_region
                # turns codes into names and known cities into their country.
                from lib.region_norm import canonical_region
                region = canonical_region(r["country"] or r["area"]) or ""
                origin_region = canonical_region(r["country"] or r["area"]) or None

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
