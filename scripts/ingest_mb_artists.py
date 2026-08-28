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

# Built on first use, not at import. `sp = make_client()` here meant that
# IMPORTING this module required Spotify credentials, so a clean checkout could
# not load it at all — which is how tests/test_album_walk_spans_the_career.py
# came to pass on this laptop and fail everywhere else. Nothing about reading
# the module needs a client; only the three calls that reach Spotify do.
_sp = None


def sp_client():
    """The gated Spotify client — cooldown-guarded and globally paced."""
    global _sp
    if _sp is None:
        _sp = make_client()
    return _sp

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
# The career-start window, as MusicBrainz lifespan-begin years. An artist who
# STARTED in this window has a back catalogue in the starved decades; one who
# started in 2019 cannot have a 1990s record however we walk them.
#
# Imported rather than restated: lib/era.py is the one definition, so this
# cannot drift away from what the picker and pipeline/discover.py believe.
from lib.era import CAREER_FIRST_FROM as ERA_FIRST_FROM  # noqa: E402
from lib.era import CAREER_FIRST_TO as ERA_FIRST_TO      # noqa: E402


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
# HARD API CAP, MEASURED — NOT the documented one. Spotify documents limit<=50
# for /artists/{id}/albums; under the Development Mode restriction this app is
# subject to, anything above 10 returns 400. Probed against prod 2026-08-18:
# limit 10 OK, 11/12/15/16/18/20/49/50 all HTTP 400. Setting this to 50 on the
# strength of the docs took a working hourly job to ingested=0, errors=
# {'spotify_400': 50} on the very next run. Do not raise it without probing.
ALBUM_PAGE = 10      # albums per artist_albums call
ALBUM_SCAN = 3       # albums actually opened, SPREAD across the catalogue
TRACK_PAGE = 50      # tracks per album (one call covers almost every album)


def spread_albums(items, n=ALBUM_SCAN):
    """The albums to open, IN PRIORITY ORDER: oldest first, then spread.

    Read the caller before changing this. resolve_track() returns the first
    acceptable track it finds and stops, and only ONE track per artist is ever
    inserted — so in the ordinary case this function decides exactly one thing:
    which album the artist's single pool entry comes from. That is the oldest
    one. The rest of the list is fallback order, used only when the oldest
    album is unreachable or entirely filtered out as trash.

    So the yield is not "three albums per artist"; it is "the earliest record
    we can reach", with two spares. It is also CHEAPER than what it replaced —
    the common path now costs one album_tracks call, not three.

    THIS USED TO BE `items[:ALBUM_SCAN]` — the three NEWEST, because that is
    the order artist_albums returns. Pure recency, applied to every artist DIG
    has ever ingested, and it is the single reason the pool has almost no back
    catalogue: an artist who debuted in 1988 and still releases contributed a
    2024 single and nothing else. Measured 2026-08-18, the 2,489 tracks this
    path produced were 72 pre-2000 (2.9%) and 1,750 from the 2020s (70%),
    against a queue holding 3,457 artists who began in the 80s, 4,632 in the
    90s and 3,383 in the 2000s. The catalogue was never missing. It was being
    converted to contemporary tracks at the door.

    The spares are spread rather than adjacent — evenly spaced positions
    including both ends, so a 1988-2024 career falls back to ~2006 and then
    2024 rather than to two more 1988 pressings.

    The caller feeds this BOTH ends of the catalogue (see resolve_track): the
    newest page and, for an artist with more releases than one page holds, the
    far page too. ALBUM_PAGE is capped at 10 by the API, so without that second
    call a prolific artist's whole visible window can post-date 2015.

    Pure, so the rule can be tested without a Spotify account.

    Undated albums sort LAST, and are kept rather than dropped. Both halves
    matter. Dropping them would re-bias the walk toward well-tagged — which is
    to say Western, which is to say recent — releases, and a missing
    release_date is common on exactly the long tail this queue exists for. But
    an undated album cannot fill a decade either (its track lands with no year
    and the era weighting treats it as neutral), so it must not take the oldest
    slot from a record that can. Sorting them to the end gives them the slots
    nothing else is competing for: an artist with three releases still has all
    three walked.
    """
    if n < 1:
        return []
    albums = [a for a in (items or []) if isinstance(a, dict) and a.get("id")]
    dated = sorted((a for a in albums if a.get("release_date")),
                   key=lambda a: a["release_date"])
    undated = [a for a in albums if not a.get("release_date")]

    # Not enough dated records to spread across — take them all and let the
    # undated ones fill whatever is left over.
    if len(dated) <= n:
        return (dated + undated)[:n]

    if n == 1:
        return [dated[0]]
    last = len(dated) - 1
    picked, seen = [], set()
    for i in range(n):
        idx = round(i * last / (n - 1))
        if idx not in seen:
            seen.add(idx)
            picked.append(dated[idx])
    return picked


# A 429 that carries no Retry-After is still a 429. Spotify sends those as the
# soft warning before the hard ban, and the old code read `Retry-After: 0` and
# slept for zero seconds — so a run that met the warning answered it by asking
# again immediately, as fast as the network allowed.
#
# That is not a hypothetical. The run at mb_ingest.log:82258 took
# `spotify_429_brief` 181 times in 112 seconds, ingested nothing, and ended
# with the first app-wide 22-hour ban this project had ever seen. Every
# successful run before it had been hourly with no cooldown at all; every one
# after has been one success per ~12 hours. One run of hammering cost the lane
# an order of magnitude of throughput, permanently.
#
# So a brief 429 now backs off for real, and a run that keeps meeting them
# stops. A run that is mostly 429s is not making progress — it is only
# deepening the hole it is already in.
BRIEF_429_MIN_SLEEP = 5      # seconds; the floor when Spotify names no number
BRIEF_429_MAX_SLEEP = 60     # seconds; the ceiling on the exponential
BRIEF_429_BUDGET = 8         # brief 429s a single run may absorb before it quits
BRIEF_429_STAND_DOWN = 1800  # seconds recorded when the budget is spent

_brief_429_seen = 0


def _abort_if_locked_out(e, family: str) -> dict:
    """Shared 429 handling. A cooldown longer than a blip ends the RUN.

    Recursing/retrying here was a bug once: every retry reset Spotify's counter
    and kept us perpetually locked out. Persist it instead, so the next cron
    tick and every other script short-circuit in pre-flight without a call.

    `family` is which endpoint got banned, and it is not optional: Spotify
    bans one endpoint at a time, so recording the wrong name quarantines a
    service that is answering perfectly well.
    """
    global _brief_429_seen

    from lib.spotify_health import record_429 as _record_429
    wait = int(e.headers.get("Retry-After", 0)) if getattr(e, "headers", None) else 0
    if wait > 60:
        _record_429(wait, family)
        print(f"  RATE LIMITED on {family} for {wait}s — aborting run "
              f"(cron will pick up after cooldown)")
        raise SystemExit(0)

    _brief_429_seen += 1
    if _brief_429_seen > BRIEF_429_BUDGET:
        # We do not know how long Spotify wants; it declined to say. Half an
        # hour is chosen to be long enough that the next cron tick does not
        # walk straight back into this, and short enough that it costs one
        # slot rather than quarantining an endpoint that may be fine.
        _record_429(BRIEF_429_STAND_DOWN, family)
        print(f"  {_brief_429_seen} brief 429s on {family} in one run — "
              f"standing down for {BRIEF_429_STAND_DOWN}s rather than keep "
              f"asking. This is the escalation that bought a 22h ban once.")
        raise SystemExit(0)

    # Exponential on our own count, floored, because `wait` is usually 0 here.
    nap = min(BRIEF_429_MAX_SLEEP,
              max(wait, BRIEF_429_MIN_SLEEP * (2 ** (_brief_429_seen - 1))))
    print(f"  rate limited on {family} (Retry-After={wait or 'absent'}) — "
          f"backing off {nap}s [{_brief_429_seen}/{BRIEF_429_BUDGET}]")
    time.sleep(nap)
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
        # Ask for albums AND singles because a great many enumerated artists
        # (the long tail this queue exists for) have released only singles.
        albums = sp_client().artist_albums(spotify_id, album_type="album,single",
                                  limit=ALBUM_PAGE)
    except spotipy.SpotifyException as e:
        if e.http_status == 429:
            return _abort_if_locked_out(e, "artist_albums")
        return {"_error": f"spotify_{e.http_status}"}
    except Exception as e:
        return {"_error": f"network: {str(e)[:80]}"}

    items = (albums or {}).get("items") or []
    if not items:
        return {"_error": "artist_has_no_albums"}

    # REACH BACK PAST THE PAGE. artist_albums returns roughly newest first and
    # the page is capped at ALBUM_PAGE=10 by the API, so for anyone prolific
    # the whole window can post-date 2015 — the recency filter again, with more
    # steps. `total` says where the far end is, so one more call fetches it.
    #
    # It reaches back, it does not reliably reach the DEBUT, and the difference
    # is worth stating. Spotify's ordering here is not strictly chronological
    # (album groups sort together, reissues carry their repress date), so the
    # far page is older on average rather than oldest. Probed 2026-08-18 on
    # Grasshopper, 36 releases, career from 1985: the newest page spans
    # 1995-2026 and the far page 1991-2025. Real gain, not the debut.
    #
    # Still cheaper than what it replaced: two album pages plus the single
    # album_tracks call the first acceptable track ends on, against the old
    # 1 + 3. Failure here is not fatal — the newest page alone is a worse
    # answer, not a broken one.
    total = (albums or {}).get("total") or len(items)
    if total > ALBUM_PAGE:
        try:
            oldest = sp_client().artist_albums(spotify_id, album_type="album,single",
                                      limit=ALBUM_PAGE,
                                      offset=max(0, total - ALBUM_PAGE))
            items = ((oldest or {}).get("items") or []) + items
        except spotipy.SpotifyException as e:
            if e.http_status == 429:
                return _abort_if_locked_out(e, "artist_albums")
        except Exception:
            pass

    # Oldest first, then spread — see spread_albums.
    items = spread_albums(items, ALBUM_SCAN)

    saw_any_track = False
    # First acceptable track, held back only when a title was asked for. With
    # no `prefer_title` this stays None and the loop returns immediately, so
    # the call count and the answer are exactly what they were before.
    fallback = None
    for al in items:
        if not al.get("id"):
            continue
        try:
            tr = sp_client().album_tracks(al["id"], limit=TRACK_PAGE)
        except spotipy.SpotifyException as e:
            if e.http_status == 429:
                return _abort_if_locked_out(e, "album_tracks")
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
    p.add_argument("--era-first", action="store_true",
                   help=f"walk artists whose career begins {ERA_FIRST_FROM}-"
                        f"{ERA_FIRST_TO} before the rest, to fill the decades "
                        "the pool is starved of. Country water-filling still "
                        "decides the order within the tier.")
    args = p.parse_args()

    # Bail before touching Spotify if our app key is in cooldown.
    from lib.spotify_health import pre_flight_or_exit
    # The two endpoints this run cannot work without. Probing `artists/{id}`
    # instead — as pre-flight did until 2026-08-18 — passed 552 times and let
    # 235 of those runs walk straight into an `artist_albums` ban.
    pre_flight_or_exit("ingest_mb_artists",
                       families=["artist_albums", "album_tracks"])

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

    # ERA TIER FIRST, then water-filling within it.
    #
    # --era-first puts artists whose MusicBrainz lifespan begins in the starved
    # decades ahead of the rest. It does NOT replace the country water-filling
    # below — that still decides the order inside each tier, so an era-first
    # run is still drained least-fed-country-first and cannot re-introduce the
    # crawl-order bias this query was written to kill. It only says which
    # eleven thousand of the thirty-nine thousand pending artists get walked
    # first, and it exhausts itself: once they are ingested the tier is empty
    # and the ordering is exactly what it was.
    #
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
                   ) AS pos,
                   -- 0 = this artist's career starts in a decade the pool is
                   -- starved of, 1 = everything else. See --era-first.
                   CASE WHEN %s AND lifespan_begin ~ '^[0-9]{{4}}'
                             AND left(lifespan_begin, 4)::int
                                 BETWEEN %s AND %s
                        THEN 0 ELSE 1 END AS era_rank
            FROM mb_artists {where}
        ),
        fed AS (
            SELECT country, count(*) AS n FROM mb_artists
            WHERE ingested_at IS NOT NULL GROUP BY country
        )
        SELECT q.mbid, q.name, q.country, q.area, q.spotify_id, q.mb_tags
        FROM queue q
        LEFT JOIN fed ON fed.country IS NOT DISTINCT FROM q.country
        ORDER BY q.era_rank,
                 COALESCE(fed.n, 0) + q.pos, q.pos, q.country
        LIMIT %s
        """,
        (args.era_first, ERA_FIRST_FROM, ERA_FIRST_TO, *params, args.limit))

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
                if is_over_cap(cur, primary,
                               artist_name=track.get("artist")):
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
