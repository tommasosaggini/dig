#!/usr/bin/env python3
"""
DIG — MusicBrainz artist enumeration.

Walks MusicBrainz's browse-by-country endpoint to enumerate every artist
they list, with URL relationships included so we can extract Spotify IDs
(when present) without a separate search call. Results land in
`mb_artists`. The companion script `ingest_mb_artists.py` then resolves
each row to a real track in our discovery pool.

Why this exists: search-driven discovery (genre × region × decade →
Spotify search) caps out around 16k distinct artists for our user
because Spotify's search ranking hides the long tail. MusicBrainz lists
~2M artists with country/era/genre tags; enumerating them gives the
bottom of the discovery cone airtime it currently never gets.

Usage:
  scripts/enumerate_mb_artists.py [--country BR] [--max-pages 50]
  scripts/enumerate_mb_artists.py --countries BR,AR,MX,CO,PE,CL,VE,UY,PY,BO

Rate limit: 1 req/sec to MusicBrainz (enforced via sleep). MB browse
returns 100 artists per request; ~360k artists/hour upper bound at full
throughput. Real countries are smaller (Brazil has ~30k MB artists,
takes ~5 min to enumerate fully).
"""

import argparse
import os
import sys
import time
import requests
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

ENV_PATH = os.path.join(ROOT, ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from lib.db import fetchall, fetchone, get_conn

MB_URL = "https://musicbrainz.org/ws/2/artist"
MB_AREA_URL = "https://musicbrainz.org/ws/2/area"
MB_HEADERS = {
    "User-Agent": "DIG-MusicDiscovery/1.0 (https://ohdig.com; admin@ohdig.com)"
}
MB_RATE_LIMIT_S = 1.05  # MB asks for 1 req/sec; small buffer
PAGE_SIZE = 100         # MB max for browse


def lookup_country_area_mbid(iso2: str) -> str | None:
    """Resolve an ISO 3166-1 alpha-2 country code (e.g. 'BR') to the
    MusicBrainz area MBID we need for `?area=` browse queries.

    The MB search response strips iso-1-codes, so we trust the search-by-
    iso1 filter and take the first Country-typed hit. Search by `iso1:XX`
    on the area endpoint is precise enough — only one country area matches
    per ISO code.
    """
    try:
        resp = requests.get(
            MB_AREA_URL,
            params={"query": f"iso1:{iso2}", "fmt": "json", "limit": 5},
            headers=MB_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        for area in (resp.json().get("areas") or []):
            if area.get("type") == "Country":
                return area.get("id")
    except Exception:
        return None
    return None


def extract_spotify_id(url: str) -> str | None:
    """Pull a Spotify artist ID out of an open.spotify.com/artist/<id> URL."""
    if not url:
        return None
    # Accept open.spotify.com/artist/ID and possibly open.spotify.com/intl-xx/artist/ID
    import re
    m = re.search(r"open\.spotify\.com(?:/intl-[a-z]{2,4})?/artist/([0-9A-Za-z]+)", url)
    return m.group(1) if m else None


def upsert_mb_artist(cur, a: dict, country: str | None) -> bool:
    """Insert one artist row from MB JSON. Returns True if new (insert)
    or False if existing (skipped on conflict)."""
    mbid = a.get("id")
    if not mbid:
        return False
    name = a.get("name") or ""
    sort_name = a.get("sort-name") or ""
    type_ = a.get("type")
    gender = a.get("gender")
    area = (a.get("area") or {}).get("name") if a.get("area") else None
    begin_area = (a.get("begin-area") or {}).get("name") if a.get("begin-area") else None
    life = a.get("life-span") or {}
    lb = life.get("begin")
    le = life.get("end")
    tags = [t.get("name") for t in (a.get("tags") or []) if t.get("name")]

    spotify_url = None
    spotify_id = None
    for rel in a.get("relations") or []:
        if (rel.get("type") in ("streaming", "free streaming")
            and rel.get("url", {}).get("resource", "").startswith(
                ("https://open.spotify.com/", "http://open.spotify.com/"))):
            spotify_url = rel["url"]["resource"]
            spotify_id = extract_spotify_id(spotify_url)
            if spotify_id:
                break

    cur.execute(
        """
        INSERT INTO mb_artists (
          mbid, name, sort_name, country, area, begin_area, type, gender,
          lifespan_begin, lifespan_end, mb_tags, spotify_url, spotify_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (mbid) DO UPDATE SET
          spotify_url = COALESCE(EXCLUDED.spotify_url, mb_artists.spotify_url),
          spotify_id  = COALESCE(EXCLUDED.spotify_id,  mb_artists.spotify_id),
          mb_tags     = CASE
            WHEN array_length(EXCLUDED.mb_tags, 1) > 0
              THEN EXCLUDED.mb_tags
            ELSE mb_artists.mb_tags
          END
        """,
        (mbid, name, sort_name, country or a.get("country"), area, begin_area,
         type_, gender, lb, le, tags, spotify_url, spotify_id))
    return cur.rowcount > 0


def enumerate_country(country: str, max_pages: int | None = None,
                      verbose: bool = True) -> dict:
    """Walk MB's browse-by-area pages until exhausted (or max_pages).

    Resumes from `mb_enum_state` so re-runs pick up where we left off.
    Returns counts.
    """
    scope = f"country:{country}"

    # Resolve the country code to an MB area MBID — `?country=` doesn't work
    # on the artist endpoint; we need `?area=<area-mbid>`.
    area_mbid = lookup_country_area_mbid(country)
    if not area_mbid:
        print(f"  could not resolve area MBID for ISO code {country!r}")
        return {"new": 0, "updated": 0, "with_spotify": 0, "total_pages": 0}
    if verbose:
        print(f"  area MBID for {country}: {area_mbid}")
    time.sleep(MB_RATE_LIMIT_S)  # rate-limit between area lookup and first browse

    state = fetchone(
        "SELECT last_offset, total_count, done FROM mb_enum_state WHERE scope = %s",
        (scope,))
    offset = state["last_offset"] if state else 0
    if state and state["done"]:
        if verbose: print(f"  {scope}: already complete ({state['total_count']} artists), skipping")
        return {"new": 0, "updated": 0, "with_spotify": 0, "total_pages": 0}

    new_count = 0
    updated_count = 0
    with_spotify = 0
    pages_done = 0
    total = state["total_count"] if state else None

    while True:
        if max_pages is not None and pages_done >= max_pages:
            break
        try:
            resp = requests.get(
                MB_URL,
                params={
                    "area": area_mbid,
                    "fmt": "json",
                    "limit": PAGE_SIZE,
                    "offset": offset,
                    "inc": "url-rels+tags",
                },
                headers=MB_HEADERS,
                timeout=20)
        except Exception as e:
            print(f"  network err at offset {offset}: {e}; backing off 30s")
            time.sleep(30)
            continue

        if resp.status_code == 503:
            print(f"  MB 503 at offset {offset}; backing off 30s")
            time.sleep(30)
            continue
        if resp.status_code != 200:
            print(f"  MB {resp.status_code} at offset {offset}; aborting "
                  f"(body: {resp.text[:200]})")
            break

        data = resp.json()
        artists = data.get("artists", [])
        total = data.get("artist-count", total)
        if total is None:
            total = data.get("artist-count") or 0
        if not artists:
            # Either past the end or empty country
            done = True
            break

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for a in artists:
                    is_new = upsert_mb_artist(cur, a, country)
                    if is_new:
                        new_count += 1
                    else:
                        updated_count += 1
                    if any(extract_spotify_id((rel.get("url") or {}).get("resource"))
                           for rel in (a.get("relations") or [])
                           if rel.get("type") in ("streaming", "free streaming")):
                        with_spotify += 1
                # Update state
                cur.execute(
                    """
                    INSERT INTO mb_enum_state (scope, last_offset, total_count, done, last_run_at)
                    VALUES (%s, %s, %s, FALSE, NOW())
                    ON CONFLICT (scope) DO UPDATE SET
                      last_offset = EXCLUDED.last_offset,
                      total_count = EXCLUDED.total_count,
                      last_run_at = NOW()
                    """,
                    (scope, offset + len(artists), total))
            conn.commit()
        finally:
            conn.close()

        pages_done += 1
        offset += len(artists)
        if verbose and pages_done % 5 == 0:
            print(f"    {scope}: page {pages_done}, offset {offset}/{total}, "
                  f"+{new_count} new (so far)")

        if offset >= (total or 0) or len(artists) < PAGE_SIZE:
            done = True
            break

        time.sleep(MB_RATE_LIMIT_S)

    if 'done' in dir() and done:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE mb_enum_state SET done = TRUE WHERE scope = %s",
                    (scope,))
            conn.commit()
        finally:
            conn.close()

    return {"new": new_count, "updated": updated_count,
            "with_spotify": with_spotify, "total_pages": pages_done,
            "offset": offset, "total": total}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--country", help="Single ISO country code, e.g. BR")
    p.add_argument("--countries",
                   help="Comma-separated list, e.g. BR,AR,MX,CO,PE")
    p.add_argument("--max-pages", type=int, default=None,
                   help="Cap pages per country (each page = 100 artists)")
    args = p.parse_args()

    if not args.country and not args.countries:
        # Default test slice — small enough to run in a couple minutes
        args.countries = "BR"
        if args.max_pages is None:
            args.max_pages = 5  # 500 artists, ~10s

    if args.country:
        countries = [args.country]
    else:
        countries = [c.strip().upper() for c in args.countries.split(",") if c.strip()]

    started = datetime.now(timezone.utc)
    grand_new = 0
    grand_with_spotify = 0
    for country in countries:
        print(f"\n=== Enumerating MusicBrainz country={country} ===")
        out = enumerate_country(country, max_pages=args.max_pages)
        print(f"  result: +{out['new']} new, {out['updated']} re-seen, "
              f"{out['with_spotify']} have Spotify URL, "
              f"pages={out['total_pages']}, offset={out.get('offset')}/{out.get('total')}")
        grand_new += out["new"]
        grand_with_spotify += out["with_spotify"]

    print(f"\n=== TOTAL ===  new={grand_new}, with_spotify={grand_with_spotify}, "
          f"elapsed={(datetime.now(timezone.utc) - started).total_seconds():.1f}s")
    # Snapshot
    r = fetchone("SELECT COUNT(*) AS n, COUNT(spotify_id) AS sp FROM mb_artists")
    print(f"  mb_artists table: {r['n']:,} total, {r['sp']:,} with spotify_id")


if __name__ == "__main__":
    main()
