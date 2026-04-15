#!/usr/bin/env python3
"""
scripts/backfill_regions.py

Look up every track's primary artist on MusicBrainz to determine their actual
origin country, then write it to tracks.origin_region.

This separates "where the track was discovered" (tracks.region — the Spotify
market bucket) from "where the artist is actually from" (tracks.origin_region).

MusicBrainz rate limit: 1 request/second.
Results are cached in .cache/mb_artist_cache.json so the script is resumable.

Usage:
    python scripts/backfill_regions.py [--dry-run] [--limit N]
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import psycopg2.extras

from lib.db import get_conn

# ── MusicBrainz ───────────────────────────────────────────────────────────────

MB_URL = "https://musicbrainz.org/ws/2/artist"
MB_HEADERS = {
    "User-Agent": "DIG-MusicDiscovery/1.0 (https://ohdig.com; admin@ohdig.com)"
}

CACHE_FILE = os.path.join(ROOT, "scripts", "mb_artist_cache.json")


def mb_lookup(artist_name: str) -> dict | None:
    """
    Query MusicBrainz for the top hit for this artist name.
    Returns the raw artist dict (with 'score', 'country', 'area', 'begin-area')
    or None on error / no results.
    """
    try:
        resp = requests.get(
            MB_URL,
            params={"query": f'artist:"{artist_name}"', "limit": 3, "fmt": "json"},
            headers=MB_HEADERS,
            timeout=12,
        )
        if resp.status_code == 503:
            print("  MB 503 — backing off 30s")
            time.sleep(30)
            return mb_lookup(artist_name)
        if resp.status_code != 200:
            return None
        hits = resp.json().get("artists", [])
        if not hits:
            return None
        # Pick highest-scored hit; MusicBrainz returns 0–100
        return max(hits, key=lambda a: a.get("score", 0))
    except Exception as exc:
        print(f"  MB error for {artist_name!r}: {exc}")
        return None


# ── ISO country code → our REGION_GEO key ─────────────────────────────────────

ISO2_TO_REGION = {
    "US": "USA", "CA": "Canada", "MX": "Mexico",
    "CU": "Cuba", "JM": "Jamaica", "HT": "Haiti", "DO": "Dominican Republic",
    "TT": "Trinidad", "BB": "Barbados", "BS": "Bahamas", "PR": "Puerto Rico",
    "GT": "Guatemala", "CR": "Costa Rica", "PA": "Panama",
    "HN": "Honduras", "SV": "El Salvador", "NI": "Nicaragua",

    "BR": "Brazil", "AR": "Argentina", "CO": "Colombia", "CL": "Chile",
    "PE": "Peru", "VE": "Venezuela", "BO": "Bolivia", "EC": "Ecuador",
    "PY": "Paraguay", "UY": "Uruguay", "GY": "Guyana", "SR": "Suriname",

    "GB": "UK", "IE": "Ireland", "FR": "France", "DE": "Germany",
    "IT": "Italy", "ES": "Spain", "PT": "Portugal", "NL": "Netherlands",
    "BE": "Belgium", "CH": "Switzerland", "AT": "Austria", "LU": "Luxembourg",
    "SE": "Sweden", "NO": "Norway", "DK": "Denmark", "FI": "Finland",
    "IS": "Iceland",
    "GR": "Greece", "CY": "Cyprus", "MT": "Malta",
    "HR": "Croatia", "RS": "Serbia", "SI": "Slovenia",
    "BA": "Bosnia", "AL": "Albania", "MK": "North Macedonia",
    "XK": "Kosovo", "ME": "Montenegro",
    "PL": "Poland", "CZ": "Czech Republic", "SK": "Slovakia",
    "HU": "Hungary", "RO": "Romania", "BG": "Bulgaria",
    "UA": "Ukraine", "BY": "Belarus",
    "LT": "Lithuania", "LV": "Latvia", "EE": "Estonia", "MD": "Moldova",
    "RU": "Russia", "AZ": "Azerbaijan", "AM": "Armenia", "GE": "Georgia",

    "TR": "Turkey", "IR": "Iran", "IQ": "Iraq",
    "IL": "Israel", "PS": "Palestine", "LB": "Lebanon",
    "JO": "Jordan", "SY": "Syria", "YE": "Yemen",
    "SA": "Saudi Arabia", "AE": "UAE", "KW": "Kuwait",
    "QA": "Qatar", "BH": "Bahrain", "OM": "Oman",

    "KZ": "Kazakhstan", "UZ": "Uzbekistan", "KG": "Kyrgyzstan",
    "TJ": "Tajikistan", "TM": "Turkmenistan",
    "AF": "Afghanistan", "PK": "Pakistan",

    "MA": "Morocco", "DZ": "Algeria", "TN": "Tunisia",
    "LY": "Libya", "EG": "Egypt", "SD": "Sudan",

    "NG": "Nigeria", "GH": "Ghana", "SN": "Senegal", "GN": "Guinea",
    "CI": "Ivory Coast", "CM": "Cameroon", "ML": "Mali",
    "BF": "Burkina Faso", "NE": "Niger", "TD": "Chad",
    "ET": "Ethiopia", "SO": "Somalia", "ER": "Eritrea",
    "KE": "Kenya", "UG": "Uganda", "TZ": "Tanzania",
    "RW": "Rwanda", "BI": "Burundi",
    "CD": "DR Congo", "CG": "Congo",
    "AO": "Angola", "ZM": "Zambia", "ZW": "Zimbabwe",
    "MZ": "Mozambique", "MW": "Malawi",
    "ZA": "South Africa", "NA": "Namibia", "BW": "Botswana",
    "MG": "Madagascar", "MU": "Mauritius",

    "IN": "India", "BD": "Bangladesh", "LK": "Sri Lanka",
    "NP": "Nepal", "BT": "Bhutan", "MV": "Maldives",

    "CN": "China", "JP": "Japan", "KR": "South Korea", "KP": "North Korea",
    "TW": "Taiwan", "HK": "Hong Kong", "MO": "Macau", "MN": "Mongolia",

    "TH": "Thailand", "VN": "Vietnam", "KH": "Cambodia", "LA": "Laos",
    "MM": "Myanmar", "MY": "Malaysia", "SG": "Singapore",
    "ID": "Indonesia", "PH": "Philippines",
    "BN": "Brunei", "TL": "East Timor",

    "AU": "Australia", "NZ": "New Zealand",
    "PG": "Papua New Guinea", "FJ": "Fiji",
}

# Some MusicBrainz area names that don't cleanly map from ISO codes
AREA_NAME_TO_REGION = {
    "United States": "USA", "United States of America": "USA",
    "United Kingdom": "UK", "England": "UK", "Scotland": "UK",
    "Wales": "UK", "Northern Ireland": "UK",
    "South Korea": "South Korea", "Republic of Korea": "South Korea",
    "Democratic Republic of the Congo": "DR Congo",
    "Republic of the Congo": "Congo",
    "Ivory Coast": "Ivory Coast", "Côte d'Ivoire": "Ivory Coast",
    "United Arab Emirates": "UAE",
    "Bosnia and Herzegovina": "Bosnia",
    "North Macedonia": "North Macedonia",
    "Czechia": "Czech Republic",
    "Palestine": "Palestine", "State of Palestine": "Palestine",
    "Trinidad and Tobago": "Trinidad",
}


def resolve_region(mb_artist: dict | None) -> str | None:
    """Map a MusicBrainz artist object to one of our region keys."""
    if not mb_artist:
        return None
    score = mb_artist.get("score", 0)
    if isinstance(score, str):
        score = int(score)
    if score < 70:
        return None

    # 1. ISO country code (most reliable)
    iso = mb_artist.get("country")
    if iso:
        region = ISO2_TO_REGION.get(iso.upper())
        if region:
            return region

    # 2. Area name
    for key in ("area", "begin-area"):
        area = mb_artist.get(key) or {}
        name = area.get("name", "")
        if name:
            region = AREA_NAME_TO_REGION.get(name) or ISO2_TO_REGION.get(name)
            if region:
                return region

    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill tracks.origin_region via MusicBrainz")
    parser.add_argument("--dry-run", action="store_true", help="Lookup only, don't write to DB")
    parser.add_argument("--limit", type=int, default=0, help="Process at most N artists (0 = all)")
    args = parser.parse_args()

    # ── Load tracks ──────────────────────────────────────────────────────────
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, artist, region, origin_region
                FROM tracks
                ORDER BY artist
            """)
            rows = list(cur.fetchall())
    finally:
        conn.close()

    print(f"Loaded {len(rows)} tracks from DB")

    # Group by artist, skip tracks that already have an origin_region
    artist_to_ids: dict[str, list[str]] = defaultdict(list)
    already_done = 0
    for row in rows:
        artist = (row["artist"] or "").strip()
        if not artist:
            continue
        if row.get("origin_region"):
            already_done += 1
            continue
        artist_to_ids[artist].append(row["id"])

    print(f"{already_done} tracks already have origin_region — skipping")
    print(f"{len(artist_to_ids)} unique artists remaining")

    # ── Load / init cache ────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    cache: dict[str, str | None] = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        print(f"Loaded {len(cache)} cached MusicBrainz results")

    # ── Lookup loop ──────────────────────────────────────────────────────────
    updates: dict[str, str] = {}   # track_id → origin_region
    artists_list = sorted(artist_to_ids.items())
    if args.limit:
        artists_list = artists_list[: args.limit]

    found = 0
    for idx, (artist, track_ids) in enumerate(artists_list):
        if artist in cache:
            region = cache[artist]
        else:
            print(f"[{idx+1}/{len(artists_list)}] {artist!r}", end=" … ", flush=True)
            mb = mb_lookup(artist)
            region = resolve_region(mb)

            # If no match and artist string contains commas/feat/&, try just the primary name
            if not region and any(sep in artist for sep in [",", " feat", " ft.", " & ", " x ", " vs"]):
                primary = re.split(r",| feat\.?| ft\.| & | x | vs\.?", artist, maxsplit=1)[0].strip()
                if primary and primary != artist and primary not in cache:
                    time.sleep(1.1)
                    mb2 = mb_lookup(primary)
                    region = resolve_region(mb2)
                    if region:
                        cache[primary] = region

            cache[artist] = region
            print(region or "(not found)")
            time.sleep(1.1)

            if idx % 50 == 0:
                with open(CACHE_FILE, "w") as f:
                    json.dump(cache, f, indent=2)

        if region:
            found += 1
            for tid in track_ids:
                updates[tid] = region

    # Final cache flush
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

    print(f"\nResolved {found}/{len(artists_list)} artists → {len(updates)} tracks to update")

    if args.dry_run:
        print("DRY RUN — not writing to DB")
        # Print a sample
        sample = list(updates.items())[:20]
        for tid, reg in sample:
            row = next((r for r in rows if r["id"] == tid), None)
            if row:
                print(f"  {row['artist']!r}: {row['region']} → {reg}")
        return

    if not updates:
        print("Nothing to update.")
        return

    # ── Write to DB ──────────────────────────────────────────────────────────
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            batch = [(v, k) for k, v in updates.items()]
            psycopg2.extras.execute_batch(
                cur,
                "UPDATE tracks SET origin_region = %s WHERE id = %s",
                batch,
                page_size=500,
            )
        conn.commit()
        print(f"Updated {len(updates)} tracks with origin_region ✓")
    except Exception as exc:
        conn.rollback()
        print(f"DB error: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
