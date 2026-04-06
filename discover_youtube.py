#!/usr/bin/env python3
"""
DIG — YouTube discovery fetcher.

Searches YouTube for music in regions/genres where Spotify is thin.
Outputs tracks in the same format as discovery.json, with source='youtube'.

Usage:
  python3 discover_youtube.py                # fetch a batch
  python3 discover_youtube.py --merge        # merge into discovery.json

Requires YOUTUBE_API_KEY in .env
YouTube Data API v3 quota: 10,000 units/day. Each search = 100 units → ~100 searches/day.
We budget 50 searches per run to leave room for multiple runs.
"""

import json
import os
import sys
import time
import random
from datetime import datetime, timezone

DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(DIR, ".env")
DISCOVERY_PATH = os.path.join(DIR, "discovery.json")
YT_DISCOVERY_PATH = os.path.join(DIR, "discovery_youtube.json")
LEDGER_PATH = os.path.join(DIR, "ledger.json")
CATALOG_PATH = os.path.join(DIR, "catalog.json")

# Load .env
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
if not API_KEY:
    print("Error: YOUTUBE_API_KEY not found in .env")
    print("Get one at https://console.cloud.google.com/apis/credentials")
    print("Enable 'YouTube Data API v3' for your project.")
    sys.exit(1)

import urllib.request
import urllib.parse

def yt_search(query, region_code=None, max_results=10):
    """Search YouTube for music videos. Returns list of video metadata."""
    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "videoCategoryId": "10",  # Music category
        "maxResults": max_results,
        "key": API_KEY,
    }
    if region_code:
        params["regionCode"] = region_code

    url = "https://www.googleapis.com/youtube/v3/search?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            return data.get("items", [])
    except Exception as e:
        print(f"  YouTube API error: {e}")
        return []


def yt_video_details(video_ids):
    """Get duration and stats for videos."""
    if not video_ids:
        return {}
    params = {
        "part": "contentDetails,statistics",
        "id": ",".join(video_ids),
        "key": API_KEY,
    }
    url = "https://www.googleapis.com/youtube/v3/videos?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            result = {}
            for item in data.get("items", []):
                vid = item["id"]
                duration_iso = item.get("contentDetails", {}).get("duration", "")
                views = int(item.get("statistics", {}).get("viewCount", 0))
                result[vid] = {"duration_iso": duration_iso, "views": views}
            return result
    except:
        return {}


def parse_duration_iso(iso):
    """Parse ISO 8601 duration (PT3M45S) to seconds."""
    import re
    m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso or '')
    if not m:
        return 0
    h, mi, s = (int(x) if x else 0 for x in m.groups())
    return h * 3600 + mi * 60 + s


def extract_artist_title(snippet):
    """Try to parse artist and title from YouTube video title.
    Common formats: 'Artist - Title', 'Artist — Title', 'Artist「Title」'
    """
    title = snippet.get("title", "")
    channel = snippet.get("channelTitle", "")

    # Try common separators
    for sep in [" - ", " — ", " – ", " | ", "「"]:
        if sep in title:
            parts = title.split(sep, 1)
            artist = parts[0].strip()
            track = parts[1].strip().rstrip("」").rstrip(")")
            # Clean common suffixes
            for suffix in ["(Official Video)", "(Official Audio)", "(Official Music Video)",
                          "(Audio)", "(Lyrics)", "(MV)", "[Official Video]", "[MV]",
                          "(Official Visualizer)", "【MV】", "(Music Video)"]:
                track = track.replace(suffix, "").strip()
                artist = artist.replace(suffix, "").strip()
            return artist, track

    # Fallback: use channel as artist, title as track name
    track = title
    for suffix in ["(Official Video)", "(Official Audio)", "(Official Music Video)",
                  "(Audio)", "(Lyrics)", "(MV)", "[Official Video]", "[MV]",
                  "(Official Visualizer)", "【MV】", "(Music Video)"]:
        track = track.replace(suffix, "").strip()
    return channel.replace(" - Topic", ""), track


# ── Regions especially strong on YouTube but weak on Spotify ──
YT_REGIONS = {
    "Myanmar": {"code": "MM", "queries": [
        "myanmar music", "မြန်မာသီချင်း", "myanmar traditional", "thangyat",
        "myanmar hip hop", "burmese pop",
    ]},
    "Cambodia": {"code": "KH", "queries": [
        "cambodian music", "khmer song", "cambodian psych rock", "chapei dong veng",
        "khmer traditional", "cambodian pop",
    ]},
    "Laos": {"code": "LA", "queries": [
        "lao music", "lam lao", "isan music", "lao pop",
    ]},
    "Mongolia": {"code": "MN", "queries": [
        "mongolian music", "throat singing mongolia", "morin khuur",
        "mongolian hip hop", "mongolian folk",
    ]},
    "Central Asia": {"code": "KZ", "queries": [
        "kazakh music", "uzbek music", "dombra", "shashmaqam",
        "turkmen music", "kyrgyz music",
    ]},
    "Tibet": {"code": None, "queries": [
        "tibetan music", "tibetan singing bowl", "tibetan folk song",
    ]},
    "Nepal": {"code": "NP", "queries": [
        "nepali music", "nepali folk", "madal music", "newari music",
    ]},
    "West Africa": {"code": "NG", "queries": [
        "griot music", "kora music", "balafon", "wassoulou",
        "manding guitar", "desert blues tuareg",
    ]},
    "East Africa": {"code": "KE", "queries": [
        "taarab music", "benga music", "singeli", "ethio-jazz",
        "ugandan music", "nyatiti",
    ]},
    "Central Africa": {"code": "CD", "queries": [
        "congolese rumba", "soukous", "likembe music", "pygmy music",
        "bikutsi", "makossa",
    ]},
    "Pacific Islands": {"code": "FJ", "queries": [
        "polynesian music", "hawaiian slack key", "tongan music",
        "samoan music", "fijian music", "tahitian music",
    ]},
    "Middle East": {"code": "SA", "queries": [
        "oud music", "maqam", "dabke", "khaleeji",
        "yemeni music", "iraqi maqam",
    ]},
    "Iran": {"code": None, "queries": [
        "persian classical music", "dastgah", "tar instrument",
        "kamancheh", "santur iran", "persian folk",
    ]},
    "North Africa": {"code": "MA", "queries": [
        "gnawa music", "chaabi algérien", "raï music",
        "andalusi music", "amazigh music",
    ]},
    "China": {"code": "HK", "queries": [
        "guqin music", "erhu music", "pipa music",
        "chinese folk", "chinese underground", "chinese ambient",
    ]},
    "Hong Kong": {"code": "HK", "queries": [
        "hong kong indie", "canto underground", "hong kong jazz",
    ]},
    "Japan": {"code": "JP", "queries": [
        "japanese ambient", "japanese noise", "min'yō",
        "gagaku", "shamisen", "japanese underground",
    ]},
    "India": {"code": "IN", "queries": [
        "raga music", "baul music", "qawwali",
        "indian classical", "lavani", "bhajan",
    ]},
    "South America": {"code": "AR", "queries": [
        "charango music", "zampoña", "andean music",
        "murga uruguaya", "candombe", "cumbia digital",
    ]},
    "Caribbean": {"code": "JM", "queries": [
        "steelpan trinidad", "calypso classic", "mento jamaica",
        "tumba curaçao", "biguine martinique",
    ]},
}

# Load ledger for dedup
ledger_set = set()
if os.path.exists(LEDGER_PATH):
    with open(LEDGER_PATH) as f:
        ledger_set = set(k.lower() for k in json.load(f).get("known", []))


def is_known(artist, title):
    a, t = artist.lower(), title.lower()
    return (f"{a} - {t}" in ledger_set or t in ledger_set or
            any(a in k for k in ledger_set))


def run_discovery(max_searches=50):
    """Run a batch of YouTube discovery searches."""
    yt_discovery = {}
    if os.path.exists(YT_DISCOVERY_PATH):
        with open(YT_DISCOVERY_PATH) as f:
            yt_discovery = json.load(f)

    # Collect all (region, query) pairs and shuffle for fairness
    all_searches = []
    for region, cfg in YT_REGIONS.items():
        for q in cfg["queries"]:
            all_searches.append((region, cfg.get("code"), q))

    random.shuffle(all_searches)
    searches_done = 0

    for region, region_code, query in all_searches:
        if searches_done >= max_searches:
            break

        print(f"  {region}: '{query}'")
        items = yt_search(query, region_code=region_code, max_results=8)
        searches_done += 1

        if not items:
            time.sleep(0.2)
            continue

        # Get video details for duration filtering
        video_ids = [item["id"]["videoId"] for item in items if "videoId" in item.get("id", {})]
        details = yt_video_details(video_ids)
        searches_done += 1  # costs 1 unit (videos.list with <=50 ids)

        tracks = []
        for item in items:
            vid = item.get("id", {}).get("videoId")
            if not vid:
                continue

            snippet = item.get("snippet", {})
            artist, title = extract_artist_title(snippet)
            if not artist or not title:
                continue

            # Filter by duration: skip very short (<1 min) or very long (>15 min)
            dur = parse_duration_iso(details.get(vid, {}).get("duration_iso", ""))
            if dur < 60 or dur > 900:
                continue

            if is_known(artist, title):
                continue

            thumb = snippet.get("thumbnails", {}).get("medium", {}).get("url", "")

            tracks.append({
                "name": title,
                "artist": artist,
                "id": f"yt:{vid}",
                "youtube_id": vid,
                "source": "youtube",
                "album": "",
                "popularity": details.get(vid, {}).get("views", 0),
                "query": query,
                "thumbnail": thumb,
                "duration_s": dur,
            })

        # Deduplicate
        region_tracks = yt_discovery.get(region, [])
        existing_ids = set(t["id"] for t in region_tracks)
        for t in tracks:
            if t["id"] not in existing_ids:
                region_tracks.append(t)
                existing_ids.add(t["id"])

        yt_discovery[region] = region_tracks
        time.sleep(0.2)

    # Save YouTube discovery
    with open(YT_DISCOVERY_PATH, "w") as f:
        json.dump(yt_discovery, f, indent=2)

    total = sum(len(v) for v in yt_discovery.values())
    print(f"\n✓ {total} YouTube tracks across {len(yt_discovery)} regions")
    print(f"  Searches used: {searches_done} (budget: {max_searches})")

    return yt_discovery


def merge_into_discovery():
    """Merge YouTube tracks into the main discovery.json."""
    if not os.path.exists(YT_DISCOVERY_PATH):
        print("No YouTube discovery data. Run without --merge first.")
        return

    with open(YT_DISCOVERY_PATH) as f:
        yt_disc = json.load(f)

    discovery = {}
    if os.path.exists(DISCOVERY_PATH):
        with open(DISCOVERY_PATH) as f:
            discovery = json.load(f)

    # Merge: add YouTube tracks to each region, dedup by normalized artist+title
    added = 0
    for region, yt_tracks in yt_disc.items():
        existing = discovery.get(region, [])
        # Build dedup set from existing tracks (normalized)
        existing_keys = set()
        for t in existing:
            key = f"{t['artist'].lower()}|{t['name'].lower()}"
            existing_keys.add(key)

        for t in yt_tracks:
            key = f"{t['artist'].lower()}|{t['name'].lower()}"
            if key not in existing_keys:
                existing.append(t)
                existing_keys.add(key)
                added += 1

        discovery[region] = existing

    with open(DISCOVERY_PATH, "w") as f:
        json.dump(discovery, f)

    print(f"✓ Merged {added} YouTube tracks into discovery.json")

    # Feed new genres into catalog
    if os.path.exists(CATALOG_PATH):
        with open(CATALOG_PATH) as f:
            catalog = json.load(f)
        now = datetime.now(timezone.utc).isoformat()
        new_genres = 0
        for region, tracks in yt_disc.items():
            for t in tracks:
                genre = t.get("query", "")
                if genre and genre not in catalog.get("genres", {}):
                    catalog.setdefault("genres", {})[genre] = {"source": "youtube", "added": now}
                    new_genres += 1
        if new_genres:
            print(f"  {new_genres} new genres added to catalog")
            with open(CATALOG_PATH, "w") as f:
                json.dump(catalog, f, indent=2)


if __name__ == "__main__":
    if "--merge" in sys.argv:
        merge_into_discovery()
    else:
        print("\n🎬 YOUTUBE DISCOVERY\n")
        run_discovery()
        print("\nRun with --merge to add these to the main discovery pool.")
