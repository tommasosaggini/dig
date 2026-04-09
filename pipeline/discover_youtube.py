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

import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.discovery_lock import locked_update
from lib.track_filter import is_trash

DIR = ROOT
ENV_PATH = os.path.join(ROOT, ".env")
DISCOVERY_PATH = os.path.join(DIR, "discovery.json")
YT_DISCOVERY_PATH = os.path.join(DIR, "discovery_youtube.json")
LEDGER_PATH = os.path.join(DIR, "ledger.json")
CATALOG_PATH = os.path.join(DIR, "catalog.json")
YT_CHANNELS_CACHE_PATH = os.path.join(DIR, "yt_channels_cache.json")

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

# ── Curated YouTube channels — deep music goldmines ──
# Each channel is a portal into authentic music from specific scenes.
# Channel IDs need to be resolved on first run (search by name).
YT_CHANNELS = {
    # African music
    "Awesome Tapes From Africa": {"region": "West Africa", "tags": ["traditional", "pop", "experimental"]},
    "Nyege Nyege Tapes": {"region": "East Africa", "tags": ["electronic", "experimental", "singeli"]},
    "Sahel Sounds": {"region": "West Africa", "tags": ["desert blues", "traditional", "folk"]},
    "Analog Africa": {"region": "West Africa", "tags": ["funk", "highlife", "afrobeat"]},

    # Electronic / experimental
    "Boiler Room": {"region": "global", "tags": ["electronic", "dj", "live"]},
    "Cercle": {"region": "France", "tags": ["electronic", "ambient", "live"]},
    "HÖR Berlin": {"region": "Germany", "tags": ["techno", "house", "electronic"]},
    "The Lot Radio": {"region": "USA", "tags": ["eclectic", "underground", "dj"]},
    "NTS Radio": {"region": "UK", "tags": ["eclectic", "underground", "experimental"]},
    "Rinse FM": {"region": "UK", "tags": ["electronic", "grime", "bass"]},

    # Live sessions / discovery
    "KEXP": {"region": "USA", "tags": ["indie", "world", "live"]},
    "COLORS": {"region": "Germany", "tags": ["soul", "hiphop", "pop", "global"]},
    "Sofar Sounds": {"region": "global", "tags": ["indie", "acoustic", "live"]},
    "Audiotree Live": {"region": "USA", "tags": ["indie", "rock", "experimental"]},
    "Tiny Desk Concerts": {"region": "USA", "tags": ["eclectic", "live"]},

    # Asian music
    "Asian Music Channel": {"region": "Japan", "tags": ["japanese", "ambient", "experimental"]},

    # Latin America
    "Sangre Nueva": {"region": "South America", "tags": ["latin", "underground", "cumbia"]},
    "ZZK Records": {"region": "Argentina", "tags": ["digital cumbia", "electronic", "latin"]},

    # Middle East / North Africa
    "Habibi Funk Records": {"region": "North Africa", "tags": ["funk", "soul", "arabic"]},

    # Ambient / experimental
    "Hainbach": {"region": "Germany", "tags": ["ambient", "modular", "experimental"]},
    "dublab": {"region": "USA", "tags": ["eclectic", "experimental", "ambient"]},

    # Vinyl / crate digging
    "Vinyl Factory": {"region": "UK", "tags": ["eclectic", "vinyl", "underground"]},

    # Techno / electronic labels
    "music from memory": {"region": "Netherlands", "tags": ["ambient", "new age", "balearic", "experimental"]},
    "Noka": {"region": "global", "tags": ["techno", "electronic", "ambient"]},
    "ballacid": {"region": "global", "tags": ["acid", "techno", "electronic"]},
}

# ── User-added channels (loaded from disk, persists across runs) ──
_USER_CHANNELS_PATH = os.path.join(DIR, "yt_user_channels.json")

def _load_user_channels():
    """Load user-added channels that expand the built-in list."""
    if os.path.exists(_USER_CHANNELS_PATH):
        try:
            with open(_USER_CHANNELS_PATH) as f:
                return json.load(f)
        except:
            pass
    return {}

def add_channel(name, region="global", tags=None):
    """Add a channel to the user-defined list (persisted across runs)."""
    channels = _load_user_channels()
    channels[name] = {"region": region, "tags": tags or ["eclectic"]}
    with open(_USER_CHANNELS_PATH, "w") as f:
        json.dump(channels, f, indent=2)
    print(f"  Added channel: {name} [{region}]")

def get_all_channels():
    """Merge built-in channels with user-added ones."""
    all_ch = dict(YT_CHANNELS)
    all_ch.update(_load_user_channels())
    return all_ch


def _load_channel_cache():
    """Load the channel ID cache from disk."""
    if os.path.exists(YT_CHANNELS_CACHE_PATH):
        with open(YT_CHANNELS_CACHE_PATH) as f:
            return json.load(f)
    return {}


def _save_channel_cache(cache):
    """Persist the channel ID cache to disk."""
    with open(YT_CHANNELS_CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


def resolve_channel_id(channel_name):
    """Search YouTube for a channel by name and return (channel_id, uploads_playlist_id).

    Results are cached in yt_channels_cache.json so each channel is only
    resolved once (saves API quota on subsequent runs).
    Returns (None, None) if the channel cannot be found.
    """
    cache = _load_channel_cache()
    if channel_name in cache:
        entry = cache[channel_name]
        return entry.get("channel_id"), entry.get("uploads_id")

    params = {
        "part": "snippet",
        "q": channel_name,
        "type": "channel",
        "maxResults": 1,
        "key": API_KEY,
    }
    url = "https://www.googleapis.com/youtube/v3/search?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            items = data.get("items", [])
            if not items:
                return None, None
            channel_id = items[0]["id"]["channelId"]
            # Uploads playlist: replace leading 'UC' with 'UU'
            uploads_id = "UU" + channel_id[2:] if channel_id.startswith("UC") else None
            cache[channel_name] = {"channel_id": channel_id, "uploads_id": uploads_id}
            _save_channel_cache(cache)
            return channel_id, uploads_id
    except Exception as e:
        print(f"  Channel resolve error for '{channel_name}': {e}")
        return None, None


def parse_video_title(title):
    """Try to extract artist and track name from a YouTube video title.

    Common patterns:
    - 'Artist - Track'
    - 'Artist - Track (Official Video)'
    - 'Artist | Track'
    - 'Track by Artist'
    - 'Artist: Track'
    - 'Artist "Track"'
    Returns (artist, track_name) or (title, '') if unparseable.
    """
    # Strip common suffixes
    cleaned = title
    for suffix_pat in [
        r'\s*[\(\[](Official\s*(Music\s*)?Video|Official\s*Audio|Live|Lyric\s*Video|'
        r'Official\s*Visualizer|Audio|Lyrics|MV|Music\s*Video|Video\s*Oficial|'
        r'Acoustic|Remix|Full\s*Session|Live\s+on\s+KEXP|Live\s+Session)[\)\]]',
        r'\s*[\(\[].*?session.*?[\)\]]',
        r'\s*【.*?】',
    ]:
        cleaned = re.sub(suffix_pat, '', cleaned, flags=re.IGNORECASE).strip()

    # Try separators: ' - ', ' | ', ' : '
    for sep in [' - ', ' — ', ' – ', ' | ', ': ']:
        if sep in cleaned:
            parts = cleaned.split(sep, 1)
            artist = parts[0].strip()
            track = parts[1].strip()
            if artist and track:
                return artist, track

    # Try "by" pattern: 'Track by Artist'
    by_match = re.match(r'^(.+?)\s+by\s+(.+)$', cleaned, re.IGNORECASE)
    if by_match:
        track, artist = by_match.group(1).strip(), by_match.group(2).strip()
        if artist and track:
            return artist, track

    # Try quoted track: 'Artist "Track"'
    quote_match = re.match(r'^(.+?)\s+["""](.+?)["""]', cleaned)
    if quote_match:
        artist, track = quote_match.group(1).strip(), quote_match.group(2).strip()
        if artist and track:
            return artist, track

    # Fallback: return full title as track name with empty artist
    return cleaned, ''


def _is_compilation(title):
    """Return True if the video title suggests a compilation or mix."""
    skip_words = ["mix", "compilation", "playlist", "best of", "top 10", "top 20",
                  "full album", "complete album", "greatest hits", "nonstop",
                  "megamix", "1 hour", "2 hour", "3 hour"]
    lower = title.lower()
    return any(w in lower for w in skip_words)


def mine_channel(channel_name, config, max_videos=20):
    """Fetch recent uploads from a YouTube channel and extract music tracks.

    Returns (tracks_list, api_calls_used).
    """
    api_calls = 0

    channel_id, uploads_id = resolve_channel_id(channel_name)
    api_calls += 1  # search costs 100 units (only on first resolve, but count it)

    if not uploads_id:
        print(f"    Could not resolve channel: {channel_name}")
        return [], api_calls

    # Fetch recent uploads via playlistItems
    params = {
        "part": "snippet",
        "playlistId": uploads_id,
        "maxResults": max_videos,
        "key": API_KEY,
    }
    url = "https://www.googleapis.com/youtube/v3/playlistItems?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            items = data.get("items", [])
    except Exception as e:
        print(f"    Playlist fetch error for '{channel_name}': {e}")
        return [], api_calls
    api_calls += 1

    if not items:
        return [], api_calls

    # Get video details for duration filtering
    video_ids = []
    for item in items:
        vid = item.get("snippet", {}).get("resourceId", {}).get("videoId")
        if vid:
            video_ids.append(vid)

    details = yt_video_details(video_ids) if video_ids else {}
    api_calls += 1

    tracks = []
    region = config.get("region", "global")

    for item in items:
        snippet = item.get("snippet", {})
        vid = snippet.get("resourceId", {}).get("videoId")
        if not vid:
            continue

        title = snippet.get("title", "")

        # Skip compilations and mixes
        if _is_compilation(title):
            continue

        # Filter by duration: skip very short (<1 min) or very long (>15 min)
        dur = parse_duration_iso(details.get(vid, {}).get("duration_iso", ""))
        if dur < 60 or dur > 900:
            continue

        # Parse artist/track from title
        artist, track_name = parse_video_title(title)
        if not artist and not track_name:
            continue

        # If artist is empty, use channel name as artist
        if not artist:
            artist = channel_name
        if not track_name:
            track_name = title

        if is_known(artist, track_name):
            continue

        thumb = snippet.get("thumbnails", {}).get("medium", {}).get("url", "")

        tracks.append({
            "name": track_name,
            "artist": artist,
            "id": f"yt:{vid}",
            "youtube_id": vid,
            "album": channel_name,
            "query": f"channel:{channel_name}",
            "source": "youtube",
            "thumbnail": thumb,
            "duration_s": dur,
            "popularity": details.get(vid, {}).get("views", 0),
        })

    return tracks, api_calls


# Load ledger for dedup
ledger_set = set()
if os.path.exists(LEDGER_PATH):
    with open(LEDGER_PATH) as f:
        ledger_set = set(k.lower() for k in json.load(f).get("known", []))


def is_known(artist, title):
    a, t = artist.lower(), title.lower()
    return (f"{a} - {t}" in ledger_set or t in ledger_set or
            any(a in k for k in ledger_set))


def run_discovery(max_searches=50, channel_budget=50):
    """Run a batch of YouTube discovery: mine curated channels."""
    yt_discovery = {}
    if os.path.exists(YT_DISCOVERY_PATH):
        with open(YT_DISCOVERY_PATH) as f:
            yt_discovery = json.load(f)

    total_api_calls = 0

    # ── Phase 1: Channel mining (priority) ──
    print("  Phase 1: Mining curated channels\n")
    all_channels = get_all_channels()
    channel_list = list(all_channels.items())
    random.shuffle(channel_list)
    channels_mined = 0
    channel_tracks_total = 0

    for channel_name, config in channel_list:
        if total_api_calls >= channel_budget:
            break

        region = config.get("region", "global")
        print(f"  [{region}] {channel_name} ...", end=" ", flush=True)
        tracks, api_used = mine_channel(channel_name, config, max_videos=20)
        total_api_calls += api_used
        channels_mined += 1

        if tracks:
            # Deduplicate into region bucket
            region_tracks = yt_discovery.get(region, [])
            existing_ids = set(t["id"] for t in region_tracks)
            added = 0
            for t in tracks:
                if t["id"] not in existing_ids:
                    region_tracks.append(t)
                    existing_ids.add(t["id"])
                    added += 1
            yt_discovery[region] = region_tracks
            channel_tracks_total += added
            print(f"{added} tracks")
        else:
            print("0 tracks")

        time.sleep(0.2)

    print(f"\n  Channels mined: {channels_mined}/{len(all_channels)}")
    print(f"  Channel tracks found: {channel_tracks_total}")
    print(f"  API calls used (channels): {total_api_calls}\n")

    # ── Phase 2: Keyword searches (disabled — shallow results) ──
    # Channel mining produces much higher quality tracks than "region + genre" searches.
    # Keeping this code for potential future use with better query strategies.
    keyword_budget = 0  # was: max_searches - total_api_calls
    if keyword_budget > 0:
        print(f"  Phase 2: Keyword searches (budget: {keyword_budget})\n")

        all_searches = []
        for region, cfg in YT_REGIONS.items():
            for q in cfg["queries"]:
                all_searches.append((region, cfg.get("code"), q))

        random.shuffle(all_searches)
        keyword_calls = 0

        for region, region_code, query in all_searches:
            if keyword_calls >= keyword_budget:
                break

            print(f"  {region}: '{query}'")
            items = yt_search(query, region_code=region_code, max_results=8)
            keyword_calls += 1

            if not items:
                time.sleep(0.2)
                continue

            # Get video details for duration filtering
            video_ids = [item["id"]["videoId"] for item in items if "videoId" in item.get("id", {})]
            details = yt_video_details(video_ids)
            keyword_calls += 1  # costs 1 unit (videos.list with <=50 ids)

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

        total_api_calls += keyword_calls
    else:
        print("  Phase 2: Skipped (no budget remaining)\n")

    # Save YouTube discovery
    with open(YT_DISCOVERY_PATH, "w") as f:
        json.dump(yt_discovery, f, indent=2)

    total = sum(len(v) for v in yt_discovery.values())
    print(f"\n  {total} YouTube tracks across {len(yt_discovery)} regions")
    print(f"  Total API calls: {total_api_calls} (budget: {max_searches})")

    return yt_discovery


def merge_into_discovery():
    """Merge YouTube tracks into the main discovery pool (PostgreSQL)."""
    from lib.artist_db import register_tracks

    if not os.path.exists(YT_DISCOVERY_PATH):
        print("No YouTube discovery data. Run without --merge first.")
        return

    with open(YT_DISCOVERY_PATH) as f:
        yt_disc = json.load(f)

    # Use locked_update for atomic read-modify-write
    added_count = [0]  # mutable container so the closure can modify it

    def _merge(discovery):
        for region, yt_tracks in yt_disc.items():
            existing = discovery.get(region, [])
            # Build dedup set from existing tracks (normalized)
            existing_keys = set()
            for t in existing:
                key = f"{t['artist'].lower()}|{t['name'].lower()}"
                existing_keys.add(key)

            new_tracks = []
            for t in yt_tracks:
                if is_trash(t.get("name", "")):
                    continue
                key = f"{t['artist'].lower()}|{t['name'].lower()}"
                if key not in existing_keys:
                    existing.append(t)
                    existing_keys.add(key)
                    new_tracks.append(t)
                    added_count[0] += 1

            discovery[region] = existing
            if new_tracks:
                register_tracks(new_tracks, region=region)

    locked_update(_merge)

    print(f"✓ Merged {added_count[0]} YouTube tracks into discovery.json")

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
