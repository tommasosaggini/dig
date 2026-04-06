#!/usr/bin/env python3
"""
Pre-fetch discovery tracks from around the world.
Uses multiple strategies so we don't only find what we already know to name:
  1. New releases per market (what's happening NOW in each country)
  2. Featured/editorial playlists per market (what locals are listening to)
  3. Random character searches per market (the unknown unknowns)
  4. Genre-hint searches as a small supplement (not the main source)
Filters out anything already in the ledger.
Outputs discovery.json for the web UI.
"""

import json
import os
import sys
import time
import random
import string

import spotipy
from spotipy.oauth2 import SpotifyOAuth

DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(DIR, ".env")

# Load .env
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    scope="streaming user-read-email user-read-private user-library-read user-top-read user-read-recently-played user-read-playback-state user-modify-playback-state",
    redirect_uri=os.environ.get("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8000/callback"),
    cache_path=os.path.join(DIR, ".spotify_token_cache"),
))

with open(os.path.join(DIR, "ledger.json")) as f:
    ledger = json.load(f)

known_lower = set()
for k in ledger["known"]:
    known_lower.add(k.lower())
    if " - " in k:
        known_lower.add(k.split(" - ")[0].strip().lower())

def is_known(artist, name=""):
    a = artist.lower()
    n = name.lower()
    full = f"{a} - {n}"
    return a in known_lower or full in known_lower or any(a in k for k in known_lower)

def extract_track(t, source=""):
    """Pull a track dict from a Spotify track object."""
    artists = t.get("artists", [])
    artist = ", ".join(a["name"] for a in artists)
    artist_ids = [a["id"] for a in artists if a.get("id")]
    return {
        "name": t.get("name", ""),
        "artist": artist,
        "artist_ids": artist_ids,
        "id": t["id"],
        "album": t.get("album", {}).get("name", ""),
        "popularity": t.get("popularity", 0),
        "query": source,
    }

# Regions mapped to Spotify market codes (ISO 3166-1 alpha-2)
# Each region can have multiple markets to pull from
REGIONS = {
    "USA": ["US"],
    "UK": ["GB"],
    "France": ["FR"],
    "Germany": ["DE"],
    "Italy": ["IT"],
    "Spain": ["ES"],
    "Portugal": ["PT"],
    "Nordic": ["SE", "NO", "FI", "DK", "IS"],
    "Netherlands": ["NL"],
    "Belgium": ["BE"],
    "Eastern Europe": ["PL", "CZ", "HU", "RO", "BG"],
    "Russia": ["RU"],  # may not work depending on Spotify availability
    "Japan": ["JP"],
    "South Korea": ["KR"],
    "China": ["HK"],  # mainland not on Spotify, use HK as proxy
    "Taiwan": ["TW"],
    "Hong Kong": ["HK"],
    "Thailand": ["TH"],
    "Vietnam": ["VN"],
    "Indonesia": ["ID"],
    "Cambodia": ["KH"],
    "Philippines": ["PH"],
    "Malaysia": ["MY"],
    "India": ["IN"],
    "South Asia": ["PK", "BD", "LK"],
    "Iran": ["TR"],  # Iran not on Spotify, use neighbors
    "Turkey": ["TR"],
    "Middle East": ["SA", "AE", "EG", "IL", "LB"],
    "West Africa": ["NG", "GH", "SN"],
    "East Africa": ["KE", "TZ", "UG"],
    "Southern Africa": ["ZA", "ZW"],
    "North Africa": ["MA", "DZ", "TN", "EG"],
    "Brazil": ["BR"],
    "Argentina": ["AR"],
    "Colombia": ["CO"],
    "Chile": ["CL"],
    "Peru": ["PE"],
    "Mexico": ["MX"],
    "Caribbean": ["JM", "TT", "DO"],
    "Canada": ["CA"],
    "Australia": ["AU"],
    "New Zealand": ["NZ"],
    "Ireland": ["IE"],
    "Switzerland": ["CH"],
    "Greece": ["GR"],
}

# Characters from various scripts for "blind" searches
# The idea: search a single character in a market and see what comes up
# This finds things we'd never think to search for
SCRIPT_CHARS = {
    # Latin + diacritics
    "latin": list("abcdefghijklmnopqrstuvwxyz"),
    # Arabic
    "arabic": list("ابتثجحخدذرزسشصضطظعغفقكلمنهوي"),
    # Thai
    "thai": list("กขคงจฉชซญดตถทนบปผพฟมยรลวศสหอ"),
    # Japanese hiragana
    "japanese": list("あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわ"),
    # Korean jamo
    "korean": list("가나다라마바사아자차카타파하"),
    # Devanagari
    "devanagari": list("अआइईउऊएऐओऔकखगघचछजझटठडढणतथदधनपफबभमयरलवशषसह"),
    # Cyrillic
    "cyrillic": list("абвгдежзиклмнопрстуфхцчшщэюя"),
    # Chinese
    "chinese": list("的一是不了人我在有他这中大来上个国到说们为子和你地出会也时要就可以"),
}

# Which script chars to try per market
MARKET_SCRIPTS = {
    "JP": "japanese", "KR": "korean", "TH": "thai",
    "SA": "arabic", "AE": "arabic", "EG": "arabic", "MA": "arabic", "DZ": "arabic",
    "TN": "arabic", "LB": "arabic",
    "IN": "devanagari", "BD": "devanagari", "PK": "arabic",
    "RU": "cyrillic",
    "HK": "chinese", "TW": "chinese",
}

_rate_limited = False

def safe_call(fn, *args, **kwargs):
    """Call a Spotify API function with retry on rate limit."""
    global _rate_limited
    if _rate_limited:
        return None  # skip all further calls this run
    try:
        result = fn(*args, **kwargs)
        time.sleep(0.15)  # gentle throttle
        return result
    except spotipy.SpotifyException as e:
        if e.http_status == 429:
            wait = int(e.headers.get("Retry-After", 5)) if hasattr(e, 'headers') and e.headers else 5
            if wait > 60:
                print(f"  (rate limited for {wait}s — saving progress and stopping)")
                _rate_limited = True
                return None
            print(f"  (rate limited, waiting {wait}s)")
            time.sleep(min(wait, 60))
            try:
                return fn(*args, **kwargs)
            except:
                _rate_limited = True
                return None
        return None
    except:
        return None

def fetch_new_releases(market, limit=10):
    """Get new album releases in a market, extract tracks."""
    tracks = []
    albums = safe_call(sp.new_releases, country=market, limit=limit)
    if not albums:
        return tracks
    for album in albums["albums"]["items"]:
        album_tracks = safe_call(sp.album_tracks, album["id"], market=market, limit=3)
        if album_tracks:
            for t in album_tracks["items"]:
                t["album"] = {"name": album.get("name", "")}
                t["popularity"] = 0
                tracks.append(extract_track(t, f"new:{market}"))
        time.sleep(0.2)
    return tracks

def fetch_featured_playlists(market, limit=3):
    """Get tracks from featured/editorial playlists in a market."""
    tracks = []
    playlists = safe_call(sp.featured_playlists, country=market, limit=limit)
    if not playlists:
        return tracks
    for pl in playlists["playlists"]["items"]:
        items = safe_call(sp.playlist_tracks, pl["id"], limit=10, market=market)
        if items:
            for item in items["items"]:
                t = item.get("track")
                if t and t.get("id"):
                    tracks.append(extract_track(t, f"playlist:{market}"))
        time.sleep(0.2)
    return tracks

def fetch_random_search(market, n=3):
    """Search random characters in the market's script. Pure serendipity."""
    tracks = []
    script = MARKET_SCRIPTS.get(market, "latin")
    chars = SCRIPT_CHARS.get(script, SCRIPT_CHARS["latin"])

    for _ in range(n):
        q = random.choice(chars)
        if random.random() > 0.5:
            q += random.choice(chars)
        offset = random.randint(0, 50)
        results = safe_call(sp.search, q=q, type="track", limit=10, offset=offset, market=market)
        if results:
            for t in results["tracks"]["items"]:
                if t.get("id"):
                    tracks.append(extract_track(t, f"random:{market}"))
        time.sleep(0.25)
    return tracks

def fetch_genre_hints(market, hints, n=2):
    """Small supplement: a couple genre-hint searches. Not the main source."""
    tracks = []
    sample = random.sample(hints, min(n, len(hints)))
    for q in sample:
        offset = random.randint(0, 30)
        results = safe_call(sp.search, q=q, type="track", limit=10, offset=offset, market=market)
        if results:
            for t in results["tracks"]["items"]:
                if t.get("id"):
                    tracks.append(extract_track(t, f"hint:{q}"))
        time.sleep(0.25)
    return tracks

# Optional genre hints — just a few per region as supplement, not primary source
GENRE_HINTS = {
    "USA": ["zydeco", "footwork", "gospel", "delta blues", "go-go"],
    "UK": ["grime", "northern soul", "jungle"],
    "France": ["musique concrete", "chanson", "zouk"],
    "Germany": ["krautrock", "schlager", "kosmische"],
    "Italy": ["canzone napoletana", "italo disco", "cantautori"],
    "Spain": ["flamenco", "copla", "rumba"],
    "Portugal": ["fado", "guitarra portuguesa"],
    "Nordic": ["joik", "kulning", "finnish tango"],
    "Netherlands": ["gabber", "levenslied"],
    "Belgium": ["new beat"],
    "Eastern Europe": ["turbofolk", "chalga", "klezmer", "csárdás", "manele"],
    "Russia": ["bard music", "estrada"],
    "Japan": ["enka", "min'yō", "kayōkyoku"],
    "South Korea": ["pansori", "trot"],
    "China": ["guqin", "erhu"],
    "Taiwan": ["hokkien pop"],
    "Hong Kong": ["cantopop"],
    "Thailand": ["mor lam", "luk thung"],
    "Vietnam": ["nhạc vàng", "cải lương"],
    "Indonesia": ["dangdut", "gamelan", "keroncong", "koplo"],
    "Cambodia": ["khmer classical"],
    "Philippines": ["kundiman", "harana"],
    "Malaysia": ["dikir barat"],
    "India": ["qawwali", "carnatic", "bhangra", "ghazal", "baul"],
    "South Asia": ["rabindra sangeet", "qawwali"],
    "Iran": ["dastgah", "bandari"],
    "Turkey": ["arabesk", "türkü", "fasıl"],
    "Middle East": ["dabke", "oud taqasim", "khaleeji", "maqam"],
    "West Africa": ["highlife", "mbalax", "griot", "wassoulou"],
    "East Africa": ["taarab", "benga", "ethio-jazz", "singeli"],
    "Southern Africa": ["mbaqanga", "maskandi", "chimurenga", "shangaan"],
    "North Africa": ["gnawa", "raï", "chaabi"],
    "Brazil": ["forró", "maracatu", "choro", "tecnobrega"],
    "Argentina": ["tango milonga", "chamame", "folklore argentino"],
    "Colombia": ["vallenato", "champeta", "currulao"],
    "Chile": ["cueca", "nueva canción"],
    "Peru": ["huayno", "chicha", "festejo", "marinera"],
    "Mexico": ["son jarocho", "huapango", "son huasteco"],
    "Caribbean": ["kompa", "calypso", "mento", "steelpan", "chutney"],
    "Canada": ["throat singing"],
    "Australia": ["didgeridoo"],
    "New Zealand": ["kapa haka"],
    "Ireland": ["sean-nós", "céilí"],
    "Switzerland": ["alphorn"],
    "Greece": ["rebetiko", "laïkó", "nisiotika"],
}


print("\n🔍 DISCOVERING NEW MUSIC\n")
print("Strategy: new releases + local playlists + random searches + genre hints\n")

discovery = {}
total_found = 0

for region, markets in REGIONS.items():
    if _rate_limited:
        print(f"  (skipping remaining regions — rate limited)")
        break
    print(f"→ {region} ({', '.join(markets)})...")
    all_tracks = []

    for market in markets:
        # 1. Random character searches — the unknown unknowns (main source)
        all_tracks.extend(fetch_random_search(market, n=5))

        # 2. Genre hint supplement — sample up to 5 hints per run
        hints = GENRE_HINTS.get(region, [])
        if hints:
            all_tracks.extend(fetch_genre_hints(market, hints, n=min(5, len(hints))))

        # 3. Try new releases + playlists (may 403 on some apps)
        all_tracks.extend(fetch_new_releases(market, limit=6))
        all_tracks.extend(fetch_featured_playlists(market, limit=2))

        time.sleep(0.5)  # breathing room between markets

        time.sleep(0.2)

    # Filter out known
    filtered = [t for t in all_tracks if not is_known(t["artist"], t["name"])]

    # Deduplicate by track id
    seen = set()
    unique = []
    for t in filtered:
        if t["id"] not in seen:
            seen.add(t["id"])
            unique.append(t)

    # Shuffle and keep up to 80 per region
    random.shuffle(unique)
    discovery[region] = unique[:80]
    total_found += len(discovery[region])

    sources = {}
    for t in discovery[region]:
        src = t["query"].split(":")[0]
        sources[src] = sources.get(src, 0) + 1
    src_str = ", ".join(f"{k}:{v}" for k, v in sorted(sources.items()))
    print(f"  ✓ {len(discovery[region])} tracks ({src_str})")

# Save
out_path = os.path.join(DIR, "discovery.json")
with open(out_path, "w") as f:
    json.dump(discovery, f)

print(f"\n✓ {total_found} discovery tracks across {len(discovery)} regions")
print(f"Saved to {out_path}")

# ── Feed back into catalog ──
CATALOG_PATH = os.path.join(DIR, "catalog.json")
if os.path.exists(CATALOG_PATH):
    print("\n📊 Feeding findings back into catalog...")
    with open(CATALOG_PATH) as f:
        catalog = json.load(f)

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    new_genres = 0

    for region, tracks in discovery.items():
        for t in tracks:
            query = t.get("query", "")
            # Extract genre from hint sources
            if ":" in query:
                prefix, val = query.split(":", 1)
                if prefix == "hint":
                    genre = val
                else:
                    continue
            else:
                continue

            # Add genre to catalog if new
            if genre and genre not in catalog.get("genres", {}):
                catalog.setdefault("genres", {})[genre] = {"source": "discovery", "added": now}
                new_genres += 1

            # Update fetched count for matching cells
            if genre:
                for decade in ["1920s","1930s","1940s","1950s","1960s","1970s","1980s","1990s","2000s","2010s","2020s"]:
                    key = f"{region}|{genre}|{decade}"
                    if key in catalog.get("cells", {}):
                        catalog["cells"][key]["fetched"] = catalog["cells"][key].get("fetched", 0) + 1
                        catalog["cells"][key]["last_fetched"] = now

    # Harvest artist genres from Spotify (artist objects carry genre tags)
    # This discovers genres we never thought to search for
    artist_ids_seen = set()
    artist_id_list = []
    for region, tracks in discovery.items():
        for t in tracks:
            for aid in t.get("artist_ids", []):
                if aid and aid not in artist_ids_seen:
                    artist_ids_seen.add(aid)
                    artist_id_list.append(aid)

    # Fetch artist details in batches of 50
    harvested = 0
    for i in range(0, min(len(artist_id_list), 500), 50):
        batch_ids = artist_id_list[i:i+50]
        try:
            artists_resp = sp.artists(batch_ids)
            for a in artists_resp.get("artists", []):
                if not a:
                    continue
                for g in a.get("genres", []):
                    if g not in catalog.get("genres", {}):
                        catalog.setdefault("genres", {})[g] = {"source": "artist_harvest", "added": now}
                        harvested += 1
            time.sleep(0.3)
        except:
            pass

    if harvested > 0:
        print(f"  {harvested} new genres harvested from artist metadata")

    if new_genres > 0:
        print(f"  {new_genres} new genres discovered and added to catalog")

    catalog["last_scan"] = now
    with open(CATALOG_PATH, "w") as f:
        json.dump(catalog, f, indent=2)
    print("  Catalog updated.")
