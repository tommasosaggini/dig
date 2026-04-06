#!/usr/bin/env python3
"""
DIG — Catalog builder.

Builds and maintains a map of the music landscape: every (region × genre × decade)
cell we know about, how deep the pool is on Spotify, and how much we've explored.

Usage:
  python3 catalog.py              # incremental scan (pick up where we left off)
  python3 catalog.py --seed       # first run: seed genres from all sources
  python3 catalog.py --status     # print coverage summary

The catalog is the intelligence layer. Discovery reads it to decide what to fetch next.
"""

import json
import os
import sys
import time
import random
from datetime import datetime, timezone

import spotipy
from spotipy.oauth2 import SpotifyOAuth

DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(DIR, ".env")
CATALOG_PATH = os.path.join(DIR, "catalog.json")
LEDGER_PATH = os.path.join(DIR, "ledger.json")

# Load .env
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    scope="streaming user-read-email user-read-private user-library-read",
    redirect_uri=os.environ.get("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8000/callback"),
    cache_path=os.path.join(DIR, ".spotify_token_cache"),
))

# ── Rate limit aware API calls ──

_call_count = 0
_call_window_start = time.time()
MAX_CALLS_PER_WINDOW = 80  # stay well under Spotify's limit
WINDOW_SECONDS = 30

def safe_call(fn, *args, **kwargs):
    """Call Spotify API with rate limit awareness."""
    global _call_count, _call_window_start

    # Throttle: if we've made too many calls in this window, wait
    elapsed = time.time() - _call_window_start
    if elapsed > WINDOW_SECONDS:
        _call_count = 0
        _call_window_start = time.time()

    if _call_count >= MAX_CALLS_PER_WINDOW:
        wait = WINDOW_SECONDS - elapsed + 1
        if wait > 0:
            print(f"  (throttling {wait:.0f}s to avoid rate limit)")
            time.sleep(wait)
        _call_count = 0
        _call_window_start = time.time()

    _call_count += 1

    try:
        return fn(*args, **kwargs)
    except spotipy.SpotifyException as e:
        if e.http_status == 429:
            wait = int(e.headers.get("Retry-After", 10)) if hasattr(e, 'headers') and e.headers else 10
            print(f"  (rate limited, waiting {wait}s)")
            time.sleep(min(wait, 60))
            try:
                _call_count += 1
                return fn(*args, **kwargs)
            except:
                return None
        return None
    except Exception as e:
        return None


# ── Regions & Markets ──

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
    "Switzerland": ["CH"],
    "Ireland": ["IE"],
    "Greece": ["GR"],
    "Eastern Europe": ["PL", "CZ", "HU", "RO", "BG"],
    "Russia": ["RU"],
    "Japan": ["JP"],
    "South Korea": ["KR"],
    "China": ["HK"],
    "Taiwan": ["TW"],
    "Hong Kong": ["HK"],
    "Thailand": ["TH"],
    "Vietnam": ["VN"],
    "Indonesia": ["ID"],
    "Cambodia": ["KH"],
    "Philippines": ["PH"],
    "Malaysia": ["MY"],
    "Myanmar": ["MM"],
    "India": ["IN"],
    "South Asia": ["PK", "BD", "LK"],
    "Iran": ["TR"],
    "Turkey": ["TR"],
    "Middle East": ["SA", "AE", "EG", "IL", "LB"],
    "West Africa": ["NG", "GH", "SN"],
    "East Africa": ["KE", "TZ", "UG"],
    "Southern Africa": ["ZA", "ZW"],
    "North Africa": ["MA", "DZ", "TN", "EG"],
    "Central Africa": ["CD", "CM"],
    "Brazil": ["BR"],
    "Argentina": ["AR"],
    "Colombia": ["CO"],
    "Chile": ["CL"],
    "Peru": ["PE"],
    "Mexico": ["MX"],
    "Caribbean": ["JM", "TT", "DO", "CU"],
    "Central America": ["CR", "PA", "GT"],
    "Canada": ["CA"],
    "Australia": ["AU"],
    "New Zealand": ["NZ"],
    "Pacific Islands": ["FJ"],
}

# ── Genre seed list ──
# Comprehensive: hand-curated micro-genres + regional traditions.
# This is the starting point — the catalog grows as we discover more.

GENRE_SEEDS = [
    # ── Africa ──
    "afrobeats", "afropop", "highlife", "jùjú", "fuji", "apala", "sakara",
    "mbalax", "griot", "wassoulou", "mandingue", "desert blues",
    "taarab", "benga", "bongo flava", "singeli", "gengetone",
    "ethio-jazz", "ethio-pop", "tizita",
    "mbaqanga", "maskandi", "isicathamiya", "kwaito", "amapiano", "gqom",
    "chimurenga", "sungura", "shangaan electro",
    "gnawa", "raï", "chaabi", "andalusi",
    "soukous", "rumba congolaise", "ndombolo",
    "bikutsi", "makossa", "coupé-décalé",
    "kuduro", "semba", "kizomba", "morna", "coladeira",
    "palm wine", "afro-funk", "afro-disco",
    # ── Americas ──
    "blues", "delta blues", "chicago blues", "electric blues",
    "jazz", "bebop", "cool jazz", "free jazz", "hard bop", "modal jazz",
    "soul", "motown", "northern soul", "neo-soul", "quiet storm",
    "funk", "p-funk", "go-go", "boogie",
    "gospel", "gospel quartet", "contemporary gospel",
    "r&b", "new jack swing", "contemporary r&b",
    "hip hop", "boom bap", "gangsta rap", "trap", "drill", "chopped and screwed",
    "footwork", "juke", "baltimore club", "jersey club",
    "house", "chicago house", "deep house", "acid house",
    "techno", "detroit techno",
    "country", "bluegrass", "appalachian folk", "old-time",
    "zydeco", "cajun",
    "rock and roll", "surf rock", "garage rock", "psychedelic rock",
    "punk", "hardcore punk", "post-punk", "emo",
    "grunge", "alternative rock", "indie rock", "shoegaze", "noise rock",
    "metal", "doom metal", "drone metal", "black metal", "death metal",
    "singer-songwriter", "americana", "folk rock",
    "disco", "italo disco", "nu-disco",
    "synth-pop", "new wave", "darkwave", "cold wave",
    "electronic", "ambient", "idm", "glitch", "vaporwave",
    "bossa nova", "tropicália", "mpb", "samba", "pagode",
    "forró", "maracatu", "choro", "tecnobrega", "baile funk", "sertanejo",
    "tango", "milonga", "chacarera", "folklore argentino", "cumbia villera",
    "vallenato", "champeta", "currulao", "cumbia",
    "huayno", "chicha", "festejo", "marinera",
    "son jarocho", "huapango", "norteño", "corridos", "ranchera", "banda",
    "reggaeton", "dembow",
    "reggae", "dub", "dancehall", "ska", "rocksteady", "mento",
    "kompa", "calypso", "soca", "steelpan", "chutney",
    # ── Europe ──
    "chanson", "musette", "yé-yé", "french pop", "french house",
    "krautrock", "kosmische", "schlager", "neue deutsche welle", "hamburger schule",
    "canzone napoletana", "cantautori", "italo pop",
    "flamenco", "copla", "rumba catalana", "spanish pop",
    "fado", "guitarra portuguesa",
    "joik", "kulning", "finnish tango", "nordic jazz", "scandinavian folk",
    "gabber", "dutch house",
    "new beat", "belgian techno",
    "rebetiko", "laïkó", "nisiotika", "entechno",
    "turbofolk", "chalga", "klezmer", "csárdás", "manele",
    "bard music", "estrada", "russian chanson",
    "celtic", "sean-nós", "céilí",
    "grime", "uk garage", "jungle", "drum and bass", "dubstep", "trip hop", "britpop",
    "northern soul", "acid jazz", "uk funky", "broken beat",
    # ── Asia ──
    "enka", "min'yō", "kayōkyoku", "city pop", "shibuya-kei", "j-pop", "visual kei",
    "japanese ambient", "japanese jazz", "noise",
    "pansori", "trot", "k-pop", "korean indie",
    "c-pop", "mandopop", "cantopop", "chinese classical",
    "taiwanese hokkien pop", "mandarin indie",
    "luk thung", "mor lam", "thai funk", "thai pop",
    "nhạc vàng", "cải lương", "v-pop",
    "dangdut", "gamelan", "keroncong", "koplo", "sundanese pop",
    "khmer classical", "cambodian psych rock",
    "kundiman", "harana", "opm",
    "dikir barat", "joget",
    "myanmar traditional", "thangyat",
    "hindustani classical", "carnatic", "ghazal", "qawwali",
    "bhangra", "bollywood", "indian folk", "baul", "rabindra sangeet", "filmi",
    "dastgah", "bandari", "persian pop",
    "arabesk", "türkü", "fasıl", "turkish psychedelic",
    "dabke", "oud", "khaleeji", "maqam",
    "israeli pop", "mizrahi",
    # ── Oceania ──
    "didgeridoo", "australian rock", "australian hip hop",
    "kapa haka", "polynesian", "pacific reggae",
    # ── Cross-cutting ──
    "world music", "lo-fi", "chillwave", "witch house",
    "experimental", "field recordings", "drone",
    "library music", "exotica", "space age pop",
    "easy listening", "lounge", "elevator music",
    "new age", "meditation", "healing",
    "children's music", "lullaby",
    "film score", "soundtrack", "video game music",
    "spoken word", "poetry", "comedy",
]

# Decades to scan
DECADES = ["1920s", "1930s", "1940s", "1950s", "1960s", "1970s", "1980s", "1990s", "2000s", "2010s", "2020s"]

# Year ranges for search
DECADE_RANGES = {
    "1920s": "1920-1929", "1930s": "1930-1939", "1940s": "1940-1949",
    "1950s": "1950-1959", "1960s": "1960-1969", "1970s": "1970-1979",
    "1980s": "1980-1989", "1990s": "1990-1999", "2000s": "2000-2009",
    "2010s": "2010-2019", "2020s": "2020-2029",
}


# ── Catalog structure ──
# catalog.json:
# {
#   "version": 2,
#   "last_scan": "2026-04-06T...",
#   "genres": { "afrobeats": { "source": "seed", "added": "..." }, ... },
#   "cells": {
#     "West Africa|afrobeats|2020s": {
#       "region": "West Africa",
#       "genre": "afrobeats",
#       "decade": "2020s",
#       "pool_size": 15000,        # estimated from Spotify search total
#       "explored": 12,            # tracks we've fetched and user has heard
#       "fetched": 40,             # tracks we've fetched into discovery pool
#       "last_scanned": "2026-04-06T...",
#       "last_fetched": "2026-04-06T...",
#     }
#   },
#   "scan_queue": ["cell_key", ...],  # cells not yet scanned, for incremental runs
# }

def load_catalog():
    if os.path.exists(CATALOG_PATH):
        with open(CATALOG_PATH) as f:
            return json.load(f)
    return {"version": 2, "last_scan": None, "genres": {}, "cells": {}, "scan_queue": []}

def save_catalog(catalog):
    catalog["last_scan"] = datetime.now(timezone.utc).isoformat()
    with open(CATALOG_PATH, "w") as f:
        json.dump(catalog, f, indent=2)

def load_ledger():
    if os.path.exists(LEDGER_PATH):
        with open(LEDGER_PATH) as f:
            return set(k.lower() for k in json.load(f).get("known", []))
    return set()


def estimate_pool_size(genre, market, decade=None):
    """Ask Spotify how many tracks match a genre+market+decade query."""
    q = f"genre:{genre}" if " " not in genre else f'genre:"{genre}"'
    if decade and decade in DECADE_RANGES:
        start, end = DECADE_RANGES[decade].split("-")
        q += f" year:{start}-{end}"

    result = safe_call(sp.search, q=q, type="track", limit=1, market=market)
    if result and "tracks" in result:
        return result["tracks"]["total"]
    return 0


def cell_key(region, genre, decade):
    return f"{region}|{genre}|{decade}"


def seed_catalog(catalog):
    """Seed the catalog with all known genres and build the scan queue."""
    now = datetime.now(timezone.utc).isoformat()

    added = 0
    for g in GENRE_SEEDS:
        if g not in catalog["genres"]:
            catalog["genres"][g] = {"source": "seed", "added": now}
            added += 1

    print(f"Genres in catalog: {len(catalog['genres'])} ({added} new)")

    # Build cells for every (region × genre × decade) combination
    # that doesn't already exist
    new_cells = 0
    for region in REGIONS:
        for genre in catalog["genres"]:
            for decade in DECADES:
                key = cell_key(region, genre, decade)
                if key not in catalog["cells"]:
                    catalog["cells"][key] = {
                        "region": region,
                        "genre": genre,
                        "decade": decade,
                        "pool_size": None,  # unknown until scanned
                        "explored": 0,
                        "fetched": 0,
                        "last_scanned": None,
                        "last_fetched": None,
                    }
                    new_cells += 1

    total = len(catalog["cells"])
    print(f"Cells in catalog: {total} ({new_cells} new)")
    print(f"  = {len(REGIONS)} regions × {len(catalog['genres'])} genres × {len(DECADES)} decades")

    # Build scan queue: cells we haven't scanned yet, randomized for fairness
    unscanned = [k for k, v in catalog["cells"].items() if v["pool_size"] is None]
    random.shuffle(unscanned)
    catalog["scan_queue"] = unscanned
    print(f"Scan queue: {len(unscanned)} cells to probe")

    save_catalog(catalog)
    return catalog


def scan_batch(catalog, batch_size=60):
    """Scan a batch of cells: estimate pool sizes on Spotify.

    Each cell = 1 API call. We do `batch_size` per run to stay within rate limits.
    At 60 cells per run, daily runs will map ~2000 cells/month.
    """
    queue = catalog.get("scan_queue", [])
    if not queue:
        # Rebuild queue from oldest-scanned cells (re-scan stale data)
        print("Scan queue empty — rebuilding from least-recently-scanned cells...")
        cells_by_age = sorted(
            catalog["cells"].items(),
            key=lambda x: x[1].get("last_scanned") or "0"
        )
        queue = [k for k, v in cells_by_age[:batch_size * 10]]
        random.shuffle(queue)
        catalog["scan_queue"] = queue

    batch = queue[:batch_size]
    scanned = 0
    nonzero = 0

    print(f"\nScanning {len(batch)} cells ({len(queue) - len(batch)} remaining in queue)...\n")

    for key in batch:
        cell = catalog["cells"].get(key)
        if not cell:
            continue

        region = cell["region"]
        genre = cell["genre"]
        decade = cell["decade"]
        markets = REGIONS.get(region, [])

        if not markets:
            cell["pool_size"] = 0
            cell["last_scanned"] = datetime.now(timezone.utc).isoformat()
            scanned += 1
            continue

        # Use first market for the region as representative
        market = markets[0]
        total = estimate_pool_size(genre, market, decade)

        cell["pool_size"] = total
        cell["last_scanned"] = datetime.now(timezone.utc).isoformat()
        scanned += 1

        if total > 0:
            nonzero += 1
            if total >= 100:
                print(f"  {region:20s} | {genre:30s} | {decade} | {total:>7,} tracks")

        # Remove from queue
        if key in catalog["scan_queue"]:
            catalog["scan_queue"].remove(key)

        # Save periodically
        if scanned % 20 == 0:
            save_catalog(catalog)

    save_catalog(catalog)
    print(f"\nScanned {scanned} cells, {nonzero} have tracks on Spotify")

    return catalog


def print_status(catalog):
    """Print a summary of catalog coverage."""
    cells = catalog.get("cells", {})
    genres = catalog.get("genres", {})
    total_cells = len(cells)
    scanned = sum(1 for c in cells.values() if c["pool_size"] is not None)
    nonzero = sum(1 for c in cells.values() if (c["pool_size"] or 0) > 0)
    total_pool = sum(c["pool_size"] or 0 for c in cells.values())
    total_fetched = sum(c["fetched"] for c in cells.values())
    total_explored = sum(c["explored"] for c in cells.values())
    queue_remaining = len(catalog.get("scan_queue", []))

    print(f"\n{'='*60}")
    print(f"  DIG CATALOG STATUS")
    print(f"{'='*60}")
    print(f"  Genres:        {len(genres)}")
    print(f"  Regions:       {len(REGIONS)}")
    print(f"  Decades:       {len(DECADES)}")
    print(f"  Total cells:   {total_cells:,}")
    print(f"  Scanned:       {scanned:,} / {total_cells:,} ({100*scanned/max(total_cells,1):.1f}%)")
    print(f"  With tracks:   {nonzero:,}")
    print(f"  Est. pool:     {total_pool:,} tracks on Spotify")
    print(f"  Fetched:       {total_fetched:,}")
    print(f"  Explored:      {total_explored:,}")
    print(f"  Queue:         {queue_remaining:,} cells left to scan")
    print(f"  Last scan:     {catalog.get('last_scan', 'never')}")
    print(f"{'='*60}")

    # Top regions by pool size
    region_pools = {}
    for c in cells.values():
        r = c["region"]
        region_pools[r] = region_pools.get(r, 0) + (c["pool_size"] or 0)

    print(f"\n  Top regions by estimated pool:")
    for r, p in sorted(region_pools.items(), key=lambda x: -x[1])[:15]:
        if p > 0:
            print(f"    {r:25s} {p:>10,}")

    # Top genres by pool size
    genre_pools = {}
    for c in cells.values():
        g = c["genre"]
        genre_pools[g] = genre_pools.get(g, 0) + (c["pool_size"] or 0)

    print(f"\n  Top genres by estimated pool:")
    for g, p in sorted(genre_pools.items(), key=lambda x: -x[1])[:15]:
        if p > 0:
            print(f"    {g:30s} {p:>10,}")

    # Least explored non-empty cells
    nonempty = [(k, c) for k, c in cells.items() if (c["pool_size"] or 0) > 0]
    by_exploration = sorted(nonempty, key=lambda x: x[1]["explored"] / max(x[1]["pool_size"], 1))

    print(f"\n  Biggest unexplored pools:")
    shown = 0
    for k, c in by_exploration:
        if c["explored"] == 0 and c["pool_size"] >= 100:
            print(f"    {c['region']:20s} | {c['genre']:25s} | {c['decade']} | {c['pool_size']:>7,} tracks")
            shown += 1
            if shown >= 15:
                break

    print()


def sync_exploration(catalog):
    """Update exploration counts from ledger + discovery data."""
    ledger = load_ledger()

    # Load discovery.json to see what's been fetched per cell
    disc_path = os.path.join(DIR, "discovery.json")
    if os.path.exists(disc_path):
        with open(disc_path) as f:
            discovery = json.load(f)

        for region, tracks in discovery.items():
            for t in tracks:
                genre = t.get("query", "")
                # Clean up source prefix
                if ":" in genre:
                    prefix, val = genre.split(":", 1)
                    if prefix == "hint":
                        genre = val
                    else:
                        continue  # can't map random/playlist/new to a genre cell

                # Try to find matching cell (we don't know decade from discovery.json alone)
                # For now, mark as fetched in the "all decades" sense
                for decade in DECADES:
                    key = cell_key(region, genre, decade)
                    if key in catalog["cells"]:
                        cell = catalog["cells"][key]
                        # Check if track is explored (in ledger)
                        track_key = f"{t['artist']} - {t['name']}".lower()
                        if track_key in ledger or t["name"].lower() in ledger:
                            cell["explored"] = cell.get("explored", 0) + 1

    save_catalog(catalog)
    print("Synced exploration data from ledger + discovery.")


# ── Main ──

if __name__ == "__main__":
    catalog = load_catalog()

    if "--seed" in sys.argv:
        print("\n🌱 SEEDING CATALOG\n")
        catalog = seed_catalog(catalog)
        print_status(catalog)

    elif "--status" in sys.argv:
        print_status(catalog)

    elif "--sync" in sys.argv:
        sync_exploration(catalog)
        print_status(catalog)

    else:
        # Default: incremental scan
        if not catalog["genres"]:
            print("No genres in catalog. Run with --seed first.")
            sys.exit(1)

        print(f"\n🔍 INCREMENTAL CATALOG SCAN\n")
        batch = 60
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            batch = int(sys.argv[1])
        catalog = scan_batch(catalog, batch_size=batch)
        print_status(catalog)
