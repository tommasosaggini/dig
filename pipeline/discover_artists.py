#!/usr/bin/env python3
"""
DIG — Artist Graph Crawler.

Crawls Spotify's related-artist graph starting from hand-picked seed
artists who are authentic, deep, respected figures in their scenes.
Discovers tracks by exploring albums of related artists up to 2 hops
from the seeds.

Outputs into discovery.json (same format as discover.py).
"""

import json
import os
import random
import sys
import time

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.discovery_lock import load_discovery, locked_update
from lib.artist_db import register_tracks
from lib.api_budget import record_call, is_exhausted, get_remaining, get_used
from lib.track_filter import is_trash

DIR = ROOT
ENV_PATH = os.path.join(ROOT, ".env")

# Load .env
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

sp = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(),
    retries=0,
    status_retries=0,
)

# ── Known track filter (read from user_ledger table, all users) ──
known_lower = set()
try:
    from lib.db import fetchall as _db_fetchall
    for row in _db_fetchall("SELECT track_key FROM user_ledger"):
        known_lower.add(row["track_key"].lower())
except Exception:
    pass

def is_known(artist, name):
    return f"{artist.lower()} - {name.lower()}" in known_lower

# ── Spotify API wrapper ──
_rate_limited = False
_api_calls = 0

def safe_call(fn, *args, **kwargs):
    global _rate_limited, _api_calls
    if _rate_limited or is_exhausted():
        if not _rate_limited and is_exhausted():
            print(f"  (shared API budget exhausted: {get_used()} calls)")
            _rate_limited = True
        return None
    try:
        result = fn(*args, **kwargs)
        _api_calls += 1
        record_call()
        time.sleep(0.5)  # 0.5s between calls — safe for Dev Mode
        return result
    except spotipy.SpotifyException as e:
        if e.http_status == 429:
            wait = int(e.headers.get("Retry-After", 5)) if hasattr(e, 'headers') and e.headers else 5
            if wait > 30:
                print(f"  (rate limited for {wait}s — stopping)")
                _rate_limited = True
                return None
            print(f"  (rate limited, waiting {wait}s)")
            time.sleep(min(wait, 30))
            try:
                result = fn(*args, **kwargs)
                _api_calls += 1
                record_call()
                return result
            except:
                _rate_limited = True
                return None
        if e.http_status == 400:
            # Development mode restriction — don't retry, don't count as rate limit
            return None
        if e.http_status == 403:
            # Endpoint not available in Development mode
            return None
        return None
    except Exception:
        return None

def extract_track(t, query="", region=""):
    """Extract track dict in the same format as discover.py."""
    artists = t.get("artists", [])
    artist = ", ".join(a["name"] for a in artists)
    artist_ids = [a["id"] for a in artists if a.get("id")]
    album = t.get("album", {})
    release_date = album.get("release_date", "")
    year = release_date[:4] if len(release_date) >= 4 else ""
    decade = year[:3] + "0s" if year else ""
    track = {
        "name": t.get("name", ""),
        "artist": artist,
        "artist_ids": artist_ids,
        "id": t["id"],
        "album": album.get("name", ""),
        "popularity": t.get("popularity", 0),
        "query": query,
        "source": "spotify",
    }
    if decade:
        track["decade"] = decade
    if year:
        track["year"] = year
    return track

# ── Discovered genres persistence (backed by genres table) ──
_seed_genres = None

def _load_seed_genres():
    global _seed_genres
    if _seed_genres is not None:
        return _seed_genres
    from lib.genres import load as db_load_genres
    _seed_genres = db_load_genres()
    return _seed_genres

def save_discovered_genres(artist_genres):
    """Persist newly discovered genres into the genres table."""
    from lib.genres import add as db_add_genres
    if artist_genres:
        db_add_genres(artist_genres, source="discovered")


# ── Region → Market mapping (same as discover.py) ──
REGIONS = {
    "USA": ["US"], "UK": ["GB"], "France": ["FR"], "Germany": ["DE"],
    "Italy": ["IT"], "Spain": ["ES"], "Portugal": ["PT"],
    "Nordic": ["SE", "NO", "FI", "DK", "IS"],
    "Netherlands": ["NL"], "Belgium": ["BE"],
    "Eastern Europe": ["PL", "CZ", "HU", "RO", "BG"],
    "Russia": ["RU"],
    "Japan": ["JP"], "South Korea": ["KR"],
    "Hong Kong": ["HK"], "Taiwan": ["TW"],
    "Thailand": ["TH"], "Vietnam": ["VN"],
    "Indonesia": ["ID"], "Cambodia": ["KH"], "Philippines": ["PH"],
    "Malaysia": ["MY"], "India": ["IN"],
    "South Asia": ["PK", "BD", "LK"],
    "Turkey": ["TR"], "Iran": ["IR"],
    "Middle East": ["SA", "AE", "EG", "IL", "LB"],
    "West Africa": ["NG", "GH", "SN"],
    "East Africa": ["KE", "TZ", "UG"],
    "Southern Africa": ["ZA", "ZW"],
    "North Africa": ["MA", "DZ", "TN"],
    "Brazil": ["BR"], "Argentina": ["AR"], "Colombia": ["CO"],
    "Chile": ["CL"], "Peru": ["PE"], "Mexico": ["MX"],
    "Caribbean": ["JM", "TT", "DO"],
    "Canada": ["CA"], "Australia": ["AU"], "New Zealand": ["NZ"],
    "Ireland": ["IE"], "Switzerland": ["CH"], "Greece": ["GR"],
    "Mongolia": ["MN"], "Nepal": ["NP"], "Myanmar": ["MM"],
    "Tibet": ["CN"],
    "Central Asia": ["KZ", "UZ", "KG"],
    "Central Africa": ["CD", "CM", "CG"],
    "Laos": ["LA"],
    "Pacific Islands": ["FJ", "PG"],
    "Central America": ["CR", "PA", "GT"],
    "South America": ["EC", "VE", "BO", "PY", "UY"],
}


# ══════════════════════════════════════════════════════════════════
# SEED ARTISTS — hand-picked icons, legends, deep-scene figures
# ══════════════════════════════════════════════════════════════════

SEED_ARTISTS = {
    "Japan": ["Haruomi Hosono", "Midori Takada", "Keiji Haino", "Cornelius", "toe"],
    "South Korea": ["이선희", "Hyukoh", "Park Jiha", "Jambinai", "Mid-Air Thief"],
    "India": ["Anoushka Shankar", "Prateek Kuhad", "Ilaiyaraaja", "Nucleya", "Paban Das Baul"],
    "Thailand": ["Khruangbin", "Paradise Bangkok Molam International Band", "Rasmee Isan Soul", "Suthep Wongkamhaeng"],
    "Vietnam": ["Tôn-Thất Tiết", "Ngọt", "Hà Lê"],
    "Indonesia": ["Senyawa", "Gabber Modus Operandi", "Detty Kurnia", "Elephant Kind"],
    "Cambodia": ["Sinn Sisamouth", "Dengue Fever", "Kak Channthy"],
    "West Africa": ["Tinariwen", "Amadou & Mariam", "Oumou Sangaré", "Mdou Moctar", "Bombino"],
    "East Africa": ["Mulatu Astatke", "Dur-Dur Band", "Sauti Sol", "Nyashinski"],
    "North Africa": ["Rachid Taha", "Nass El Ghiwane", "Souad Massi", "Gnawa Diffusion"],
    "Southern Africa": ["Ladysmith Black Mambazo", "Black Coffee", "BLK JKS", "Mahotella Queens"],
    "Central Africa": ["Staff Benda Bilili", "Konono N°1", "Kasai Allstars", "Mbongwana Star"],
    "Brazil": ["Tom Zé", "Elza Soares", "Hermeto Pascoal", "Céu", "Criolo"],
    "Argentina": ["Juana Molina", "Gotan Project", "Gustavo Cerati", "Sexteto Mayor"],
    "Colombia": ["Bomba Estéreo", "Systema Solar", "Lucrecia Dalt", "Meridian Brothers"],
    "Peru": ["Novalima", "Bareto", "Los Mirlos", "Susana Baca"],
    "Mexico": ["Café Tacvba", "Natalia Lafourcade", "Instituto Mexicano del Sonido", "Lila Downs"],
    "Caribbean": ["Boukman Eksperyans", "Calypso Rose", "Buena Vista Social Club"],
    "Turkey": ["Erkin Koray", "Baba Zula", "Barış Manço", "Gaye Su Akyol"],
    "Iran": ["Mohsen Namjoo", "Kayhan Kalhor", "Hafez Modirzadeh"],
    "Middle East": ["Fairuz", "Marcel Khalifé", "Yasmine Hamdan", "Mashrou' Leila"],
    "Nordic": ["Sigur Rós", "Nils Frahm", "Ólafur Arnalds", "Heilung", "Wardruna"],
    "Eastern Europe": ["Gogol Bordello", "Beirut", "Manu Chao", "Shantel"],
    "Russia": ["Huun-Huur-Tu", "Mumiy Troll", "Molchat Doma", "Аигел"],
    "Mongolia": ["The HU", "Hanggai", "Batzorig Vaanchig"],
    "Nepal": ["Kutumba", "Nepathya", "Sur Sudha"],
    "Central Asia": ["Sevara Nazarkhan", "Yulduz Usmanova", "DalerNazarov"],
    "Tibet": ["Yungchen Lhamo", "Techung", "Tenzin Choegyal"],
    "USA": ["Sun Ra", "Grouper", "Mdou Moctar", "William Basinski", "Sunn O)))"],
    "UK": ["Floating Points", "Burial", "Shirley Collins", "black midi", "Cosey Fanni Tutti"],
    "France": ["Serge Gainsbourg", "Rone", "Nino Ferrer", "Hélène Vogelsinger"],
    "Germany": ["Can", "Tangerine Dream", "Einstürzende Neubauten", "Pole", "Popol Vuh"],
    "Italy": ["Franco Battiato", "Ennio Morricone", "Piero Umiliani", "Nu Genea"],
    "Spain": ["Rosalía", "Paco de Lucía", "Mala Rodríguez", "Hinds"],
    "Greece": ["Vangelis", "Eleni Karaindrou", "Rotting Christ", "Mikis Theodorakis"],
    "Pacific Islands": ["Te Vaka", "Fat Freddy's Drop"],
    "Australia": ["The Avalanches", "King Gizzard & The Lizard Wizard", "GUM", "Hiatus Kaiyote"],
}


# ══════════════════════════════════════════════════════════════════
# CRAWL STATE
# ══════════════════════════════════════════════════════════════════

def load_crawl_state():
    from lib.db import get_meta
    return get_meta("artist_crawl_state") or {"crawled_ids": [], "seed_resolved": {}, "last_run": ""}

def save_crawl_state(state):
    from lib.db import set_meta
    state["last_run"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    set_meta("artist_crawl_state", state)


# ══════════════════════════════════════════════════════════════════
# CRAWL HELPERS
# ══════════════════════════════════════════════════════════════════

def resolve_artist(name, state):
    """Search Spotify to find an artist ID by name. Uses cache in state."""
    if name in state["seed_resolved"]:
        return state["seed_resolved"][name]
    results = safe_call(sp.search, q=f"artist:{name}", type="artist", limit=5)
    if not results:
        return None
    artists = results.get("artists", {}).get("items", [])
    if not artists:
        return None
    # Pick best match: exact name match preferred, then most followers
    best = None
    for a in artists:
        if a["name"].lower() == name.lower():
            best = a
            break
    if not best:
        best = max(artists, key=lambda a: a.get("followers", {}).get("total", 0))
    state["seed_resolved"][name] = best["id"]
    followers = best.get("followers", {}).get("total", 0)
    if followers >= 1000000:
        fstr = f"{followers / 1000000:.1f}M"
    elif followers >= 1000:
        fstr = f"{followers / 1000:.0f}K"
    else:
        fstr = str(followers)
    print(f"    ✓ {name} → {fstr} followers")
    return best["id"]


def search_collaborators(seed_name, region_markets, limit=5):
    """Find collaborators by searching Spotify for tracks featuring the seed artist."""
    found_names = set()
    market = random.choice(region_markets) if region_markets else "US"

    result = safe_call(sp.search, q=f'artist:"{seed_name}"', type="track", limit=10, market=market)
    if not result:
        return []

    collaborators = []
    for t in result.get("tracks", {}).get("items", []):
        for a in t.get("artists", []):
            name = a.get("name", "")
            if name.lower() != seed_name.lower() and name.lower() not in found_names:
                found_names.add(name.lower())
                collaborators.append(name)

    return collaborators[:limit]


def ai_similar_artists(artist_name, region, known_genres=None, n=3):
    """Ask Claude for artists similar to the given artist — obscure, deep cuts, same scene.

    Returns a list of artist name strings. Empty if no API key or call fails.
    Capped at n suggestions. Costs ~1 Haiku call per artist.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return []
    try:
        import anthropic
    except ImportError:
        return []

    genres_str = ", ".join((known_genres or [])[:5]) or "unknown"
    import httpx
    client = anthropic.Anthropic(
        api_key=api_key,
        max_retries=0,
        timeout=httpx.Timeout(30.0),
    )
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": f"""Music discovery app. I found the artist "{artist_name}" from {region} (genres: {genres_str}).

Suggest {n} similar artists — obscure, respected, deep cuts from the same scene or adjacent ones. NOT mainstream equivalents. Real artists searchable on Spotify.

Return ONLY a JSON array: ["Artist Name", "Artist Name", ...]"""}],
        )
        text = response.content[0].text
        start = text.find("[")
        end = text.rfind("]") + 1
        if start >= 0 and end > start:
            raw = text[start:end].replace("\n", " ")
            suggestions = json.loads(raw)
            return [a for a in suggestions if isinstance(a, str) and len(a) >= 2]
    except Exception as e:
        print(f"    (AI similar artists failed for {artist_name}: {e})")
    return []


def harvest_tracks_via_search(artist_name, region, market, all_existing_ids):
    """Find tracks by an artist using the search endpoint only.
    Spotify's restricted API only allows search, so we use multiple
    search queries with different offsets to get a diverse set of tracks."""
    new_tracks = []

    # Two searches with small offsets — limit=10 max for Development Mode apps
    offsets = [0, random.randint(5, 15)]
    for offset in offsets:
        if _rate_limited:
            break
        result = safe_call(
            sp.search, q=f'artist:"{artist_name}"',
            type="track", limit=10, offset=offset, market=market
        )
        if not result:
            continue
        for t in result.get("tracks", {}).get("items", []):
            if not t or not t.get("id"):
                continue
            if t["id"] in all_existing_ids:
                continue
            # Verify this is actually by the right artist (search can be fuzzy)
            track_artists = [a["name"].lower() for a in t.get("artists", [])]
            if not any(artist_name.lower() in a or a in artist_name.lower() for a in track_artists):
                continue
            if is_known(", ".join(a["name"] for a in t.get("artists", [])), t.get("name", "")):
                continue
            new_tracks.append(extract_track(t, query=f"artist:{artist_name}", region=region))

    return new_tracks


def determine_region_for_related(related_artist, seed_region):
    """Determine region for a related artist. Defaults to the seed's region."""
    # Could be extended with market/metadata heuristics, but for now
    # related artists from the same graph cluster tend to share a region.
    return seed_region


def collect_artist_genres(artist_ids):
    """Fetch genres from artist metadata in batches of 50."""
    all_genres = []
    for i in range(0, len(artist_ids), 50):
        batch = artist_ids[i:i+50]
        try:
            resp = safe_call(sp.artists, batch)
            if resp:
                for a in resp.get("artists", []):
                    if a:
                        all_genres.extend(a.get("genres", []))
        except:
            pass
    return all_genres


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

print("\n🕸️  DIG — ARTIST GRAPH CRAWLER\n")

# Load existing pool
discovery = load_discovery()
existing_count = sum(len(v) for v in discovery.values())

# Build set of all existing track IDs for dedup
all_existing_ids = set()
for tracks in discovery.values():
    for t in tracks:
        all_existing_ids.add(t["id"])

# Load crawl state
state = load_crawl_state()
crawled_set = set(state["crawled_ids"])

print(f"  Existing pool: {existing_count} tracks")
print(f"  Crawl state: {len(crawled_set)} artists crawled previously\n")

total_new = 0
total_artists_discovered = 0
all_new_artist_ids = []

# Tracks added since last save, keyed by region.
_pending_tracks = {}

def save_progress():
    """Merge pending new tracks into discovery.json atomically, then save crawl state."""
    global _pending_tracks
    pending = _pending_tracks
    _pending_tracks = {}
    if pending:
        def _merge(disk_data):
            for region, new_tracks in pending.items():
                existing = disk_data.get(region, [])
                existing_ids = set(t["id"] for t in existing)
                for t in new_tracks:
                    if t["id"] not in existing_ids:
                        existing.append(t)
                        existing_ids.add(t["id"])
                disk_data[region] = existing
        locked_update(_merge)
    save_crawl_state(state)

def add_tracks(region, tracks):
    """Add new tracks to discovery pool for a region."""
    global total_new
    if not tracks:
        return 0
    existing = discovery.get(region, [])
    # Final dedup
    added = []
    for t in tracks:
        if t["id"] not in all_existing_ids and not is_trash(t.get("name", "")):
            all_existing_ids.add(t["id"])
            added.append(t)
    if added:
        discovery[region] = existing + added
        _pending_tracks.setdefault(region, []).extend(added)
        register_tracks(added, region=region)
        total_new += len(added)
    return len(added)


# ═══ Phase 1: Resolve seed artists ═══
print("═══ Resolving seed artists ═══\n")

seed_ids_by_region = {}  # region → [(artist_id, artist_name), ...]

for region, artists in SEED_ARTISTS.items():
    if _rate_limited:
        break
    seed_ids_by_region[region] = []
    for name in artists:
        if _rate_limited:
            break
        aid = resolve_artist(name, state)
        if aid:
            seed_ids_by_region[region].append((aid, name))
        else:
            print(f"    ✗ {name} — not found")

save_crawl_state(state)
resolved_total = sum(len(v) for v in seed_ids_by_region.values())
print(f"\n  Resolved {resolved_total} seed artists across {len(seed_ids_by_region)} regions\n")


# ═══ Phase 2: Harvest tracks from seed artists via search ═══
print("═══ Harvesting seed artist tracks ═══\n")

for region, seeds in seed_ids_by_region.items():
    if _rate_limited:
        break
    markets = REGIONS.get(region, ["US"])
    market = random.choice(markets)

    for seed_id, seed_name in seeds:
        if _rate_limited:
            break
        if seed_id in crawled_set:
            continue

        new_tracks = harvest_tracks_via_search(
            seed_name, region, market, all_existing_ids
        )
        n = add_tracks(region, new_tracks)
        if n > 0:
            print(f"  ✓ {seed_name} [{region}] → {n} tracks")
            total_artists_discovered += 1
        all_new_artist_ids.append(seed_id)

        crawled_set.add(seed_id)
        state["crawled_ids"] = list(crawled_set)

    save_progress()

print(f"\n  Seed harvest complete: {total_new} new tracks\n")


# ═══ Phase 3: Discover collaborators and harvest their tracks ═══
print("═══ Discovering collaborators ═══\n")

# For each seed, find artists they've collaborated with, then harvest those too
seed_names_by_region = {}
for region, seeds in seed_ids_by_region.items():
    seed_names_by_region[region] = [name for _, name in seeds]

for region, seed_names in seed_names_by_region.items():
    if _rate_limited:
        break
    markets = REGIONS.get(region, ["US"])

    for seed_name in seed_names:
        if _rate_limited:
            break

        collaborators = search_collaborators(seed_name, markets, limit=3)
        for collab_name in collaborators:
            if _rate_limited:
                break
            # Use name as a simple crawl-state key
            collab_key = f"collab:{collab_name.lower()}"
            if collab_key in crawled_set:
                continue

            market = random.choice(markets)
            new_tracks = harvest_tracks_via_search(
                collab_name, region, market, all_existing_ids
            )
            n = add_tracks(region, new_tracks)
            if n > 0:
                print(f"  ✓ {collab_name} (via {seed_name}) [{region}] → {n} tracks")
                total_artists_discovered += 1

            crawled_set.add(collab_key)
            state["crawled_ids"] = list(crawled_set)

    save_progress()

print()


# ═══ Phase 4: AI-powered similar artist discovery ═══
# Ask Claude for 2-3 similar artists per seed — surfaces obscure artists
# that Spotify's collaborator graph would never reach.
# Capped at 8 seed artists per run to keep Anthropic costs in check.
print("═══ AI similar artist suggestions ═══\n")

ai_seed_sample = []
for region, seeds in seed_names_by_region.items():
    for name in seeds[:2]:   # max 2 per region
        ai_seed_sample.append((name, region))
    if len(ai_seed_sample) >= 8:
        break

ai_suggested_total = 0
for seed_name, region in ai_seed_sample:
    if _rate_limited:
        break

    suggestions = ai_similar_artists(seed_name, region, n=3)
    if not suggestions:
        continue

    markets = REGIONS.get(region, ["US"])
    for suggested_name in suggestions:
        if _rate_limited:
            break
        ai_key = f"ai:{suggested_name.lower()}"
        if ai_key in crawled_set:
            continue

        market = random.choice(markets)
        new_tracks = harvest_tracks_via_search(suggested_name, region, market, all_existing_ids)
        n = add_tracks(region, new_tracks)
        if n > 0:
            print(f"  ✓ {suggested_name} (AI → {seed_name}) [{region}] → {n} tracks")
            total_artists_discovered += 1
            ai_suggested_total += 1

        crawled_set.add(ai_key)
        state["crawled_ids"] = list(crawled_set)

    save_progress()
    time.sleep(1)   # brief pause between Anthropic calls

if ai_suggested_total:
    print(f"\n  {ai_suggested_total} tracks found via AI-suggested artists")
else:
    print("  (no new AI suggestions this run)")
print()


# ═══ Final save ═══
save_progress()

final_count = sum(len(v) for v in discovery.values())
print(f"✓ {total_new} new tracks from {total_artists_discovered} artists (pool: {existing_count} → {final_count})")
print(f"  API calls used: {_api_calls} (shared budget: {get_used()} total)")
print(f"  Artists in crawl state: {len(crawled_set)}")
print("\nDone.")
