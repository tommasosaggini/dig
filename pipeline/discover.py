#!/usr/bin/env python3
"""
DIG — Catalog-aware music discovery.

Searches Spotify across genres, regions, and decades to build
a broad, diverse discovery pool. Prioritizes unexplored cells
and filters out mainstream pop contamination.

Outputs discovery.json for the web UI.
"""

import json
import os
import re
import sys
import time
import random

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.discovery_lock import load_discovery, locked_update
from lib.artist_db import register_tracks
from lib.api_budget import record_call, is_exhausted, get_remaining, get_used
from lib.track_filter import is_trash
import lib.search_history as _sh

DIR = ROOT
ENV_PATH = os.path.join(ROOT, ".env")

# Load .env — only set values not already present in the environment
# (allows callers to override e.g. ANTHROPIC_API_KEY for testing)
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                key = key.strip()
                if key not in os.environ:
                    os.environ[key] = val.strip()

sp = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(),
    retries=0,          # disable spotipy's built-in retry sleep — safe_call() handles it
    status_retries=0,
)

# ── Known track filter — load from DB ──
known_lower = set()
try:
    from lib.db import fetchall as _fetchall
    _rows = _fetchall(
        "SELECT track_key FROM user_ledger WHERE status = 'known'"
    )
    for r in _rows:
        known_lower.add(r["track_key"].lower())
except Exception:
    pass  # DB not ready yet — skip known filter

def is_known(artist, name):
    return f"{artist.lower()} - {name.lower()}" in known_lower

# ── Search history — backed by PostgreSQL ──
search_history = {}
try:
    search_history = _sh.load()
except Exception:
    pass  # DB not ready — start empty

def record_search(query, market, count):
    key = f"{query}|{market}"
    search_history[key] = {
        "count": count,
        "last": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runs": search_history.get(key, {}).get("runs", 0) + 1,
    }

def save_search_history():
    """Flush in-memory search history to PostgreSQL."""
    try:
        _sh.save(search_history)
    except Exception as e:
        print(f"  (warn: could not save search history: {e})")

def query_freshness(query, market):
    key = f"{query}|{market}"
    return search_history.get(key, {}).get("runs", 0)

# ── Spotify API wrapper ──
_rate_limited = False

def safe_call(fn, *args, **kwargs):
    global _rate_limited
    if _rate_limited or is_exhausted():
        if not _rate_limited and is_exhausted():
            print(f"  (shared API budget exhausted: {get_used()} calls)")
            _rate_limited = True
        return None
    try:
        result = fn(*args, **kwargs)
        record_call()
        time.sleep(0.5)  # 0.5s between calls — safe for Dev Mode
        return result
    except spotipy.SpotifyException as e:
        if e.http_status == 429:
            wait = int(e.headers.get("Retry-After", 5)) if hasattr(e, 'headers') and e.headers else 5
            if wait > 60:
                print(f"  (rate limited for {wait}s — stopping)")
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
    except Exception:
        return None

def extract_track(t, source="", decade=""):
    artists = t.get("artists", [])
    artist = ", ".join(a["name"] for a in artists)
    artist_ids = [a["id"] for a in artists if a.get("id")]
    # Get release year from album
    album = t.get("album", {})
    release_date = album.get("release_date", "")
    year = release_date[:4] if len(release_date) >= 4 else ""
    if not decade and year:
        decade = year[:3] + "0s"  # "1975" → "1970s"
    # Extract genre from catalog query (e.g. "catalog:fado year:1970-1979" → "fado")
    genres = []
    if source.startswith("catalog:"):
        genre_part = source[len("catalog:"):].split(" year:")[0].strip()
        if genre_part:
            genres = [genre_part]

    track = {
        "name": t.get("name", ""),
        "artist": artist,
        "artist_ids": artist_ids,
        "id": t["id"],
        "album": album.get("name", ""),
        "popularity": t.get("popularity", 0),
        "query": source,
        "source": "spotify",
    }
    if genres:
        track["genres"] = genres
    if decade:
        track["decade"] = decade
    if year:
        track["year"] = year
    return track

# ── Region → Market mapping ──
# Includes orphan regions that YouTube discovery found
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

# ── Genre pool — the full landscape to explore ──
GENRE_POOL = {
    "traditional": [
        "fado", "flamenco", "tango", "rebetiko", "enka", "qawwali", "ghazal",
        "gamelan", "gagaku", "pansori", "raï", "gnawa", "griot", "highlife",
        "mbalax", "benga", "taarab", "mbaqanga", "chimurenga", "calypso",
        "mento", "son jarocho", "huayno", "forró", "choro", "cueca",
        "joik", "sean-nós", "klezmer", "csárdás", "throat singing",
        "carnatic", "hindustani classical", "guqin", "erhu", "pipa",
        "min'yō", "kayōkyoku", "mor lam", "luk thung", "dangdut",
        "keroncong", "kundiman", "dikir barat", "bhangra", "baul",
        "rabindra sangeet", "chaabi", "dabke", "khaleeji", "maqam",
        "washboard", "cajun", "zydeco", "delta blues", "appalachian",
        "polka", "alpine folk", "balkan brass", "sevdalinka", "mugham",
        "tibetan chanting", "kazakh dombra", "uzbek shashmaqam",
        "kyrgyz komuz", "congolese rumba", "lao lam",
        "polynesian chant", "fijian meke", "papuan kundu",
    ],
    "electronic": [
        "techno", "house", "ambient", "drum and bass", "dubstep", "trance",
        "gabber", "breakcore", "idm", "glitch", "vaporwave", "synthwave",
        "electro", "acid house", "deep house", "minimal techno",
        "psytrance", "hardstyle", "future garage", "uk garage",
        "footwork", "juke", "gqom", "amapiano", "baile funk",
        "kuduro", "singeli", "mahraganat", "budots", "koplo",
        "new beat", "ebm", "industrial", "noise music", "power electronics",
        "dark ambient", "drone", "microsound", "granular",
        "space ambient", "ambient dub", "future bass", "tropical house",
        "progressive house", "chillwave",
    ],
    "rock": [
        "krautrock", "shoegaze", "post-punk", "noise rock", "math rock",
        "post-rock", "stoner rock", "doom metal", "black metal",
        "death metal", "grindcore", "powerviolence", "hardcore punk",
        "crust punk", "sludge metal", "prog rock", "psychedelic rock",
        "garage rock", "surf rock", "rockabilly", "new wave",
        "gothic rock", "ethereal wave", "coldwave", "dream pop",
        "indie rock", "emo", "screamo", "folk rock",
    ],
    "jazz_soul": [
        "free jazz", "ethio-jazz", "afrobeat", "latin jazz", "bossa nova",
        "samba", "mpb", "tropicália", "northern soul", "deep funk",
        "gospel", "spirituals", "doo-wop", "neo-soul", "quiet storm",
        "acid jazz", "fusion", "smooth jazz", "big band", "bebop",
        "cool jazz", "modal jazz", "avant-garde jazz",
        "r&b", "new jack swing", "contemporary r&b", "slow jams",
    ],
    "hip_hop": [
        "boom bap", "trap", "drill", "grime", "phonk", "lo-fi hip hop",
        "chopped and screwed", "crunk", "g-funk", "conscious hip hop",
        "abstract hip hop", "jazz rap", "cloud rap", "memphis rap",
        "uk hip hop", "french rap", "latin trap",
    ],
    "pop_experimental": [
        "art pop", "chamber pop", "baroque pop", "hyperpop", "pc music",
        "city pop", "cantopop", "mandopop", "j-pop", "k-pop",
        "italo disco", "eurobeat", "schlager", "chanson", "canzone napoletana",
        "musique concrete", "tape music", "field recordings",
        "spectral music", "microtonal", "just intonation",
        "bedroom pop", "singer-songwriter", "lo-fi indie",
    ],
    "reggae_caribbean": [
        "roots reggae", "dub", "dancehall", "ska", "rocksteady",
        "lovers rock", "ragga", "kompa", "soca", "chutney",
        "steelpan", "reggaeton", "dembow",
    ],
    "classical": [
        "baroque", "romantic era", "contemporary classical", "minimalism",
        "opera", "lieder", "choral", "sacred music", "gregorian chant",
        "gamelan composition", "gagaku composition",
        "musical theater", "broadway",
    ],
    # NEW CATEGORIES
    "country_americana": [
        "country", "bluegrass", "honky-tonk", "outlaw country", "americana",
        "country blues", "western swing", "country rock", "alt-country",
        "country folk", "tejano", "norteño", "corridos", "ranchera",
    ],
    "latin": [
        "cumbia", "bachata", "vallenato", "merengue", "salsa",
        "bolero", "trova", "nueva canción", "latin rock",
        "boogaloo", "mambo", "cha-cha-chá", "rumba",
    ],
    "ambient_meditative": [
        "meditation music", "sound bath", "binaural", "nature sounds",
        "new age", "healing music", "tibetan bowls", "crystal bowls",
        "ambient folk", "slowcore", "sadcore", "funeral doom",
        "spoken word", "poetry", "sound poetry",
        "tibetan singing bowls", "overtone singing",
    ],
}

DECADES = ["1950", "1960", "1970", "1980", "1990", "2000", "2010", "2020"]

# ── Self-expanding genre pool ──
# The hardcoded GENRE_POOL is just the seed. We also load genres from the
# genres DB table (populated by Spotify metadata, AI gap analysis, etc.).
# Every run, the pool grows.

def load_expanded_genre_pool():
    """Merge hardcoded seed genres with all genres stored in the DB."""
    from lib.genres import load as db_load_genres
    from lib.db import get_meta

    all_genres = dict(GENRE_POOL)  # copy

    # Build set of all known seed genres for dedup
    seed_genres = set()
    for genres in GENRE_POOL.values():
        seed_genres.update(g.lower() for g in genres)

    new_genres = []

    # 1. Load genres from AI gap analysis (stored in catalog_meta)
    priorities = get_meta("discovery_priorities") or {}
    for g in priorities.get("missing_genres", []):
        if g.lower() not in seed_genres:
            new_genres.append(g)
            seed_genres.add(g.lower())

    # 2. Load all genres previously discovered (stored in genres table)
    for g in db_load_genres():
        if g.lower() not in seed_genres:
            new_genres.append(g)
            seed_genres.add(g.lower())

    if new_genres:
        all_genres["discovered"] = new_genres

    return all_genres


def ai_expand_genres():
    """Ask Claude to suggest genres missing from our pool. Runs once per session."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return []

    try:
        import anthropic
    except ImportError:
        return []

    # Build current genre list — sample to keep prompt short
    expanded = load_expanded_genre_pool()
    current = set()
    for genres in expanded.values():
        current.update(g.lower() for g in genres)
    # Send a sample of 200 genres max to keep the prompt manageable
    sample = sorted(current)[:200]

    import httpx
    client = anthropic.Anthropic(
        api_key=api_key,
        max_retries=0,
        timeout=httpx.Timeout(30.0),  # never block longer than 30s
    )
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            messages=[{"role": "user", "content": f"""You are a music encyclopedia. I have a music discovery app that searches Spotify.

I already cover {len(current)} genres. Here's a sample: {', '.join(sample[:100])}

List 30 music genres/styles MISSING from my pool. Focus on:
- Regional styles from underrepresented areas (Central Asia, Pacific, Southeast Asia, Balkans, etc.)
- Historical movements and forgotten scenes
- Emerging/contemporary underground scenes
- Niche subcultures and cross-genre fusions

Return ONLY a JSON array: ["genre1", "genre2", ...]
Each must work as a Spotify search term. No duplicates from the list above."""}],
        )
        text = response.content[0].text
        start = text.find("[")
        end = text.rfind("]") + 1
        if start >= 0 and end > start:
            raw = text[start:end]
            # Clean up common formatting issues
            raw = raw.replace("\n", " ").replace("\\n", " ")
            genres = json.loads(raw)
            # Filter out genres we already have
            new = [g for g in genres if isinstance(g, str) and len(g) >= 3 and g.lower() not in current]
            return new
    except Exception as e:
        print(f"  (AI genre expansion failed: {e})")

    return []

def save_discovered_genres(artist_genres):
    """Persist newly discovered genres into the genres table for future runs."""
    from lib.genres import add as db_add_genres
    if artist_genres:
        db_add_genres(artist_genres, source="discovered")

# Script characters for random "blind" searches
SCRIPT_CHARS = {
    "latin": list("abcdefghijklmnopqrstuvwxyz"),
    "arabic": list("ابتثجحخدذرزسشصضطظعغفقكلمنهوي"),
    "thai": list("กขคงจฉชซญดตถทนบปผพฟมยรลวศสหอ"),
    "japanese": list("あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほ"),
    "korean": list("가나다라마바사아자차카타파하"),
    "devanagari": list("अआइईउऊएऐओऔकखगघचछजझटठ"),
    "cyrillic": list("абвгдежзиклмнопрстуфхцчшщэюя"),
    "chinese": list("的一是不了人我在有他这中大来上个国到说们为子和你地出会也时要就可以"),
}
MARKET_SCRIPTS = {
    "JP": "japanese", "KR": "korean", "TH": "thai",
    "SA": "arabic", "AE": "arabic", "EG": "arabic", "MA": "arabic",
    "DZ": "arabic", "TN": "arabic", "LB": "arabic", "IR": "arabic",
    "IN": "devanagari", "BD": "devanagari", "PK": "arabic", "NP": "devanagari",
    "RU": "cyrillic", "HK": "chinese", "TW": "chinese", "MN": "cyrillic",
}


def search_tracks(query, market, limit=10):
    """Search Spotify, return extracted tracks."""
    offset = random.randint(0, 200)  # wider offset range for more diversity
    results = safe_call(sp.search, q=query, type="track", limit=limit, offset=offset, market=market)
    if not results:
        return []
    tracks = []
    # Extract decade from query if present
    decade = ""
    ym = re.search(r"year:(\d{4})", query)
    if ym:
        decade = ym.group(1) + "s"
    for t in results["tracks"]["items"]:
        if t and t.get("id"):
            tracks.append(extract_track(t, f"catalog:{query}", decade))
    record_search(query, market, len(tracks))
    return tracks


def pick_unexplored_cells(n=50):
    """Pick cells to explore by querying catalog_cells directly.

    Priority order:
      1. Never searched (last_scanned IS NULL) — true unknowns first
      2. Fewest explores — cells we've only touched once or twice
      3. Oldest last_scanned — cells we haven't revisited in a while
      4. Thin regions get a bonus to ensure geographic fairness

    Returns list of (region, market, genre, decade) tuples.
    """
    from lib.db import fetchall

    region_track_counts = {r: len(t) for r, t in discovery.items()}

    # Pull candidate cells from catalog_cells, ordered by exploration priority.
    # Fetch a larger pool so we can apply fairness caps after.
    rows = fetchall(
        """
        SELECT region, genre, decade, explored, last_scanned
        FROM catalog_cells
        ORDER BY
            last_scanned IS NOT NULL,       -- NULLs (never searched) first
            explored ASC,
            last_scanned ASC NULLS FIRST
        LIMIT %s
        """,
        (n * 6,),  # over-fetch so region/category caps don't starve us
    )

    print(f"  Catalog candidates: {len(rows)} cells (never searched: "
          f"{sum(1 for r in rows if r['last_scanned'] is None)})")

    picked = []
    seen_regions: dict[str, int] = {}
    seen_genre_decade: set[str] = set()

    for row in rows:
        if len(picked) >= n:
            break
        region = row["region"]
        genre  = row["genre"]
        decade = row["decade"]

        if region not in REGIONS:
            continue  # region not in our market map — skip

        # Deduplicate (genre × decade) pairs in this batch for variety
        gd_key = f"{genre}|{decade}"
        if gd_key in seen_genre_decade:
            continue
        seen_genre_decade.add(gd_key)

        # Cap per region — but allow more slots for thin regions
        track_count = region_track_counts.get(region, 0)
        region_cap = 5 if track_count < 50 else 3
        if seen_regions.get(region, 0) >= region_cap:
            continue

        market = random.choice(REGIONS[region])
        picked.append((region, market, genre, decade))
        seen_regions[region] = seen_regions.get(region, 0) + 1

    return picked


def do_random_searches(market, n=2):
    """Blind character searches — pure serendipity."""
    script = MARKET_SCRIPTS.get(market, "latin")
    chars = SCRIPT_CHARS.get(script, SCRIPT_CHARS["latin"])
    tracks = []
    for _ in range(n):
        q = random.choice(chars)
        if random.random() > 0.5:
            q += random.choice(chars)
        offset = random.randint(0, 300)
        results = safe_call(sp.search, q=q, type="track", limit=10, offset=offset, market=market)
        if results:
            for t in results["tracks"]["items"]:
                if t and t.get("id"):
                    tracks.append(extract_track(t, f"random:{market}"))
        time.sleep(0.2)
    return tracks


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

print("\n🔍 DIG — CATALOG-AWARE DISCOVERY\n")

# Load existing pool
discovery = load_discovery()
existing_count = sum(len(v) for v in discovery.values())
print(f"  Existing pool: {existing_count} tracks")
print(f"  Search history: {len(search_history)} queries recorded\n")

# Build set of all existing track IDs + artist names for dedup
all_existing_ids = set()
all_existing_artists = set()
for tracks in discovery.values():
    for t in tracks:
        all_existing_ids.add(t["id"])
        all_existing_artists.add(t.get("artist", "").lower())

total_new = 0

# Tracks added since last save, keyed by region.
# save_progress() merges these into the on-disk file and clears the buffer.
_pending_tracks = {}

def save_progress():
    """Merge pending new tracks into discovery.json atomically, then save search history."""
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
    save_search_history()

def _buffer_tracks(region, tracks, genre=""):
    """Buffer new tracks for the next save_progress() call."""
    clean = [t for t in tracks if not is_trash(t.get("name", ""))]
    if len(clean) < len(tracks):
        print(f"  (filtered {len(tracks) - len(clean)} trash tracks)")
    _pending_tracks.setdefault(region, []).extend(clean)
    register_tracks(clean, region=region, genre=genre)

# ── Phase 0: Load AI-generated priorities from DB ──
ai_strategies = []
boost_regions = []
missing_genres_list = []
try:
    from lib.db import get_meta
    priorities = get_meta("discovery_priorities") or {}
    ai_strategies = priorities.get("ai_strategies", [])
    boost_regions = priorities.get("boost_regions", [])
    missing_genres_list = priorities.get("missing_genres", [])
    if priorities:
        print(f"  Loaded priorities: {len(ai_strategies)} AI strategies, {len(boost_regions)} boost regions, {len(missing_genres_list)} missing genres\n")
except Exception as e:
    print(f"  (priorities load failed: {e})")

# ── Phase 0.5: Execute AI strategies from gap analysis ──
if ai_strategies and not _rate_limited:
    print("═══ Phase 0: AI-guided gap filling ═══\n")
    for strategy in ai_strategies[:10]:
        if _rate_limited:
            break
        query = strategy.get("query", "")
        reason = strategy.get("reason", "")
        if not query:
            continue
        # Resolve human-readable region names → ISO market codes
        markets_raw = strategy.get("markets", ["US"])
        markets = []
        for m in markets_raw:
            if m in REGIONS:
                markets.append(REGIONS[m][0])
            elif len(m) == 2 and m.isupper():
                markets.append(m)
        if not markets:
            markets = ["US"]
        for market in markets[:2]:
            if _rate_limited:
                break
            tracks = search_tracks(query, market)
            new = [t for t in tracks if not is_known(t["artist"], t["name"]) and t["id"] not in all_existing_ids]
            if new:
                region_name = market
                for rname, rmarkets in REGIONS.items():
                    if market in rmarkets:
                        region_name = rname
                        break
                existing = discovery.get(region_name, [])
                discovery[region_name] = existing + new
                _buffer_tracks(region_name, new)
                for t in new:
                    all_existing_ids.add(t["id"])
                total_new += len(new)
                print(f"  ✓ AI strategy: {query} ({market}) → {len(new)} new ({reason})")
            time.sleep(0.2)
    save_progress()

# ── AI genre expansion — ask Claude what we're missing, then grow the map ──
print("═══ Expanding genre pool via AI ═══\n")
ai_genres = ai_expand_genres()
if ai_genres:
    save_discovered_genres(ai_genres)
    from lib.db import expand_catalog_for_new_genres
    new_cells = expand_catalog_for_new_genres(ai_genres)
    print(f"  +{len(ai_genres)} new genres from AI: {', '.join(ai_genres[:10])}{'...' if len(ai_genres) > 10 else ''}")
    if new_cells:
        print(f"  +{new_cells} new catalog cells created (these genres × all known regions × all decades)")
else:
    print("  (no new genres from AI)")
save_progress()

# ── Phase 1: Catalog-guided exploration (85% of effort) ──
print("\n═══ Phase 1: Hunting catalog gaps ═══\n")
from lib.db import mark_cell_explored
cells = pick_unexplored_cells(n=40)
for region, market, genre, decade in cells:
    if _rate_limited:
        break
    decade_num = decade.rstrip("s")   # catalog_cells stores "2020s"; Spotify wants "2020"
    query = f"{genre} year:{decade_num}-{int(decade_num)+9}"
    tracks = search_tracks(query, market)
    new = [t for t in tracks if not is_known(t["artist"], t["name"]) and t["id"] not in all_existing_ids]
    if new:
        existing = discovery.get(region, [])
        discovery[region] = existing + new
        _buffer_tracks(region, new, genre=genre)
        for t in new:
            all_existing_ids.add(t["id"])
        total_new += len(new)
        print(f"  ✓ {region} / {genre} / {decade}s → {len(new)} new tracks")
    # Always mark explored — even empty results tell us the cell is sparse
    mark_cell_explored(region, genre, decade, len(new))
    time.sleep(0.2)

save_progress()
print(f"  (checkpoint: {total_new} new so far)")

# ── Phase 1.5: Targeted boost for underrepresented regions ──
print(f"\n═══ Phase 1.5: Boosting thin regions ═══\n")
region_track_counts = {}
for region, tracks in discovery.items():
    region_track_counts[region] = len(tracks)

expanded_pool = load_expanded_genre_pool()
all_genre_list = []
for genres in expanded_pool.values():
    all_genre_list.extend(genres)

thin_regions = []
for region in REGIONS:
    if region_track_counts.get(region, 0) < 50:
        thin_regions.append(region)

if thin_regions:
    print(f"  Found {len(thin_regions)} thin regions (< 50 tracks): {', '.join(thin_regions)}")
    for region in thin_regions:
        if _rate_limited:
            break
        markets = REGIONS[region]
        market = random.choice(markets)
        for _ in range(3):
            if _rate_limited:
                break
            genre = random.choice(all_genre_list)
            decade = random.choice(DECADES)           # bare "2020"
            decade_cell = f"{decade}s"                # "2020s" — DB format
            query = f"{genre} year:{decade}-{int(decade)+9}"
            tracks = search_tracks(query, market)
            new = [t for t in tracks if not is_known(t["artist"], t["name"]) and t["id"] not in all_existing_ids]
            if new:
                existing = discovery.get(region, [])
                discovery[region] = existing + new
                _buffer_tracks(region, new, genre=genre)
                for t in new:
                    all_existing_ids.add(t["id"])
                total_new += len(new)
                print(f"  ✓ {region} boost / {genre} / {decade}s → {len(new)} new tracks")
            mark_cell_explored(region, genre, decade_cell, len(new))
            time.sleep(0.2)
    save_progress()
    print(f"  (checkpoint: {total_new} new so far)")
else:
    print("  All regions have 50+ tracks — no boost needed")

# ── Phase 2: Random exploration (10% of effort — reduced from 15%) ──
print(f"\n═══ Phase 2: Random serendipity ═══\n")
# Only 3 random regions (down from 5) — shifted budget to genre-targeted
random_regions = random.sample(list(REGIONS.items()), min(3, len(REGIONS)))
for region, markets in random_regions:
    if _rate_limited:
        break
    market = random.choice(markets)
    tracks = do_random_searches(market, n=2)
    new = [t for t in tracks if not is_known(t["artist"], t["name"]) and t["id"] not in all_existing_ids]
    if new:
        existing = discovery.get(region, [])
        discovery[region] = existing + new
        _buffer_tracks(region, new)
        for t in new:
            all_existing_ids.add(t["id"])
        total_new += len(new)
        print(f"  ✓ {region} ({market}) → {len(new)} random finds")
    time.sleep(0.2)

# ── Save ──
save_progress()

final_count = sum(len(v) for v in discovery.values())
print(f"\n✓ {total_new} new tracks added (pool: {existing_count} → {final_count})")
print(f"  Search history: {len(search_history)} queries tracked")

# ── Skip sp.artists() batch calls — blocked in Spotify Development Mode ──
# Genre harvesting from artist metadata requires Extended Quota.
# Genres are instead inferred from search queries and AI expansion.

print("\nDone.")
