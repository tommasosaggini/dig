#!/usr/bin/env python3
"""
DIG — Pool gap analyzer.

Reviews the current discovery pool and generates discovery_priorities.json
so the next discover.py run knows exactly what's underrepresented.

Uses Claude to interpret the distribution and suggest specific search
strategies for filling gaps across genres, regions, decades, and vibes.

Run weekly (or after each discover.py run) to keep the feedback loop tight.
"""

import json
from lib.jsonparse import extract_json
import os
import sys
import time
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
DIR = ROOT

# Load .env
from lib.env import load_env
load_env()

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


# ══════════════════════════════════════════════
# GENRE FAMILIES — used to measure + enforce balance across the pool.
# No family should exceed ~20% share of the pool; strategy suggestions must
# explicitly target under-represented families, not just missing genres.
# Matches are substring-based on either the query-derived genre or the
# tracks.genres column.
# ══════════════════════════════════════════════
GENRE_FAMILIES: dict[str, list[str]] = {
    "folk_traditional": [
        "fado", "rebetiko", "qawwali", "gamelan", "carnatic", "hindustani",
        "gnawa", "griot", "benga", "highlife", "taarab", "joik", "sean-nós",
        "klezmer", "csárdás", "throat singing", "mbalax", "choro", "forró",
        "huayno", "mbaqanga", "chimurenga", "raï", "samba", "bossa nova",
        "tango", "cantautori", "folk", "traditional", "world music", "polka",
        "celtic", "pansori", "enka", "gagaku", "ghazal", "mpb", "cumbia",
        "vallenato", "flamenco", "rumba", "son", "son jarocho", "tropicália",
    ],
    "classical": [
        "baroque", "romantic era", "contemporary classical", "minimalism",
        "opera", "lieder", "choral", "sacred music", "gregorian",
        "neoclassical", "chamber music", "symphonic", "orchestral",
    ],
    "pop": [
        "j-pop", "k-pop", "cantopop", "mandopop", "city pop", "art pop",
        "chamber pop", "baroque pop", "hyperpop", "pc music", "italo disco",
        "eurobeat", "schlager", "chanson", "indie pop", "dream pop", "pop",
    ],
    "rock": [
        "krautrock", "shoegaze", "post-punk", "noise rock", "math rock",
        "post-rock", "stoner rock", "doom metal", "black metal", "death metal",
        "grindcore", "powerviolence", "hardcore punk", "punk", "metal",
        "garage rock", "prog rock", "rock", "psychedelic rock", "indie rock",
    ],
    "jazz": [
        "free jazz", "ethio-jazz", "afrobeat", "latin jazz", "bebop",
        "cool jazz", "hard bop", "fusion", "smooth jazz", "jazz",
    ],
    "electronic": [
        "techno", "house", "ambient", "drum and bass", "dubstep", "trance",
        "gabber", "breakcore", "idm", "glitch", "vaporwave", "synthwave",
        "electro", "acid house", "deep house", "minimal techno", "psytrance",
        "hardstyle", "future garage", "uk garage", "footwork", "juke", "gqom",
        "amapiano", "baile funk", "kuduro", "singeli", "mahraganat", "budots",
        "koplo", "new beat", "dub techno", "electronic", "afrohouse",
    ],
    "hiphop_rnb": [
        "boom bap", "trap", "drill", "grime", "phonk", "lo-fi hip hop",
        "soul", "funk", "northern soul", "deep funk", "r&b", "rap",
        "hip hop", "hip-hop", "neo soul", "jazz rap", "conscious rap",
        "afrobeats trap",
    ],
    "reggae_caribbean": [
        "roots reggae", "dub", "dancehall", "ska", "rocksteady", "lovers rock",
        "ragga", "kompa", "soca", "chutney", "reggaeton", "dembow", "reggae",
        "mento", "calypso",
    ],
    "country_americana": [
        "country", "americana", "bluegrass", "folk revival", "outlaw country",
        "appalachian",
    ],
}


def classify_family(label: str) -> str | None:
    """Map a genre-ish label to one of the families above. Substring match.

    Returns None if nothing matches — caller can count these as "uncategorized".
    """
    if not label:
        return None
    s = label.lower()
    # Longest-keyword-first so "hip hop" beats "hop" etc.
    for fam, keys in GENRE_FAMILIES.items():
        for k in sorted(keys, key=len, reverse=True):
            if k in s:
                return fam
    return None

# ══════════════════════════════════════════════
# ANALYSIS
# ══════════════════════════════════════════════

print("\n📊 DIG — POOL GAP ANALYSIS\n")

try:
    from lib.discovery_lock import load_discovery as _load_discovery
    discovery = _load_discovery()
except Exception as e:
    print(f"  Could not load discovery pool from DB: {e}")
    sys.exit(1)

# ── Collect stats ──
region_counts = {}
source_counts = Counter()
energy_counts = Counter()
mood_words = Counter()
texture_words = Counter()
feel_words = Counter()
use_case_words = Counter()
query_genres = Counter()
labeled_count = 0
total = 0

for region, tracks in discovery.items():
    region_counts[region] = len(tracks)
    for t in tracks:
        total += 1
        source_counts[t.get("source", "spotify")] += 1

        # Extract genre from query field
        query = t.get("query", "")
        if ":" in query:
            genre_part = query.split(":", 1)[1].strip()
            # Remove year filter if present
            if " year:" in genre_part:
                genre_part = genre_part.split(" year:")[0].strip()
            if genre_part:
                query_genres[genre_part] += 1

        labels = t.get("labels", {})
        if labels:
            labeled_count += 1
            energy_counts[labels.get("energy", "unknown")] += 1
            # Split multi-word labels into individual terms for analysis
            for word in labels.get("mood", "").lower().split():
                mood_words[word] += 1
            for word in labels.get("texture", "").lower().split():
                texture_words[word] += 1
            for word in labels.get("feel", "").lower().split():
                feel_words[word] += 1
            for word in labels.get("use_case", "").lower().split():
                use_case_words[word] += 1

print(f"  Total tracks: {total}")
print(f"  Labeled: {labeled_count}")
print(f"  Regions: {len(region_counts)}")
print(f"  Sources: {dict(source_counts)}")

# ── Region distribution ──
print(f"\n── Region distribution ──")
sorted_regions = sorted(region_counts.items(), key=lambda x: x[1], reverse=True)
for region, count in sorted_regions:
    bar = "█" * (count // 10)
    print(f"  {region:25s} {count:4d} {bar}")

# ── Find gaps ──
# Regions with very few tracks
median_count = sorted(region_counts.values())[len(region_counts) // 2] if region_counts else 0
thin_regions = [r for r, c in region_counts.items() if c < median_count * 0.3]

# ── Genre family distribution — the principled view of whether the pool
# is balanced. Combine query-derived genres with stored tracks.genres so we
# see both the intent of the search and the reality of the tracks.
family_counts: Counter = Counter()
family_uncategorized = 0
try:
    from lib.db import fetchall as _db_fetchall_fam
    rows = _db_fetchall_fam(
        "SELECT query, genres FROM tracks"
    )
    for r in rows:
        candidates: list[str] = []
        q = r.get("query") or ""
        if q.startswith("catalog:"):
            candidates.append(q[len("catalog:"):].split(" year:")[0].strip())
        for g in (r.get("genres") or [])[:3]:
            candidates.append(str(g))
        fam = None
        for c in candidates:
            fam = classify_family(c)
            if fam:
                break
        if fam:
            family_counts[fam] += 1
        else:
            family_uncategorized += 1
except Exception as e:
    print(f"  (family distribution unavailable: {e})")

family_total = sum(family_counts.values()) + family_uncategorized
family_share: dict[str, float] = {
    k: family_counts[k] / family_total for k in GENRE_FAMILIES if family_total
}
over_represented = [k for k, v in family_share.items() if v > 0.20]
under_represented = [k for k in GENRE_FAMILIES if family_share.get(k, 0.0) < 0.05]

# ── Country-level coverage via origin_region (MusicBrainz) ──
# The existing region_counts uses macro buckets ("West Africa", "Caribbean",
# "South Asia"...) which hide country-level gaps. origin_region is populated
# by the MusicBrainz backfill and gives us country-level granularity. Feed
# this into Claude so strategies can directly target Madagascar, Mozambique,
# Syria, etc. rather than just "Africa" or "Middle East".
origin_counts: Counter = Counter()
try:
    from lib.db import fetchall as _db_fetchall_origin
    rows = _db_fetchall_origin(
        "SELECT origin_region, COUNT(*) AS n FROM tracks "
        "WHERE origin_region IS NOT NULL AND origin_region <> '' "
        "GROUP BY origin_region ORDER BY 2 DESC"
    )
    for r in rows:
        origin_counts[r["origin_region"]] = r["n"]
except Exception as e:
    print(f"  (origin_region stats unavailable: {e})")

# Countries we'd expect to see at least 25 tracks from but don't
_CANON_COUNTRIES = [
    # Africa
    "Nigeria", "Ghana", "Senegal", "Mali", "Ivory Coast", "Guinea", "Niger",
    "Kenya", "Tanzania", "Ethiopia", "Somalia", "Uganda", "DR Congo", "Congo",
    "Angola", "Zimbabwe", "South Africa", "Mozambique", "Madagascar",
    "Cape Verde", "Sudan", "Eritrea", "Egypt", "Morocco", "Algeria", "Tunisia",
    "Libya", "Cameroon",
    # Middle East
    "Iran", "Iraq", "Syria", "Lebanon", "Palestine", "Israel",
    "Saudi Arabia", "Yemen", "UAE", "Jordan", "Turkey",
    # South/SE Asia
    "India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal", "Bhutan",
    "Afghanistan", "Thailand", "Vietnam", "Cambodia", "Laos", "Myanmar",
    "Indonesia", "Philippines", "Malaysia", "Singapore",
    # East Asia / Central Asia
    "Mongolia", "Tibet", "Taiwan", "Hong Kong",
    "Kazakhstan", "Uzbekistan", "Tajikistan", "Kyrgyzstan", "Turkmenistan",
    # Latin America
    "Mexico", "Brazil", "Argentina", "Colombia", "Peru", "Chile", "Venezuela",
    "Ecuador", "Bolivia", "Paraguay", "Uruguay", "Cuba", "Puerto Rico",
    "Dominican Republic", "Jamaica", "Trinidad", "Haiti", "Bahamas",
    "Barbados", "Panama", "Costa Rica", "Guatemala", "Honduras",
    "El Salvador", "Nicaragua",
    # Europe (non-Anglo)
    "Greece", "Russia", "Ukraine", "Poland", "Czech Republic", "Hungary",
    "Romania", "Serbia", "Bulgaria", "Armenia", "Georgia", "Azerbaijan",
    "Albania", "Bosnia", "Croatia", "Slovakia", "Lithuania", "Latvia",
    "Estonia",
]
zero_coverage = [c for c in _CANON_COUNTRIES if origin_counts.get(c, 0) == 0]
thin_countries = [
    (c, origin_counts.get(c, 0)) for c in _CANON_COUNTRIES
    if 0 < origin_counts.get(c, 0) < 25
]
thin_countries.sort(key=lambda x: x[1])

# Anglo over-concentration at origin level
anglo = ("USA", "UK", "Canada", "Australia", "New Zealand", "Ireland")
anglo_origin_total = sum(origin_counts.get(c, 0) for c in anglo)
origin_total = sum(origin_counts.values())
anglo_share = (anglo_origin_total / origin_total) if origin_total else 0.0

# ── Native-script coverage per region ──
# Discovery searches are nearly all in Latin transliteration, which means we
# miss the deepest catalogue: Hindi-script filmi, Greek-script rebetiko,
# Cyrillic Russian estrada, Arabic-script raï/chaabi, etc. We measure the
# share of tracks per region whose name OR artist actually uses the expected
# native script — when it's low, we prompt Claude to write native-script
# queries.
import re as _re_script
_SCRIPT_RX = {
    "cjk":        _re_script.compile(r'[一-鿿぀-ヿ가-힯]'),
    "cyrillic":   _re_script.compile(r'[Ѐ-ӿԀ-ԯ]'),
    "arabic":     _re_script.compile(r'[؀-ۿݐ-ݿࢠ-ࣿ]'),
    "devanagari": _re_script.compile(r'[ऀ-ॿ]'),
    "bengali":    _re_script.compile(r'[ঀ-৿]'),
    "tamil":      _re_script.compile(r'[஀-௿]'),
    "thai":       _re_script.compile(r'[฀-๿]'),
    "lao":        _re_script.compile(r'[຀-໿]'),
    "khmer":      _re_script.compile(r'[ក-៿]'),
    "myanmar":    _re_script.compile(r'[က-႟]'),
    "hebrew":     _re_script.compile(r'[֐-׿]'),
    "greek":      _re_script.compile(r'[Ͱ-Ͽ]'),
    "ethiopic":   _re_script.compile(r'[ሀ-፿]'),
}
# Region → expected native script + example native genre/idiom anchors so
# Claude doesn't have to guess. Anchors are deliberately mixed: real genre
# names, characteristic idioms, classic-era keywords.
_REGION_NATIVE = {
    "Japan":         ("cjk",      ["演歌", "歌謡曲", "シティポップ", "民謡", "雅楽", "邦楽"]),
    "South Korea":   ("cjk",      ["트로트", "발라드", "가요", "민요", "판소리", "K-POP"]),
    "China":         ("cjk",      ["民歌", "戏曲", "古典音乐", "粤语流行", "民族音乐"]),
    "Taiwan":        ("cjk",      ["台語歌", "南管", "民謠", "原住民音樂"]),
    "Hong Kong":     ("cjk",      ["粵語流行曲", "廣東歌"]),
    "Russia":        ("cyrillic", ["шансон", "русский рок", "советская эстрада", "авторская песня"]),
    "Eastern Europe":("cyrillic", ["народная музыка", "шансон", "поп"]),
    "Ukraine":       ("cyrillic", ["українська пісня", "кобзар"]),
    "Middle East":   ("arabic",   ["طرب", "شعبي", "مطرب", "أغنية", "قوالي"]),
    "North Africa":  ("arabic",   ["راي", "شعبي مغربي", "ملحون", "غناوة", "الأغنية الجزائرية"]),
    "Iran":          ("arabic",   ["موسیقی سنتی", "پاپ ایرانی", "ترانه", "آواز ایرانی"]),
    "India":         ("devanagari", ["फ़िल्मी संगीत", "ग़ज़ल", "क़व्वाली", "लोक संगीत", "शास्त्रीय संगीत"]),
    "South Asia":    ("devanagari", ["क़व्वाली", "बौल", "रवीन्द्र संगीत", "শাস্ত্রীয় সঙ্গীত"]),
    "Thailand":      ("thai",     ["เพลงไทย", "ลูกทุ่ง", "หมอลำ", "ลูกกรุง", "เพลงพื้นบ้าน"]),
    "Laos":          ("lao",      ["ເພງລາວ", "ໝໍລຳ"]),
    "Cambodia":      ("khmer",    ["ភ្លេងខ្មែរ", "ចាប៉ីដងវែង", "ព្រះរាជទ្រព្យ"]),
    "Myanmar":       ("myanmar",  ["မြန်မာဂီတ", "သီချင်း"]),
    "Greece":        ("greek",    ["λαϊκό", "ρεμπέτικο", "έντεχνο", "δημοτικό"]),
}
script_coverage = {}
for region, (script, _anchors) in _REGION_NATIVE.items():
    rx = _SCRIPT_RX.get(script)
    if not rx:
        continue
    region_tracks = []
    for t in discovery.get(region, []):
        region_tracks.append(t)
    n_total = len(region_tracks)
    if n_total == 0:
        continue
    n_native = sum(
        1 for t in region_tracks
        if rx.search(t.get("name") or "") or rx.search(t.get("artist") or "")
    )
    pct = n_native / n_total
    script_coverage[region] = {
        "script": script, "native": n_native, "total": n_total, "pct": round(pct, 3)
    }
# Regions where native-script coverage is below 40% — Claude should target
# these with native-script queries.
script_gap_regions = [
    {"region": r, "script": v["script"], "pct": v["pct"],
     "tracks_total": v["total"], "tracks_native": v["native"]}
    for r, v in sorted(script_coverage.items(), key=lambda x: x[1]["pct"])
    if v["pct"] < 0.40
]

# ── Available native-script vocab ──
# The `genres` table is the source of truth for what queries DIG can make.
# When script_gap_regions is non-empty, surface the native-script vocab
# entries that haven't yet been searched as a usable pool. Claude can pick
# from these directly — no need to memorize cultural-knowledge dictionaries
# in the prompt; the database has them.
native_vocab_by_script: dict[str, list[str]] = {}
try:
    from lib.db import fetchall as _db_fetchall_vocab
    rows = _db_fetchall_vocab("SELECT genre FROM genres ORDER BY genre")
    for r in rows:
        g = r["genre"] or ""
        for script_name, rx in _SCRIPT_RX.items():
            if rx.search(g):
                native_vocab_by_script.setdefault(script_name, []).append(g)
                break
except Exception as e:
    print(f"  (native-script vocab listing unavailable: {e})")

# ── Catalog coverage stats (how mapped is the grid?) ──
print(f"\n── Catalog cell coverage ──")
catalog_stats = {}
try:
    from lib.db import fetchone as _db_fetchone, fetchall as _db_fetchall
    total_cells  = (_db_fetchone("SELECT COUNT(*) AS n FROM catalog_cells") or {}).get("n", 0)
    virgin_cells = (_db_fetchone("SELECT COUNT(*) AS n FROM catalog_cells WHERE last_scanned IS NULL") or {}).get("n", 0)
    explored_cells = total_cells - virgin_cells
    pct_explored = explored_cells / total_cells * 100 if total_cells else 0

    # Most searched genres (by total explores across all cells)
    top_explored_genres = _db_fetchall(
        "SELECT genre, SUM(explored) AS n FROM catalog_cells WHERE explored > 0 GROUP BY genre ORDER BY n DESC LIMIT 15"
    )
    # Genres with cells but zero explores — the true unknown territory
    virgin_genre_sample = _db_fetchall(
        """SELECT genre, COUNT(*) AS cells
           FROM catalog_cells WHERE last_scanned IS NULL
           GROUP BY genre ORDER BY cells DESC LIMIT 20"""
    )
    # Regions with the most uncharted cells
    virgin_regions = _db_fetchall(
        """SELECT region, COUNT(*) AS virgin_cells
           FROM catalog_cells WHERE last_scanned IS NULL
           GROUP BY region ORDER BY virgin_cells DESC LIMIT 15"""
    )
    catalog_stats = {
        "total_cells": total_cells,
        "explored_cells": explored_cells,
        "virgin_cells": virgin_cells,
        "pct_explored": round(pct_explored, 2),
        "top_explored_genres": [r["genre"] for r in top_explored_genres],
        "most_virgin_genres": [r["genre"] for r in virgin_genre_sample],
        "most_virgin_regions": [r["region"] for r in virgin_regions],
    }
    print(f"  Total cells:    {total_cells:,}")
    print(f"  Explored:       {explored_cells:,} ({pct_explored:.1f}%)")
    print(f"  Never searched: {virgin_cells:,}")
    if virgin_genre_sample:
        print(f"  Sample unexplored genres: {', '.join(r['genre'] for r in virgin_genre_sample[:8])}")
except Exception as e:
    print(f"  (catalog stats unavailable: {e})")

# Energy distribution
print(f"\n── Energy distribution ──")
for energy, count in energy_counts.most_common():
    pct = count / labeled_count * 100 if labeled_count else 0
    print(f"  {energy:20s} {count:4d} ({pct:.1f}%)")

# Genre coverage
print(f"\n── Top genres found ──")
for genre, count in query_genres.most_common(20):
    print(f"  {genre:30s} {count:4d}")

# ── Generate priorities ──
print(f"\n── Generating priorities ──\n")

# Known genre landscape (from discover.py GENRE_POOL)
ALL_KNOWN_GENRES = [
    "fado", "flamenco", "tango", "rebetiko", "enka", "qawwali", "ghazal",
    "gamelan", "gagaku", "pansori", "raï", "gnawa", "griot", "highlife",
    "mbalax", "benga", "taarab", "mbaqanga", "chimurenga", "calypso",
    "mento", "son jarocho", "huayno", "forró", "choro", "cueca",
    "joik", "sean-nós", "klezmer", "csárdás", "throat singing",
    "carnatic", "hindustani classical", "guqin", "erhu", "pipa",
    "techno", "house", "ambient", "drum and bass", "dubstep", "trance",
    "gabber", "breakcore", "idm", "glitch", "vaporwave", "synthwave",
    "electro", "acid house", "deep house", "minimal techno",
    "psytrance", "hardstyle", "future garage", "uk garage",
    "footwork", "juke", "gqom", "amapiano", "baile funk",
    "kuduro", "singeli", "mahraganat", "budots", "koplo",
    "krautrock", "shoegaze", "post-punk", "noise rock", "math rock",
    "post-rock", "stoner rock", "doom metal", "black metal",
    "death metal", "grindcore", "powerviolence", "hardcore punk",
    "free jazz", "ethio-jazz", "afrobeat", "latin jazz", "bossa nova",
    "samba", "mpb", "tropicália", "northern soul", "deep funk",
    "boom bap", "trap", "drill", "grime", "phonk", "lo-fi hip hop",
    "art pop", "chamber pop", "baroque pop", "hyperpop", "pc music",
    "city pop", "cantopop", "mandopop", "j-pop", "k-pop",
    "italo disco", "eurobeat", "schlager", "chanson",
    "roots reggae", "dub", "dancehall", "ska", "rocksteady",
    "lovers rock", "ragga", "kompa", "soca", "chutney",
    "reggaeton", "dembow",
    "baroque", "romantic era", "contemporary classical", "minimalism",
    "opera", "lieder", "choral", "sacred music", "gregorian chant",
]

# Genres we have vs genres we should have
found_genres = set(query_genres.keys())
missing_genres = [g for g in ALL_KNOWN_GENRES if g not in found_genres]

# Build priorities
priorities = {
    "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "pool_size": total,
    "labeled": labeled_count,

    # Regions that need more tracks
    "boost_regions": thin_regions,

    # Genres never found in the pool
    "missing_genres": missing_genres[:50],

    # Catalog grid coverage snapshot
    "catalog_coverage": catalog_stats,

    # Energy balance — what's overrepresented vs underrepresented
    "energy_distribution": dict(energy_counts),

    # Top mood/texture/feel terms (for understanding current vibes)
    "top_moods": [w for w, _ in mood_words.most_common(30)],
    "top_textures": [w for w, _ in texture_words.most_common(20)],
    "top_feels": [w for w, _ in feel_words.most_common(20)],
    "top_use_cases": [w for w, _ in use_case_words.most_common(20)],

    # Country-level (origin_region / MusicBrainz) coverage — finer-grained
    # than the macro region buckets so downstream can target specific gaps.
    "zero_coverage_countries": zero_coverage,
    "thin_countries": [{"country": c, "tracks": n} for c, n in thin_countries],
    "anglo_origin_share": round(anglo_share, 3),

    # Native-script coverage per region — drives the prompt's native-script
    # quota. Discovery has been searching almost exclusively in Latin
    # transliteration; the deepest catalog (Hindi-script filmi, Arabic
    # raï, Cyrillic estrada, …) sits behind native-script queries.
    "script_gap_regions": script_gap_regions,
}

# ── Ask Claude for strategic recommendations ──
if ANTHROPIC_API_KEY:
    import anthropic
    import httpx as _httpx
    client = anthropic.Anthropic(
        api_key=ANTHROPIC_API_KEY,
        max_retries=0,
        timeout=_httpx.Timeout(60.0),
    )

    catalog_section = ""
    if catalog_stats:
        catalog_section = f"""
Catalog grid coverage ({catalog_stats['total_cells']:,} total cells = region × genre × decade):
- Explored: {catalog_stats['explored_cells']:,} ({catalog_stats['pct_explored']}%)
- Never searched: {catalog_stats['virgin_cells']:,}
- Genres we search most: {catalog_stats['top_explored_genres'][:10]}
- Genres with most unexplored cells: {catalog_stats['most_virgin_genres'][:10]}
- Regions with most unexplored cells: {catalog_stats['most_virgin_regions'][:10]}
"""

    # Country-level coverage view — this is what the pool actually looks
    # like at origin-country granularity (MusicBrainz-derived).
    top_countries_for_prompt = origin_counts.most_common(40)
    thin_countries_preview = thin_countries[:25]

    summary = f"""Current DIG discovery pool stats:
- {total} tracks across {len(region_counts)} macro regions
- {labeled_count} have AI labels
{catalog_section}
Region (macro bucket) distribution — top 25 shown:
{json.dumps(dict(sorted_regions[:25]), indent=2)}

COUNTRY-LEVEL COVERAGE (origin_region from MusicBrainz, more accurate than macro):
  top 40 countries by track count:
{json.dumps([[c, n] for c, n in top_countries_for_prompt], ensure_ascii=False)}

  zero-coverage countries (canonical music cultures with 0 tracks from them):
{json.dumps(zero_coverage)}

  thin countries (< 25 tracks — these need explicit targeting):
{json.dumps([[c, n] for c, n in thin_countries_preview])}

  Anglo-origin share (USA+UK+CA+AU+NZ+IE): {anglo_share*100:.1f}% of origin-populated tracks

NATIVE-SCRIPT COVERAGE PER REGION (diagnostic — regions where the share of
tracks containing the expected non-Latin script in name or artist is low.
This usually means the deepest catalog hasn't been searched yet because it
sits behind native-script queries):
{json.dumps(script_gap_regions, ensure_ascii=False)}

NATIVE-SCRIPT VOCAB AVAILABLE (these are entries in the genres table — pick
the ones whose script matches the gap regions above and use them as queries.
Latin-transliteration searches CANNOT reach the catalog these unlock):
{json.dumps(native_vocab_by_script, ensure_ascii=False)}

Energy distribution: {dict(energy_counts)}

Top 20 genres found: {json.dumps([g for g,_ in query_genres.most_common(20)])}

Genres from our target list NOT yet found: {json.dumps(missing_genres[:30])}

Thin macro regions (< 30% of median): {thin_regions}

Top mood words: {[w for w,_ in mood_words.most_common(15)]}
Top texture words: {[w for w,_ in texture_words.most_common(15)]}"""

    # Build a concrete family-balance brief so Claude doesn't just stack
    # folk on folk. Every run declares what's over- and under-represented.
    family_lines = []
    for fam in sorted(GENRE_FAMILIES.keys(), key=lambda k: family_share.get(k, 0), reverse=True):
        share = family_share.get(fam, 0.0)
        family_lines.append(f"  - {fam}: {share * 100:.1f}%")
    family_brief = "\n".join(family_lines)
    over_txt = ", ".join(over_represented) if over_represented else "none"
    under_txt = ", ".join(under_represented) if under_represented else "none"

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1500,
            messages=[{"role": "user", "content": f"""You are a music curator analyzing a discovery pool's coverage gaps.

{summary}

GENRE-FAMILY DISTRIBUTION (must be balanced — no family > 20%):
{family_brief}
Over-represented (>20%): {over_txt}
Under-represented (<5%): {under_txt}

Your job is to produce 10 search strategies that PUSH THE POOL TOWARD BALANCE
across BOTH genre-family AND GEOGRAPHIC ORIGIN.

Hard rules — genre family balance:
- At most 1 strategy per over-represented family.
- At least 6 strategies must target under-represented families.
- The 10 strategies must collectively span at least 6 different families.
- Every strategy must name a SPECIFIC genre (e.g. "shoegaze", "uk garage",
  "boom bap", "bossa nova", "salegy", "morna", "mor lam") — never a free-form
  multi-word descriptor. Native-language genre names are strongly preferred
  for regions where they exist (salegy for Madagascar, marrabenta for
  Mozambique, morna for Cape Verde, muwashshah for Syria, etc.).
- Genres should be real canonical names, not query-style strings.

Hard rules — geographic origin balance (the pool is currently {anglo_share*100:.1f}%
Anglo-origin; we want to cut that):
- AT MOST 2 of the 10 strategies may be Anglo-market-dominated (USA/UK/CA/
  AU/NZ/IE as the ONLY listed markets).
- AT LEAST 4 strategies MUST target non-Western origin countries (any country
  outside USA/UK/CA/AU/NZ/IE/Western Europe).
- AT LEAST 1 strategy MUST target a zero-coverage country from the list above
  (Madagascar, Mozambique, Cape Verde, Syria, Paraguay, etc.) — pick one
  and use the appropriate native-language genre term.
- AT LEAST 2 strategies MUST target countries in the "thin countries" list
  (<25 tracks) — use genres specifically characteristic of those countries.
- Prefer markets that are the *origin* of the genre, not just big Anglo markets
  that happen to have the genre as an import.

Note on script: the "NATIVE-SCRIPT VOCAB AVAILABLE" block above lists the
genres table's non-Latin entries. For any region in the script-gap list,
prefer a vocab entry in that region's script — those queries reach the
deepest catalog that Latin-transliteration searches cannot. Pick the entry
whose script and idiom best fits the gap; you may add a decade or
qualifier in the same script.

Still honour:
1. Decades we're thin on (pre-1990 is currently sparse — try 1960s-1980s for
   non-Anglo scenes that peaked then, e.g. salegy 1970s, morna 1950s-1970s,
   rebetiko 1930s-1950s, mor lam 1970s-1980s, chimurenga 1970s-1980s).
2. DIG's philosophy: "Miley Cyrus and Chaweewan Damnern sit in the same pool"
   — include mainstream pop/rock/electronic/hip-hop ALONGSIDE regional music,
   but not at the expense of geographic balance.

Format as JSON array of objects:
[{{"query": "<genre> [optional decade]", "markets": ["XX","YY"], "family": "<family_name>", "origin_countries": ["Madagascar"], "reason": "why — cite the gap"}}]

Return ONLY the JSON array, no explanation."""}],
        )
        text = response.content[0].text
        strategies = extract_json(text)
        if isinstance(strategies, list):
            priorities["ai_strategies"] = strategies
            print(f"  Claude suggested {len(strategies)} search strategies")
            for s in strategies:
                print(f"    → {s['query']} in {s['markets']}: {s['reason']}")
    except Exception as e:
        print(f"  (Claude strategy generation failed: {e})")

# ── Save priorities to DB (catalog_meta) ──
try:
    from lib.db import set_meta
    set_meta("discovery_priorities", priorities)
    print(f"\n  Missing genres: {len(missing_genres)}")
    print(f"  Thin regions: {len(thin_regions)}")
    print(f"\n✓ Priorities saved to catalog_meta (key: discovery_priorities)")
    print(f"  Next discover.py run will use these to guide its search.")
except Exception as e:
    import json as _json
    priorities_path = os.path.join(DIR, "discovery_priorities.json")
    with open(priorities_path, "w") as f:
        _json.dump(priorities, f, indent=2)
    print(f"\n✓ Priorities saved to {priorities_path} (DB unavailable: {e})")
