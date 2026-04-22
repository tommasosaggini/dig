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
import os
import sys
import time
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
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

    summary = f"""Current DIG discovery pool stats:
- {total} tracks across {len(region_counts)} regions
- {labeled_count} have AI labels
{catalog_section}
Region distribution (tracks per region):
{json.dumps(dict(sorted_regions), indent=2)}

Energy distribution: {dict(energy_counts)}

Top 20 genres found: {json.dumps([g for g,_ in query_genres.most_common(20)])}

Genres from our target list NOT yet found: {json.dumps(missing_genres[:30])}

Thin regions (< 30% of median): {thin_regions}

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

Your job is to produce 10 search strategies that PUSH THE POOL TOWARD BALANCE.

Hard rules:
- At most 1 strategy per over-represented family.
- At least 6 strategies must target under-represented families.
- The 10 strategies must collectively span at least 6 different families.
- Every strategy must name a SPECIFIC genre (e.g. "shoegaze", "uk garage",
  "boom bap", "bossa nova") — never a free-form multi-word descriptor.
- Genres should be real canonical names, not query-style strings.

Still honour:
1. Under-represented regions (especially thin ones listed above).
2. Decades we're thin on (pre-1990 is currently sparse).
3. DIG's philosophy: "Miley Cyrus and Chaweewan Damnern sit in the same pool"
   — explicitly include mainstream pop/rock/electronic/hip-hop, not only
   traditional/regional music.

Format as JSON array of objects:
[{{"query": "<genre> [optional decade]", "markets": ["XX","YY"], "family": "<family_name>", "reason": "why"}}]

Return ONLY the JSON array, no explanation."""}],
        )
        text = response.content[0].text
        start = text.find("[")
        end = text.rfind("]") + 1
        if start >= 0 and end > start:
            strategies = json.loads(text[start:end])
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
