#!/usr/bin/env python3
"""
Build a music map from your ledger.
Looks up artists on MusicBrainz for geography, uses Spotify data for years.
Generates an HTML visualization.
"""

import json
import os
import time
import re
import requests
from collections import defaultdict, Counter
from urllib.parse import quote_plus

DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_PATH = os.path.join(DIR, "ledger.json")
SPOTIFY_RAW = os.path.join(DIR, "spotify_raw.json")
ARTIST_CACHE = os.path.join(DIR, "artist_cache.json")
MAP_HTML = os.path.join(DIR, "map.html")

MB_BASE = "https://musicbrainz.org/ws/2"
MB_HEADERS = {
    "User-Agent": "MusicRadar/0.1 (music discovery tool)",
    "Accept": "application/json",
}

# Region groupings
COUNTRY_TO_REGION = {
    # West Africa
    "NG": "West Africa", "GH": "West Africa", "SN": "West Africa",
    "CI": "West Africa", "ML": "West Africa", "BF": "West Africa",
    "CM": "West Africa", "BJ": "West Africa", "TG": "West Africa",
    # East Africa
    "KE": "East Africa", "TZ": "East Africa", "ET": "East Africa",
    "UG": "East Africa", "RW": "East Africa",
    # Southern Africa
    "ZA": "Southern Africa", "MZ": "Southern Africa", "ZW": "Southern Africa",
    "AO": "Southern Africa", "BW": "Southern Africa",
    # North Africa
    "MA": "North Africa", "DZ": "North Africa", "TN": "North Africa",
    "EG": "North Africa", "LY": "North Africa",
    # Western Europe
    "FR": "France", "BE": "Belgium", "NL": "Netherlands",
    "LU": "Benelux",
    # Southern Europe
    "IT": "Italy", "ES": "Spain", "PT": "Portugal", "GR": "Greece",
    # UK & Ireland
    "GB": "UK", "IE": "Ireland",
    # Nordic
    "SE": "Nordic", "NO": "Nordic", "DK": "Nordic",
    "FI": "Nordic", "IS": "Nordic",
    # Central Europe
    "DE": "Germany", "AT": "Germany/Austria", "CH": "Switzerland",
    # Eastern Europe
    "PL": "Eastern Europe", "CZ": "Eastern Europe", "SK": "Eastern Europe",
    "HU": "Eastern Europe", "RO": "Eastern Europe", "BG": "Eastern Europe",
    "HR": "Eastern Europe", "RS": "Eastern Europe", "SI": "Eastern Europe",
    "UA": "Eastern Europe", "BY": "Eastern Europe", "RU": "Russia",
    # Turkey & Middle East
    "TR": "Turkey", "IL": "Middle East", "LB": "Middle East",
    "PS": "Middle East", "SY": "Middle East", "IQ": "Middle East",
    "IR": "Iran", "SA": "Middle East", "AE": "Middle East",
    # South Asia
    "IN": "India", "PK": "South Asia", "BD": "South Asia",
    "LK": "South Asia", "NP": "South Asia",
    # East Asia
    "JP": "Japan", "KR": "South Korea", "CN": "China",
    "TW": "Taiwan", "HK": "Hong Kong", "MO": "China",
    # Southeast Asia
    "TH": "Thailand", "VN": "Vietnam", "ID": "Indonesia",
    "PH": "Philippines", "MY": "Malaysia", "SG": "Singapore",
    "KH": "Cambodia", "LA": "Laos", "MM": "Myanmar",
    # Oceania
    "AU": "Australia", "NZ": "New Zealand",
    # North America
    "US": "USA", "CA": "Canada", "MX": "Mexico",
    # Caribbean
    "JM": "Caribbean", "CU": "Caribbean", "TT": "Caribbean",
    "HT": "Caribbean", "DO": "Caribbean", "PR": "Caribbean",
    "BB": "Caribbean",
    # Central America
    "GT": "Central America", "HN": "Central America", "CR": "Central America",
    "PA": "Central America", "NI": "Central America",
    # South America
    "BR": "Brazil", "AR": "Argentina", "CO": "Colombia",
    "CL": "Chile", "PE": "Peru", "VE": "Venezuela",
    "EC": "South America", "UY": "South America", "PY": "South America",
    "BO": "South America",
    # XW = Worldwide
    "XW": "[Worldwide]",
}


def load_artist_cache():
    if os.path.exists(ARTIST_CACHE):
        with open(ARTIST_CACHE) as f:
            return json.load(f)
    return {}


def save_artist_cache(cache):
    with open(ARTIST_CACHE, "w") as f:
        json.dump(cache, f, indent=2)


def lookup_artist_mb(name, cache):
    """Look up an artist on MusicBrainz. Returns dict with country, tags, etc."""
    key = name.lower().strip()
    if key in cache:
        return cache[key]

    try:
        url = f"{MB_BASE}/artist/?query=artist:{quote_plus(name)}&limit=3&fmt=json"
        resp = requests.get(url, headers=MB_HEADERS, timeout=10)
        if resp.status_code == 503:
            time.sleep(2)
            resp = requests.get(url, headers=MB_HEADERS, timeout=10)
        data = resp.json()

        if data.get("artists"):
            # Pick best match
            for artist in data["artists"]:
                score = artist.get("score", 0)
                if score >= 80:
                    country = artist.get("country", "")
                    area = artist.get("area", {}).get("name", "")
                    begin = artist.get("life-span", {}).get("begin", "")
                    tags = [t["name"] for t in artist.get("tags", [])[:10]]
                    result = {
                        "name": artist.get("name", name),
                        "country": country,
                        "area": area,
                        "region": COUNTRY_TO_REGION.get(country, country or "Unknown"),
                        "begin": begin,
                        "tags": tags,
                        "mb_id": artist.get("id", ""),
                    }
                    cache[key] = result
                    return result

        cache[key] = {"name": name, "country": "", "region": "Unknown", "area": "", "tags": [], "begin": ""}
        return cache[key]

    except Exception as e:
        cache[key] = {"name": name, "country": "", "region": "Unknown", "area": "", "tags": [], "begin": "", "error": str(e)}
        return cache[key]


def extract_artists_and_years():
    """Extract unique artists and track years from ledger + spotify data."""
    with open(LEDGER_PATH) as f:
        ledger = json.load(f)

    artists = set()
    track_years = {}  # artist -> set of years

    # From Spotify raw data (has dates)
    if os.path.exists(SPOTIFY_RAW):
        with open(SPOTIFY_RAW) as f:
            sp = json.load(f)
        for t in sp.get("saved_tracks", []):
            artist = t.get("artist", "")
            added = t.get("added_at", "")
            year = added[:4] if added else ""
            if artist:
                artists.add(artist)
                if year:
                    track_years.setdefault(artist, set()).add(year)
        for t in sp.get("top_tracks", []):
            if t.get("artist"):
                artists.add(t["artist"])
        for a in sp.get("top_artists", []):
            if a.get("name"):
                artists.add(a["name"])

    # From ledger
    for entry in ledger["known"]:
        if " - " in entry:
            artist = entry.split(" - ")[0].strip()
            artists.add(artist)
        else:
            artists.add(entry.strip())

    return artists, track_years


def build_map_data(artists, track_years, cache):
    """Look up all artists and build the map data."""
    total = len(artists)
    looked_up = 0
    new_lookups = 0

    for i, artist in enumerate(sorted(artists)):
        if artist.lower().strip() in cache:
            looked_up += 1
            continue

        lookup_artist_mb(artist, cache)
        new_lookups += 1
        looked_up += 1

        if new_lookups % 10 == 0:
            print(f"  ...{looked_up}/{total} artists ({new_lookups} new lookups)")
            save_artist_cache(cache)

        # MusicBrainz rate limit: 1 req/sec
        time.sleep(1.1)

    save_artist_cache(cache)
    print(f"  ✓ {total} artists processed ({new_lookups} new lookups)")
    return cache


def generate_html(cache, track_years):
    """Generate the HTML map visualization."""

    # Build region × decade matrix
    region_decade = defaultdict(lambda: defaultdict(list))
    region_count = Counter()
    all_tags = Counter()

    for key, info in cache.items():
        region = info.get("region", "Unknown")
        if region == "Unknown" or not region:
            continue
        region_count[region] += 1

        # Try to get decade from begin year or track years
        years = set()
        if info.get("begin"):
            try:
                y = int(info["begin"][:4])
                years.add(y)
            except (ValueError, IndexError):
                pass

        # Add track years for this artist
        name = info.get("name", key)
        for artist_key, ty in track_years.items():
            if artist_key.lower() == key or name.lower() in artist_key.lower():
                years.update(int(y) for y in ty if y.isdigit())

        for tag in info.get("tags", []):
            all_tags[tag] += 1

        if years:
            for y in years:
                decade = f"{(y // 10) * 10}s"
                region_decade[region][decade].append(info.get("name", key))
        else:
            region_decade[region]["?"].append(info.get("name", key))

    # Get all decades and regions sorted
    all_decades = sorted(set(d for rd in region_decade.values() for d in rd.keys() if d != "?"))
    if "?" in set(d for rd in region_decade.values() for d in rd.keys()):
        all_decades.append("?")

    all_regions = sorted(region_decade.keys(), key=lambda r: -sum(len(v) for v in region_decade[r].values()))

    # Top tags
    top_tags = all_tags.most_common(30)

    # Build HTML
    html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Music Radar — Your Map</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    background: #0a0a0a; color: #e0e0e0; font-family: 'SF Mono', 'Fira Code', monospace;
    padding: 30px;
}
h1 { color: #fff; margin-bottom: 5px; font-size: 24px; }
.subtitle { color: #666; margin-bottom: 30px; font-size: 14px; }
.stats { display: flex; gap: 30px; margin-bottom: 30px; }
.stat { background: #111; padding: 15px 20px; border-radius: 8px; }
.stat .num { font-size: 28px; color: #fff; font-weight: bold; }
.stat .label { color: #666; font-size: 12px; }

.section-title { color: #888; font-size: 14px; margin: 30px 0 15px; text-transform: uppercase; letter-spacing: 2px; }

/* Map grid */
.map-container { overflow-x: auto; margin-bottom: 40px; }
table { border-collapse: collapse; width: 100%; }
th {
    color: #666; font-size: 11px; padding: 8px 6px; text-align: left;
    position: sticky; top: 0; background: #0a0a0a;
    white-space: nowrap;
}
td { padding: 4px 6px; vertical-align: top; }
.region-label {
    color: #999; font-size: 12px; white-space: nowrap;
    padding-right: 15px; font-weight: bold;
    position: sticky; left: 0; background: #0a0a0a;
}
.cell {
    min-width: 60px; min-height: 40px; border-radius: 4px;
    padding: 4px; font-size: 10px; line-height: 1.3;
    position: relative; cursor: default;
}
.cell.empty { background: #111; }
.cell.filled { background: #1a1a2e; }
.cell .count {
    position: absolute; top: 2px; right: 4px;
    font-size: 9px; color: #444;
}
.cell .artists { color: #8888cc; font-size: 10px; }
.cell:hover { background: #222244 !important; }
.cell:hover .artists { color: #aaaaff; }

/* Heat levels */
.heat-1 { background: #0d1117; }
.heat-2 { background: #161b22; }
.heat-3 { background: #1a1e3a; }
.heat-4 { background: #1e2450; }
.heat-5 { background: #253060; }
.heat-6 { background: #2d3a75; }

/* Tags */
.tags { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 30px; }
.tag {
    background: #111; padding: 4px 10px; border-radius: 12px;
    font-size: 11px; color: #888;
}
.tag .tag-count { color: #444; margin-left: 4px; }

/* Blank zones */
.blank-zones { margin-bottom: 30px; }
.blank-zone {
    background: #0d0d0d; border: 1px dashed #222; padding: 12px 16px;
    border-radius: 6px; margin-bottom: 8px; color: #555; font-size: 13px;
}
.blank-zone strong { color: #888; }

/* Region bar chart */
.bar-chart { margin-bottom: 30px; }
.bar-row { display: flex; align-items: center; margin-bottom: 4px; }
.bar-label { width: 150px; font-size: 11px; color: #888; text-align: right; padding-right: 10px; }
.bar { height: 16px; border-radius: 3px; min-width: 2px; }
.bar-count { font-size: 10px; color: #444; margin-left: 6px; }
</style>
</head>
<body>
<h1>YOUR MUSIC MAP</h1>
<p class="subtitle">geography × time — coloured where you've been, blank where you haven't</p>

<div class="stats">
    <div class="stat"><div class="num">""" + str(len(cache)) + """</div><div class="label">artists mapped</div></div>
    <div class="stat"><div class="num">""" + str(len([r for r in region_count if r != "Unknown"])) + """</div><div class="label">regions explored</div></div>
    <div class="stat"><div class="num">""" + str(len(all_decades) - (1 if "?" in all_decades else 0)) + """</div><div class="label">decades spanned</div></div>
    <div class="stat"><div class="num">""" + str(len(all_tags)) + """</div><div class="label">unique tags</div></div>
</div>

<div class="section-title">Region Distribution</div>
<div class="bar-chart">
"""
    # Bar chart
    max_count = max(region_count.values()) if region_count else 1
    colors = ["#4444aa", "#5555bb", "#6666cc", "#7777dd", "#8888ee", "#44aa44", "#aa4444", "#aaaa44", "#44aaaa", "#aa44aa"]
    for i, (region, count) in enumerate(region_count.most_common(25)):
        if region == "Unknown":
            continue
        bar_width = int((count / max_count) * 300)
        color = colors[i % len(colors)]
        html += f'<div class="bar-row"><div class="bar-label">{region}</div><div class="bar" style="width:{bar_width}px;background:{color}"></div><div class="bar-count">{count}</div></div>\n'

    html += """</div>

<div class="section-title">Geography × Decade</div>
<div class="map-container">
<table>
<tr><th></th>"""

    for d in all_decades:
        html += f"<th>{d}</th>"
    html += "</tr>\n"

    for region in all_regions:
        if region == "Unknown":
            continue
        html += f'<tr><td class="region-label">{region}</td>'
        for decade in all_decades:
            artists_in_cell = region_decade[region].get(decade, [])
            count = len(artists_in_cell)
            if count == 0:
                html += '<td><div class="cell empty"></div></td>'
            else:
                heat = min(6, count)
                names = ", ".join(artists_in_cell[:5])
                if count > 5:
                    names += f" +{count - 5}"
                html += f'<td><div class="cell filled heat-{heat}" title="{names}"><span class="count">{count}</span><span class="artists">{", ".join(artists_in_cell[:2])}</span></div></td>'
        html += "</tr>\n"

    html += """</table>
</div>

<div class="section-title">Your Tags</div>
<div class="tags">
"""
    for tag, count in top_tags:
        html += f'<span class="tag">{tag}<span class="tag-count">×{count}</span></span>\n'

    html += """</div>

<div class="section-title">Blank Zones — Where You Haven't Been</div>
<div class="blank-zones">
"""

    # Find blank zones
    all_possible_regions = [
        "West Africa", "East Africa", "Southern Africa", "North Africa",
        "France", "Italy", "Spain", "Portugal", "UK", "Ireland",
        "Germany", "Nordic", "Netherlands", "Belgium", "Eastern Europe", "Russia",
        "Turkey", "Middle East", "Iran",
        "India", "South Asia",
        "Japan", "South Korea", "China", "Taiwan", "Hong Kong",
        "Thailand", "Vietnam", "Indonesia", "Philippines", "Cambodia",
        "Australia", "New Zealand",
        "USA", "Canada", "Mexico", "Caribbean",
        "Brazil", "Argentina", "Colombia", "Chile", "Peru",
    ]

    explored = set(region_count.keys())
    blank = [r for r in all_possible_regions if r not in explored]

    if blank:
        for region in blank:
            html += f'<div class="blank-zone">⬛ <strong>{region}</strong> — unexplored</div>\n'
    else:
        html += '<div class="blank-zone">You\'ve touched every region! But how deep?</div>\n'

    html += """</div>

<p style="color:#333; font-size:11px; margin-top:40px;">Generated by music-radar · """ + str(len(cache)) + """ artists · hover cells for details</p>
</body>
</html>"""

    with open(MAP_HTML, "w") as f:
        f.write(html)
    print(f"  ✓ Map saved to {MAP_HTML}")


if __name__ == "__main__":
    print("\n🗺️  BUILDING YOUR MUSIC MAP\n")

    artists, track_years = extract_artists_and_years()
    print(f"Found {len(artists)} unique artists")

    cache = load_artist_cache()
    print(f"Cache has {len(cache)} artists already")

    cache = build_map_data(artists, track_years, cache)

    generate_html(cache, track_years)
    print(f"\nOpen: file://{MAP_HTML}")
