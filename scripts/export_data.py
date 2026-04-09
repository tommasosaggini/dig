#!/usr/bin/env python3
"""Export all music-radar data as a single JSON for the web UI."""

import json
import os
from collections import defaultdict, Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR = ROOT

with open(os.path.join(DIR, "artist_cache.json")) as f:
    cache = json.load(f)
with open(os.path.join(DIR, "ledger.json")) as f:
    ledger = json.load(f)
with open(os.path.join(DIR, "spotify_raw.json")) as f:
    spotify = json.load(f)

# Build artist entries with all metadata
artists = []
for key, info in cache.items():
    if not info.get("name"):
        continue
    artists.append({
        "name": info.get("name", key),
        "region": info.get("region", "Unknown"),
        "country": info.get("country", ""),
        "area": info.get("area", ""),
        "begin": info.get("begin", "")[:4] if info.get("begin") else "",
        "tags": info.get("tags", []),
        "known": True,
    })

# Build the "world" — regions and decades that exist but user hasn't explored
# Using MusicBrainz general knowledge of where music comes from
world_regions = [
    "USA", "UK", "France", "Germany", "Italy", "Spain", "Portugal",
    "Nordic", "Netherlands", "Belgium", "Eastern Europe", "Russia",
    "Japan", "South Korea", "China", "Taiwan", "Hong Kong",
    "Thailand", "Vietnam", "Indonesia", "Cambodia", "Philippines", "Malaysia",
    "India", "South Asia", "Iran", "Turkey", "Middle East",
    "West Africa", "East Africa", "Southern Africa", "North Africa",
    "Brazil", "Argentina", "Colombia", "Chile", "Peru", "Mexico",
    "Caribbean", "Canada", "Australia", "New Zealand",
    "Ireland", "Switzerland", "Greece",
]

world_decades = ["1950s", "1960s", "1970s", "1980s", "1990s", "2000s", "2010s", "2020s"]

# Rough estimate of how much music exists per region (relative scale 1-100)
world_density = {
    "USA": 100, "UK": 70, "France": 40, "Germany": 45, "Italy": 30,
    "Spain": 25, "Portugal": 15, "Nordic": 25, "Netherlands": 20,
    "Belgium": 15, "Eastern Europe": 20, "Russia": 20,
    "Japan": 50, "South Korea": 30, "China": 25, "Taiwan": 10, "Hong Kong": 10,
    "Thailand": 10, "Vietnam": 8, "Indonesia": 12, "Cambodia": 5,
    "Philippines": 8, "Malaysia": 6,
    "India": 35, "South Asia": 10, "Iran": 8, "Turkey": 12, "Middle East": 10,
    "West Africa": 20, "East Africa": 10, "Southern Africa": 10, "North Africa": 8,
    "Brazil": 35, "Argentina": 15, "Colombia": 12, "Chile": 8,
    "Peru": 6, "Mexico": 20, "Caribbean": 15,
    "Canada": 20, "Australia": 18, "New Zealand": 8,
    "Ireland": 10, "Switzerland": 8, "Greece": 8,
}

# Count user's exploration per region and decade
user_region_count = Counter()
user_decade_count = Counter()
user_region_decade = defaultdict(lambda: defaultdict(list))
user_tag_count = Counter()

for a in artists:
    r = a["region"]
    if r and r != "Unknown":
        user_region_count[r] += 1
    if a["begin"]:
        try:
            d = f"{(int(a['begin']) // 10) * 10}s"
            user_decade_count[d] += 1
            if r and r != "Unknown":
                user_region_decade[r][d].append(a["name"])
        except:
            pass
    for t in a["tags"]:
        user_tag_count[t] += 1

# Build grid data
grid = []
for region in world_regions:
    for decade in world_decades:
        user_artists = user_region_decade.get(region, {}).get(decade, [])
        world_size = world_density.get(region, 5)
        grid.append({
            "region": region,
            "decade": decade,
            "explored": len(user_artists),
            "world_size": world_size,
            "artists": user_artists[:10],
        })

# All unique tags
all_tags = sorted(user_tag_count.items(), key=lambda x: -x[1])

# Liked tracks
liked = ledger.get("liked", [])

# Known tracks (full list)
known = ledger.get("known", [])

# Spotify tracks with more detail
sp_tracks = []
for t in spotify.get("saved_tracks", []):
    sp_tracks.append({
        "name": t.get("name", ""),
        "artist": t.get("artist", ""),
        "album": t.get("album", ""),
        "added": t.get("added_at", "")[:10],
    })

# Build Spotify track list with IDs, mapped to artist cache for region info
sp_tracks_full = []
for t in spotify.get("saved_tracks", []):
    artist_name = t.get("artist", "")
    artist_key = artist_name.lower().strip()
    artist_info = cache.get(artist_key, {})
    sp_tracks_full.append({
        "name": t.get("name", ""),
        "artist": artist_name,
        "album": t.get("album", ""),
        "id": t.get("id", ""),
        "added": t.get("added_at", "")[:10],
        "region": artist_info.get("region", ""),
        "tags": artist_info.get("tags", [])[:5],
    })

export = {
    "artists": artists,
    "grid": grid,
    "regions": world_regions,
    "decades": world_decades,
    "world_density": world_density,
    "user_region_count": dict(user_region_count),
    "user_decade_count": dict(user_decade_count),
    "tags": all_tags[:50],
    "liked": liked,
    "known": known,
    "spotify_tracks": sp_tracks_full,
    "stats": {
        "total_artists": len(artists),
        "total_known": len(known),
        "total_liked": len(liked),
        "regions_explored": len([r for r in world_regions if user_region_count.get(r, 0) > 0]),
        "regions_total": len(world_regions),
    }
}

out_path = os.path.join(DIR, "data.json")
with open(out_path, "w") as f:
    json.dump(export, f)
print(f"Exported to {out_path} ({os.path.getsize(out_path) // 1024}kb)")
