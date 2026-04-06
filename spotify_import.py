#!/usr/bin/env python3
"""
Import your Spotify library into the music-radar ledger.

Setup:
  1. Go to https://developer.spotify.com/dashboard
  2. Create an app
  3. Set redirect URI to: http://127.0.0.1:8888/callback
  4. Copy your Client ID and Client Secret
  5. Create a .env file in this directory:
       SPOTIPY_CLIENT_ID=your_client_id
       SPOTIPY_CLIENT_SECRET=your_client_secret
       SPOTIPY_REDIRECT_URI=http://127.0.0.1:8888/callback

Usage:
  python3 spotify_import.py              # import saved tracks + top artists
  python3 spotify_import.py --full       # also import all playlists
  python3 spotify_import.py --stats      # show your listening profile without importing
"""

import os
import json
import sys
from collections import Counter

DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_PATH = os.path.join(DIR, "ledger.json")
ENV_PATH = os.path.join(DIR, ".env")
SPOTIFY_CACHE = os.path.join(DIR, "spotify_raw.json")

# Load .env manually
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

import spotipy
from spotipy.oauth2 import SpotifyOAuth


def get_spotify():
    scope = "user-library-read user-top-read user-read-recently-played playlist-read-private"
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        scope=scope,
        redirect_uri=os.environ.get("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8888/callback"),
        cache_path=os.path.join(DIR, ".spotify_token_cache"),
    ))


def fetch_saved_tracks(sp, limit=None):
    """Fetch all saved/liked tracks."""
    print("→ Fetching saved tracks...")
    tracks = []
    results = sp.current_user_saved_tracks(limit=50)
    while results:
        for item in results["items"]:
            t = item["track"]
            if t:
                tracks.append({
                    "name": t.get("name", ""),
                    "artist": ", ".join(a.get("name", "") for a in t.get("artists", [])),
                    "album": t.get("album", {}).get("name", ""),
                    "id": t.get("id"),
                    "genres": [],
                    "popularity": t.get("popularity", 0),
                    "duration_ms": t.get("duration_ms", 0),
                    "added_at": item.get("added_at", ""),
                })
        if limit and len(tracks) >= limit:
            break
        if results["next"]:
            results = sp.next(results)
        else:
            break
        print(f"  ...{len(tracks)} tracks so far")
    print(f"  ✓ {len(tracks)} saved tracks")
    return tracks


def fetch_top_artists(sp):
    """Fetch top artists (short, medium, long term)."""
    print("→ Fetching top artists...")
    artists = {}
    for term in ["short_term", "medium_term", "long_term"]:
        results = sp.current_user_top_artists(limit=50, time_range=term)
        for a in results["items"]:
            if a["id"] not in artists:
                artists[a["id"]] = {
                    "name": a.get("name", ""),
                    "genres": a.get("genres", []),
                    "popularity": a.get("popularity", 0),
                    "terms": [term],
                }
            else:
                artists[a["id"]]["terms"].append(term)
    print(f"  ✓ {len(artists)} top artists")
    return list(artists.values())


def fetch_top_tracks(sp):
    """Fetch top tracks."""
    print("→ Fetching top tracks...")
    tracks = []
    for term in ["short_term", "medium_term", "long_term"]:
        results = sp.current_user_top_tracks(limit=50, time_range=term)
        for t in results["items"]:
            tracks.append({
                "name": t["name"],
                "artist": ", ".join(a["name"] for a in t["artists"]),
                "id": t["id"],
                "term": term,
            })
    # Deduplicate
    seen = set()
    unique = []
    for t in tracks:
        if t["id"] not in seen:
            seen.add(t["id"])
            unique.append(t)
    print(f"  ✓ {len(unique)} top tracks")
    return unique


def fetch_audio_features(sp, track_ids):
    """Fetch audio features for tracks (energy, tempo, etc.)."""
    print("→ Fetching audio features...")
    features = {}
    # Batch in groups of 100
    for i in range(0, len(track_ids), 100):
        batch = track_ids[i:i+100]
        try:
            results = sp.audio_features(batch)
            for f in results:
                if f:
                    features[f["id"]] = {
                        "energy": f["energy"],
                        "danceability": f["danceability"],
                        "valence": f["valence"],
                        "tempo": f["tempo"],
                        "acousticness": f["acousticness"],
                        "instrumentalness": f["instrumentalness"],
                        "speechiness": f["speechiness"],
                        "liveness": f["liveness"],
                        "loudness": f["loudness"],
                    }
        except Exception as e:
            print(f"  (skipped batch: {e})")
        if i % 500 == 0 and i > 0:
            print(f"  ...{i}/{len(track_ids)} features")
    print(f"  ✓ {len(features)} audio features")
    return features


def fetch_playlists(sp):
    """Fetch all user playlists and their tracks."""
    print("→ Fetching playlists...")
    playlists = []
    results = sp.current_user_playlists(limit=50)
    while results:
        for pl in results["items"]:
            playlist_tracks = []
            try:
                tr = sp.playlist_tracks(pl["id"], limit=100)
                while tr:
                    for item in tr["items"]:
                        t = item.get("track")
                        if t and t.get("id"):
                            playlist_tracks.append({
                                "name": t["name"],
                                "artist": ", ".join(a["name"] for a in t["artists"]),
                                "id": t["id"],
                            })
                    if tr["next"]:
                        tr = sp.next(tr)
                    else:
                        break
            except Exception:
                pass
            playlists.append({
                "name": pl["name"],
                "id": pl["id"],
                "tracks": playlist_tracks,
            })
            print(f"  playlist: {pl['name']} ({len(playlist_tracks)} tracks)")
        if results["next"]:
            results = sp.next(results)
        else:
            break
    print(f"  ✓ {len(playlists)} playlists")
    return playlists


def import_to_ledger(saved_tracks, top_artists, top_tracks):
    """Merge Spotify data into the ledger."""
    if os.path.exists(LEDGER_PATH):
        with open(LEDGER_PATH) as f:
            ledger = json.load(f)
    else:
        ledger = {"known": [], "liked": [], "disliked": [], "vibes": {}}

    if "liked" not in ledger:
        ledger["liked"] = []

    known_lower = {k.lower() for k in ledger["known"]}
    liked_lower = {(k["track"].lower() if isinstance(k, dict) else k.lower()) for k in ledger["liked"]}
    added_known = 0
    added_liked = 0

    # Saved/liked tracks → both known AND liked
    for t in saved_tracks:
        entry = f"{t['artist']} - {t['name']}"
        entry_lower = entry.lower()
        if entry_lower not in known_lower:
            ledger["known"].append(entry)
            known_lower.add(entry_lower)
            added_known += 1
        if entry_lower not in liked_lower:
            ledger["liked"].append({"track": entry, "vibe": []})
            liked_lower.add(entry_lower)
            added_liked += 1

    # Top tracks → known + liked (you listened to them a lot)
    for t in top_tracks:
        entry = f"{t['artist']} - {t['name']}"
        entry_lower = entry.lower()
        if entry_lower not in known_lower:
            ledger["known"].append(entry)
            known_lower.add(entry_lower)
            added_known += 1
        if entry_lower not in liked_lower:
            ledger["liked"].append({"track": entry, "vibe": []})
            liked_lower.add(entry_lower)
            added_liked += 1

    # Top artists → known only (we know of them, not specific tracks)
    for a in top_artists:
        if a["name"].lower() not in known_lower:
            ledger["known"].append(a["name"])
            known_lower.add(a["name"].lower())
            added_known += 1

    with open(LEDGER_PATH, "w") as f:
        json.dump(ledger, f, indent=2)

    print(f"\n✓ Added {added_known} to known, {added_liked} to liked ({len(ledger['known'])} known, {len(ledger['liked'])} liked total)")

    # Also add to history.json with status 'saved'
    history_path = os.path.join(DIR, "history.json")
    history = []
    if os.path.exists(history_path):
        with open(history_path) as f:
            history = json.load(f)

    history_ids = {h.get("id") for h in history}
    added_history = 0
    for t in saved_tracks:
        if t.get("id") and t["id"] not in history_ids:
            history.append({
                "track": t["name"],
                "artist": t["artist"],
                "id": t["id"],
                "region": "",
                "status": "saved",
                "time": 0,  # imported, no real timestamp
            })
            history_ids.add(t["id"])
            added_history += 1

    with open(history_path, "w") as f:
        json.dump(history, f)

    print(f"  {added_history} tracks added to history as 'saved'")


def show_stats(saved_tracks, top_artists, audio_features):
    """Show your listening profile — the map."""
    print("\n📊 YOUR MUSIC MAP\n")

    # Genre map
    genre_count = Counter()
    for a in top_artists:
        for g in a["genres"]:
            genre_count[g] += 1

    if genre_count:
        print("GENRES (top 20):")
        for genre, count in genre_count.most_common(20):
            bar = "█" * count
            print(f"  {genre:30s} {bar} ({count})")

    # Audio features averages
    if audio_features:
        features = list(audio_features.values())
        dims = ["energy", "danceability", "valence", "acousticness", "instrumentalness"]
        print("\nDIMENSIONS (0-1 scale):")
        for dim in dims:
            vals = [f[dim] for f in features if dim in f]
            if vals:
                avg = sum(vals) / len(vals)
                bar = "█" * int(avg * 20) + "░" * (20 - int(avg * 20))
                print(f"  {dim:20s} {bar} {avg:.2f}")

        tempos = [f["tempo"] for f in features if "tempo" in f]
        if tempos:
            avg_tempo = sum(tempos) / len(tempos)
            print(f"  {'tempo':20s} avg {avg_tempo:.0f} BPM (range: {min(tempos):.0f}-{max(tempos):.0f})")

    # Explored vs unexplored
    explored_genres = set(genre_count.keys())
    print(f"\n✓ You've explored {len(explored_genres)} genres")
    print(f"✓ {len(saved_tracks)} saved tracks")
    print(f"✓ {len(top_artists)} top artists")


if __name__ == "__main__":
    args = sys.argv[1:]

    if not os.environ.get("SPOTIPY_CLIENT_ID"):
        print("Missing Spotify credentials.")
        print(f"Create {ENV_PATH} with:")
        print("  SPOTIPY_CLIENT_ID=your_id")
        print("  SPOTIPY_CLIENT_SECRET=your_secret")
        print("  SPOTIPY_REDIRECT_URI=http://127.0.0.1:8888/callback")
        sys.exit(1)

    sp = get_spotify()
    print("\n🎵 SPOTIFY IMPORT\n")

    # Fetch core data
    saved = fetch_saved_tracks(sp)
    top_artists = fetch_top_artists(sp)
    top_tracks = fetch_top_tracks(sp)

    # Get audio features for all unique tracks
    all_ids = list({t["id"] for t in saved if t.get("id")} | {t["id"] for t in top_tracks if t.get("id")})
    features = fetch_audio_features(sp, all_ids)

    # Fetch playlists if --full
    playlists = []
    if "--full" in args:
        playlists = fetch_playlists(sp)

    # Save raw data
    raw = {
        "saved_tracks": saved,
        "top_artists": top_artists,
        "top_tracks": top_tracks,
        "audio_features": features,
        "playlists": playlists,
    }
    with open(SPOTIFY_CACHE, "w") as f:
        json.dump(raw, f, indent=2)
    print(f"\n✓ Raw data saved to {SPOTIFY_CACHE}")

    if "--stats" in args:
        show_stats(saved, top_artists, features)
    else:
        show_stats(saved, top_artists, features)
        import_to_ledger(saved, top_artists, top_tracks)
