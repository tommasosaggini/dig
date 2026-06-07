#!/usr/bin/env python3
"""DIG — build a one-shot Spotify playlist for offline listening.

Server-side port of the frontend's stratified coverage-gap explorer:
filters out anything the user has heard, drops same-artist clusters, and
weighted-random samples by genre/country coverage gap so picks lean
hard toward unexplored cells. Then creates a Spotify playlist and POSTs
the picks in batches of 100. The user downloads the playlist in Spotify
mobile for offline play.

Usage:
  scripts/build_flight_playlist.py --user 1199795449 --size 500 \\
      --name "DIG · Flight"
"""

import argparse
import json
import math
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

if os.path.exists(os.path.join(ROOT, ".env")):
    with open(os.path.join(ROOT, ".env")) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from lib.db import fetchall, fetchone


def _all_artists(s: str) -> list[str]:
    if not s:
        return []
    return [a.strip().lower() for a in s.split(",") if a.strip()]


def get_user_coverage(user_id: str) -> dict:
    genre_rows = fetchall(
        """
        SELECT g, count(*)::int AS plays
        FROM user_history h JOIN tracks t ON t.id = h.track_id
        CROSS JOIN LATERAL unnest(t.genres) AS g
        WHERE h.user_id = %s GROUP BY g
        """, (user_id,))
    country_rows = fetchall(
        """
        SELECT COALESCE(t.origin_region, t.region) AS c, count(*)::int AS plays
        FROM user_history h JOIN tracks t ON t.id = h.track_id
        WHERE h.user_id = %s AND COALESCE(t.origin_region, t.region) IS NOT NULL
        GROUP BY COALESCE(t.origin_region, t.region)
        """, (user_id,))
    return {
        "genres":    {r["g"]: r["plays"] for r in genre_rows},
        "countries": {r["c"]: r["plays"] for r in country_rows},
    }


def get_heard_set(user_id: str) -> set[str]:
    rows = fetchall(
        "SELECT track_id FROM user_history WHERE user_id = %s",
        (user_id,))
    return {r["track_id"] for r in rows if r["track_id"]}


def get_pool() -> list[dict]:
    """Every track in the catalogue with the fields the picker needs."""
    return fetchall(
        """
        SELECT id, name, artist, region, origin_region, genres
        FROM tracks
        WHERE id IS NOT NULL AND id NOT LIKE 'yt:%'
        """)


def stratified_pick(pool: list[dict], heard: set[str], coverage: dict,
                    size: int, recent_n: int = 6) -> list[dict]:
    """Iterative stratified explorer. Each iteration:
       - Stage 1 anti-cluster filter (no artist overlap with last N picks;
         no country/genre overlap unless rare).
       - Stage 2 weighted-random by coverage gap.
       - Append pick, push to "recent" set, remove from eligible.
    """
    eligible = [t for t in pool if t.get("id") and t["id"] not in heard]
    by_id = {t["id"]: t for t in eligible}
    chosen = []
    recent_artists: list[str] = []
    recent_countries: list[str] = []
    recent_top_genres: list[str] = []

    # Coverage tallies — start from server-side counts, increment as we pick.
    g_count = dict(coverage["genres"])
    c_count = dict(coverage["countries"])

    eligible_ids = set(by_id.keys())
    print(f"  pool eligible: {len(eligible_ids)} tracks (excluding {len(heard)} heard)")

    while len(chosen) < size and eligible_ids:
        # STAGE 1 — anti-cluster filter
        recent_artist_set = set(recent_artists[-recent_n * 4:])  # all artists in recent N picks
        recent_country_set = set(recent_countries[-recent_n:])
        recent_top_genre_set = set(recent_top_genres[-recent_n:])

        filtered_ids = []
        for tid in eligible_ids:
            t = by_id[tid]
            artists = _all_artists(t.get("artist") or "")
            if any(a in recent_artist_set for a in artists):
                continue
            country = t.get("origin_region") or t.get("region")
            if country and country != "Unknown" and country in recent_country_set:
                if (c_count.get(country, 0) > 20):
                    continue
            top_genre = (t.get("genres") or [None])[0]
            if top_genre and top_genre in recent_top_genre_set:
                if (g_count.get(top_genre, 0) > 20):
                    continue
            filtered_ids.append(tid)

        # If filter is too aggressive (small remaining pool), fall back to all eligible
        candidate_ids = filtered_ids if len(filtered_ids) >= 50 else list(eligible_ids)

        # STAGE 2 — weighted random by coverage gap
        SAMPLE_N = min(2000, len(candidate_ids))
        sample_ids = random.sample(candidate_ids, SAMPLE_N) if len(candidate_ids) > SAMPLE_N else candidate_ids

        weights = []
        for tid in sample_ids:
            t = by_id[tid]
            min_g_plays = math.inf
            for g in (t.get("genres") or []):
                p = g_count.get(g, 0)
                if p < min_g_plays:
                    min_g_plays = p
            if not math.isfinite(min_g_plays):
                min_g_plays = 50
            country = t.get("origin_region") or t.get("region")
            if not country or country == "Unknown":
                country_plays = 200
            else:
                country_plays = c_count.get(country, 0)
            genre_gap = 1.0 / (1 + min_g_plays)
            country_gap = 1.0 / (1 + country_plays)
            w = (genre_gap * country_gap) ** 0.6
            weights.append(w)

        total = sum(weights)
        if total <= 0:
            picked_id = random.choice(sample_ids)
        else:
            r = random.random() * total
            picked_id = sample_ids[0]
            for i, tid in enumerate(sample_ids):
                r -= weights[i]
                if r <= 0:
                    picked_id = tid
                    break

        picked = by_id[picked_id]
        chosen.append(picked)
        eligible_ids.discard(picked_id)

        # Update recents + tallies
        for a in _all_artists(picked.get("artist") or ""):
            recent_artists.append(a)
        country = picked.get("origin_region") or picked.get("region") or ""
        recent_countries.append(country)
        recent_top_genres.append((picked.get("genres") or [None])[0] or "")
        if country:
            c_count[country] = c_count.get(country, 0) + 1
        for g in (picked.get("genres") or []):
            g_count[g] = g_count.get(g, 0) + 1

        if len(chosen) % 50 == 0:
            print(f"  picked {len(chosen)}/{size} so far")

    return chosen


def get_token(user_id: str) -> str:
    """Fetch + refresh-if-needed access token for the user."""
    import spotipy
    from spotipy.oauth2 import SpotifyOAuth

    row = fetchone("SELECT token_data FROM user_tokens WHERE user_id = %s", (user_id,))
    if not row:
        raise RuntimeError(f"No token for user {user_id}")
    token_info = row["token_data"]
    if isinstance(token_info, str):
        token_info = json.loads(token_info)

    sp_oauth = SpotifyOAuth(
        client_id=os.environ["SPOTIPY_CLIENT_ID"],
        client_secret=os.environ["SPOTIPY_CLIENT_SECRET"],
        redirect_uri=os.environ["SPOTIPY_REDIRECT_URI"],
        scope=token_info.get("scope") or "playlist-modify-private",
    )
    if sp_oauth.is_token_expired(token_info):
        new_token = sp_oauth.refresh_access_token(token_info["refresh_token"])
        if not new_token.get("scope"):
            new_token["scope"] = token_info.get("scope") or ""
        # Persist
        from lib.db import get_conn
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE user_tokens SET token_data = %s::JSONB, updated_at = NOW() "
                    "WHERE user_id = %s", (json.dumps(new_token), user_id))
            conn.commit()
        finally:
            conn.close()
        token_info = new_token

    scope = token_info.get("scope") or ""
    if "playlist-modify-private" not in scope.split() and "playlist-modify-public" not in scope.split():
        raise RuntimeError(
            f"Token lacks playlist-modify-private. Current scope: {scope}\n"
            f"User must hit /login to re-grant scope.")

    return token_info["access_token"]


def create_playlist(token: str, user_id: str, name: str, description: str) -> dict:
    """Create a private playlist. Spotify accepts both /v1/me/playlists
    (no user_id needed) and /v1/users/{id}/playlists; we try /me first
    because it's the modern shape and avoids any dev-mode allowlist
    quirks tied to the user-id path. Falls back if /me 4xx's."""
    body = json.dumps({
        "name": name,
        "description": description,
        "public": False,
    }).encode()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    for url in (
        "https://api.spotify.com/v1/me/playlists",
        f"https://api.spotify.com/v1/users/{urllib.parse.quote(user_id)}/playlists",
    ):
        req = urllib.request.Request(url, data=body, method="POST", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                print(f"  created via {url}")
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as exc:
            try:
                err_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = "<no body>"
            print(f"  {url} → {exc.code}: {err_body[:600]}")
    raise RuntimeError("Both playlist-create endpoints returned errors. See above.")


def add_tracks(token: str, playlist_id: str, track_ids: list[str]) -> int:
    """Add tracks in batches of 100 via spotipy (handles auth + edge
    cases more reliably than raw urllib for this endpoint)."""
    import spotipy
    sp = spotipy.Spotify(auth=token)
    # Brief settle for newly-created playlist
    time.sleep(1.0)
    added = 0
    for i in range(0, len(track_ids), 100):
        batch = track_ids[i:i + 100]
        try:
            sp.playlist_add_items(playlist_id, batch)
        except spotipy.SpotifyException as exc:
            print(f"  add-tracks batch {i}-{i+len(batch)} → {exc.http_status}: {exc.msg[:300]}")
            raise
        added += len(batch)
        print(f"  posted {added}/{len(track_ids)}")
        time.sleep(0.5)
    return added


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--user", required=True, help="Spotify user_id")
    p.add_argument("--size", type=int, default=500)
    p.add_argument("--name", default="DIG · Flight")
    p.add_argument("--description",
                   default="500 exploratory picks from DIG — coverage-gap weighted, anti-cluster filtered. Mark for offline in Spotify.")
    args = p.parse_args()

    print(f"Building flight playlist for user={args.user} size={args.size}")
    token = get_token(args.user)
    print("  got token with playlist-modify scope")

    print("Loading pool + history + coverage…")
    pool = get_pool()
    heard = get_heard_set(args.user)
    coverage = get_user_coverage(args.user)
    print(f"  pool: {len(pool)} tracks, heard: {len(heard)}, "
          f"genres covered: {len(coverage['genres'])}, "
          f"countries covered: {len(coverage['countries'])}")

    print(f"Picking {args.size} tracks via stratified explorer…")
    t0 = time.time()
    picks = stratified_pick(pool, heard, coverage, args.size)
    print(f"  picked {len(picks)} in {time.time() - t0:.1f}s")

    if len(picks) < args.size:
        print(f"  WARNING: only got {len(picks)} (pool exhausted).")

    print(f"Creating Spotify playlist: {args.name!r}")
    playlist = create_playlist(token, args.user, args.name, args.description)
    print(f"  created: {playlist['external_urls']['spotify']}")

    print("Adding tracks…")
    add_tracks(token, playlist["id"], [p["id"] for p in picks])

    # Brief stats so we can verify the explorer worked
    by_country = {}
    by_genre = {}
    for t in picks:
        c = t.get("origin_region") or t.get("region") or "(none)"
        by_country[c] = by_country.get(c, 0) + 1
        for g in (t.get("genres") or []):
            by_genre[g] = by_genre.get(g, 0) + 1
    top_c = sorted(by_country.items(), key=lambda x: -x[1])[:10]
    top_g = sorted(by_genre.items(), key=lambda x: -x[1])[:10]
    print(f"\n  top countries in selection: {top_c}")
    print(f"  top genres in selection: {top_g}")
    print(f"  unique countries: {len(by_country)}, unique genres: {len(by_genre)}")

    print(f"\nDone. Playlist URL: {playlist['external_urls']['spotify']}")
    print("Open in Spotify mobile → Download / Make available offline.")


if __name__ == "__main__":
    main()
