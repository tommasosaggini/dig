#!/usr/bin/env python3
"""
DIG — Import user's Spotify Liked Songs into the pool.

Fetches all tracks from the user's Liked Songs playlist, ingests those not
already in the pool, registers their artists, and marks all as 'saved' in
user_history. Also seeds unknown artists into the artists table for the
deep crawler to expand from.

Usage:  python3 scripts/import_likes.py [--dry-run]
"""
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

ENV_PATH = os.path.join(ROOT, ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from lib.db import get_conn, fetchone, fetchall
from lib.track_filter import is_trash


def get_user_spotify(user_id: str):
    """Get an authenticated Spotify client for the given user_id."""
    row = fetchone("SELECT token_data FROM user_tokens WHERE user_id = %s", (user_id,))
    if not row:
        raise RuntimeError(f"No token found for user {user_id}")
    token_info = row["token_data"]
    if isinstance(token_info, str):
        token_info = json.loads(token_info)
    # CRITICAL: pass the token's existing scope, NOT a hardcoded narrow one.
    # Spotipy stamps the refreshed token's scope from the constructor when the
    # refresh response omits one, so a hardcoded narrow scope ("user-library-
    # read") will silently overwrite a full-scope token on first refresh and
    # break /token validation app-wide. We only need the token's existing
    # scope here — we're just reading saved tracks.
    original_scope = token_info.get("scope") or "user-library-read"
    sp_oauth = SpotifyOAuth(
        client_id=os.environ.get("SPOTIPY_CLIENT_ID"),
        client_secret=os.environ.get("SPOTIPY_CLIENT_SECRET"),
        redirect_uri=os.environ.get("SPOTIPY_REDIRECT_URI"),
        scope=original_scope,
    )
    if sp_oauth.is_token_expired(token_info):
        new_token = sp_oauth.refresh_access_token(token_info["refresh_token"])
        # Belt-and-braces: never let the persisted scope be narrower than what
        # was already on disk. If the response somehow returned a narrower
        # scope, keep the original.
        if not new_token.get("scope") or len(new_token["scope"].split()) < len(original_scope.split()):
            new_token["scope"] = original_scope
        token_info = new_token
        from lib.db import get_conn as _gc
        conn = _gc()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE user_tokens SET token_data = %s::JSONB, updated_at = NOW() WHERE user_id = %s",
                    (json.dumps(token_info), user_id),
                )
            conn.commit()
        finally:
            conn.close()
    from lib.spotify_gate import make_client
    return make_client(auth=token_info["access_token"])  # gated (lib/spotify_gate.py)


def import_likes_for_user(user_id: str, dry_run: bool = False):
    """Pull all Liked Songs for a user and reflect them into DIG's tables.

    Idempotent — only inserts new tracks/artists/saves; updates ledger 'liked'.
    Returns a summary dict (added, registered, saved).
    """
    sp = get_user_spotify(user_id)

    # Fetch all Liked Songs
    print(f"[{user_id}] Fetching Liked Songs from Spotify...")
    liked = []
    offset = 0
    while True:
        results = sp.current_user_saved_tracks(limit=50, offset=offset)
        items = results.get("items", [])
        if not items:
            break
        for item in items:
            t = item.get("track")
            if not t or not t.get("id"):
                continue
            artists = t.get("artists", [])
            release = (t.get("album") or {}).get("release_date", "")
            year = release[:4] if len(release) >= 4 else ""
            liked.append({
                "id": t["id"],
                "name": t.get("name", ""),
                "artist": ", ".join(a["name"] for a in artists),
                "artist_ids": [a["id"] for a in artists if a.get("id")],
                "album": (t.get("album") or {}).get("name", ""),
                "popularity": t.get("popularity", 0),
                "year": year,
                "decade": (year[:3] + "0s") if year else "",
            })
        offset += len(items)
        if not results.get("next"):
            break
    print(f"[{user_id}] Total Liked Songs: {len(liked)}")

    # Check what's already in pool
    existing_ids = set(r["id"] for r in fetchall("SELECT id FROM tracks"))
    existing_artist_ids = set(r["spotify_id"] for r in fetchall(
        "SELECT spotify_id FROM artists WHERE spotify_id IS NOT NULL"))
    existing_saves = set(r["track_id"] for r in fetchall(
        "SELECT track_id FROM user_history WHERE user_id = %s AND status = 'saved'",
        (user_id,)))

    need_pool = [l for l in liked if l["id"] not in existing_ids]
    need_save = [l for l in liked if l["id"] not in existing_saves]
    need_artists = set()
    for l in liked:
        for aid in l["artist_ids"]:
            if aid not in existing_artist_ids:
                need_artists.add(aid)

    # Filter junk
    junk = 0
    clean_pool = []
    for l in need_pool:
        if is_trash(l["name"], l["artist"], l["album"]):
            junk += 1
            continue
        clean_pool.append(l)

    print(f"[{user_id}]   Need to add to pool: {len(clean_pool)} (filtered {junk} junk)")
    print(f"[{user_id}]   Need save status: {len(need_save)}")
    print(f"[{user_id}]   New artists to register: {len(need_artists)}")

    if dry_run:
        print(f"[{user_id}] --dry-run. Pass without flag to apply.")
        return {"liked": len(liked), "added": 0, "registered": 0, "saved": 0}

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 1. Insert tracks into pool
            added = 0
            for t in clean_pool:
                cur.execute(
                    """
                    INSERT INTO tracks (id, name, artist, artist_ids, album, popularity,
                                        source, region, decade, year, query)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (t["id"], t["name"], t["artist"], t["artist_ids"],
                     t["album"], t["popularity"], "spotify", "",
                     t["decade"], t["year"], "import:liked"),
                )
                added += 1

            # 2. Register unknown artists
            registered = 0
            for l in liked:
                for i, aid in enumerate(l["artist_ids"]):
                    if aid in existing_artist_ids:
                        continue
                    parts = l["artist"].split(",")
                    name = parts[i].strip() if i < len(parts) else parts[0].strip()
                    cur.execute(
                        """
                        INSERT INTO artists (slug, name, spotify_id, sources, explore_depth, discovered_via)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (name.lower().replace(" ", "-")[:100], name, aid,
                         ["spotify"], 0, "import:liked"),
                    )
                    existing_artist_ids.add(aid)
                    registered += 1

            # 3. Mark all as saved in user_history
            saved = 0
            now_ms = int(time.time() * 1000)
            for l in need_save:
                cur.execute(
                    """
                    INSERT INTO user_history (user_id, track_id, track_name, artist,
                                              region, status, listened_at)
                    VALUES (%s, %s, %s, %s, %s, 'saved', %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (user_id, l["id"], l["name"], l["artist"], "", now_ms),
                )
                saved += 1

            # 4. Mark in user_ledger as liked
            for l in liked:
                key = f"{l['artist']} - {l['name']}".lower()
                cur.execute(
                    """
                    INSERT INTO user_ledger (user_id, track_key, status)
                    VALUES (%s, %s, 'liked')
                    ON CONFLICT (user_id, track_key) DO UPDATE SET status = 'liked'
                    """,
                    (user_id, key),
                )

        conn.commit()
        print(f"[{user_id}] DONE — pool +{added}, artists +{registered}, saves +{saved}")
        return {"liked": len(liked), "added": added, "registered": registered, "saved": saved}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def import_likes_for_all_users():
    """Run import_likes_for_user against every user with a stored token.
    Used by the periodic cron sync."""
    rows = fetchall("SELECT user_id FROM user_tokens")
    print(f"Syncing likes for {len(rows)} users")
    for r in rows:
        try:
            import_likes_for_user(r["user_id"])
        except Exception as e:
            print(f"[{r['user_id']}] FAILED: {e!r}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", help="Spotify user_id to import for")
    parser.add_argument("--all-users", action="store_true",
                        help="Run for every user with a stored token (cron mode)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.all_users:
        import_likes_for_all_users()
    elif args.user:
        import_likes_for_user(args.user, dry_run=args.dry_run)
    else:
        parser.error("provide --user <id> or --all-users")


if __name__ == "__main__":
    main()
