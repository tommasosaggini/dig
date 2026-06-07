#!/usr/bin/env python3
"""DIG — Bulk-mark every track in a Spotify playlist as `listened` in
the user's history. Used after offline trips (flights, no-signal stays)
where the user listened to a DIG-built playlist via Spotify mobile and
wants the encounter recorded in DIG's ledger.

Idempotent: tracks already in user_history get their listened_at refreshed
to "now" and status promoted to 'listened' if they were a softer status,
but never demoted (a 'saved' or 'disliked' status survives).

Usage:
  scripts/mark_playlist_listened.py --user 1199795449 \\
      --playlist 6GDzbJPTDfxt71a24hl9Jh
  # also accepts the full open.spotify.com URL via --playlist
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()

from lib.db import fetchall, fetchone, get_conn


def _parse_playlist_id(s: str) -> str:
    """Accept raw id or full open.spotify.com URL."""
    m = re.search(r"playlist/([0-9A-Za-z]+)", s)
    return m.group(1) if m else s


def get_token(user_id: str) -> str:
    """Reuse build_flight_playlist's token-fetch + refresh approach."""
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
        scope=token_info.get("scope") or "playlist-read-private",
    )
    if sp_oauth.is_token_expired(token_info):
        new_token = sp_oauth.refresh_access_token(token_info["refresh_token"])
        if not new_token.get("scope"):
            new_token["scope"] = token_info.get("scope") or ""
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
    return token_info["access_token"]


def fetch_playlist_tracks(token: str, playlist_id: str) -> list[dict]:
    """Pull every track from a playlist. Pages of 100 via `next` cursor."""
    out = []
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks?limit=100"
    while url:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=15) as r:
            page = json.loads(r.read().decode())
        for it in page.get("items", []):
            t = it.get("track")
            if not t or not t.get("id"):
                continue
            out.append({
                "id": t["id"],
                "name": t.get("name") or "",
                "artist": ", ".join(a.get("name", "") for a in (t.get("artists") or [])),
            })
        url = page.get("next")
    return out


def mark_listened(user_id: str, tracks: list[dict], dry_run: bool = False) -> dict:
    """Insert / promote tracks in user_history with status='listened'.

    For each track:
      - If no row exists for (user_id, track_id): insert as listened.
      - If a row exists with status in ('listened', 'skipped'): bump
        listened_at to now and set status='listened'.
      - If status is 'saved' or 'disliked': leave alone (don't downgrade
        an explicit signal).
    """
    inserted = 0
    promoted = 0
    untouched = 0
    now_ms = int(time.time() * 1000)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for t in tracks:
                cur.execute(
                    "SELECT status FROM user_history WHERE user_id = %s AND track_id = %s LIMIT 1",
                    (user_id, t["id"]))
                row = cur.fetchone()
                if not row:
                    if not dry_run:
                        cur.execute(
                            """
                            INSERT INTO user_history
                              (user_id, track_id, track_name, artist, region, status, listened_at, mode)
                            VALUES (%s, %s, %s, %s, %s, 'listened', %s, 'flight')
                            """,
                            (user_id, t["id"], t["name"], t["artist"], "", now_ms))
                    inserted += 1
                elif row[0] in ("saved", "disliked"):
                    untouched += 1
                else:
                    if not dry_run:
                        cur.execute(
                            """
                            UPDATE user_history
                            SET status = 'listened', listened_at = %s
                            WHERE user_id = %s AND track_id = %s
                            """, (now_ms, user_id, t["id"]))
                    promoted += 1
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return {"inserted": inserted, "promoted": promoted, "untouched": untouched}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--user", required=True)
    p.add_argument("--playlist", required=True,
                   help="Spotify playlist ID or open.spotify.com URL")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    pid = _parse_playlist_id(args.playlist)
    print(f"Marking playlist {pid} as listened for user {args.user}…")
    token = get_token(args.user)
    tracks = fetch_playlist_tracks(token, pid)
    print(f"  fetched {len(tracks)} tracks from Spotify playlist")

    result = mark_listened(args.user, tracks, dry_run=args.dry_run)
    label = "DRY-RUN " if args.dry_run else ""
    print(f"\n{label}Done.")
    print(f"  inserted (new listen):      {result['inserted']}")
    print(f"  promoted (was skipped):     {result['promoted']}")
    print(f"  untouched (saved/disliked): {result['untouched']}")
    print(f"  total tracks in playlist:   {len(tracks)}")


if __name__ == "__main__":
    main()
