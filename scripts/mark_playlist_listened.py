#!/usr/bin/env python3
"""DIG — Record that a Spotify playlist crossed the user's path. Used after
offline trips (flights, no-signal stays) where the user played a DIG-built
playlist via Spotify mobile and wants the encounter in DIG's ledger.

IT WRITES 'served', NOT 'listened'
----------------------------------
It used to write 'listened' for every track in the playlist, which is how 472
rows came to claim a full listen without a single one of them ever having been
measured — a third of every unmeasured 'listened' row in the table on
2026-08-18. Nobody was there to see whether the listener heard track 40, or
fell asleep at track 6, and a playlist being queued is not evidence about any
individual track in it. 'served' is the status that says exactly that.

--listened is still there for when the claim is genuinely "I heard all of
these", because sometimes it is. It is a flag rather than the default so that
asserting a listen is a thing someone decides to do.

Never overwrites evidence. A row already sitting at 'listened' or 'skipped' was
written by something that actually watched the playback; a bulk assertion made
after the fact does not get to overrule it. 'saved' and 'disliked' likewise.

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

if os.path.exists(os.path.join(ROOT, ".env")):
    with open(os.path.join(ROOT, ".env")) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

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


def mark_listened(user_id: str, tracks: list[dict], dry_run: bool = False,
                  status: str = "served") -> dict:
    """Insert / promote tracks in user_history.

    For each track:
      - If no row exists for (user_id, track_id): insert with `status`.
      - If a row exists at 'served': bump listened_at to now and set `status`.
      - Otherwise ('listened', 'skipped', 'saved', 'disliked'): leave alone.
        Each of those was written by something that watched the playback or by
        the listener themselves; this script watched nothing.
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
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 'flight')
                            """,
                            (user_id, t["id"], t["name"], t["artist"], "",
                             status, now_ms))
                    inserted += 1
                elif row[0] != "served":
                    untouched += 1
                else:
                    if not dry_run:
                        cur.execute(
                            """
                            UPDATE user_history
                            SET status = %s, listened_at = %s
                            WHERE user_id = %s AND track_id = %s
                            """, (status, now_ms, user_id, t["id"]))
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
    p.add_argument("--listened", action="store_true",
                   help="claim a real listen for every track, not just that "
                        "the playlist was served. Only pass this if you "
                        "actually heard them.")
    args = p.parse_args()

    status = "listened" if args.listened else "served"
    pid = _parse_playlist_id(args.playlist)
    print(f"Marking playlist {pid} as '{status}' for user {args.user}…")
    token = get_token(args.user)
    tracks = fetch_playlist_tracks(token, pid)
    print(f"  fetched {len(tracks)} tracks from Spotify playlist")

    result = mark_listened(args.user, tracks, dry_run=args.dry_run, status=status)
    label = "DRY-RUN " if args.dry_run else ""
    print(f"\n{label}Done.")
    print(f"  inserted (new row):            {result['inserted']}")
    print(f"  promoted (was served):         {result['promoted']}")
    print(f"  untouched (already evidence):  {result['untouched']}")
    print(f"  total tracks in playlist:      {len(tracks)}")


if __name__ == "__main__":
    main()
