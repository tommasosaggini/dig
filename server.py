#!/usr/bin/env python3
"""
DIG — multi-tenant server with Spotify OAuth.
Each user signs in with Spotify, gets their own history/ledger.
Discovery pool and catalog are shared (PostgreSQL-backed).
"""

import http.server
import hashlib
import hmac
import json
import os
import secrets
import sys
import time
import traceback
import urllib.parse
import urllib.request
import urllib.error

DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(DIR, ".env")

# Load .env before anything else
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

if DIR not in sys.path:
    sys.path.insert(0, DIR)

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import CacheHandler

from lib.db import get_conn, fetchone, fetchall
from lib.discovery_lock import load_discovery
from lib.ai_recommend import ai_recommend, ai_recommend_v2, journey_recommend
from lib.explore import coverage_explore

# ── In-process telemetry ring buffer ──────────────────────────────────────────
# Keeps the last N events for /api/health to surface during testing. Cheap.
_HEALTH = {
    "started_at": None,        # set on startup
    "ai_calls": [],            # list of last 30 AI Mix call summaries
    "history_writes": 0,
    "errors": [],              # last 20 errors (with timestamp)
}
_HEALTH_AI_LIMIT = 30
_HEALTH_ERR_LIMIT = 20

def _health_record_ai(event):
    _HEALTH["ai_calls"].append(event)
    if len(_HEALTH["ai_calls"]) > _HEALTH_AI_LIMIT:
        _HEALTH["ai_calls"] = _HEALTH["ai_calls"][-_HEALTH_AI_LIMIT:]

def _health_record_error(msg):
    import datetime
    _HEALTH["errors"].append({"ts": datetime.datetime.utcnow().isoformat() + "Z", "msg": str(msg)[:300]})
    if len(_HEALTH["errors"]) > _HEALTH_ERR_LIMIT:
        _HEALTH["errors"] = _HEALTH["errors"][-_HEALTH_ERR_LIMIT:]

CLIENT_ID     = os.environ.get("SPOTIPY_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("SPOTIPY_CLIENT_SECRET", "")
REDIRECT_URI  = os.environ.get("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8000/callback")
SCOPE = (
    "streaming user-read-email user-read-private user-library-read "
    "user-top-read user-read-recently-played user-read-playback-state "
    "user-modify-playback-state playlist-read-private playlist-read-collaborative"
)

# Cookie secret — loaded from DB or .cookie_secret file, generated once if missing
COOKIE_SECRET = os.environ.get("COOKIE_SECRET", "")
if not COOKIE_SECRET:
    secret_path = os.path.join(DIR, ".cookie_secret")
    if os.path.exists(secret_path):
        with open(secret_path) as f:
            COOKIE_SECRET = f.read().strip()
    else:
        COOKIE_SECRET = secrets.token_hex(32)
        with open(secret_path, "w") as f:
            f.write(COOKIE_SECRET)

WEB_DIR = os.path.join(DIR, "web")

# JSON data files the app fetches as static assets (served from project root)
_DATA_FILES = {
    "data.json", "genre_map.json",
    "track_map.json", "catalog.json", "discovery_youtube.json",
}


# ── Spotify token cache stored in PostgreSQL ──────────────────────────────────

class NoCacheHandler(CacheHandler):
    """No-op cache handler — used during the OAuth code exchange so Spotipy
    doesn't try to persist a token before we know the real user ID."""

    def get_cached_token(self):
        return None

    def save_token_to_cache(self, token_info):
        pass


class DbCacheHandler(CacheHandler):
    """spotipy CacheHandler that persists tokens in the user_tokens table."""

    def __init__(self, user_id):
        self.user_id = user_id

    def get_cached_token(self):
        row = fetchone(
            "SELECT token_data FROM user_tokens WHERE user_id = %s",
            (self.user_id,),
        )
        return row["token_data"] if row else None

    def save_token_to_cache(self, token_info):
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_tokens (user_id, token_data, updated_at)
                    VALUES (%s, %s::JSONB, NOW())
                    ON CONFLICT (user_id) DO UPDATE SET
                        token_data = EXCLUDED.token_data,
                        updated_at = NOW()
                    """,
                    (self.user_id, json.dumps(token_info)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# ── Auth helpers ──────────────────────────────────────────────────────────────

def make_sp_oauth(user_id=None):
    handler = DbCacheHandler(user_id) if user_id else None
    return SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        cache_handler=handler,
        open_browser=False,
    )


def sign_cookie(user_id):
    sig = hmac.new(COOKIE_SECRET.encode(), user_id.encode(), hashlib.sha256).hexdigest()[:16]
    return f"{user_id}:{sig}"


def verify_cookie(cookie_val):
    if not cookie_val or ":" not in cookie_val:
        return None
    user_id, sig = cookie_val.rsplit(":", 1)
    expected = hmac.new(COOKIE_SECRET.encode(), user_id.encode(), hashlib.sha256).hexdigest()[:16]
    if hmac.compare_digest(sig, expected):
        return user_id
    return None


# ── User DB helpers ───────────────────────────────────────────────────────────

def db_upsert_user(uid, display_name, email, image):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, display_name, email, image_url)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    email        = EXCLUDED.email,
                    image_url    = EXCLUDED.image_url
                """,
                (uid, display_name, email, image),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def db_get_profile(user_id):
    return fetchone("SELECT id, display_name, email, image_url FROM users WHERE id = %s", (user_id,))


def db_get_ledger(user_id):
    rows = fetchall(
        "SELECT track_key, status, vibe, reason FROM user_ledger WHERE user_id = %s",
        (user_id,),
    )
    ledger = {"known": [], "liked": [], "disliked": []}
    for r in rows:
        if r["status"] == "known":
            ledger["known"].append(r["track_key"])
        elif r["status"] == "liked":
            entry = {"track": r["track_key"]}
            if r.get("vibe"):
                entry["vibe"] = list(r["vibe"])
            ledger["liked"].append(entry)
        elif r["status"] == "disliked":
            entry = {"track": r["track_key"]}
            if r.get("reason"):
                entry["reason"] = r["reason"]
            ledger["disliked"].append(entry)
    return ledger


def db_add_known(user_id, track_key):
    """Mark a track as known (idempotent)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_ledger (user_id, track_key, status)
                VALUES (%s, %s, 'known')
                ON CONFLICT (user_id, track_key) DO NOTHING
                """,
                (user_id, track_key.lower()),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def db_unsave(user_id, track_key):
    """Remove the 'liked' status for a track and demote to 'known' so it stays
    in the dedup set but no longer counts as a positive taste signal.

    Idempotent. Also removes the matching saved entries from user_history so
    AI Mix and the heard-keys filter no longer see this as a save."""
    key = (track_key or "").lower().strip()
    if not key:
        return
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Demote ledger row from 'liked' → 'known' (or insert known if missing).
            cur.execute(
                """
                INSERT INTO user_ledger (user_id, track_key, status)
                VALUES (%s, %s, 'known')
                ON CONFLICT (user_id, track_key) DO UPDATE SET status = 'known'
                WHERE user_ledger.status = 'liked'
                """,
                (user_id, key),
            )
            # Demote any saved history entries for this track to 'listened'.
            # Match on lowercased "artist - track_name" since that's how the key is built.
            cur.execute(
                """
                UPDATE user_history
                SET status = 'listened'
                WHERE user_id = %s
                  AND status = 'saved'
                  AND LOWER(artist || ' - ' || track_name) = %s
                """,
                (user_id, key),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def db_get_history(user_id):
    rows = fetchall(
        """
        SELECT track_id AS id, track_name AS track, artist, region, status,
               listened_at AS time, played_pct, mode
        FROM user_history WHERE user_id = %s ORDER BY listened_at DESC
        """,
        (user_id,),
    )
    return [dict(r) for r in rows]


def db_save_history(user_id, history_list):
    """Replace a user's full history (called from POST /history).

    Also mirrors any `disliked` items into `user_ledger` so a persistent
    track-level dislike flag survives history pruning. Track-level only —
    never generalizes to genre/region.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_history WHERE user_id = %s", (user_id,))
            for item in history_list:
                cur.execute(
                    """
                    INSERT INTO user_history
                        (user_id, track_id, track_name, artist, region, status,
                         listened_at, played_pct, mode)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        item.get("id"),
                        item.get("track"),
                        item.get("artist"),
                        item.get("region"),
                        item.get("status"),
                        item.get("time"),
                        item.get("played_pct"),
                        item.get("mode"),
                    ),
                )
                if item.get("status") == "disliked":
                    artist = (item.get("artist") or "").strip()
                    track = (item.get("track") or "").strip()
                    if artist and track:
                        key = f"{artist} - {track}".lower()
                        cur.execute(
                            """
                            INSERT INTO user_ledger (user_id, track_key, status)
                            VALUES (%s, %s, 'disliked')
                            ON CONFLICT (user_id, track_key)
                            DO UPDATE SET status = 'disliked'
                            """,
                            (user_id, key),
                        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── HTTP Handler ──────────────────────────────────────────────────────────────

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def get_user(self):
        cookies = self.headers.get("Cookie", "")
        for part in cookies.split(";"):
            part = part.strip()
            if part.startswith("dig_session="):
                return verify_cookie(part[len("dig_session="):])
        return None

    def set_session_cookie(self, user_id):
        val = sign_cookie(user_id)
        self.send_header(
            "Set-Cookie",
            f"dig_session={val}; Path=/; HttpOnly; SameSite=Lax; Max-Age=31536000",
        )

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        user_id = self.get_user()

        # ── Auth flow ─────────────────────────────────────────────────────────

        if parsed.path == "/login":
            auth_url = make_sp_oauth().get_authorize_url()
            self.send_response(302)
            self.send_header("Location", auth_url)
            self.end_headers()
            return

        if parsed.path == "/callback":
            qs = urllib.parse.parse_qs(parsed.query)
            code = qs.get("code", [None])[0]
            if code:
                try:
                    # Exchange code for token using a no-op cache so Spotipy
                    # doesn't try to persist before we know the real user ID
                    tmp_oauth = SpotifyOAuth(
                        client_id=CLIENT_ID,
                        client_secret=CLIENT_SECRET,
                        redirect_uri=REDIRECT_URI,
                        scope=SCOPE,
                        cache_handler=NoCacheHandler(),
                    )
                    token_info = tmp_oauth.get_access_token(code, as_dict=True, check_cache=False)

                    sp = spotipy.Spotify(auth=token_info["access_token"])
                    me = sp.current_user()
                    uid = me["id"]

                    # Persist user + token
                    db_upsert_user(
                        uid,
                        me.get("display_name", uid),
                        me.get("email", ""),
                        me["images"][0]["url"] if me.get("images") else "",
                    )
                    DbCacheHandler(uid).save_token_to_cache(token_info)

                    self.send_response(302)
                    self.set_session_cookie(uid)
                    self.send_header("Location", "/")
                    self.end_headers()
                except Exception as e:
                    print(f"Auth error: {e}")
                    traceback.print_exc()
                    self.send_response(302)
                    self.send_header("Location", "/?error=auth_failed")
                    self.end_headers()
                return
            self.send_response(302)
            self.send_header("Location", "/")
            self.end_headers()
            return

        if parsed.path == "/logout":
            self.send_response(302)
            self.send_header("Set-Cookie", "dig_session=; Path=/; HttpOnly; Max-Age=0")
            self.send_header("Location", "/")
            self.end_headers()
            return

        # ── User profile ──────────────────────────────────────────────────────

        if parsed.path == "/me":
            if not user_id:
                self.send_json({"logged_in": False})
                return
            profile = db_get_profile(user_id)
            if not profile:
                self.send_json({"logged_in": False})
                return
            self.send_json({
                "logged_in": True,
                "user": {
                    "id":           profile["id"],
                    "display_name": profile["display_name"],
                    "email":        profile["email"],
                    "image":        profile["image_url"],
                },
            })
            return

        # ── Spotify token ─────────────────────────────────────────────────────

        if parsed.path == "/token":
            if not user_id:
                sp_oauth = make_sp_oauth()
                self.send_json({"error": "not_authenticated", "auth_url": sp_oauth.get_authorize_url()}, 401)
                return

            sp_oauth = make_sp_oauth(user_id=user_id)
            token_info = sp_oauth.get_cached_token()

            if not token_info:
                self.send_json({"error": "not_authenticated", "auth_url": sp_oauth.get_authorize_url()}, 401)
                return

            if sp_oauth.is_token_expired(token_info):
                try:
                    token_info = sp_oauth.refresh_access_token(token_info["refresh_token"])
                except Exception as e:
                    print(f"Token refresh failed for {user_id}: {e}")
                    self.send_json({"error": "token_refresh_failed", "auth_url": sp_oauth.get_authorize_url()}, 401)
                    return

            self.send_json({"access_token": token_info["access_token"]})
            return

        # ── Ledger ────────────────────────────────────────────────────────────

        if parsed.path == "/ledger":
            if not user_id:
                self.send_json({"known": [], "liked": [], "disliked": []})
                return
            self.send_json(db_get_ledger(user_id))
            return

        if parsed.path in ("/save", "/listened"):
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track = qs.get("track", [""])[0]
            if track:
                db_add_known(user_id, track)
            self.send_json({"ok": True})
            return

        if parsed.path == "/unsave":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track = qs.get("track", [""])[0]
            if track:
                db_unsave(user_id, track)
            self.send_json({"ok": True})
            return

        # ── Discovery pool (served from DB) ───────────────────────────────────

        if parsed.path == "/discovery":
            try:
                # Scope the pool to tracks this user hasn't heard yet. Anonymous
                # callers still get the full pool.
                self.send_json(load_discovery(user_id=user_id))
            except Exception as e:
                traceback.print_exc()
                self.send_json({"error": str(e)}, 500)
            return

        # ── History ───────────────────────────────────────────────────────────

        if parsed.path == "/history":
            if not user_id:
                self.send_json([])
                return
            self.send_json(db_get_history(user_id))
            return

        # ── Spotify Connect playback (for iOS / remote control) ─────────────
        # Uses the Spotify REST API to control playback on the user's active
        # Spotify device (mobile app, desktop app, etc.) instead of the
        # Web Playback SDK. Required for iOS where the SDK doesn't work.

        if parsed.path == "/api/play":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track_id = qs.get("track", [None])[0]
            device_id = qs.get("device", [None])[0]
            if not track_id:
                self.send_json({"error": "track param required"}, 400)
                return
            # Get a fresh token for this user
            sp_oauth = make_sp_oauth(user_id=user_id)
            token_info = sp_oauth.get_cached_token()
            if not token_info:
                self.send_json({"error": "not_authenticated", "auth_url": sp_oauth.get_authorize_url()}, 401)
                return
            if sp_oauth.is_token_expired(token_info):
                try:
                    token_info = sp_oauth.refresh_access_token(token_info["refresh_token"])
                except Exception:
                    self.send_json({"error": "token_refresh_failed"}, 401)
                    return
            # Transfer playback to the target device first (wakes it up),
            # then play the track. Without this, Spotify returns 404 if the
            # device hasn't been actively playing recently.
            token = token_info["access_token"]
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            print(f"[API play] track={track_id} device={device_id} user={user_id}")

            if device_id:
                # Step 1: Transfer playback to the device
                try:
                    transfer_body = json.dumps({"device_ids": [device_id]}).encode()
                    transfer_req = urllib.request.Request(
                        "https://api.spotify.com/v1/me/player",
                        data=transfer_body, method="PUT", headers=headers,
                    )
                    resp = urllib.request.urlopen(transfer_req)
                    print(f"[API play] transfer OK ({resp.status})")
                    import time as _time
                    _time.sleep(0.3)
                except urllib.error.HTTPError as e:
                    body = e.read().decode("utf-8", errors="replace")[:200]
                    print(f"[API play] transfer FAILED {e.code}: {body}")
                except Exception as e:
                    print(f"[API play] transfer exception: {e}")

            # Step 2: Play the track
            url = "https://api.spotify.com/v1/me/player/play"
            if device_id:
                url += f"?device_id={device_id}"
            req_body = json.dumps({"uris": [f"spotify:track:{track_id}"]}).encode()
            req = urllib.request.Request(url, data=req_body, method="PUT", headers=headers)
            try:
                resp = urllib.request.urlopen(req)
                print(f"[API play] play OK ({resp.status})")
                self.send_json({"ok": True, "status": resp.status})
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")[:300]
                print(f"[API play] play FAILED {e.code}: {body}")
                self.send_json({"error": f"spotify_{e.code}", "detail": body}, e.code if e.code < 500 else 502)
            return

        if parsed.path == "/api/queue":
            # Add a track to Spotify's native playback queue
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track_id = qs.get("track", [None])[0]
            if not track_id:
                self.send_json({"error": "track param required"}, 400)
                return
            sp_oauth = make_sp_oauth(user_id=user_id)
            token_info = sp_oauth.get_cached_token()
            if not token_info:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            if sp_oauth.is_token_expired(token_info):
                try:
                    token_info = sp_oauth.refresh_access_token(token_info["refresh_token"])
                except Exception:
                    self.send_json({"error": "token_refresh_failed"}, 401)
                    return
            try:
                sp = spotipy.Spotify(auth=token_info["access_token"])
                sp.add_to_queue(f"spotify:track:{track_id}")
                self.send_json({"ok": True})
            except Exception as e:
                print(f"[API queue] error: {e}")
                self.send_json({"error": str(e)[:200]}, 500)
            return

        if parsed.path == "/api/devices":
            # List user's available Spotify playback devices
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            sp_oauth = make_sp_oauth(user_id=user_id)
            token_info = sp_oauth.get_cached_token()
            if not token_info:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            if sp_oauth.is_token_expired(token_info):
                try:
                    token_info = sp_oauth.refresh_access_token(token_info["refresh_token"])
                except Exception:
                    self.send_json({"error": "token_refresh_failed"}, 401)
                    return
            try:
                sp = spotipy.Spotify(auth=token_info["access_token"])
                devices = sp.devices()
                self.send_json(devices)
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
            return

        if parsed.path == "/api/pause":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            device_id = qs.get("device", [None])[0]
            sp_oauth = make_sp_oauth(user_id=user_id)
            token_info = sp_oauth.get_cached_token()
            if not token_info or sp_oauth.is_token_expired(token_info):
                try:
                    token_info = sp_oauth.refresh_access_token(token_info["refresh_token"])
                except Exception:
                    self.send_json({"error": "token_refresh_failed"}, 401)
                    return
            try:
                sp = spotipy.Spotify(auth=token_info["access_token"])
                sp.pause_playback(device_id=device_id)
                self.send_json({"ok": True})
            except Exception as e:
                # 403/404 = nothing playing — not an error worth surfacing
                self.send_json({"ok": True, "note": "nothing_to_pause"})
            return

        if parsed.path == "/api/resume":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            device_id = qs.get("device", [None])[0]
            sp_oauth = make_sp_oauth(user_id=user_id)
            token_info = sp_oauth.get_cached_token()
            if not token_info or sp_oauth.is_token_expired(token_info):
                try:
                    token_info = sp_oauth.refresh_access_token(token_info["refresh_token"])
                except Exception:
                    self.send_json({"error": "token_refresh_failed"}, 401)
                    return
            try:
                sp = spotipy.Spotify(auth=token_info["access_token"])
                sp.start_playback(device_id=device_id)
                self.send_json({"ok": True})
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
            return

        # ── Taste profile (pre-computed from DB for tailored mode) ─────────────

        if parsed.path == "/api/taste-profile":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            # Include saves + deep listens (>=60%) + dislikes + instant skips (<10%).
            # Instant skips act as negative evidence so tailored mode downweights
            # moods/genres the user routinely bails on.
            rows = fetchall(
                """
                SELECT t.label_energy AS energy, t.label_mood AS mood,
                       t.label_texture AS texture, t.label_feel AS feel,
                       t.genres, t.region, t.decade,
                       h.status, h.played_pct
                FROM user_history h
                JOIN tracks t ON t.id = h.track_id
                WHERE h.user_id = %s
                  AND t.label_energy IS NOT NULL
                  AND (h.status = 'saved'
                       OR h.status = 'disliked'
                       OR (h.played_pct IS NOT NULL AND h.played_pct >= 60)
                       OR (h.status = 'skipped' AND h.played_pct IS NOT NULL
                           AND h.played_pct < 10))
                """,
                (user_id,),
            )
            # Individual dimension counts, weighted by signal strength
            profile = {"energies": {}, "moods": {}, "genres": {}, "regions": {}, "decades": {}}
            feel_pairs = {}
            for r in rows:
                # Weight: saves strongest, deep-listens moderate, dislikes negative,
                # instant skips weakly negative (bailing is not the same as hating).
                status = r.get("status")
                pct = r.get("played_pct")
                if status == "saved":
                    w = 3.0
                elif status == "disliked":
                    w = -2.0
                elif status == "skipped" and pct is not None and pct < 10:
                    w = -0.3
                elif pct is not None and pct >= 80:
                    w = 2.0
                elif pct is not None and pct >= 60:
                    w = 1.0
                else:
                    w = 0.5

                e = r.get("energy") or ""
                m = r.get("mood") or ""
                tex = r.get("texture") or ""
                if e: profile["energies"][e] = profile["energies"].get(e, 0) + w
                if m: profile["moods"][m] = profile["moods"].get(m, 0) + w
                if r.get("region"): profile["regions"][r["region"]] = profile["regions"].get(r["region"], 0) + w
                if r.get("decade"): profile["decades"][r["decade"]] = profile["decades"].get(r["decade"], 0) + w
                for g in (r["genres"] or [])[:2]:
                    profile["genres"][g] = profile["genres"].get(g, 0) + w
                # Co-occurrence pairs (only for positive signals)
                if w > 0:
                    if e and m:
                        feel_pairs[f"{e}|{m}"] = feel_pairs.get(f"{e}|{m}", 0) + w
                    if e and tex:
                        tex1 = tex.split(",")[0].strip()
                        if tex1: feel_pairs[f"{e}|{tex1}"] = feel_pairs.get(f"{e}|{tex1}", 0) + w
                    if m and tex:
                        tex1 = tex.split(",")[0].strip()
                        if tex1: feel_pairs[f"{m}|{tex1}"] = feel_pairs.get(f"{m}|{tex1}", 0) + w
                    if e and m and tex:
                        tex1 = tex.split(",")[0].strip()
                        if tex1: feel_pairs[f"{e}|{m}|{tex1}"] = feel_pairs.get(f"{e}|{m}|{tex1}", 0) + w
            self.send_json({
                "profile": profile,
                "feel_pairs": feel_pairs,
                "saves_matched": len(rows),
            })
            return

        # ── Session sync (cross-device handoff) ────────────────────────────────

        if parsed.path == "/api/session":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            row = fetchone(
                "SELECT state, updated_at FROM user_session WHERE user_id = %s",
                (user_id,),
            )
            if row:
                import datetime
                age_s = (datetime.datetime.now(datetime.timezone.utc) - row["updated_at"]).total_seconds()
                self.send_json({
                    "state": row["state"],
                    "age_seconds": round(age_s, 1),
                    "updated_at": row["updated_at"].isoformat(),
                })
            else:
                self.send_json({"state": None})
            return

        # ── Health / monitoring ───────────────────────────────────────────────

        if parsed.path == "/api/health":
            import datetime
            qs = urllib.parse.parse_qs(parsed.query)
            include_ai = qs.get("ai", ["1"])[0] == "1"
            payload = {
                "now":          datetime.datetime.utcnow().isoformat() + "Z",
                "started_at":   _HEALTH["started_at"],
                "history_writes": _HEALTH["history_writes"],
                "ai_calls_count": len(_HEALTH["ai_calls"]),
                "recent_errors":  _HEALTH["errors"][-10:],
            }
            if include_ai:
                payload["ai_calls_recent"] = _HEALTH["ai_calls"][-10:]
            # Pool snapshot (cheap — count rows in tracks table)
            try:
                row = fetchone("SELECT COUNT(*) AS n FROM tracks WHERE source != 'youtube' OR source IS NULL")
                payload["pool_size"] = row["n"] if row else None
            except Exception as e:
                payload["pool_size_error"] = str(e)
            # Recent listen-pct distribution for the calling user
            if user_id:
                try:
                    rows = fetchall(
                        """
                        SELECT
                          COUNT(*) FILTER (WHERE played_pct IS NOT NULL) AS n_with_pct,
                          AVG(played_pct) FILTER (WHERE played_pct IS NOT NULL) AS avg_pct,
                          COUNT(*) FILTER (WHERE played_pct >= 70) AS deep_listens,
                          COUNT(*) FILTER (WHERE played_pct < 10) AS instant_skips
                        FROM user_history
                        WHERE user_id = %s AND listened_at > %s
                        """,
                        (user_id, int(time.time() * 1000) - 7 * 24 * 3600 * 1000),
                    )
                    payload["user_listen_stats_7d"] = dict(rows[0]) if rows else None
                except Exception as e:
                    payload["user_listen_stats_error"] = str(e)
            self.send_json(payload)
            return

        # ── Static data files (served from project root) ──────────────────────

        fname = parsed.path.lstrip("/")
        if fname in _DATA_FILES:
            filepath = os.path.join(DIR, fname)
            if os.path.exists(filepath):
                content = open(filepath, "rb").read()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(content)
            else:
                self.send_response(404)
                self.end_headers()
            return

        # ── Static web assets (served from web/) ─────────────────────────────

        if parsed.path == "/":
            self.path = "/app.html"

        # Prevent browsers from caching stale HTML
        if self.path.endswith(".html") or parsed.path == "/":
            filepath = os.path.join(WEB_DIR, self.path.lstrip("/"))
            if os.path.exists(filepath):
                content = open(filepath, "rb").read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            else:
                self.send_response(404)
                self.end_headers()
            return

        super().do_GET()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        user_id = self.get_user()

        # ── Session sync (cross-device heartbeat) ─────────────────────────────

        if parsed.path == "/api/session":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            length = int(self.headers.get("Content-Length", 0))
            body_raw = self.rfile.read(length) if length else b"{}"
            try:
                state = json.loads(body_raw.decode() or "{}")
            except Exception:
                state = {}
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO user_session (user_id, state, updated_at)
                        VALUES (%s, %s::jsonb, NOW())
                        ON CONFLICT (user_id) DO UPDATE
                        SET state = EXCLUDED.state, updated_at = NOW()
                        """,
                        (user_id, json.dumps(state, ensure_ascii=False)),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
            finally:
                conn.close()
            self.send_json({"ok": True})
            return

        # ── Client-side diagnostic log ────────────────────────────────────────
        # Mirrors browser-side events into journalctl so the firstplay/skip
        # timelines are visible server-side. Doesn't require auth.
        if parsed.path == "/api/client-log":
            length = int(self.headers.get("Content-Length", 0))
            body_raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(body_raw.decode() or "{}")
            except Exception:
                body = {"raw": body_raw[:300].decode("utf-8", errors="replace")}
            tag = (body.get("tag") or "client").strip()[:40]
            msg = (body.get("msg") or "").strip()[:500]
            data = body.get("data")
            who = user_id or "anon"
            print(f"[CLIENT {tag}] user={who} {msg}"
                  + (f" data={json.dumps(data)[:300]}" if data else ""))
            self.send_json({"ok": True})
            return

        if parsed.path == "/history":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                db_save_history(user_id, json.loads(body.decode()))
                _HEALTH["history_writes"] += 1
            except Exception as e:
                _health_record_error(f"history write: {e}")
                raise
            self.send_json({"ok": True})
            return

        # ── Journey (seeded, infinite) ────────────────────────────────────────

        if parsed.path == "/api/journey":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            length = int(self.headers.get("Content-Length", 0))
            body_raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(body_raw.decode() or "{}")
            except Exception:
                body = {}
            seed = body.get("seed") or {}
            block_index = int(body.get("block_index", 0))
            previous_journey = body.get("previous_journey") or []
            n = int(body.get("n", 8))
            n = max(4, min(15, n))
            fe_ids = body.get("recent_ids") or []
            if not isinstance(fe_ids, list): fe_ids = []
            result = journey_recommend(user_id, seed, block_index=block_index,
                                       previous_journey=previous_journey, n=n,
                                       frontend_recent_ids=fe_ids[:200])
            import datetime
            _health_record_ai({
                "ts":   datetime.datetime.utcnow().isoformat() + "Z",
                "user": user_id,
                "kind": "journey",
                "seed": result.get("meta", {}).get("seed"),
                "block_index": block_index,
                "ok":   not result.get("error"),
                "err":  result.get("error"),
                "meta": result.get("meta"),
                "n_returned": len(result.get("recommendations", [])),
            })
            if result.get("error"):
                _health_record_error(f"journey: {result['error']}")
            self.send_json(result)
            return

        # ── AI Mix recommendation ─────────────────────────────────────────────

        if parsed.path == "/api/ai-recommend":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            length = int(self.headers.get("Content-Length", 0))
            body_raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(body_raw.decode() or "{}")
            except Exception:
                body = {}
            n = int(body.get("n", 10))
            n = max(3, min(20, n))  # clamp
            # COVERAGE-DRIVEN exploration: surface tracks from cells the user
            # has NEVER been served from, prioritizing unheard artists. No
            # taste anchoring, no Claude calls, no shape repetition. Pure
            # breadth, sampled randomly from the unexplored frontier.
            # Older Claude-anchored modes still callable via mode=v2 / mode=v1.
            mode = (body.get("mode") or "explore").strip().lower()
            if mode == "v2":
                fe_ids = body.get("recent_ids") or []
                fe_artists = body.get("recent_artists") or []
                if not isinstance(fe_ids, list): fe_ids = []
                if not isinstance(fe_artists, list): fe_artists = []
                result = ai_recommend_v2(user_id, n=n,
                                         frontend_recent_ids=fe_ids[:200],
                                         frontend_recent_artists=fe_artists[:200])
            elif mode == "v1":
                result = ai_recommend(user_id, n=n)
            else:
                # Default: coverage explore, pool-grounded. Pass the frontend
                # history so the pick respects tracks the user just played
                # but hasn't yet synced to the DB.
                fe_ids = body.get("recent_ids") or []
                fe_artists = body.get("recent_artists") or []
                if not isinstance(fe_ids, list): fe_ids = []
                if not isinstance(fe_artists, list): fe_artists = []
                result = coverage_explore(user_id, n=n,
                                          frontend_recent_ids=fe_ids[:200],
                                          frontend_recent_artists=fe_artists[:200])
            # Telemetry
            import datetime
            _health_record_ai({
                "ts":   datetime.datetime.utcnow().isoformat() + "Z",
                "user": user_id,
                "n":    n,
                "ok":   not result.get("error"),
                "err":  result.get("error"),
                "meta": result.get("meta"),
                "n_returned": len(result.get("recommendations", [])),
            })
            if result.get("error"):
                _health_record_error(f"ai-recommend: {result['error']}")
            self.send_json(result)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        path_str = str(args)
        if any(x in path_str for x in ["/token", "/listened", "/save", "/history", "/me"]):
            return
        super().log_message(format, *args)


if __name__ == "__main__":
    import datetime
    import time
    _HEALTH["started_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "127.0.0.1")
    print(f"\n🎵 DIG running at http://{host}:{port}\n")
    server = http.server.HTTPServer((host, port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
