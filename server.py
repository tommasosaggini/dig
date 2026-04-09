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
import traceback
import urllib.parse

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

CLIENT_ID     = os.environ.get("SPOTIPY_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("SPOTIPY_CLIENT_SECRET", "")
REDIRECT_URI  = os.environ.get("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8000/callback")
SCOPE = (
    "streaming user-read-email user-read-private user-library-read "
    "user-top-read user-read-recently-played user-read-playback-state "
    "user-modify-playback-state"
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


def db_get_history(user_id):
    rows = fetchall(
        """
        SELECT track_id AS id, track_name AS track, artist, region, status, listened_at AS time
        FROM user_history WHERE user_id = %s ORDER BY listened_at DESC
        """,
        (user_id,),
    )
    return [dict(r) for r in rows]


def db_save_history(user_id, history_list):
    """Replace a user's full history (called from POST /history)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_history WHERE user_id = %s", (user_id,))
            for item in history_list:
                cur.execute(
                    """
                    INSERT INTO user_history
                        (user_id, track_id, track_name, artist, region, status, listened_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        item.get("id"),
                        item.get("track"),
                        item.get("artist"),
                        item.get("region"),
                        item.get("status"),
                        item.get("time"),
                    ),
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

        # ── Discovery pool (served from DB) ───────────────────────────────────

        if parsed.path == "/discovery":
            try:
                self.send_json(load_discovery())
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

        if parsed.path == "/history":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            db_save_history(user_id, json.loads(body.decode()))
            self.send_json({"ok": True})
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        path_str = str(args)
        if any(x in path_str for x in ["/token", "/listened", "/save", "/history", "/me"]):
            return
        super().log_message(format, *args)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"\n🎵 DIG running at http://127.0.0.1:{port}\n")
    server = http.server.HTTPServer(("127.0.0.1", port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
