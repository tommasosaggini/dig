#!/usr/bin/env python3
"""
DIG — multi-tenant server with Spotify OAuth.
Each user signs in with Spotify, gets their own history/ledger.
Discovery pool and catalog are shared.
"""

import http.server
import hashlib
import hmac
import json
import os
import secrets
import urllib.parse
import urllib.request

DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(DIR, ".env")
USERS_DIR = os.path.join(DIR, "users")
os.makedirs(USERS_DIR, exist_ok=True)

# Load .env
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

import spotipy
from spotipy.oauth2 import SpotifyOAuth

CLIENT_ID = os.environ.get("SPOTIPY_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("SPOTIPY_CLIENT_SECRET", "")
REDIRECT_URI = os.environ.get("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8000/callback")
COOKIE_SECRET = os.environ.get("COOKIE_SECRET", "")
SCOPE = "streaming user-read-email user-read-private user-library-read user-top-read user-read-recently-played user-read-playback-state user-modify-playback-state"

# Generate a cookie secret if not set
if not COOKIE_SECRET:
    secret_path = os.path.join(DIR, ".cookie_secret")
    if os.path.exists(secret_path):
        with open(secret_path) as f:
            COOKIE_SECRET = f.read().strip()
    else:
        COOKIE_SECRET = secrets.token_hex(32)
        with open(secret_path, "w") as f:
            f.write(COOKIE_SECRET)


def make_sp_oauth(cache_path=None):
    return SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        cache_path=cache_path,
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


def user_dir(user_id):
    safe_id = user_id.replace("/", "_").replace("..", "_")
    d = os.path.join(USERS_DIR, safe_id)
    os.makedirs(d, exist_ok=True)
    return d


def user_file(user_id, filename, default=None):
    path = os.path.join(user_dir(user_id), filename)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default if default is not None else []


def save_user_file(user_id, filename, data):
    path = os.path.join(user_dir(user_id), filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIR, **kwargs)

    def get_user(self):
        cookies = self.headers.get("Cookie", "")
        for part in cookies.split(";"):
            part = part.strip()
            if part.startswith("dig_session="):
                return verify_cookie(part[len("dig_session="):])
        return None

    def set_session_cookie(self, user_id):
        val = sign_cookie(user_id)
        self.send_header("Set-Cookie", f"dig_session={val}; Path=/; HttpOnly; SameSite=Lax; Max-Age=31536000")

    def send_json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        user_id = self.get_user()

        # --- Auth flow ---
        if parsed.path == "/login":
            sp_oauth = make_sp_oauth()
            auth_url = sp_oauth.get_authorize_url()
            self.send_response(302)
            self.send_header("Location", auth_url)
            self.end_headers()
            return

        if parsed.path == "/callback":
            qs = urllib.parse.parse_qs(parsed.query)
            code = qs.get("code", [None])[0]
            if code:
                # Use a temp cache to get the token
                tmp_cache = os.path.join(DIR, ".tmp_token_cache")
                sp_oauth = make_sp_oauth(cache_path=tmp_cache)
                try:
                    token_info = sp_oauth.get_access_token(code)
                    # Get user profile
                    sp = spotipy.Spotify(auth=token_info["access_token"])
                    me = sp.current_user()
                    uid = me["id"]

                    # Save token to user dir
                    cache_path = os.path.join(user_dir(uid), ".spotify_token_cache")
                    with open(cache_path, "w") as f:
                        json.dump(token_info, f)

                    # Save user profile
                    save_user_file(uid, "profile.json", {
                        "id": uid,
                        "display_name": me.get("display_name", uid),
                        "email": me.get("email", ""),
                        "image": me["images"][0]["url"] if me.get("images") else "",
                    })

                    # Init empty data files if new user
                    udir = user_dir(uid)
                    if not os.path.exists(os.path.join(udir, "history.json")):
                        save_user_file(uid, "history.json", [])
                    if not os.path.exists(os.path.join(udir, "ledger.json")):
                        save_user_file(uid, "ledger.json", {"known": [], "liked": [], "disliked": []})

                    self.send_response(302)
                    self.set_session_cookie(uid)
                    self.send_header("Location", "/")
                    self.end_headers()
                except Exception as e:
                    self.send_response(302)
                    self.send_header("Location", "/?error=auth_failed")
                    self.end_headers()
                finally:
                    if os.path.exists(tmp_cache):
                        os.remove(tmp_cache)
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

        if parsed.path == "/me":
            if not user_id:
                self.send_json({"logged_in": False})
                return
            profile = user_file(user_id, "profile.json", {})
            self.send_json({"logged_in": True, "user": profile})
            return

        # --- Token ---
        if parsed.path == "/token":
            if not user_id:
                sp_oauth = make_sp_oauth()
                self.send_json({"error": "not_authenticated", "auth_url": sp_oauth.get_authorize_url()}, 401)
                return

            cache_path = os.path.join(user_dir(user_id), ".spotify_token_cache")
            sp_oauth = make_sp_oauth(cache_path=cache_path)
            token_info = sp_oauth.get_cached_token()

            if not token_info:
                self.send_json({"error": "not_authenticated", "auth_url": sp_oauth.get_authorize_url()}, 401)
                return

            if sp_oauth.is_token_expired(token_info):
                token_info = sp_oauth.refresh_access_token(token_info["refresh_token"])

            self.send_json({"access_token": token_info["access_token"]})
            return

        # --- Per-user data ---
        if parsed.path == "/save":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track = qs.get("track", [""])[0]
            if track:
                ledger = user_file(user_id, "ledger.json", {"known": [], "liked": [], "disliked": []})
                if track.lower() not in {k.lower() for k in ledger["known"]}:
                    ledger["known"].append(track)
                    save_user_file(user_id, "ledger.json", ledger)
            self.send_json({"ok": True})
            return

        if parsed.path == "/listened":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track = qs.get("track", [""])[0]
            if track:
                ledger = user_file(user_id, "ledger.json", {"known": [], "liked": [], "disliked": []})
                if track.lower() not in {k.lower() for k in ledger["known"]}:
                    ledger["known"].append(track)
                    save_user_file(user_id, "ledger.json", ledger)
            self.send_json({"ok": True})
            return

        if parsed.path == "/history":
            if not user_id:
                self.send_json([])
                return
            history = user_file(user_id, "history.json", [])
            self.send_json(history)
            return

        # --- Shared data (no auth needed) ---
        if parsed.path == "/":
            self.path = "/app.html"

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
            save_user_file(user_id, "history.json", json.loads(body.decode()))
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
    port = 8000
    print(f"\n🎵 DIG running at http://127.0.0.1:{port}\n")
    server = http.server.HTTPServer(("127.0.0.1", port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
