#!/usr/bin/env python3
"""
DIG — local server with Spotify auth token endpoint.
Serves the app and provides a /token endpoint for the Web Playback SDK.
"""

import http.server
import json
import os
import urllib.parse

DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(DIR, ".env")

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

SCOPE = "streaming user-read-email user-read-private user-library-read user-top-read user-read-recently-played user-read-playback-state user-modify-playback-state"

sp_oauth = SpotifyOAuth(
    scope=SCOPE,
    redirect_uri=os.environ.get("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8888/callback"),
    cache_path=os.path.join(DIR, ".spotify_token_cache"),
)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIR, **kwargs)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/callback":
            # Spotify auth callback
            qs = urllib.parse.parse_qs(parsed.query)
            code = qs.get("code", [None])[0]
            if code:
                try:
                    sp_oauth.get_access_token(code)
                except Exception as e:
                    pass
            # Redirect to app
            self.send_response(302)
            self.send_header("Location", "/")
            self.end_headers()
            return

        if parsed.path == "/token":
            # Return a fresh access token
            token_info = sp_oauth.get_cached_token()
            if not token_info:
                # Need to authenticate first
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                auth_url = sp_oauth.get_authorize_url()
                self.wfile.write(json.dumps({"error": "not_authenticated", "auth_url": auth_url}).encode())
                return

            if sp_oauth.is_token_expired(token_info):
                token_info = sp_oauth.refresh_access_token(token_info["refresh_token"])

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"access_token": token_info["access_token"]}).encode())
            return

        if parsed.path == "/save":
            # Save a track to the ledger
            qs = urllib.parse.parse_qs(parsed.query)
            track = qs.get("track", [""])[0]
            region = qs.get("region", [""])[0]
            if track:
                ledger_path = os.path.join(DIR, "ledger.json")
                with open(ledger_path) as f:
                    ledger = json.load(f)
                if track.lower() not in {k.lower() for k in ledger["known"]}:
                    ledger["known"].append(track)
                    with open(ledger_path, "w") as f:
                        json.dump(ledger, f, indent=2)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True}).encode())
            return

        if parsed.path == "/listened":
            # Mark track as listened (add to ledger known)
            qs = urllib.parse.parse_qs(parsed.query)
            track = qs.get("track", [""])[0]
            if track:
                ledger_path = os.path.join(DIR, "ledger.json")
                with open(ledger_path) as f:
                    ledger = json.load(f)
                if track.lower() not in {k.lower() for k in ledger["known"]}:
                    ledger["known"].append(track)
                    with open(ledger_path, "w") as f:
                        json.dump(ledger, f, indent=2)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True}).encode())
            return

        if parsed.path == "/history":
            # Persist full session history (with statuses)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            history_path = os.path.join(DIR, "history.json")
            if os.path.exists(history_path):
                with open(history_path) as f:
                    self.wfile.write(f.read().encode())
            else:
                self.wfile.write(b"[]")
            return

        # Default: serve files
        if parsed.path == "/":
            self.path = "/app.html"
        super().do_GET()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/history":
            # Save full session history to disk
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            history_path = os.path.join(DIR, "history.json")
            with open(history_path, "w") as f:
                f.write(body.decode())
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True}).encode())
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        # Quiet logging
        if "/token" in str(args) or "/listened" in str(args) or "/save" in str(args) or "/history" in str(args):
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
