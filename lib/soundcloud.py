"""
DIG — SoundCloud resolver (in-app discovery + streaming).

SoundCloud is a DISCOVERY/STREAMING source only. Its API Terms forbid
downloading/storing audio, so — unlike Bandcamp — we never persist a file and
this path is NOT used by the Instagram clip pipeline. We resolve a fresh,
signed, expiring HLS stream URL at play time, exactly like Bandcamp.

Auth: OAuth 2.1 client_credentials (public resources only — search, resolve,
playback). The token endpoint is HARD rate-limited (50 tokens / 12h per app,
30 / h per IP), so the access token is CACHED to disk and reused; we only mint a
new one when the cached one is within 60s of expiry.

Streaming (post-2025 migration): progressive MP3 is gone. GET /tracks/{id}/streams
returns `hls_aac_160_url` / `hls_aac_96_url` — HLS .m3u8 playlists. Browsers need
hls.js (or native HLS on Safari/iOS) to play them. See docs/SOUNDCLOUD_INTEGRATION.md.

Pool identity: id = "sc:<track_id>", source = "soundcloud".

Env: SOUNDCLOUD_CLIENT_ID, SOUNDCLOUD_CLIENT_SECRET (.env).
"""
import base64
import json
import os
import re
import threading
import time

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TOKEN_URL = "https://secure.soundcloud.com/oauth/token"
API_BASE = "https://api.soundcloud.com"
_TOKEN_CACHE = os.path.join(ROOT, ".soundcloud_token.json")
_TIMEOUT = 20

# Serialise token refresh across threads in this process so a burst of plays
# doesn't mint several tokens at once (and burn the 50/12h budget).
_token_lock = threading.Lock()


class SoundCloudError(Exception):
    pass


def _creds():
    cid = os.environ.get("SOUNDCLOUD_CLIENT_ID")
    secret = os.environ.get("SOUNDCLOUD_CLIENT_SECRET")
    if not cid or not secret:
        raise SoundCloudError("SOUNDCLOUD_CLIENT_ID / SOUNDCLOUD_CLIENT_SECRET not set in .env")
    return cid, secret


def _read_cached_token():
    try:
        with open(_TOKEN_CACHE) as f:
            d = json.load(f)
        if d.get("access_token") and d.get("expires_at", 0) > time.time() + 60:
            return d
    except Exception:
        pass
    return None


def _write_cached_token(d):
    try:
        with open(_TOKEN_CACHE, "w") as f:
            json.dump(d, f)
        os.chmod(_TOKEN_CACHE, 0o600)
    except Exception:
        pass


def _mint_token():
    """Fetch a fresh client_credentials token (HTTP Basic auth)."""
    cid, secret = _creds()
    basic = base64.b64encode(f"{cid}:{secret}".encode()).decode()
    r = requests.post(
        TOKEN_URL,
        headers={"Authorization": f"Basic {basic}",
                 "Content-Type": "application/x-www-form-urlencoded",
                 "Accept": "application/json; charset=utf-8"},
        data={"grant_type": "client_credentials"},
        timeout=_TIMEOUT,
    )
    if r.status_code != 200:
        raise SoundCloudError(f"token mint failed: HTTP {r.status_code} {r.text[:200]}")
    d = r.json()
    d["expires_at"] = time.time() + int(d.get("expires_in", 3600))
    return d


def _get_token(force=False):
    """Cached access token, refreshing only when needed (rate-limit aware)."""
    if not force:
        cached = _read_cached_token()
        if cached:
            return cached["access_token"]
    with _token_lock:
        if not force:                       # re-check inside the lock
            cached = _read_cached_token()
            if cached:
                return cached["access_token"]
        d = _mint_token()
        _write_cached_token(d)
        return d["access_token"]


def _api_get(path, params=None):
    """Authenticated GET with a single 401-triggered token refresh + retry."""
    url = path if path.startswith("http") else f"{API_BASE}{path}"
    for attempt in (0, 1):
        token = _get_token(force=(attempt == 1))
        r = requests.get(
            url,
            headers={"Authorization": f"OAuth {token}",
                     "Accept": "application/json; charset=utf-8"},
            params=params, timeout=_TIMEOUT,
        )
        if r.status_code == 401 and attempt == 0:
            continue                        # token went stale mid-flight — refresh once
        if r.status_code == 429:
            raise SoundCloudError("rate limited (HTTP 429)")
        if r.status_code >= 400:
            raise SoundCloudError(f"HTTP {r.status_code}: {r.text[:200]}")
        return r.json()
    raise SoundCloudError("auth failed after refresh")


# ── identity helpers ──────────────────────────────────────────────────────────

def make_id(track_id):
    return f"sc:{track_id}"


def parse_id(track_id):
    """'sc:12345' -> '12345'; None if not a SoundCloud id."""
    m = re.match(r"^sc:(\d+)$", track_id or "")
    return m.group(1) if m else None


def _track_to_pool(t):
    """Normalise a SoundCloud track object into DIG's pool-track shape."""
    user = t.get("user") or {}
    art = (t.get("artwork_url") or user.get("avatar_url") or "")
    if art:
        art = art.replace("-large.", "-t500x500.")   # bigger cover
    genre = (t.get("genre") or "").strip()
    return {
        "id": make_id(t.get("id")),
        "name": t.get("title") or "",
        "artist": user.get("username") or "",
        "album": "",
        "art": art,
        "genres": [genre.lower()] if genre else [],
        "duration": int((t.get("duration") or 0)),       # ms
        "permalink": t.get("permalink_url") or "",
        "source": "soundcloud",
    }


# ── public API ────────────────────────────────────────────────────────────────

def search(query, limit=50, only_playable=True):
    """Search tracks. Returns normalised pool-track dicts."""
    params = {"q": query, "limit": min(limit, 200), "linked_partitioning": "true"}
    if only_playable:
        params["access"] = "playable"
    data = _api_get("/tracks", params)
    items = data.get("collection", data) if isinstance(data, dict) else data
    return [_track_to_pool(t) for t in (items or []) if t.get("id")]


def get_stream(track_id):
    """Resolve a FRESH HLS stream for a track id. Returns
    {ok, url, url_lo, preview, kind} — `url` is hls_aac_160 (.m3u8). Never cache
    it (signed + expiring). `ok` is False with an `error` on failure."""
    tid = parse_id(track_id) or str(track_id)
    try:
        d = _api_get(f"/tracks/{tid}/streams")
    except SoundCloudError as e:
        return {"ok": False, "error": str(e)}
    url = d.get("hls_aac_160_url") or d.get("hls_aac_96_url")
    if not url:
        return {"ok": False, "error": "no_hls_url"}
    return {
        "ok": True,
        "url": url,                                  # preferred (AAC 160, HLS)
        "url_lo": d.get("hls_aac_96_url") or "",     # fallback (AAC 96, HLS)
        "preview": d.get("preview_mp3_128_url") or "",
        "kind": "hls",                               # tells the client to use hls.js
    }


# ── discovery (genre-driven, for ingest) ──────────────────────────────────────
# Mirrors lib/bandcamp.discover: enumerate genres for breadth. SoundCloud genres
# are free-text; searching by genre keyword + access=playable is the robust path.

GENRES = [
    "techno", "house", "deep house", "electro", "ambient", "idm", "dub techno",
    "minimal", "drum and bass", "jungle", "breakbeat", "downtempo", "experimental",
    "lo-fi house", "acid", "trance", "garage", "footwork", "leftfield",
]


def discover(genre, limit=50):
    """One genre's worth of streamable tracks for the ingest pipeline."""
    return search(genre, limit=limit, only_playable=True)
