#!/usr/bin/env python3
"""
DIG — multi-tenant server with Spotify OAuth.
Each user signs in with Spotify, gets their own history/ledger.
Discovery pool and catalog are shared (PostgreSQL-backed).
"""

import http.server
import gzip
import hashlib
import hmac
import json
import os
import re
import secrets
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
import urllib.error
import smtplib
from email.message import EmailMessage

DIR = os.path.dirname(os.path.abspath(__file__))

if DIR not in sys.path:
    sys.path.insert(0, DIR)

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import CacheHandler

from lib.env import load_env
load_env()
from lib.db import get_conn, fetchone, fetchall, execute
from lib import ig_queue
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

def _evt(category, **fields):
    """Structured event log: '[category] k1=v1 k2=v2 ...'.
    Keys with spaces get quoted; everything else is bare. Stays on one line
    so `docker logs | grep '[category]' ` works cleanly."""
    parts = []
    for k, v in fields.items():
        if v is None:
            continue
        if isinstance(v, str) and (" " in v or '"' in v):
            v = '"' + v.replace('"', '\\"') + '"'
        parts.append(f"{k}={v}")
    print(f"[{category}] " + " ".join(parts), flush=True)


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
    "playlist-modify-private playlist-modify-public user-library-modify "
    "user-top-read user-read-recently-played user-read-playback-state "
    "user-modify-playback-state playlist-read-private playlist-read-collaborative"
)
# Scopes the bidirectional save/unsave needs. Tokens issued before a scope was
# added will lack it — the handlers fall back to DIG-only behaviour and tell
# the frontend to prompt a re-auth via /reconnect.
#
# `user-library-modify` and `playlist-modify-public` were added when saves
# became a user choice: the default is still the auto-created private "DIG"
# playlist, but a user can now point saves at their Liked Songs or at any
# playlist of their own, and a playlist they picked may well be public.
_PLAYLIST_MODIFY_SCOPE = "playlist-modify-private"
_LIBRARY_MODIFY_SCOPE = "user-library-modify"

# Save destinations. Default is deliberately the DIG playlist: it is
# reversible, visible, and never touches a library the user curates by hand.
SAVE_DEST_DIG = "dig_playlist"
SAVE_DEST_LIKED = "liked_songs"
SAVE_DEST_PLAYLIST = "playlist"
SAVE_DESTINATIONS = (SAVE_DEST_DIG, SAVE_DEST_LIKED, SAVE_DEST_PLAYLIST)
# The Spotify Web Playback SDK refuses to authenticate (fires
# `authentication_error` on every init) unless the access token carries these
# scopes. A token first granted when SCOPE was narrower is preserved narrow
# across refresh (see _user_token_or_refresh), so such a user is PERMANENTLY
# locked out of SDK playback until they re-consent. We detect the gap at
# /token and signal the client to prompt a forced-consent reconnect (/reconnect)
# instead of letting the SDK retry-loop forever.
_SDK_REQUIRED_SCOPES = {"streaming", "user-read-email", "user-read-private"}
DIG_PLAYLIST_NAME = "DIG"
DIG_PLAYLIST_DESC = "Saved from diiiiiiiig.xyz — your DIG discoveries."


def _user_token_or_refresh(user_id):
    """Return the user's token dict (refreshed if expired), bypassing
    Spotipy's `validate_token` scope-subset check.

    The subset check rejects any stored token whose `scope` isn't a superset
    of the SCOPE constant. That's wrong for our use case: a token with
    fewer scopes is still a valid auth credential; only individual API
    calls that need a missing scope should fail (with a clear 403). The
    subset check turned a scope-narrowing bug into a hard auth lockout.

    Also preserves the original scope across refresh — Spotipy stamps the
    constructor's scope onto a refreshed token if the response omits it,
    which can silently widen or narrow the persisted scope.

    Phase timings are emitted to surface where p99 latency lives:
    cache_ms (DB read), expired_ms (clock check), refresh_ms (Spotify
    /api/token round-trip), save_ms (DB write back).
    """
    if not user_id:
        return None
    t0 = time.time()
    cache = DbCacheHandler(user_id)
    token_info = cache.get_cached_token()
    cache_ms = int((time.time() - t0) * 1000)
    if not token_info:
        _evt("token-internal", user=user_id, outcome="no_token", cache_ms=cache_ms)
        return None
    sp_oauth = make_sp_oauth(user_id=user_id)
    t1 = time.time()
    expired = sp_oauth.is_token_expired(token_info)
    expired_ms = int((time.time() - t1) * 1000)
    if not expired:
        _evt("token-internal", user=user_id, outcome="cached",
             cache_ms=cache_ms, expired_ms=expired_ms,
             total_ms=int((time.time() - t0) * 1000))
        return token_info
    original_scope = token_info.get("scope") or ""
    t2 = time.time()
    try:
        new_token = sp_oauth.refresh_access_token(token_info["refresh_token"])
    except Exception as exc:
        refresh_ms = int((time.time() - t2) * 1000)
        _evt("token-internal", user=user_id, outcome="refresh_failed",
             cache_ms=cache_ms, expired_ms=expired_ms, refresh_ms=refresh_ms,
             err=repr(exc)[:160])
        _evt("token", user=user_id, action="refresh_failed", err=repr(exc)[:160])
        return None
    refresh_ms = int((time.time() - t2) * 1000)
    if original_scope:
        new_token["scope"] = original_scope
    t3 = time.time()
    cache.save_token_to_cache(new_token)
    save_ms = int((time.time() - t3) * 1000)
    _evt("token-internal", user=user_id, outcome="refreshed",
         cache_ms=cache_ms, expired_ms=expired_ms,
         refresh_ms=refresh_ms, save_ms=save_ms,
         total_ms=int((time.time() - t0) * 1000))
    return new_token


def _token_has_playlist_modify(user_id: str) -> bool:
    """True if the stored token grants playlist-modify-private. Used to
    decide whether to push DIG saves up to the user's DIG playlist or just
    record locally (DIG-only)."""
    if not user_id:
        return False
    row = fetchone("SELECT token_data FROM user_tokens WHERE user_id = %s", (user_id,))
    if not row or not row.get("token_data"):
        return False
    scope = (row["token_data"] or {}).get("scope") or ""
    return _PLAYLIST_MODIFY_SCOPE in scope.split()


def _get_or_create_dig_playlist(user_id):
    """Return the Spotify playlist id for this user's "DIG" playlist,
    creating it lazily on first use. The id is cached in users.dig_playlist_id.

    If the user previously had a DIG playlist but deleted it on Spotify, the
    add-tracks call will 404 — the caller (`_spotify_playlist_call`) detects
    that, clears the column, and retries via this function to recreate.
    """
    if not user_id:
        return None
    row = fetchone("SELECT dig_playlist_id FROM users WHERE id = %s", (user_id,))
    if row and row.get("dig_playlist_id"):
        return row["dig_playlist_id"]
    # Lazy create
    token_info = _user_token_or_refresh(user_id)
    if not token_info:
        return None
    body = json.dumps({
        "name": DIG_PLAYLIST_NAME,
        "description": DIG_PLAYLIST_DESC,
        "public": False,
    }).encode()
    req = urllib.request.Request(
        f"https://api.spotify.com/v1/users/{urllib.parse.quote(user_id)}/playlists",
        data=body, method="POST",
        headers={
            "Authorization": f"Bearer {token_info['access_token']}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
        playlist_id = data.get("id")
        if not playlist_id:
            _evt("dig-playlist", user=user_id, action="create", outcome="error", reason="no_id_in_response")
            return None
        # Persist
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET dig_playlist_id = %s WHERE id = %s",
                            (playlist_id, user_id))
            conn.commit()
        finally:
            conn.close()
        _evt("dig-playlist", user=user_id, action="create", outcome="ok", playlist=playlist_id)
        return playlist_id
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            body = "<no body>"
        _evt("dig-playlist", user=user_id, action="create", outcome="error",
             status=exc.code, body=body)
        return None
    except Exception as exc:
        _evt("dig-playlist", user=user_id, action="create", outcome="error",
             reason=repr(exc)[:200])
        return None


def _token_has_scope(user_id: str, scope_name: str) -> bool:
    if not user_id:
        return False
    row = fetchone("SELECT token_data FROM user_tokens WHERE user_id = %s", (user_id,))
    scope = ((row or {}).get("token_data") or {}).get("scope") or ""
    return scope_name in scope.split()


def _save_scope_ok(user_id: str) -> bool:
    """Whether the stored token can mirror to this user's chosen destination.

    Liked Songs needs a different scope from playlists, so asking "can we
    mirror?" only has an answer once you know where they're mirroring to. A
    false here makes the frontend prompt a re-link rather than silently
    dropping the save.
    """
    dest, _ = get_save_destination(user_id)
    if dest == SAVE_DEST_LIKED:
        return _token_has_scope(user_id, _LIBRARY_MODIFY_SCOPE)
    return _token_has_scope(user_id, _PLAYLIST_MODIFY_SCOPE)


def get_save_destination(user_id: str):
    """(destination, playlist_id) for this user, falling back to the default.

    A 'playlist' destination with no id stored is treated as unset rather than
    broken — otherwise deleting the chosen playlist would silently stop saves
    from mirroring at all.
    """
    row = fetchone(
        "SELECT save_destination, save_playlist_id FROM users WHERE id = %s",
        (user_id,))
    dest = (row or {}).get("save_destination") or SAVE_DEST_DIG
    pid = (row or {}).get("save_playlist_id")
    if dest == SAVE_DEST_PLAYLIST and not pid:
        return SAVE_DEST_DIG, None
    if dest not in SAVE_DESTINATIONS:
        return SAVE_DEST_DIG, None
    return dest, pid


def _spotify_library_call(user_id: str, action: str, track_id: str) -> bool:
    """Add or remove one track from the user's Spotify Liked Songs."""
    token_info = _user_token_or_refresh(user_id)
    if not token_info:
        _evt("spotify-library", action=action, user=user_id, id=track_id,
             ok=False, reason="no_token")
        return False
    req = urllib.request.Request(
        "https://api.spotify.com/v1/me/tracks",
        data=json.dumps({"ids": [track_id]}).encode(),
        method="PUT" if action == "add" else "DELETE",
        headers={"Authorization": f"Bearer {token_info['access_token']}",
                 "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            ok = 200 <= resp.status < 300
        _evt("spotify-library", action=action, user=user_id, id=track_id, ok=ok)
        return ok
    except Exception as exc:
        _evt("spotify-library", action=action, user=user_id, id=track_id,
             ok=False, reason=repr(exc)[:120])
        return False


def mirror_save(user_id: str, action: str, track_id: str) -> bool:
    """Mirror a DIG save/unsave to wherever this user asked it to go.

    DIG's own history stays the source of truth either way — a failure here is
    logged and swallowed, never surfaced as a failed like.
    """
    dest, pid = get_save_destination(user_id)
    if dest == SAVE_DEST_LIKED:
        if not _token_has_scope(user_id, _LIBRARY_MODIFY_SCOPE):
            _evt("save-mirror", user=user_id, dest=dest, ok=False,
                 reason="missing_library_modify_scope")
            return False
        return _spotify_library_call(user_id, action, track_id)
    if not _token_has_scope(user_id, _PLAYLIST_MODIFY_SCOPE):
        _evt("save-mirror", user=user_id, dest=dest, ok=False,
             reason="missing_playlist_modify_scope")
        return False
    return _spotify_playlist_call(user_id, action, track_id,
                                  playlist_id=pid if dest == SAVE_DEST_PLAYLIST else None)


def _spotify_playlist_call(user_id: str, action: str, track_id: str,
                           playlist_id: str = None) -> bool:
    """Add or remove one track from the user's "DIG" playlist.
    `action` is "add" or "remove". Refreshes the token if expired. Returns
    True on 2xx, False on any failure (logged but not raised — DIG's local
    state is the source of truth).

    Provisions the DIG playlist on first call. If we have a stale playlist
    id (user deleted the playlist on Spotify) the add returns 404; we clear
    the column and recreate on the next save."""
    if not user_id or not track_id or action not in ("add", "remove"):
        return False
    token_info = _user_token_or_refresh(user_id)
    if not token_info:
        _evt("spotify-playlist", action=action, user=user_id, id=track_id,
             ok=False, reason="no_token")
        return False

    def _do_call(playlist_id):
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        uri = f"spotify:track:{track_id}"
        if action == "add":
            method = "POST"
            body = json.dumps({"uris": [uri]}).encode()
        else:
            method = "DELETE"
            body = json.dumps({"tracks": [{"uri": uri}]}).encode()
        req = urllib.request.Request(
            url, data=body, method=method,
            headers={
                "Authorization": f"Bearer {token_info['access_token']}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.status

    # A caller-supplied id means the user chose one of their own playlists;
    # only the DIG playlist is ours to provision or recreate.
    is_dig_playlist = playlist_id is None
    try:
        if is_dig_playlist:
            playlist_id = _get_or_create_dig_playlist(user_id)
        if not playlist_id:
            _evt("spotify-playlist", action=action, user=user_id, id=track_id,
                 ok=False, reason="no_playlist_provisioned")
            return False
        status = _do_call(playlist_id)
        ok = 200 <= status < 300
        _evt("spotify-playlist", action=action, user=user_id, id=track_id,
             playlist=playlist_id, ok=ok, status=status)
        return ok
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            body = "<no body>"
        # 404 on add usually means the cached playlist was deleted on
        # Spotify. Clear the column and retry once with a fresh playlist.
        if exc.code == 404 and action == "add" and is_dig_playlist:
            _evt("spotify-playlist", action=action, user=user_id, id=track_id,
                 ok=False, status=404, body=body, reason="stale_playlist_id_clearing")
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET dig_playlist_id = NULL WHERE id = %s", (user_id,))
                conn.commit()
            finally:
                conn.close()
            try:
                new_id = _get_or_create_dig_playlist(user_id)
                if not new_id:
                    return False
                status = _do_call(new_id)
                ok = 200 <= status < 300
                _evt("spotify-playlist", action=action, user=user_id, id=track_id,
                     playlist=new_id, ok=ok, status=status, retried=True)
                return ok
            except Exception as exc2:
                _evt("spotify-playlist", action=action, user=user_id, id=track_id,
                     ok=False, reason=f"retry_failed:{exc2!r}"[:160])
                return False
        _evt("spotify-playlist", action=action, user=user_id, id=track_id,
             ok=False, status=exc.code, body=body)
        return False
    except Exception as exc:
        _evt("spotify-playlist", action=action, user=user_id, id=track_id,
             ok=False, reason=repr(exc)[:200])
        return False

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

# ── Waitlist notification (email Tommaso when someone requests access) ─────────
# Sends via Gmail SMTP. Needs an App Password (Google account → Security →
# 2-Step Verification → App passwords) in the prod env as GMAIL_SMTP_USER +
# GMAIL_SMTP_PASS. No-ops cleanly if unset, so the form never breaks.
WAITLIST_NOTIFY_TO = os.environ.get("WAITLIST_NOTIFY_TO", "tommasosaggini@gmail.com")
_SMTP_USER = os.environ.get("GMAIL_SMTP_USER", "")
_SMTP_PASS = os.environ.get("GMAIL_SMTP_PASS", "")


def notify_new_waitlist(email, name):
    """Fire-and-forget admin email on a NEW waitlist request. Runs in a daemon
    thread so it never blocks the user's submit, and swallows all errors."""
    if not (_SMTP_USER and _SMTP_PASS):
        return

    def _send():
        try:
            msg = EmailMessage()
            msg["Subject"] = f"🎧 Dig waitlist — {email}"
            msg["From"] = _SMTP_USER
            msg["To"] = WAITLIST_NOTIFY_TO
            msg.set_content(
                f"New Dig access request.\n\n"
                f"Email: {email}\n"
                f"Name:  {name or '(none)'}\n\n"
                f"Approve here: https://diiiiiiiig.xyz/waitlist-admin.html\n"
            )
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as s:
                s.starttls()
                s.login(_SMTP_USER, _SMTP_PASS)
                s.send_message(msg)
        except Exception as e:
            print(f"[waitlist-notify] send failed: {e!r}")

    threading.Thread(target=_send, daemon=True).start()


# ── Transactional email (magic-link sign-in) ─────────────────────────────────
# Pluggable: prefer Brevo SMTP relay (verified sender, good deliverability —
# already used by TrustBuild on this server) when BREVO_SMTP_* are set, else
# fall back to Gmail SMTP. MAIL_FROM must be a Brevo-VERIFIED sender.
BREVO_SMTP_HOST = os.environ.get("BREVO_SMTP_HOST", "smtp-relay.brevo.com")
BREVO_SMTP_PORT = int(os.environ.get("BREVO_SMTP_PORT", "587"))
BREVO_SMTP_USER = os.environ.get("BREVO_SMTP_USER", "")
BREVO_SMTP_KEY = os.environ.get("BREVO_SMTP_KEY", "")
MAIL_FROM = os.environ.get("DIG_MAIL_FROM", _SMTP_USER or "tommasosaggini@gmail.com")
MAIL_FROM_NAME = os.environ.get("DIG_MAIL_FROM_NAME", "Dig")
# auto | brevo | gmail. Use 'gmail' to send natively from a personal @gmail
# sender (better than Brevo-as-gmail.com, which fails DMARC alignment). Switch
# to 'brevo' once MAIL_FROM is a Brevo-verified domain sender (e.g. a Dig domain).
MAIL_PROVIDER = os.environ.get("MAIL_PROVIDER", "auto")


def _build_email(to, subject, text, html, from_addr):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{MAIL_FROM_NAME} <{from_addr}>"
    msg["To"] = to
    msg.set_content(text)
    if html:
        msg.add_alternative(html, subtype="html")
    return msg


def send_email(to, subject, text, html=None):
    """Send one transactional email. Returns True on success. Never raises."""
    # Brevo SMTP relay (preferred).
    if BREVO_SMTP_USER and BREVO_SMTP_KEY:
        try:
            msg = _build_email(to, subject, text, html, MAIL_FROM)
            with smtplib.SMTP(BREVO_SMTP_HOST, BREVO_SMTP_PORT, timeout=15) as s:
                s.starttls()
                s.login(BREVO_SMTP_USER, BREVO_SMTP_KEY)
                s.send_message(msg)
            return True
        except Exception as e:
            print(f"[mail] brevo smtp failed: {e!r}; trying gmail")
    # Gmail SMTP fallback.
    if _SMTP_USER and _SMTP_PASS:
        try:
            msg = _build_email(to, subject, text, html, _SMTP_USER)
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as s:
                s.starttls()
                s.login(_SMTP_USER, _SMTP_PASS)
                s.send_message(msg)
            return True
        except Exception as e:
            print(f"[mail] gmail smtp failed: {e!r}")
    print("[mail] no email transport configured")
    return False


def db_create_login_token(email, guest_id):
    """Mint a single-use, 30-minute magic-link token. Returns the token."""
    token = secrets.token_urlsafe(32)
    execute(
        """
        INSERT INTO login_tokens (token, email, guest_id, created_at, expires_at, used)
        VALUES (%s, %s, %s, NOW(), NOW() + INTERVAL '30 minutes', false)
        """,
        (token, email, guest_id),
    )
    return token


def db_consume_login_token(token):
    """Validate + single-use-consume a token. Returns {email, guest_id} or None."""
    if not token:
        return None
    row = fetchone(
        "SELECT email, guest_id FROM login_tokens "
        "WHERE token = %s AND used = false AND expires_at > NOW()",
        (token,),
    )
    if row:
        execute("UPDATE login_tokens SET used = true WHERE token = %s", (token,))
    return row


# JSON data files the app fetches as static assets (served from project root)
_DATA_FILES = {
    "data.json", "genre_map.json",
    "track_map.json", "catalog.json", "discovery_youtube.json",
}

# These were served raw, uncompressed, uncached, and re-read from disk on every
# request. On a phone that is ~1.1 MB of avoidable transfer per app launch
# (genre_map 755K + data 359K), downloaded IN PARALLEL with the /discovery
# fetch that actually blocks playback — so it stole bandwidth from the critical
# path. They gzip ~76%.
#
# catalog.json is 47 MB, so we do NOT hold every _DATA_FILE in RAM or gzip it
# per request: files above this cap are streamed raw and uncompressed, exactly
# as before. The app's startup path only touches the small ones.
_STATIC_CACHE_MAX_BYTES = 8 * 1024 * 1024
# Ceiling on a single JSON request body. The largest legitimate one is the
# bulk-history POST at a few hundred KB; this leaves ample room.
_MAX_JSON_BODY_BYTES = 8 * 1024 * 1024
_static_cache = {}          # fname -> {mtime, size, etag, raw, gz}
_static_cache_lock = threading.Lock()


def _static_entry(fname, filepath):
    """Return a cached {etag, raw, gz} for a small data file, or None if the
    file is too big to cache. Re-reads only when mtime/size change, so a cron
    run that rewrites the file is picked up without a server restart."""
    st = os.stat(filepath)
    if st.st_size > _STATIC_CACHE_MAX_BYTES:
        return None
    with _static_cache_lock:
        hit = _static_cache.get(fname)
        if hit and hit["mtime"] == st.st_mtime and hit["size"] == st.st_size:
            return hit
    raw = open(filepath, "rb").read()
    entry = {
        "mtime": st.st_mtime,
        "size": st.st_size,
        # Content-derived, so it stays correct even if mtime moves without an
        # edit (deploy, rsync, touch) — avoids serving a stale 304.
        "etag": '"%s"' % hashlib.sha1(raw).hexdigest()[:16],
        "raw": raw,
        "gz": gzip.compress(raw, 5),
    }
    with _static_cache_lock:
        _static_cache[fname] = entry
    return entry


_JS_SRC_RE = re.compile(rb'(?:src|href)="(/js/[A-Za-z0-9_./-]+\.js)"')
# `from './env.js'` / `import './x.js'` — the specifiers a browser resolves
# itself, which never pass through the HTML and so were never stamped.
_JS_IMPORT_RE = re.compile(rb"""(['"])(\./[A-Za-z0-9_.-]+\.js)\1""")

_js_graph_cache = {}        # rel -> {sig, hash, body, gz}
_js_graph_lock = threading.Lock()


def _js_bytes(rel):
    """Raw bytes of web/<rel>, via the mtime-checked static cache."""
    entry = _static_entry(rel, os.path.join(WEB_DIR, rel))
    if entry:
        return entry["raw"]
    with open(os.path.join(WEB_DIR, rel), "rb") as fh:
        return fh.read()


def _js_deps(rel, raw):
    """The modules `rel` imports directly, as web-relative paths."""
    base = rel.rsplit("/", 1)[0]
    out = []
    for m in _JS_IMPORT_RE.finditer(raw):
        dep = f"{base}/{m.group(2).decode()[2:]}"
        if dep not in out and os.path.isfile(os.path.join(WEB_DIR, dep)):
            out.append(dep)
    return out


def _js_module(rel, _stack=()):
    """A module ready to serve: its transitive-closure hash and rewritten body.

    THE HASH COVERS THE WHOLE GRAPH, not just this file. A module's own bytes
    are not enough to name what a browser will end up running: if app.js were
    stamped with only its own hash, editing env.js would change nothing about
    app.js's URL, a client holding an immutable copy would never re-fetch it,
    and — because the import specifier inside it is stamped too — would never
    learn there is a new env.js either. Editing a leaf would silently reach
    nobody. Hashing the closure makes any change anywhere change every URL that
    can reach it, which is the only version of this that is safe to serve
    `immutable`.

    The BODY is rewritten so `from './env.js'` becomes `from './env.js?v=…'`.
    Without it the browser resolves bare specifiers itself, those requests never
    pass through the HTML, and every imported module falls back to revalidating
    on each launch — a round trip per module on exactly the mobile path this
    caching exists to keep quiet.
    """
    if rel in _stack:                      # import cycle: hash what we have
        return {"hash": "", "body": b"", "gz": b""}
    raw = _js_bytes(rel)
    deps = _js_deps(rel, raw)
    sub = {d: _js_module(d, _stack + (rel,)) for d in deps}
    sig = hashlib.sha1(raw + b"".join(
        s["hash"].encode() for s in sub.values())).hexdigest()[:16]

    with _js_graph_lock:
        hit = _js_graph_cache.get(rel)
        if hit and hit["sig"] == sig:
            return hit

    body = raw
    for dep, s in sub.items():
        name = dep.rsplit("/", 1)[1].encode()
        body = re.sub(rb"(['\"])\./" + re.escape(name) + rb"\1",
                      b"'./" + name + b"?v=" + s["hash"].encode() + b"'", body)
    out = {"sig": sig, "hash": sig, "body": body, "gz": gzip.compress(body, 5)}
    with _js_graph_lock:
        _js_graph_cache[rel] = out
    return out


def _stamp_module_urls(html: bytes) -> bytes:
    """Rewrite `/js/x.js` in served HTML to `/js/x.js?v=<content hash>`.

    The deploy is scp onto a running server with no build step and no CI, and
    app.html is read from disk per request — so the HTML is always fresh, but a
    plain `<script src="/js/x.js">` is not: the browser would keep whatever it
    cached, and a deploy would leave people running a stale module against new
    markup. That is a worse failure than no caching at all, because it is
    invisible and it varies per user.

    Stamping the hash into the URL makes the two agree by construction. A
    changed module gets a new URL and is fetched; an unchanged one keeps its URL
    and is not requested at all — better than revalidation, which would cost a
    round trip per module per launch on a phone, on the one path where latency
    is most felt.

    Failure here must never take the page down: an unstamped URL still loads,
    it just falls back to the conservative no-cache headers below.
    """
    def stamp(m):
        url = m.group(1).decode()
        try:
            digest = _js_module(url.lstrip("/"))["hash"]
        except OSError:
            return m.group(0)
        if not digest:
            return m.group(0)
        return m.group(0).replace(m.group(1), m.group(1) + b"?v=" + digest.encode())
    return _JS_SRC_RE.sub(stamp, html)


def _spotify_devices(headers):
    """The user's Spotify Connect devices. [] on any failure — this is a
    recovery path and must never be the thing that fails a play."""
    try:
        req = urllib.request.Request(
            "https://api.spotify.com/v1/me/player/devices", headers=headers)
        with urllib.request.urlopen(req, timeout=6) as r:
            return json.loads(r.read().decode("utf-8")).get("devices") or []
    except Exception:
        return []


def _pick_playback_device(devices, wanted_id=None):
    """Which device to wake when Spotify says there is no active one.

    `PUT /me/player/play` with no device_id requires an ALREADY-ACTIVE device.
    A phone app that got backgrounded stays in this list but flips to
    is_active=false, and every device-less play then 404s "No active device
    found" — while the device sits right there, waitable. That was the whole
    bug: iOS pins no device (Connect mode), so nothing ever transferred, the
    "retry without device" reissued the identical failing request, and playback
    fell through to a deep link that silently did nothing on a locked phone.

    is_restricted devices reject API control outright, so they're never
    candidates. Preference order: the device the CALLER named → already active
    → a phone (on iOS that IS the user's player).

    There is deliberately no "whatever is left" fallback any more. Every
    account-wide device is a candidate here, including a laptop in another
    building: on 2026-07-31 a phone outside the house dispatched a track, sent
    no device id (42% of plays do), and this function handed it an idle `DIG`
    web-SDK device belonging to a Mac at home. Spotify answered 204 because the
    device was listed, the audio came out of the Mac, and the phone showed a
    progress bar stuck at 0 — the poll read truthPos=0 forever while the
    interpolator kept walking it forward, which is the bar "cycling 0 to 1".

    The client learned this rule already and says so in app.html: never adopt an
    inactive `DIG`, because it routes playback to a device nobody is listening
    to. Returning None here is not a dead end — it produces the `no_device`
    404 the client already handles with the "Spotify went to sleep" banner,
    which is the honest answer when we cannot tell which speaker the user is
    actually next to. Guessing is what put the music in an empty house.
    """
    usable = [d for d in devices if d.get("id") and not d.get("is_restricted")]
    if not usable:
        return None
    for pred in (lambda d: wanted_id and d.get("id") == wanted_id,
                 lambda d: d.get("is_active"),
                 lambda d: d.get("type") == "Smartphone"):
        for d in usable:
            if pred(d):
                return d
    return None


def _bootstrap_sample(disc, limit):
    """Region-balanced subset of the {region: [tracks]} discovery map.

    The full pool is ~10 MB / ~28k tracks and the client cannot play ANYTHING
    until it has downloaded and parsed all of it — that was the 20-30s cold
    start on mobile. So the client now asks for a small first batch, starts
    playing, and pulls the full pool in the background.

    Round-robin across regions rather than truncating: the picker's contract is
    to differ from recent plays on artist/genre/country and to favour
    under-served cells, and a first-N slice would hand it one or two regions and
    silently break both guarantees for the opening tracks.
    """
    buckets = [(r, ts) for r, ts in disc.items() if isinstance(ts, list) and ts]
    if not buckets or limit <= 0:
        return disc
    out = {r: [] for r, _ in buckets}
    taken, i = 0, 0
    while taken < limit:
        progressed = False
        for region, tracks in buckets:
            if i >= len(tracks):
                continue
            out[region].append(tracks[i])
            taken += 1
            progressed = True
            if taken >= limit:
                break
        if not progressed:
            break                     # every region exhausted before hitting limit
        i += 1
    return {r: ts for r, ts in out.items() if ts}


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

def make_sp_oauth(user_id=None, force_consent=False):
    handler = DbCacheHandler(user_id) if user_id else None
    return SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        cache_handler=handler,
        open_browser=False,
        # Force the consent screen so a user whose stored token is missing
        # scopes re-grants the FULL set. Without this, Spotify silently reissues
        # the same narrow grant and the SDK auth lockout persists.
        show_dialog=force_consent,
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


def _bandcamp_backfill_genres(track_id, tags, location=""):
    """Enrich a Bandcamp track's stored genres from the rich tag set returned by
    a play-time /api/bandcamp/resolve (sub-genres), at ZERO extra Bandcamp calls
    — we already fetch these tags on every play. Normalizes via
    bandcamp.normalize_genres (drops the artist's city + place tags, splits
    slash-joined genres, aliases loose spellings), and ALSO re-normalizes the
    existing genres so old pollution (e.g. a 'montreal' that slipped in before
    this filter) self-heals on the next play. Idempotent; writes only when the
    list actually changes. Daemon thread, off the hot playback path; best-effort.
    """
    try:
        from lib import bandcamp
        loc_tokens = bandcamp.location_tokens(location)
        row = fetchone("SELECT genres FROM tracks WHERE id = %s", (track_id,))
        if row is None:
            return
        existing = list(row.get("genres") or [])
        # Clean existing (heal prior pollution) + fold in the fresh tags, in one
        # normalized pass; existing first so the original primary genre stays.
        merged = bandcamp.normalize_genres(existing + list(tags or []), loc_tokens)
        merged = merged[:10]  # cap — keeps the array bounded
        if merged != existing:
            execute("UPDATE tracks SET genres = %s WHERE id = %s", (merged, track_id))
    except Exception:
        pass  # enrichment is best-effort; playback already succeeded


def db_get_profile(user_id):
    return fetchone("SELECT id, display_name, email, image_url FROM users WHERE id = %s", (user_id,))


# ── Access control (invite-only waitlist) ─────────────────────────────────────
ADMIN_UID = os.environ.get("ADMIN_UID", "")


def ensure_access_schema():
    """Idempotent: add the approved flag + access_requests table."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "approved BOOLEAN NOT NULL DEFAULT false"
            )
            # Playback source per history row ('spotify' | 'bandcamp' | …).
            # Legacy rows are NULL → treated as 'spotify' by readers. Previously
            # source was only inferable from a 'bc:' track_id prefix; storing it
            # explicitly makes it queryable (per-source analytics/filtering).
            cur.execute(
                "ALTER TABLE user_history ADD COLUMN IF NOT EXISTS source TEXT"
            )
            # Where a DIG save is mirrored on Spotify. Default keeps the
            # existing behaviour — an auto-created private "DIG" playlist — so
            # nobody's library changes shape without asking.
            #   'dig_playlist'  the playlist DIG makes for them (default)
            #   'liked_songs'   their Spotify Liked Songs
            #   'playlist'      a playlist they picked; id in save_playlist_id
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                "save_destination TEXT NOT NULL DEFAULT 'dig_playlist'"
            )
            cur.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS save_playlist_id TEXT"
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS access_requests (
                    id          BIGSERIAL PRIMARY KEY,
                    email       TEXT UNIQUE NOT NULL,
                    name        TEXT NOT NULL DEFAULT '',
                    spotify_uid TEXT,
                    status      TEXT NOT NULL DEFAULT 'pending',  -- pending | approved
                    source      TEXT NOT NULL DEFAULT 'form',     -- form | spotify
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
        conn.commit()
    finally:
        conn.close()


def is_approved(user_id):
    if not user_id:
        return False
    if user_id and user_id == ADMIN_UID:
        return True
    row = fetchone("SELECT approved FROM users WHERE id = %s", (user_id,))
    return bool(row and row["approved"])


def reconcile_access(uid, email, name):
    """On login: promote to approved if their email was approved, else record
    a pending access request keyed by their verified Spotify email."""
    if uid == ADMIN_UID:
        execute("UPDATE users SET approved = true WHERE id = %s", (uid,))
        return
    prof = fetchone("SELECT approved FROM users WHERE id = %s", (uid,))
    if prof and prof["approved"]:
        return
    if email:
        ar = fetchone(
            "SELECT status FROM access_requests WHERE lower(email) = lower(%s)",
            (email,),
        )
        if ar and ar["status"] == "approved":
            execute("UPDATE users SET approved = true WHERE id = %s", (uid,))
            return
        execute(
            """
            INSERT INTO access_requests (email, name, spotify_uid, status, source)
            VALUES (%s, %s, %s, 'pending', 'spotify')
            ON CONFLICT (email) DO UPDATE SET
                name = EXCLUDED.name, spotify_uid = EXCLUDED.spotify_uid
            """,
            (email, name or "", uid),
        )


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


def db_undislike(user_id, track_key):
    """Remove the 'disliked' status for a track and demote to 'known'.

    Mirrors db_unsave's behaviour for dislikes: clears the ledger row's
    'disliked' flag (down to 'known') and demotes any 'disliked' history
    rows back to 'listened'."""
    key = (track_key or "").lower().strip()
    if not key:
        return
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_ledger (user_id, track_key, status)
                VALUES (%s, %s, 'known')
                ON CONFLICT (user_id, track_key) DO UPDATE SET status = 'known'
                WHERE user_ledger.status = 'disliked'
                """,
                (user_id, key),
            )
            cur.execute(
                """
                UPDATE user_history
                SET status = 'listened'
                WHERE user_id = %s
                  AND status = 'disliked'
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


def db_get_user_coverage(user_id):
    """Per-user genre, country and ARTIST exposure counts, derived live from
    user_history × tracks. Used by the Discovery picker to weight against
    over-played cells — implements ARCHITECTURE.md Principle 1 (breadth
    first; every region/genre deserves a foothold).

    Country axis prefers `origin_region` (MusicBrainz country) and falls
    back to `region` (macro). Returns:
        {"genres":    {"ambient dub": 46, "indie rock": 20, ...},
         "countries": {"USA": 160, "Japan": 42, ...},
         "artists":   {"otim alpha": 4, ...}}

    THE ARTIST AXIS WAS MISSING, and its absence was the whole of the
    "why do I keep hearing the same artists" complaint. The picker weights
    toward genres the listener has played least, and Spotify's taxonomy is
    hyper-specific: measured 2026-08-01, 1,894 of the pool's 3,736 genres
    (51%) belong to exactly ONE artist. "acholi music" is five tracks, four
    of them Otim Alpha. So "explore an unheard genre" and "play this one
    artist again" are, half the time, the same instruction — and nothing
    counted artists, so nothing could tell them apart.

    Measured over this user's 10,976 plays: 1,583 were a repeat artist
    (14.4%), against 1,010 (9.2%) expected from drawing the same number of
    tracks uniformly at random from the pool. The picker was 1.6x WORSE than
    chance on the axis the listener actually perceives.

    Counted per collaborator and lower-cased to match the client's
    _allArtists(), so "Otim Alpha, Umoja" credits Otim Alpha too — the
    listener hears the same voice either way.
    """
    if not user_id:
        return {"genres": {}, "countries": {}, "artists": {}}
    genre_rows = fetchall(
        """
        SELECT g, count(*)::int AS plays
        FROM user_history h
        JOIN tracks t ON t.id = h.track_id
        CROSS JOIN LATERAL unnest(t.genres) AS g
        WHERE h.user_id = %s
        GROUP BY g
        """,
        (user_id,),
    )
    country_rows = fetchall(
        """
        SELECT COALESCE(t.origin_region, t.region) AS c, count(*)::int AS plays
        FROM user_history h
        JOIN tracks t ON t.id = h.track_id
        WHERE h.user_id = %s
          AND COALESCE(t.origin_region, t.region) IS NOT NULL
        GROUP BY COALESCE(t.origin_region, t.region)
        """,
        (user_id,),
    )
    # Split in Python rather than SQL: the artist column is a display string
    # ("Otim Alpha, Umoja"), the split rule has to match the client's
    # _allArtists() exactly, and having it in two languages is how the two
    # sides would drift into disagreeing about who you have heard.
    artist_rows = fetchall(
        "SELECT artist FROM user_history WHERE user_id = %s AND artist IS NOT NULL",
        (user_id,),
    )
    artists = {}
    for r in artist_rows:
        for name in str(r["artist"]).split(","):
            name = name.strip().lower()
            if name:
                artists[name] = artists.get(name, 0) + 1

    return {
        "genres":    {r["g"]: r["plays"] for r in genre_rows},
        "countries": {r["c"]: r["plays"] for r in country_rows},
        "artists":   artists,
    }


def db_get_history(user_id):
    rows = fetchall(
        """
        SELECT track_id AS id, track_name AS track, artist, region, status,
               listened_at AS time, played_pct, mode,
               COALESCE(source,
                        CASE WHEN track_id LIKE 'bc:%%' THEN 'bandcamp'
                             ELSE 'spotify' END) AS source
        FROM user_history WHERE user_id = %s ORDER BY listened_at DESC
        """,
        (user_id,),
    )
    return [dict(r) for r in rows]


def db_save_history(user_id, history_list):
    """MERGE a user's history (called from POST /history).

    Used to be `DELETE WHERE user_id` + re-insert the browser's localStorage
    wholesale — the server was a mirror of one device. That had two costs and
    the second is why this changed:

      1. A browser whose localStorage had been cleared wiped the server copy
         on its first sync.
      2. It made a second writer impossible. lib/spotify_sync pulls the plays
         and likes that DIG never dispatched (26 of the last 50 plays were
         missing on 2026-08-03), and every one of those rows lived only until
         the next /history POST — minutes.

    Now it upserts on (user_id, track_id) and the server holds the UNION of
    what every writer knows. Conflicts resolve by dig_status_rank(), so an
    automatic 'listened' can never overwrite the listener's 'saved' —
    including the real race this opens up, where the browser posts a stale
    'listened' for a track the pull has since learned was liked on Spotify.

    Also mirrors any `disliked` items into `user_ledger` so a persistent
    track-level dislike flag survives history pruning. Track-level only —
    never generalizes to genre/region.
    """
    from lib.spotify_sync import UPSERT_HISTORY_SQL
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for item in history_list:
                if not item or not item.get("id"):
                    continue          # no key to merge on; a row we could never update
                # Persist the playback source. Trust the client's value; if
                # absent, infer from the id form ('bc:' = Bandcamp) so it's
                # correct even for older clients / replayed rows.
                src = item.get("source")
                if not src:
                    src = "bandcamp" if str(item.get("id") or "").startswith("bc:") else "spotify"
                cur.execute(UPSERT_HISTORY_SQL, (
                    user_id,
                    item.get("id"),
                    item.get("track"),
                    item.get("artist"),
                    item.get("region"),
                    item.get("status"),
                    item.get("time"),
                    item.get("played_pct"),
                    item.get("mode"),
                    src,
                ))
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


def _spawn_history_sync(user_id):
    """Run the Spotify listened/liked pull off the request thread.

    Fire-and-forget by design: nothing downstream waits on it, and a Spotify
    failure must never reach the /history response. sync_user rate-gates
    itself, so calling this on every /history GET is cheap — a page reload
    inside the window does no network work at all.
    """
    def _run():
        try:
            from lib import spotify_sync
            summary = spotify_sync.sync_user(
                user_id, _user_token_or_refresh(user_id))
            if summary.get("ran"):
                _evt("spotify-sync", user=user_id, trigger="history",
                     **{k: v for k, v in summary.items()
                        if k not in ("user", "ran")})
        except Exception as e:
            _evt("spotify-sync", user=user_id, trigger="history",
                 ok=False, err=repr(e)[:200])
            _health_record_error(f"spotify sync: {e}")
    threading.Thread(target=_run, daemon=True).start()


# ── Instagram pipeline triggers (run off the request thread) ──────────────────

def _ig_run_propose(n=None):
    try:
        from pipeline.ig_propose import propose
        propose(n)
    except Exception as e:
        print(f"[ig propose] {e!r}")


def _ig_run_resolve(item_id, skip=0):
    try:
        import glob
        from lib.ig_audio import resolve_audio, AudioResolveError
        item = ig_queue.get_item(item_id)
        if not item:
            return
        if skip:
            # Clear the rejected download first: yt-dlp writes source.<ext> and
            # a different candidate may land on a different extension, which
            # would otherwise leave two source files and let the old one win.
            for f in glob.glob(os.path.join(ig_queue.item_dir(item_id), "source.*")):
                try:
                    os.remove(f)
                except OSError:
                    pass
        try:
            r = resolve_audio(item, skip=skip)
            ig_queue.set_audio(item_id, r["source"], r["path"],
                               r["duration_ms"], r.get("artwork_url"))
            # New audio means the old clip and render describe a file that no
            # longer exists in that form.
            ig_queue.update_item(item_id, rendered_at=None)
        except AudioResolveError as e:
            ig_queue.set_audio_failed(item_id, str(e))
    except Exception as e:
        print(f"[ig resolve] {e!r}")


def _ig_run_render(item_id):
    try:
        from pipeline.ig_render import render_item
        item = ig_queue.get_item(item_id)
        if item:
            render_item(item)
    except Exception as e:
        print(f"[ig render] {e!r}")
        try:
            ig_queue.update_item(item_id, error=str(e)[:500])
        except Exception:
            pass


# ── HTTP Handler ──────────────────────────────────────────────────────────────

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def handle_one_request(self):
        # A client that hangs up mid-response makes every subsequent write
        # raise. send_json/serve_file_with_range guard their own writes, but
        # the stdlib's static serving and send_error() do not — and vulnerability
        # scanners fire-and-forget constantly, so each one dumped a traceback.
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def send_head(self):
        # Scanners also send %00 inside paths. SimpleHTTPRequestHandler only
        # guards its open() against OSError, so a NUL escapes as ValueError and
        # the request dies with a traceback and no response at all.
        try:
            return super().send_head()
        except ValueError:
            self.send_error(404, "File not found")
            return None

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

    # ── Guest = anonymous account (Bandcamp-only, no Spotify) ──
    # Identity lives in the signed HttpOnly dig_session (id = "guest:<rand>"), so
    # likes/history persist server-side per guest and a later email/Spotify
    # registration just attaches to the SAME id (taste preserved). A separate
    # JS-readable dig_mode flag lets app.html decide guest mode synchronously
    # (before Player.init) without exposing the identity.
    def is_guest_session(self, user_id):
        return bool(user_id and user_id.startswith("guest:"))

    def set_guest_mode_cookie(self):
        self.send_header("Set-Cookie", "dig_mode=guest; Path=/; SameSite=Lax; Max-Age=31536000")

    def clear_guest_mode_cookie(self):
        self.send_header("Set-Cookie", "dig_mode=; Path=/; Max-Age=0")

    def read_json_body(self):
        """Parse the request body as JSON, or {} if it is missing or malformed.

        Eight POST handlers spelled this out themselves, in three shapes. `{}`
        is the right default for all of them: each then reads named fields with
        `.get()` and validates, so a malformed body lands on the same path as a
        missing one.

        TWO CALLERS DELIBERATELY DO NOT USE THIS, and both would lose something
        real if they did:

          /api/client-log keeps the raw bytes on a parse failure — a malformed
          client log IS the diagnostic, so discarding it defeats the endpoint.

          /history needs the byte count and the exception TYPE for its telemetry.
          Mobile networks cut multi-hundred-KB POSTs mid-character; that path
          answers with a soft 400 and logs how far the body got, after an
          unhandled 500 whose stack trace is described in the comment there.

        A `strict=` flag was written for the second one and then removed: it
        raised a generic error, which is not what that caller needs, so the flag
        would have had no users. Neither of those is "parse the body" — they are
        "parse the body, and describe the failure" — and merging them here would
        cost the description.

        The length is capped because Content-Length is the client's claim, not a
        fact. A socket read only yields the bytes actually sent, so an inflated
        header stalls rather than allocating — but the cap keeps one request
        from tying up a thread on an unbounded read.
        """
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(min(length, _MAX_JSON_BODY_BYTES))
        try:
            return json.loads(raw.decode() or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        # gzip large payloads when the client accepts it. /discovery is ~10 MB
        # uncompressed (~1.9 MB gzipped); on a mobile connection the raw payload
        # stalled past the 30s proxy timeout → ConnectionReset → the client never
        # got its tracks → playback "did nothing". gzip is ~5x smaller and lands
        # well inside the window.
        encoding = None
        accept = (self.headers.get("Accept-Encoding") or "")
        if "gzip" in accept and len(body) > 1024:
            try:
                body = gzip.compress(body, 5)
                encoding = "gzip"
            except Exception:
                encoding = None
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        if encoding:
            self.send_header("Content-Encoding", encoding)
            self.send_header("Vary", "Accept-Encoding")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Client navigated away / reloaded mid-send — harmless, don't spam
            # the log with a traceback.
            pass

    def serve_file_with_range(self, path, content_type):
        """Stream a file with HTTP Range support (so the dashboard's <audio>/
        <video> can seek). Used for IG-admin media previews."""
        if not os.path.exists(path):
            self.send_response(404)
            self.end_headers()
            return
        size = os.path.getsize(path)
        rng = self.headers.get("Range")
        start, end = 0, size - 1
        status = 200
        if rng and rng.startswith("bytes="):
            try:
                s, _, e = rng[len("bytes="):].partition("-")
                start = int(s) if s else 0
                end = int(e) if e else size - 1
                end = min(end, size - 1)
                status = 206
            except Exception:
                start, end, status = 0, size - 1, 200
        length = end - start + 1
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(length))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            with open(path, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(65536, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        user_id = self.get_user()
        self._req_user = user_id  # consumed by log_message
        self._req_t0 = time.time()

        # ── Public IG media ───────────────────────────────────────────────────
        # Instagram fetches the video server-side when creating a media
        # container, so feed.mp4/story.mp4 must be reachable WITHOUT auth —
        # /admin/ig/preview is ADMIN_UID-gated and unusable for that. This is
        # the only unauthenticated view of media/ig/, so it is deliberately
        # narrow: numeric item id, and a fixed set of filenames. Nothing here
        # maps caller input onto a path segment, so traversal isn't reachable.
        if parsed.path.startswith("/ig-media/"):
            IG_PUBLIC_FILES = {
                "feed.mp4": "video/mp4",
                "story.mp4": "video/mp4",
                "card_feed.png": "image/png",
                "card_story.png": "image/png",
            }
            bits = parsed.path[len("/ig-media/"):].split("/")
            if (len(bits) == 2 and bits[0].isdigit()
                    and bits[1] in IG_PUBLIC_FILES):
                self.serve_file_with_range(
                    os.path.join(ig_queue.item_dir(bits[0]), bits[1]),
                    IG_PUBLIC_FILES[bits[1]])
            else:
                self.send_response(404)
                self.end_headers()
            return

        # ── Auth flow ─────────────────────────────────────────────────────────

        if parsed.path == "/login":
            auth_url = make_sp_oauth().get_authorize_url()
            self.send_response(302)
            self.send_header("Location", auth_url)
            self.end_headers()
            return

        if parsed.path == "/reconnect":
            # Forced re-consent. Used when a user's stored token is missing
            # required scopes (e.g. `streaming`) — plain /login would silently
            # reuse the narrow grant, so we set show_dialog to make Spotify
            # re-prompt and issue a token carrying the full current SCOPE.
            auth_url = make_sp_oauth(user_id=user_id, force_consent=True).get_authorize_url()
            self.send_response(302)
            self.send_header("Location", auth_url)
            self.end_headers()
            return

        # Magic-link sign-in: verify the token, attach the email to the visitor's
        # existing guest account (taste preserved — same id), and log them in.
        if parsed.path == "/auth/verify":
            qs = urllib.parse.parse_qs(parsed.query)
            token = qs.get("token", [""])[0]
            row = db_consume_login_token(token)
            if not row:
                self.send_response(302)
                self.send_header("Location", "/?login=expired")
                self.end_headers()
                return
            email = (row["email"] or "").strip().lower()
            guest_id = row["guest_id"]
            existing = fetchone(
                "SELECT id FROM users WHERE lower(email) = lower(%s) LIMIT 1", (email,))
            if existing:
                target = existing["id"]            # returning user → their account
            elif guest_id and db_get_profile(guest_id):
                execute("UPDATE users SET email = %s WHERE id = %s", (email, guest_id))
                target = guest_id                  # upgrade this guest in place
            else:
                target = "guest:" + secrets.token_hex(12)
                db_upsert_user(target, "You", email, None)
            self.send_response(302)
            self.set_session_cookie(target)        # signed identity
            self.set_guest_mode_cookie()           # still Bandcamp mode (no Spotify)
            self.send_header("Location", "/?login=ok")
            self.end_headers()
            _evt("magic_link_login", email=email[:60], upgraded=bool(guest_id and not existing))
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
                    existing = fetchone("SELECT id FROM users WHERE id = %s", (uid,))
                    db_upsert_user(
                        uid,
                        me.get("display_name", uid),
                        me.get("email", ""),
                        me["images"][0]["url"] if me.get("images") else "",
                    )
                    reconcile_access(uid, me.get("email", ""), me.get("display_name", ""))
                    DbCacheHandler(uid).save_token_to_cache(token_info)
                    granted_scope = (token_info.get("scope") or "")
                    _evt("signin", user=uid,
                         display_name=me.get("display_name", "")[:40],
                         returning=bool(existing),
                         has_playlist_modify=(_PLAYLIST_MODIFY_SCOPE in granted_scope.split()),
                         scope_count=len(granted_scope.split()))

                    # Kick off a background import of the user's Spotify Liked
                    # Songs so DIG's pool/ledger reflects them. Non-blocking so
                    # the redirect is instant even for big libraries.
                    def _bg_import_likes(user_id=uid):
                        try:
                            from scripts.import_likes import import_likes_for_user
                            import_likes_for_user(user_id)
                        except Exception as exc:
                            print(f"[likes-sync on signin] {user_id}: FAILED {exc!r}")
                    threading.Thread(target=_bg_import_likes, daemon=True).start()

                    self.send_response(302)
                    self.set_session_cookie(uid)
                    self.send_header("Location", "/")
                    self.end_headers()
                except Exception as e:
                    _evt("signin", ok=False, err=repr(e)[:200])
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

        # Where this user's saves are mirrored, plus the playlists they could
        # point them at. One call so the settings panel opens fully populated.
        if parsed.path == "/api/save-destination":
            if not user_id:
                self.send_json({"error": "not_logged_in"}, 401)
                return
            dest, pid = get_save_destination(user_id)
            playlists = []
            if _token_has_scope(user_id, "playlist-read-private"):
                token_info = _user_token_or_refresh(user_id)
                if token_info:
                    try:
                        req = urllib.request.Request(
                            "https://api.spotify.com/v1/me/playlists?limit=50",
                            headers={"Authorization":
                                     f"Bearer {token_info['access_token']}"})
                        with urllib.request.urlopen(req, timeout=8) as resp:
                            data = json.loads(resp.read().decode())
                        # Only playlists they can actually write to — ones they
                        # own, or collaborative ones. Playlists they merely
                        # follow would fail on the first add.
                        # No track count: this endpoint omits `tracks`, and a
                        # hardcoded 0 next to a full playlist reads as a bug.
                        playlists = [
                            {"id": p["id"], "name": p.get("name") or "(untitled)"}
                            for p in (data.get("items") or [])
                            if p and ((p.get("owner") or {}).get("id") == user_id
                                      or p.get("collaborative"))
                        ]
                    except Exception as exc:
                        _evt("save-destination", user=user_id, ok=False,
                             reason=repr(exc)[:120])
            self.send_json({
                "destination": dest,
                "playlist_id": pid,
                "dig_playlist_id": (fetchone(
                    "SELECT dig_playlist_id FROM users WHERE id = %s",
                    (user_id,)) or {}).get("dig_playlist_id"),
                "playlists": playlists,
                "can_write": _save_scope_ok(user_id),
            })
            return

        if parsed.path == "/me":
            profile = db_get_profile(user_id) if user_id else None
            has_email = bool(profile and profile.get("email"))
            # Anonymous guest = guest: id with no email attached yet.
            if user_id and user_id.startswith("guest:") and not has_email:
                self.send_json({"logged_in": False, "guest": True})
                return
            if not user_id or not profile:
                self.send_json({"logged_in": False, "guest": False})
                return
            # Registered (email magic-link) OR Spotify user.
            self.send_json({
                "logged_in": True,
                "guest":     False,
                "registered": has_email,
                "approved":  is_approved(user_id),
                "is_admin":  user_id == ADMIN_UID,
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
                _evt("token", user="anon", outcome="401", reason="anon_request")
                self.send_json({"error": "not_authenticated",
                                "auth_url": make_sp_oauth().get_authorize_url()}, 401)
                return
            if not is_approved(user_id):
                _evt("token", user=user_id, outcome="403", reason="not_approved")
                self.send_json({"error": "pending_approval"}, 403)
                return
            token_info = _user_token_or_refresh(user_id)
            if not token_info:
                _evt("token", user=user_id, outcome="401", reason="no_token_or_refresh_failed")
                self.send_json({"error": "not_authenticated",
                                "auth_url": make_sp_oauth(user_id=user_id).get_authorize_url()}, 401)
                return
            granted = set((token_info.get("scope") or "").split())
            missing = _SDK_REQUIRED_SCOPES - granted
            _evt("token", user=user_id, outcome="ok",
                 scope_count=len(granted), needs_reauth=bool(missing))
            resp = {"access_token": token_info["access_token"]}
            if missing:
                # Token can't drive the SDK. Tell the client to prompt a
                # forced-consent reconnect rather than build a player that will
                # only fire authentication_error.
                resp["needs_reauth"] = True
                resp["missing_scopes"] = sorted(missing)
            self.send_json(resp)
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
                _evt("action", path=parsed.path, user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track = qs.get("track", [""])[0]
            track_id = qs.get("id", [""])[0]
            if track:
                db_add_known(user_id, track)
            # Mirror to the user's "DIG" Spotify playlist when the user has
            # granted the new scope; otherwise tell the client we need a
            # re-auth. Saves never touch the user's Spotify Liked Songs.
            needs_relink = False
            mirrored = False
            if parsed.path == "/save" and track_id:
                if _save_scope_ok(user_id):
                    mirrored = mirror_save(user_id, "add", track_id)
                else:
                    needs_relink = True
            _evt("action", path=parsed.path, user=user_id,
                 track=track[:80], id=track_id or "-",
                 mirrored_to_dig_playlist=mirrored, needs_relink=needs_relink)
            self.send_json({"ok": True, "needs_relink": needs_relink})
            return

        if parsed.path == "/unsave":
            if not user_id:
                _evt("action", path="/unsave", user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track = qs.get("track", [""])[0]
            track_id = qs.get("id", [""])[0]
            if track:
                db_unsave(user_id, track)
            needs_relink = False
            mirrored = False
            if track_id:
                if _save_scope_ok(user_id):
                    mirrored = mirror_save(user_id, "remove", track_id)
                else:
                    needs_relink = True
            _evt("action", path="/unsave", user=user_id,
                 track=track[:80], id=track_id or "-",
                 mirrored_to_dig_playlist=mirrored, needs_relink=needs_relink)
            self.send_json({"ok": True, "needs_relink": needs_relink})
            return

        if parsed.path == "/undislike":
            if not user_id:
                _evt("action", path="/undislike", user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track = qs.get("track", [""])[0]
            if track:
                db_undislike(user_id, track)
            _evt("action", path="/undislike", user=user_id, track=track[:80])
            self.send_json({"ok": True})
            return

        # ── Discovery pool (served from DB) ───────────────────────────────────

        if parsed.path == "/discovery":
            t0 = time.time()
            try:
                # Scope the pool to tracks this user hasn't heard yet. Anonymous
                # callers still get the full pool.
                disc = load_discovery(user_id=user_id)
                # Anyone not Spotify-approved (guests, email accounts, pending
                # Spotify) can only play Bandcamp — don't ship them ~28k Spotify
                # tracks they can't play.
                if not is_approved(user_id):
                    disc = {
                        r: [t for t in ts if t.get("source") == "bandcamp"]
                        for r, ts in disc.items() if isinstance(ts, list)
                    }
                    disc = {r: ts for r, ts in disc.items() if ts}
                # Bootstrap batch: ?limit=N returns a small region-balanced
                # slice so the client can start playing in ~2s instead of
                # waiting on the full ~10 MB. Applied AFTER the approval
                # filter so the batch contains only playable tracks.
                full_count = sum(len(v) for v in disc.values() if isinstance(v, list))
                try:
                    limit = int(urllib.parse.parse_qs(parsed.query).get("limit", ["0"])[0])
                except ValueError:
                    limit = 0
                if limit > 0:
                    disc = _bootstrap_sample(disc, limit)
                track_count = sum(len(v) for v in disc.values() if isinstance(v, list))
                _evt("discovery", user=user_id or "anon",
                     regions=len(disc), tracks=track_count,
                     partial=bool(limit > 0 and track_count < full_count),
                     ms=int((time.time() - t0) * 1000))
                self.send_json(disc)
            except Exception as e:
                _evt("discovery", user=user_id or "anon", ok=False,
                     err=repr(e)[:200], ms=int((time.time() - t0) * 1000))
                traceback.print_exc()
                self.send_json({"error": str(e)}, 500)
            return

        # ── History ───────────────────────────────────────────────────────────

        if parsed.path == "/history":
            if not user_id:
                self.send_json([])
                return
            # Pull anything Spotify played or the listener liked outside DIG —
            # OFF THE CRITICAL PATH. The whole app boots inside
            # `loadHistory().then(...)`, so every millisecond spent here is
            # milliseconds before the first track paints. Two gated Spotify
            # calls (lib/spotify_gate paces at 1.5s) would put 3-4s in front of
            # every cold open, which is a worse bug than the one being fixed.
            #
            # So: answer from the DB now, sync behind it. The rows land a few
            # seconds later and the client picks them up with its one delayed
            # re-fetch (see _refreshHistoryFromServer in app.js). sync_user
            # claims its rate-gate slot BEFORE its network calls, so two tabs
            # opening at once still cost one sync, not two.
            _spawn_history_sync(user_id)
            self.send_json(db_get_history(user_id))
            return

        # ── Spotify Connect playback (for iOS / remote control) ─────────────
        # Uses the Spotify REST API to control playback on the user's active
        # Spotify device (mobile app, desktop app, etc.) instead of the
        # Web Playback SDK. Required for iOS where the SDK doesn't work.

        if parsed.path == "/api/play":
            if not user_id:
                _evt("transport", action="play", user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            device_id = qs.get("device", [None])[0]
            # Multi-track context: the iOS Connect path sends `tracks=id1,id2,...`
            # so Spotify auto-advances to DIG's next picks NATIVELY when a track
            # ends — critical on iPhone, where backgrounded JS timers can't drive
            # the next play and the queue would otherwise stall. Falls back to the
            # legacy single `track` param.
            tracks_param = qs.get("tracks", [None])[0]
            if tracks_param:
                track_ids = [t.strip() for t in tracks_param.split(",") if t.strip()]
            else:
                single = qs.get("track", [None])[0]
                track_ids = [single] if single else []
            # position_ms forces start-from-zero on a fresh play. Without it
            # Spotify Connect resumes a re-issued track at the device's lingering
            # position — the "skip starts mid-track" bug. Re-prime calls may pass
            # a non-zero value to resume the current track in place.
            try:
                position_ms = max(0, int(qs.get("position_ms", ["0"])[0]))
            except (ValueError, TypeError):
                position_ms = 0
            # THE TRANSFER IS THE ONLY THING THAT CAN STRAND THE MUSIC.
            #
            # `PUT /me/player {play:false}` PAUSES the device, and the play is
            # what starts it again. So a play that fails after a transfer that
            # succeeded leaves the phone silent, and only then. Measured on this
            # account, one axis explains every reported symptom:
            #
            #   transfer  990ms → 204, play 204   works ("stopped for half a
            #                                     second" — that IS the transfer)
            #   transfer 2250ms → 204, play 502   device left paused → silence
            #                                     (2026-08-03 15:18, "Ari Ari")
            #   transfer 3505ms → 404             device gone → Bandcamp + banner
            #                                     (2026-08-04 01:50)
            #   no transfer     → play 204/298ms  position preserved
            #
            # The transfer exists to WAKE A SLEEPING DEVICE ("Spotify returns 404
            # if the device hasn't been actively playing recently"). A device
            # that is playing right now does not need waking — and every case
            # above was a device that was playing, which is why DIG was adopting
            # it. So the caller that has just READ the state may say so, and then
            # a failed command costs nothing: the song keeps playing.
            #
            # Deliberately caller-asserted rather than probed. Probing would cost
            # the round-trip this is here to avoid, and the only honest evidence
            # is a state read the caller has already done.
            skip_transfer = qs.get("no_transfer", ["0"])[0] == "1"
            if not track_ids:
                _evt("transport", action="play", user=user_id, outcome="400", reason="missing_track")
                self.send_json({"error": "track param required"}, 400)
                return
            track_id = track_ids[0]  # representative id for telemetry below
            token_info = _user_token_or_refresh(user_id)
            if not token_info:
                _evt("transport", action="play", user=user_id, id=track_id, outcome="401", reason="no_token")
                self.send_json({"error": "not_authenticated",
                                "auth_url": make_sp_oauth(user_id=user_id).get_authorize_url()}, 401)
                return
            # Transfer playback to the target device first (wakes it up),
            # then play the track. Without this, Spotify returns 404 if the
            # device hasn't been actively playing recently.
            token = token_info["access_token"]
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            t_total = time.time()
            transfer_ms = None
            transfer_status = None
            transfer_err = None
            # Set when the transfer's own 404 forced a different device. The
            # client pins whatever comes back on success, so it has to be told.
            recovered_by_transfer = None
            if device_id and skip_transfer:
                # Not silent: this is the branch that decides whether a failure
                # is audible, so it has to be visible when reading a bad night's
                # log back. If a play ever 404s here, the existing wake-and-
                # reissue recovery below still runs — it just does the transfer
                # at the moment it is actually needed instead of unconditionally.
                _evt("transport", action="play", user=user_id, id=track_id,
                     device=device_id, outcome="transfer_skipped",
                     reason="caller_says_device_is_playing")
            if device_id and not skip_transfer:
                # Step 1: Transfer playback to the device
                t_transfer = time.time()
                try:
                    # play=false: wake the device WITHOUT resuming whatever was
                    # playing — a plain transfer carries the previous track's
                    # position onto the device, and the following play then
                    # inherits it ("next song starts mid-track" bug).
                    transfer_body = json.dumps({"device_ids": [device_id], "play": False}).encode()
                    transfer_req = urllib.request.Request(
                        "https://api.spotify.com/v1/me/player",
                        data=transfer_body, method="PUT", headers=headers,
                    )
                    resp = urllib.request.urlopen(transfer_req, timeout=8)
                    transfer_status = resp.status
                    import time as _time
                    _time.sleep(0.3)
                except urllib.error.HTTPError as e:
                    transfer_status = e.code
                    transfer_err = e.read().decode("utf-8", errors="replace")[:200]
                except Exception as e:
                    transfer_err = repr(e)[:200]
                transfer_ms = int((time.time() - t_transfer) * 1000)
                # A 404 ON THE TRANSFER ALREADY ANSWERED THE QUESTION: the
                # device the caller named does not exist any more. Playing at it
                # anyway is asking a second time, and Spotify does not always
                # answer the same way — 2026-08-02 06:16:46 it came back 500,
                # which is NOT the 404 the recovery below is gated on, so the
                # recovery never ran and the client got a 502 for a device that
                # had been gone for 3.5 seconds. Drop the name here and let the
                # recovery pick a live one on its own terms.
                #
                # Deliberately NOT `device_id = None`. A device-less play needs
                # an already-active device and Spotify picks it, which is how a
                # phone outside the house once put its music on an idle `DIG`
                # web player on a Mac at home — see _pick_playback_device. If
                # this server cannot name a device it is willing to play to, the
                # honest answer is the no_device 404 the client already handles
                # with the banner, not a guess about which room to fill.
                if transfer_status == 404:
                    seen = _spotify_devices(headers)
                    alt = _pick_playback_device([d for d in seen
                                                 if d.get("id") != device_id], None)
                    _evt("transport", action="play", user=user_id, id=track_id,
                         device=device_id, outcome="transfer_404_device_gone",
                         transfer_ms=transfer_ms, devices_seen=len(seen),
                         replacement=(alt or {}).get("name") or "-")
                    if not alt:
                        self.send_json({"error": "spotify_404", "no_device": True,
                                        "detail": "named device is gone"}, 404)
                        return
                    device_id = alt["id"]
                    recovered_by_transfer = alt

            # Step 2: Play the track. Both steps are wrapped so the
            # NO_ACTIVE_DEVICE recovery below can reissue them against a device
            # it woke itself.
            def _issue_play(dev):
                url = "https://api.spotify.com/v1/me/player/play"
                if dev:
                    url += f"?device_id={dev}"
                # WHICH of the uris to start on. Not decoration: without an
                # offset Spotify starts at "the first item in play order", and
                # play order is the DEVICE's shuffle setting — so with shuffle
                # on it starts on a random member of DIG's 25-track look-ahead
                # and the song DIG is showing never plays.
                #
                # Measured 2026-08-05, iPhone Connect session 11:03-11:15:
                # 11 dispatches, 11 landed elsewhere — ctxPos 14, 9, 15, 2, 17,
                # 23, 13, 2, 7, 5, 3 of 25, never 0. The proof it is the START
                # INDEX and not a stale read is that two re-assertions re-sent
                # the byte-identical context and landed somewhere ELSE again
                # (23 then 13; 14 then 9): a deterministic bug cannot do that,
                # a shuffled start order does it every time. Same account on
                # the Mac never showed it, because the SDK path plays one track
                # and has no list to shuffle.
                #
                # Deliberately an offset rather than turning the listener's
                # shuffle off. Their Spotify setting is theirs; all this asks
                # is that the play begin where we said. Everything after track
                # 0 is DIG's own picks either way, so a shuffled tail is a
                # scrambled order of the right music, not the wrong music.
                req_body = json.dumps({
                    "uris": [f"spotify:track:{tid}" for tid in track_ids],
                    "offset": {"position": 0},
                    "position_ms": position_ms,
                }).encode()
                return urllib.request.urlopen(
                    urllib.request.Request(url, data=req_body, method="PUT", headers=headers),
                    timeout=8)

            def _force_seek_zero(dev):
                # Deterministic start-from-zero. position_ms in the play body is
                # unreliable when the play follows a device transfer (Spotify can
                # inherit the transferred position), so when we asked for 0 we
                # force it with an explicit seek. Best-effort — a seek failure
                # must never fail the play. Converges to 0 even if it races the
                # track switch (seeking old→0 then new loads at 0, or new→0).
                if position_ms != 0:
                    return None
                try:
                    seek_url = "https://api.spotify.com/v1/me/player/seek?position_ms=0"
                    if dev:
                        seek_url += f"&device_id={dev}"
                    seek_req = urllib.request.Request(seek_url, data=b"", method="PUT", headers=headers)
                    return urllib.request.urlopen(seek_req, timeout=5).status
                except urllib.error.HTTPError as se:
                    return se.code
                except Exception:
                    return -1

            t_play = time.time()
            recovered = None
            try:
                try:
                    resp = _issue_play(device_id)
                except urllib.error.HTTPError as first:
                    if first.code != 404:
                        raise
                    # "No active device found": the device is REAL and listed,
                    # just asleep. Wake it explicitly and reissue. Reissuing the
                    # same device-less call — what the iOS client used to do —
                    # can only fail identically, which is why playback died on a
                    # backgrounded phone instead of recovering.
                    body404 = first.read().decode("utf-8", errors="replace")[:200]
                    # Capture the raw list, not just the verdict. "no device
                    # available" reads identically whether Spotify returned
                    # nothing at all or returned devices this server refuses to
                    # play to (restricted, or not the caller's own) — and those
                    # are opposite problems: the first needs the app opened, the
                    # second is a bug in _pick_playback_device. Answering that
                    # from the log is the difference between one round of
                    # guessing and none.
                    seen = _spotify_devices(headers)
                    dev = _pick_playback_device(seen, device_id)
                    if not dev:
                        _evt("transport", action="play", user=user_id, id=track_id,
                             device=device_id or "-", outcome="error", play_status=404,
                             body=body404, recovery="no_device_available",
                             devices_seen=len(seen),
                             devices=[
                                 f"{d.get('name')}/{d.get('type')}"
                                 f"{'/active' if d.get('is_active') else ''}"
                                 f"{'/restricted' if d.get('is_restricted') else ''}"
                                 for d in seen[:5]
                             ],
                             total_ms=int((time.time() - t_total) * 1000))
                        self.send_json({"error": "spotify_404", "detail": body404,
                                        "no_device": True}, 404)
                        return
                    try:
                        wake = json.dumps({"device_ids": [dev["id"]], "play": False}).encode()
                        urllib.request.urlopen(urllib.request.Request(
                            "https://api.spotify.com/v1/me/player",
                            data=wake, method="PUT", headers=headers), timeout=8)
                    except Exception:
                        pass            # the reissue below is the real test
                    time.sleep(0.4)     # Spotify needs a beat to mark it active
                    recovered = dev
                    device_id = dev["id"]
                    resp = _issue_play(device_id)
                play_status = resp.status
                play_ms = int((time.time() - t_play) * 1000)
                seek_status = _force_seek_zero(device_id)
                # Either recovery counts. The client pins `device` from a
                # successful play, so a transfer-forced replacement has to be
                # reported the same way a play-forced one is — otherwise the
                # client goes on naming a device this request already proved gone.
                recovered = recovered or recovered_by_transfer
                _evt("transport", action="play", user=user_id, id=track_id,
                     device=device_id or "-", outcome="ok",
                     transfer_ms=transfer_ms, transfer_status=transfer_status,
                     play_ms=play_ms, play_status=play_status, seek_status=seek_status,
                     recovered=(recovered or {}).get("name"),
                     total_ms=int((time.time() - t_total) * 1000))
                self.send_json({"ok": True, "status": play_status,
                                "device": device_id,
                                "recovered": bool(recovered),
                                "device_name": (recovered or {}).get("name")})
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")[:300]
                play_ms = int((time.time() - t_play) * 1000)
                _evt("transport", action="play", user=user_id, id=track_id,
                     device=device_id or "-", outcome="error",
                     transfer_ms=transfer_ms, transfer_status=transfer_status,
                     play_ms=play_ms, play_status=e.code, body=body,
                     total_ms=int((time.time() - t_total) * 1000))
                # Report WHICH device this failed on. The server picks and wakes
                # one during its own 404 recovery, and the client never learned
                # the id — so a client-side retry after a transient 5xx went out
                # device-less and 404'd against the very device the server had
                # just woken. Measured 2026-07-31: 502 on 177ee437…, retry on
                # `device=-`, 404, "Spotify unreachable", Bandcamp.
                self.send_json({"error": f"spotify_{e.code}", "detail": body,
                                "device": device_id},
                               e.code if e.code < 500 else 502)
            except Exception as e:
                play_ms = int((time.time() - t_play) * 1000)
                _evt("transport", action="play", user=user_id, id=track_id,
                     device=device_id or "-", outcome="exception",
                     transfer_ms=transfer_ms, play_ms=play_ms,
                     err=repr(e)[:200],
                     total_ms=int((time.time() - t_total) * 1000))
                self.send_json({"error": "exception", "detail": repr(e)[:200]}, 500)
            return

        if parsed.path == "/api/queue":
            # Add a track to Spotify's native playback queue
            if not user_id:
                _evt("transport", action="queue", user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            track_id = qs.get("track", [None])[0]
            if not track_id:
                _evt("transport", action="queue", user=user_id, outcome="400", reason="missing_track")
                self.send_json({"error": "track param required"}, 400)
                return
            token_info = _user_token_or_refresh(user_id)
            if not token_info:
                _evt("transport", action="queue", user=user_id, id=track_id, outcome="401", reason="no_token")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            try:
                sp = spotipy.Spotify(auth=token_info["access_token"])
                sp.add_to_queue(f"spotify:track:{track_id}")
                _evt("transport", action="queue", user=user_id, id=track_id, outcome="ok")
                self.send_json({"ok": True})
            except Exception as e:
                _evt("transport", action="queue", user=user_id, id=track_id,
                     outcome="error", err=repr(e)[:160])
                self.send_json({"error": str(e)[:200]}, 500)
            return

        if parsed.path == "/api/devices":
            if not user_id:
                _evt("transport", action="devices", user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            token_info = _user_token_or_refresh(user_id)
            if not token_info:
                _evt("transport", action="devices", user=user_id, outcome="401", reason="no_token")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            try:
                sp = spotipy.Spotify(auth=token_info["access_token"])
                devices = sp.devices()
                _evt("transport", action="devices", user=user_id, outcome="ok",
                     count=len((devices or {}).get("devices") or []))
                self.send_json(devices)
            except Exception as e:
                _evt("transport", action="devices", user=user_id, outcome="error", err=repr(e)[:160])
                self.send_json({"error": str(e)}, 500)
            return

        if parsed.path == "/api/pause":
            if not user_id:
                _evt("transport", action="pause", user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            device_id = qs.get("device", [None])[0]
            token_info = _user_token_or_refresh(user_id)
            if not token_info:
                _evt("transport", action="pause", user=user_id, outcome="401", reason="no_token")
                self.send_json({"error": "token_refresh_failed"}, 401)
                return
            try:
                sp = spotipy.Spotify(auth=token_info["access_token"])
                sp.pause_playback(device_id=device_id)
                _evt("transport", action="pause", user=user_id, device=device_id or "-", outcome="ok")
                self.send_json({"ok": True})
            except Exception as e:
                # 403/404 = nothing playing — not an error worth surfacing
                _evt("transport", action="pause", user=user_id, device=device_id or "-",
                     outcome="nothing_to_pause", err=repr(e)[:120])
                self.send_json({"ok": True, "note": "nothing_to_pause"})
            return

        if parsed.path == "/api/resume":
            if not user_id:
                _evt("transport", action="resume", user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            qs = urllib.parse.parse_qs(parsed.query)
            device_id = qs.get("device", [None])[0]
            token_info = _user_token_or_refresh(user_id)
            if not token_info:
                _evt("transport", action="resume", user=user_id, outcome="401", reason="no_token")
                self.send_json({"error": "token_refresh_failed"}, 401)
                return
            try:
                sp = spotipy.Spotify(auth=token_info["access_token"])
                recovered = None
                try:
                    sp.start_playback(device_id=device_id)
                except spotipy.SpotifyException as first:
                    # "No active device found" on RESUME, which /api/play has
                    # recovered from for months and this endpoint never did:
                    # it called start_playback(device_id=None) and gave up, so
                    # pressing play did nothing at all. Observed 2026-07-31 on
                    # "Vallenato X Ella" — Spotify held the track loaded and
                    # paused at 0 (the cover was on screen), the user pressed
                    # play twice, and both taps 404'd here. A paused Connect
                    # device stops being ACTIVE, which is exactly the state a
                    # resume has to deal with; refusing it is refusing the one
                    # case the button exists for.
                    if first.http_status != 404:
                        raise
                    headers = {"Authorization": f"Bearer {token_info['access_token']}",
                               "Content-Type": "application/json"}
                    dev = _pick_playback_device(_spotify_devices(headers), device_id)
                    if not dev:
                        raise
                    wake = json.dumps({"device_ids": [dev["id"]], "play": False}).encode()
                    try:
                        urllib.request.urlopen(urllib.request.Request(
                            "https://api.spotify.com/v1/me/player",
                            data=wake, method="PUT", headers=headers), timeout=8)
                    except Exception:
                        pass            # the reissue below is the real test
                    time.sleep(0.4)     # Spotify needs a beat to mark it active
                    recovered = dev
                    device_id = dev["id"]
                    sp.start_playback(device_id=device_id)
                _evt("transport", action="resume", user=user_id, device=device_id or "-",
                     outcome="ok", recovered=(recovered or {}).get("name"))
                self.send_json({"ok": True, "device": device_id,
                                "recovered": bool(recovered)})
            except spotipy.SpotifyException as e:
                # A device that has gone away is the CLIENT's situation to
                # handle, not a fault in this server. Flattening it to 500 was
                # why "HTTP 500 on GET /api/resume?device=…" kept arriving in
                # the platform health digest for what is really a stale device
                # id — indistinguishable, in that report, from a crash.
                # /api/play already answers 404 + no_device here; resume now
                # agrees with it, so the client's existing handling applies.
                status = e.http_status if 400 <= (e.http_status or 0) < 500 else 502
                _evt("transport", action="resume", user=user_id, device=device_id or "-",
                     outcome="error", status=status, err=repr(e)[:160])
                self.send_json({"error": str(e), "no_device": status == 404}, status)
            except Exception as e:
                _evt("transport", action="resume", user=user_id, device=device_id or "-",
                     outcome="error", status=500, err=repr(e)[:160])
                self.send_json({"error": str(e)}, 500)
            return

        # ── Taste profile (pre-computed from DB for tailored mode) ─────────────

        if parsed.path == "/api/coverage":
            # Per-user genre + country play-count map. Cheap (two grouped
            # queries against user_history × tracks). Frontend Discovery
            # picker uses this to penalise over-played cells. Anonymous
            # callers get an empty map (picker behaves as before).
            t0 = time.time()
            cov = db_get_user_coverage(user_id) if user_id else {"genres": {}, "countries": {}}
            _evt("coverage", user=user_id or "anon",
                 genres=len(cov["genres"]), countries=len(cov["countries"]),
                 ms=int((time.time() - t0) * 1000))
            self.send_json(cov)
            return

        if parsed.path == "/api/taste-profile":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            # Include saves + deep listens (>=60%) + dislikes + early skips (<25%).
            # Early skips act as negative evidence so tailored mode downweights
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
                           AND h.played_pct < 25))
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
                elif status == "skipped" and pct is not None and pct < 25:
                    w = -0.5
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

        # ── Bandcamp: resolve a fresh full-track stream URL at play time ───────
        # Bandcamp stream URLs are signed + expire in hours, so the pool stores
        # only the stable identity ('bc:<band_id>:<track_id>') and the player
        # asks for a fresh URL right before playing. No auth, no Spotify quota.
        if parsed.path == "/api/bandcamp/resolve":
            from lib import bandcamp
            qs = urllib.parse.parse_qs(parsed.query)
            track_id = (qs.get("id", [""])[0]).strip()
            band, tid = bandcamp.parse_id(track_id)
            if not band:
                self.send_json({"ok": False, "error": "bad_id"}, 400)
                return
            r = bandcamp.resolve_stream(band, tid)
            self.send_json(r, 200 if r.get("ok") else 502)
            # Free genre enrichment: the resolve already carries the full tag
            # set (sub-genres + city). Backfill it into tracks.genres off-thread
            # so every play upgrades a thin 1-genre Bandcamp row toward parity
            # with Spotify labeling — no extra Bandcamp calls, no added latency.
            if r.get("ok") and r.get("tags"):
                threading.Thread(
                    target=_bandcamp_backfill_genres,
                    args=(track_id, r.get("tags"), r.get("location") or ""),
                    daemon=True,
                ).start()
            return

        # ── SoundCloud: resolve a fresh HLS stream URL at play time ───────────
        # Like Bandcamp, SoundCloud stream URLs are signed + expire, so the pool
        # stores only the stable id ('sc:<track_id>') and the player asks for a
        # fresh HLS (.m3u8) URL right before playing. Streaming/discovery only —
        # SoundCloud ToS forbids downloading, so this never feeds the IG pipeline.
        if parsed.path == "/api/soundcloud/resolve":
            from lib import soundcloud
            qs = urllib.parse.parse_qs(parsed.query)
            track_id = (qs.get("id", [""])[0]).strip()
            if not soundcloud.parse_id(track_id):
                self.send_json({"ok": False, "error": "bad_id"}, 400)
                return
            try:
                r = soundcloud.get_stream(track_id)
            except Exception as e:
                r = {"ok": False, "error": f"{type(e).__name__}: {e}"}
            self.send_json(r, 200 if r.get("ok") else 502)
            return

        # ── Static data files (served from project root) ──────────────────────

        fname = parsed.path.lstrip("/")
        if fname in _DATA_FILES:
            filepath = os.path.join(DIR, fname)
            if not os.path.exists(filepath):
                self.send_response(404)
                self.end_headers()
                return

            entry = _static_entry(fname, filepath)
            if entry is None:
                # Oversized (catalog.json): stream raw, uncompressed, as before.
                content = open(filepath, "rb").read()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(content)
                return

            # Revalidation: a repeat launch sends If-None-Match and gets a
            # ~200-byte 304 instead of re-downloading three quarters of a MB.
            if self.headers.get("If-None-Match") == entry["etag"]:
                self.send_response(304)
                self.send_header("ETag", entry["etag"])
                self.send_header("Cache-Control", "public, max-age=300")
                # Same Vary/CORS as the 200 below: a 304 that drops them lets a
                # shared cache key the entry without the encoding dimension and
                # hand a gzipped body to a client that never asked for one.
                self.send_header("Vary", "Accept-Encoding")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                return

            body = entry["raw"]
            encoding = None
            if "gzip" in (self.headers.get("Accept-Encoding") or ""):
                body = entry["gz"]
                encoding = "gzip"

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            if encoding:
                self.send_header("Content-Encoding", encoding)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", entry["etag"])
            # Short max-age: the discovery cron rewrites these every ~3h, and
            # the ETag catches any change once max-age lapses.
            self.send_header("Cache-Control", "public, max-age=300")
            self.send_header("Vary", "Accept-Encoding")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
            return

        # ── Admin: list access requests ──────────────────────────────────────

        if parsed.path == "/admin/waitlist":
            if user_id != ADMIN_UID:
                self.send_json({"error": "forbidden"}, 403)
                return
            rows = fetchall(
                "SELECT email, name, spotify_uid, status, source, "
                "created_at::text FROM access_requests "
                "WHERE status <> 'ignored' ORDER BY "
                "(status='pending') DESC, created_at DESC"
            )
            self.send_json({"requests": rows})
            return

        # ── Admin: Instagram curation queue ──────────────────────────────────
        if parsed.path.startswith("/admin/ig/"):
            if user_id != ADMIN_UID:
                self.send_json({"error": "forbidden"}, 403)
                return

            if parsed.path == "/admin/ig/queue":
                self.send_json({"queue": ig_queue.list_queue(),
                                "cadence_hours": ig_queue.CADENCE_HOURS,
                                "publisher": ig_queue.publisher_health()})
                return

            if parsed.path == "/admin/ig/candidates":
                # Liked tracks not yet queued, for the manual "add" picker.
                #
                # Two exclusions, not one. Matching on track_id alone leaves the
                # same song visible when it is saved under a second Spotify id —
                # a single, an album cut and a compilation are three ids for one
                # recording, and re-adding one is a duplicate post, not a new
                # one. The name+artist pass catches those: La Lupe's "Puro
                # Teatro" was already scheduled under one id and still offered
                # itself under another.
                rows = fetchall(
                    """
                    SELECT t.id, t.name, t.artist, t.album, t.genres, t.year
                    FROM user_history h JOIN tracks t ON t.id = h.track_id
                    WHERE h.user_id = %s AND h.status = 'saved'
                      AND t.id NOT IN (
                        SELECT track_id FROM ig_post_queue
                        WHERE track_id IS NOT NULL AND status <> 'skipped')
                      AND NOT EXISTS (
                        SELECT 1 FROM ig_post_queue q
                        WHERE q.status <> 'skipped'
                          AND lower(btrim(q.track_name)) = lower(btrim(t.name))
                          AND lower(btrim(q.artist))     = lower(btrim(t.artist)))
                    ORDER BY h.listened_at DESC LIMIT 5000
                    """,
                    (ADMIN_UID,),
                )
                self.send_json({"candidates": rows})
                return

            # Media streaming for the dashboard (waveform + previews).
            qs = urllib.parse.parse_qs(parsed.query)
            iid = (qs.get("id", [""])[0]).strip()
            if parsed.path == "/admin/ig/peaks" and iid.isdigit():
                # Precomputed waveform envelope — a few KB instead of the whole
                # track, so the clip picker paints instantly.
                p = os.path.join(ig_queue.item_dir(iid), "peaks.json")
                if os.path.exists(p):
                    self.serve_file_with_range(p, "application/json")
                else:
                    self.send_response(404)
                    self.end_headers()
                return

            if parsed.path == "/admin/ig/audio" and iid.isdigit():
                # The source keeps whatever codec YouTube served (m4a, opus,
                # sometimes mp3) instead of being transcoded on download, so
                # the extension is no longer fixed — find it rather than
                # assume .mp3 and 404 the waveform picker.
                d = ig_queue.item_dir(iid)
                _AUDIO_TYPES = {".m4a": "audio/mp4", ".mp4": "audio/mp4",
                                ".mp3": "audio/mpeg", ".opus": "audio/ogg",
                                ".webm": "audio/webm", ".ogg": "audio/ogg"}
                path = ctype = None
                for name in sorted(os.listdir(d)) if os.path.isdir(d) else []:
                    stem, ext = os.path.splitext(name)
                    if stem == "source" and ext in _AUDIO_TYPES:
                        path, ctype = os.path.join(d, name), _AUDIO_TYPES[ext]
                        break
                if path:
                    self.serve_file_with_range(path, ctype)
                else:
                    self.send_response(404)
                    self.end_headers()
                return
            if parsed.path == "/admin/ig/preview" and iid.isdigit():
                fmt = (qs.get("fmt", ["feed"])[0]).strip()
                fmap = {
                    "feed": ("feed.mp4", "video/mp4"),
                    "story": ("story.mp4", "video/mp4"),
                    "clip": ("clip.mp3", "audio/mpeg"),
                    "card_feed": ("card_feed.png", "image/png"),
                    "card_story": ("card_story.png", "image/png"),
                }
                fname, ctype = fmap.get(fmt, fmap["feed"])
                self.serve_file_with_range(
                    os.path.join(ig_queue.item_dir(iid), fname), ctype)
                return

            self.send_json({"error": "not_found"}, 404)
            return

        # ── Static web assets (served from web/) ─────────────────────────────

        _new_guest_id = None
        _clear_guest_mode = False
        _set_guest_mode = False
        if parsed.path == "/":
            # Zero-friction: ANYONE lands straight in the app and can play
            # immediately. Approved Spotify users get the full pool; everyone
            # else gets Bandcamp mode as an anonymous account (so their taste
            # persists and can later be claimed by email or Spotify). No gate.
            if is_approved(user_id):
                self.path = "/app.html"
                _clear_guest_mode = True   # a logged-in Spotify user isn't a guest
            else:
                self.path = "/app.html"
                _set_guest_mode = True
                wants_html = "text/html" in (self.headers.get("Accept", "") or "")
                if not user_id and wants_html:
                    # First-time human visitor → mint an anonymous account.
                    _new_guest_id = "guest:" + secrets.token_hex(12)
                    db_upsert_user(_new_guest_id, "Guest", None, None)

        # ── Browser modules ──────────────────────────────────────────────────
        # Served here rather than through SimpleHTTPRequestHandler so the
        # caching is deliberate. Two cases, and the difference is whether the
        # client can prove which version it asked for:
        #   ?v=<hash>  the URL names the content, so it can be cached forever.
        #   bare       someone typed it, or the stamp failed — revalidate, since
        #              a stale module against fresh markup fails invisibly.
        if parsed.path.startswith("/js/") and parsed.path.endswith(".js"):
            rel = parsed.path.lstrip("/")
            filepath = os.path.join(WEB_DIR, rel)
            # The path is pattern-restricted above, but normalise anyway: this
            # is the one route that maps a URL onto the filesystem.
            if (not os.path.normpath(filepath).startswith(WEB_DIR + os.sep)
                    or not os.path.isfile(filepath)):
                self.send_response(404)
                self.end_headers()
                return
            mod = _js_module(rel)
            etag = '"%s"' % mod["hash"]
            versioned = "v=" in (parsed.query or "")

            if self.headers.get("If-None-Match") == etag:
                self.send_response(304)
                self.send_header("ETag", etag)
                self.send_header("Vary", "Accept-Encoding")
                self.end_headers()
                return

            body, encoding = mod["body"], None
            if "gzip" in (self.headers.get("Accept-Encoding") or ""):
                body, encoding = mod["gz"], "gzip"
            self.send_response(200)
            self.send_header("Content-Type", "text/javascript; charset=utf-8")
            self.send_header("Cache-Control",
                             "public, max-age=31536000, immutable" if versioned
                             else "no-cache")
            self.send_header("ETag", etag)
            if encoding:
                self.send_header("Content-Encoding", encoding)
            self.send_header("Vary", "Accept-Encoding")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # Prevent browsers from caching stale HTML
        if self.path.endswith(".html") or parsed.path == "/":
            filepath = os.path.join(WEB_DIR, self.path.lstrip("/"))
            if os.path.exists(filepath):
                content = _stamp_module_urls(open(filepath, "rb").read())
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
                if _new_guest_id:
                    self.set_session_cookie(_new_guest_id)  # signed HttpOnly identity
                    self.set_guest_mode_cookie()            # JS-readable flag
                elif _clear_guest_mode:
                    self.clear_guest_mode_cookie()
                elif _set_guest_mode and "dig_mode=guest" not in (self.headers.get("Cookie", "") or ""):
                    self.set_guest_mode_cookie()
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
        self._req_user = user_id
        self._req_t0 = time.time()

        # ── Where saves go on Spotify ────────────────────────────────────────
        if parsed.path == "/api/save-destination":
            if not user_id:
                self.send_json({"error": "not_logged_in"}, 401)
                return
            body = self.read_json_body()
            dest = (body.get("destination") or "").strip()
            pid = (body.get("playlist_id") or "").strip() or None
            if dest not in SAVE_DESTINATIONS:
                self.send_json({"error": "bad_destination"}, 400)
                return
            if dest == SAVE_DEST_PLAYLIST and not pid:
                self.send_json({"error": "playlist_id_required"}, 400)
                return
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET save_destination = %s, "
                        "save_playlist_id = %s WHERE id = %s",
                        (dest, pid if dest == SAVE_DEST_PLAYLIST else None, user_id))
                conn.commit()
            finally:
                conn.close()
            _evt("save-destination", user=user_id, action="set", dest=dest)
            # Changing to Liked Songs needs a scope older tokens don't carry,
            # so tell the frontend whether a re-link is required before the
            # next save silently fails to mirror.
            self.send_json({"ok": True, "destination": dest,
                            "playlist_id": pid,
                            "needs_relink": not _save_scope_ok(user_id)})
            return

        # ── Magic-link sign-in: request a link ───────────────────────────────
        if parsed.path == "/auth/request":
            body = self.read_json_body()
            email = (body.get("email") or "").strip().lower()
            if (body.get("hp") or "").strip():          # honeypot
                self.send_json({"ok": True})
                return
            if "@" not in email or "." not in email.split("@")[-1]:
                self.send_json({"error": "invalid_email"}, 400)
                return
            # Attach to the current guest account so registration preserves taste.
            guest_id = user_id if (user_id and user_id.startswith("guest:")) else None
            try:
                token = db_create_login_token(email, guest_id)
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
                return
            link = f"https://diiiiiiiig.xyz/auth/verify?token={token}"
            text = (f"Tap to sign in to Dig and keep your taste:\n\n{link}\n\n"
                    f"This link expires in 30 minutes. If you didn't request it, ignore this email.")
            html = (f'<p>Tap to sign in to Dig and keep your taste:</p>'
                    f'<p><a href="{link}" style="display:inline-block;background:#FF2010;'
                    f'color:#fff;padding:11px 18px;border-radius:10px;text-decoration:none;'
                    f'font-weight:700">Sign in to Dig</a></p>'
                    f'<p style="color:#888;font-size:12px">This link expires in 30 minutes. '
                    f"If you didn't request it, ignore this email.</p>")
            threading.Thread(
                target=lambda: send_email(email, "Your Dig sign-in link", text, html),
                daemon=True,
            ).start()
            _evt("magic_link_request", email=email[:60], upgrade=bool(guest_id))
            self.send_json({"ok": True})
            return

        # ── Waitlist: public email capture ───────────────────────────────────

        if parsed.path == "/waitlist":
            body = self.read_json_body()
            email = (body.get("email") or "").strip().lower()
            name = (body.get("name") or "").strip()
            # Honeypot: a hidden form field no human fills. If it's non-empty a
            # bot submitted — fake success (so it doesn't retry) and store nothing.
            if (body.get("hp") or "").strip():
                _evt("waitlist_bot_blocked", email=email[:60])
                self.send_json({"ok": True})
                return
            if "@" not in email or "." not in email.split("@")[-1]:
                self.send_json({"error": "invalid_email"}, 400)
                return
            try:
                conn = get_conn()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO access_requests (email, name, status, source)
                            VALUES (%s, %s, 'pending', 'form')
                            ON CONFLICT (email) DO UPDATE
                                SET name = CASE WHEN access_requests.name = ''
                                                THEN EXCLUDED.name ELSE access_requests.name END
                            RETURNING (xmax = 0) AS inserted
                            """,
                            (email, name),
                        )
                        row = cur.fetchone()
                        is_new = bool(row and row[0])
                    conn.commit()
                finally:
                    conn.close()
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
                return
            _evt("waitlist_request", email=email[:60], new=is_new)
            # Email Tommaso only on a genuinely new request (not a re-submit).
            if is_new:
                notify_new_waitlist(email, name)
            self.send_json({"ok": True})
            return

        # ── Admin: approve an access request ─────────────────────────────────

        if parsed.path == "/admin/approve":
            if user_id != ADMIN_UID:
                self.send_json({"error": "forbidden"}, 403)
                return
            body = self.read_json_body()
            email = (body.get("email") or "").strip().lower()
            if not email:
                self.send_json({"error": "email_required"}, 400)
                return
            execute("UPDATE access_requests SET status='approved' "
                    "WHERE lower(email)=lower(%s)", (email,))
            # If they've already logged in with this email, flip their flag now.
            execute("UPDATE users SET approved=true WHERE lower(email)=lower(%s)",
                    (email,))
            _evt("waitlist_approve", email=email[:60])
            self.send_json({"ok": True})
            return

        # ── Admin: ignore an access request (bot / spam) ─────────────────────
        if parsed.path == "/admin/ignore":
            if user_id != ADMIN_UID:
                self.send_json({"error": "forbidden"}, 403)
                return
            body = self.read_json_body()
            email = (body.get("email") or "").strip().lower()
            if not email:
                self.send_json({"error": "email_required"}, 400)
                return
            execute("UPDATE access_requests SET status='ignored' "
                    "WHERE lower(email)=lower(%s)", (email,))
            _evt("waitlist_ignore", email=email[:60])
            self.send_json({"ok": True})
            return

        # ── Admin: Instagram curation queue (mutations) ──────────────────────
        if parsed.path.startswith("/admin/ig/"):
            if user_id != ADMIN_UID:
                self.send_json({"error": "forbidden"}, 403)
                return

            # Raw-body audio upload (manual fallback for tracks not auto-resolved).
            if parsed.path == "/admin/ig/item/audio":
                qs = urllib.parse.parse_qs(parsed.query)
                iid = (qs.get("id", [""])[0]).strip()
                if not iid.isdigit() or not ig_queue.get_item(int(iid)):
                    self.send_json({"error": "bad_id"}, 400)
                    return
                length = int(self.headers.get("Content-Length", 0))
                if length <= 0:
                    self.send_json({"error": "empty"}, 400)
                    return
                out_dir = ig_queue.item_dir(iid)
                os.makedirs(out_dir, exist_ok=True)
                dest = os.path.join(out_dir, "source.mp3")
                remaining = length
                with open(dest, "wb") as f:
                    while remaining > 0:
                        chunk = self.rfile.read(min(65536, remaining))
                        if not chunk:
                            break
                        f.write(chunk)
                        remaining -= len(chunk)
                from lib.ig_audio import probe_duration_ms
                ig_queue.set_audio(int(iid), "upload", dest,
                                   probe_duration_ms(dest), None)
                self.send_json({"ok": True, "item": ig_queue.get_item(int(iid))})
                return

            # JSON-body mutations.
            body = self.read_json_body()
            iid = body.get("id")

            if parsed.path == "/admin/ig/add":
                new_id = ig_queue.add_item(
                    track_id=(body.get("track_id") or None),
                    track_name=body.get("track_name"),
                    artist=body.get("artist"),
                    status="needs_audio")  # manual add = already chosen
                if not new_id:
                    self.send_json({"error": "duplicate_or_failed"}, 409)
                    return
                self.send_json({"ok": True, "item": ig_queue.get_item(new_id)})
                return

            if parsed.path == "/admin/ig/item/approve":
                self.send_json({"ok": True, "item": ig_queue.approve_candidate(iid)})
                return

            if parsed.path == "/admin/ig/item/skip":
                self.send_json({"ok": True, "item": ig_queue.skip_item(iid)})
                return

            if parsed.path == "/admin/ig/item/update":
                fields = {k: body[k] for k in (
                    "caption", "clip_start_ms", "clip_duration_ms", "scheduled_at",
                    "post_feed", "post_story", "track_name", "artist") if k in body}
                # Anything that changes what the media contains invalidates the
                # render. Without this, dragging the waveform leaves the old
                # window baked into feed.mp4 and the preview quietly lies.
                if {"clip_start_ms", "clip_duration_ms", "track_name", "artist",
                        "post_feed", "post_story"} & set(fields):
                    fields["rendered_at"] = None
                self.send_json({"ok": True, "item": ig_queue.update_item(iid, **fields)})
                return

            if parsed.path == "/admin/ig/item/unschedule":
                res = ig_queue.unschedule(iid)
                self.send_json(res, 200 if res.get("ok") else 400)
                return

            if parsed.path == "/admin/ig/item/approve-publish":
                res = ig_queue.approve_publish(iid, when=body.get("scheduled_at"))
                self.send_json(res, 200 if res.get("ok") else 400)
                return

            if parsed.path == "/admin/ig/reorder":
                ig_queue.reorder(body.get("ordered_ids") or [])
                self.send_json({"ok": True})
                return

            # Fire-and-forget triggers (dashboard polls the queue for results).
            if parsed.path == "/admin/ig/propose":
                n = body.get("n")
                threading.Thread(target=_ig_run_propose, args=(n,), daemon=True).start()
                self.send_json({"ok": True, "started": "propose"})
                return

            if parsed.path == "/admin/ig/resolve":
                if body.get("retry"):
                    self.send_json(ig_queue.request_new_source(iid))
                    return
                # Downloading needs yt-dlp, which only the studio machine has.
                # Prod answering a button with "yt-dlp not installed" is a dead
                # end, so check before promising anything: do it here when we
                # can, otherwise leave the item in needs_audio and let the cron
                # on the machine that can actually download pick it up.
                try:
                    import yt_dlp  # noqa: F401
                except ImportError:
                    self.send_json({"ok": True, "queued": True, "message":
                                    "queued — the studio machine will fetch it "
                                    "on its next pass (within ~15 min)"})
                    return
                threading.Thread(target=_ig_run_resolve, args=(iid,),
                                 daemon=True).start()
                self.send_json({"ok": True, "started": "resolve"})
                return

            if parsed.path == "/admin/ig/render":
                # Rendering needs ffmpeg, which only the studio machine has —
                # prod just serves the result. Say so plainly instead of
                # starting a thread that dies with "ffmpeg not found": the
                # item is already marked unrendered, so the studio cron will
                # pick it up on its next pass without anyone clicking.
                from shutil import which
                if not which("ffmpeg"):
                    self.send_json({
                        "ok": True, "deferred": True,
                        "message": "Queued — this post builds on the studio "
                                   "machine and appears here within a minute.",
                    })
                    return
                threading.Thread(target=_ig_run_render, args=(iid,), daemon=True).start()
                self.send_json({"ok": True, "started": "render"})
                return

            self.send_json({"error": "not_found"}, 404)
            return

        # ── Session sync (cross-device heartbeat) ─────────────────────────────

        if parsed.path == "/api/session":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            state = self.read_json_body()
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
            rendered = json.dumps(data)[:300] if data else ""
            if body.get("transient"):
                # The platform health collector scrapes this container's stdout
                # and files anything shaped like `SomeError:` as a production
                # failure. A blip the client already recovered from is not one —
                # it filed a week of "TypeError: Failed to fetch" rows that were
                # all successful retries. Keep the cause readable, drop the token
                # shape that trips the scraper.
                rendered = re.sub(r"\b(\w*Error):", r"\1", rendered)
            print(f"[CLIENT {tag}] user={who} {msg}"
                  + (f" data={rendered}" if rendered else ""))
            self.send_json({"ok": True})
            return

        if parsed.path == "/history":
            if not user_id:
                _evt("history-sync", user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                items = json.loads(body.decode())
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                # Mobile networks can truncate large multi-hundred-KB POSTs
                # mid-string. Return a soft 400 instead of crashing the
                # request thread (which then tried to send_json into a
                # half-closed socket and produced cascade BrokenPipe noise).
                #
                # UnicodeDecodeError belongs here for the same reason and was
                # the gap this guard actually left: a cut lands mid-CHARACTER
                # as often as mid-string, and then body.decode() raises before
                # json ever sees the text. Observed on a 1.9 MB /history POST —
                # "can't decode byte 0xe6 in position 1899900: unexpected end
                # of data" — which escaped as an unhandled 500 with a stack
                # trace, exactly the crash this block was written to prevent.
                _evt("history-sync", user=user_id, ok=False,
                     err=type(e).__name__, bytes_received=len(body),
                     content_length=length, detail=str(e)[:120])
                _health_record_error(f"history write JSONDecode: {e}")
                self.send_json({"error": "malformed_payload", "detail": str(e)[:120]}, 400)
                return
            try:
                # Count by status so we can tell what kind of activity is being
                # synced (saves, dislikes, listened, skipped) without dumping
                # the whole payload.
                by_status = {}
                for it in items if isinstance(items, list) else []:
                    s = (it or {}).get("status") or "?"
                    by_status[s] = by_status.get(s, 0) + 1
                db_save_history(user_id, items)
                _HEALTH["history_writes"] += 1
                _evt("history-sync", user=user_id, total=len(items),
                     **{f"n_{k}": v for k, v in by_status.items()})
            except Exception as e:
                _evt("history-sync", user=user_id, ok=False, err=repr(e)[:200])
                _health_record_error(f"history write: {e}")
                self.send_json({"error": "history_write_failed"}, 500)
                return
            self.send_json({"ok": True})
            return

        # ── Journey (seeded, infinite) ────────────────────────────────────────

        if parsed.path == "/api/journey":
            if not user_id:
                self.send_json({"error": "not_authenticated"}, 401)
                return
            body = self.read_json_body()
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
            body = self.read_json_body()
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
        # Suppress chatty endpoints that have richer structured _evt logs
        # already (or are pure heartbeats / dedup writes).
        path_str = str(args)
        if any(x in path_str for x in [
            "/token", "/listened", "/save", "/history", "/me",
            "/api/client-log", "/api/session", "/unsave", "/undislike",
        ]):
            return
        # Enrich the default access log with user + duration. Format args are
        # (request_line, status, size) per BaseHTTPRequestHandler.
        user = getattr(self, "_req_user", None) or "anon"
        t0 = getattr(self, "_req_t0", None)
        ms = int((time.time() - t0) * 1000) if t0 else None
        suffix = f' user={user}' + (f' ms={ms}' if ms is not None else '')
        try:
            sys.stdout.write(
                "%s - - [%s] %s%s\n"
                % (self.address_string(), self.log_date_time_string(),
                   (format % args), suffix)
            )
            sys.stdout.flush()
        except Exception:
            super().log_message(format, *args)


if __name__ == "__main__":
    import datetime
    import time
    _HEALTH["started_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    try:
        ensure_access_schema()
    except Exception as _e:
        print(f"[access schema] WARN: {_e!r}")
    try:
        ig_queue.ensure_ig_schema()
    except Exception as _e:
        print(f"[ig schema] WARN: {_e!r}")
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "127.0.0.1")
    print(f"\n🎵 DIG running at http://{host}:{port}\n")
    # Threaded, not http.server.HTTPServer: that one serves a single request at
    # a time, so a /discovery build (~1s), a Bandcamp/SoundCloud resolve, an
    # /api/ai-recommend LLM call or one slow client downloading the 1 MB pool
    # blocked every other visitor behind it. New guests were timing out on the
    # first fetch — "TypeError: Failed to fetch" — at a 27% rate.
    # Safe to thread: lib/db.get_conn() opens a connection per call (nothing is
    # shared), pool state lives in Postgres behind an advisory lock, and no
    # request path writes to disk.
    server = http.server.ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True  # don't let an in-flight request block shutdown
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
