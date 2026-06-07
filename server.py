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

from lib.db import get_conn, fetchone, fetchall, execute
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
    "playlist-modify-private "
    "user-top-read user-read-recently-played user-read-playback-state "
    "user-modify-playback-state playlist-read-private playlist-read-collaborative"
)
# Scope that the bidirectional save/unsave needs. Existing tokens issued
# before this scope was added will lack it — the handlers fall back to
# DIG-only behaviour and tell the frontend to prompt a re-auth. We
# intentionally do NOT request `user-library-modify`; saves go to a
# per-user "DIG" private playlist instead of Spotify Liked Songs.
_PLAYLIST_MODIFY_SCOPE = "playlist-modify-private"
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


def _spotify_playlist_call(user_id: str, action: str, track_id: str) -> bool:
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

    try:
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
        if exc.code == 404 and action == "add":
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
    """Per-user genre and country exposure counts, derived live from
    user_history × tracks. Used by the Discovery picker to weight against
    over-played cells — implements ARCHITECTURE.md Principle 1 (breadth
    first; every region/genre deserves a foothold).

    Country axis prefers `origin_region` (MusicBrainz country) and falls
    back to `region` (macro). Returns:
        {"genres":    {"ambient dub": 46, "indie rock": 20, ...},
         "countries": {"USA": 160, "Japan": 42, ...}}
    """
    if not user_id:
        return {"genres": {}, "countries": {}}
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
    return {
        "genres":    {r["g"]: r["plays"] for r in genre_rows},
        "countries": {r["c"]: r["plays"] for r in country_rows},
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
                # Persist the playback source. Trust the client's value; if
                # absent, infer from the id form ('bc:' = Bandcamp) so it's
                # correct even for older clients / replayed rows.
                src = item.get("source")
                if not src:
                    src = "bandcamp" if str(item.get("id") or "").startswith("bc:") else "spotify"
                cur.execute(
                    """
                    INSERT INTO user_history
                        (user_id, track_id, track_name, artist, region, status,
                         listened_at, played_pct, mode, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        src,
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

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        user_id = self.get_user()
        self._req_user = user_id  # consumed by log_message
        self._req_t0 = time.time()

        # ── Auth flow ─────────────────────────────────────────────────────────

        if parsed.path == "/login":
            auth_url = make_sp_oauth().get_authorize_url()
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
            _evt("token", user=user_id, outcome="ok",
                 scope_count=len((token_info.get("scope") or "").split()))
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
                if _token_has_playlist_modify(user_id):
                    mirrored = _spotify_playlist_call(user_id, "add", track_id)
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
                if _token_has_playlist_modify(user_id):
                    mirrored = _spotify_playlist_call(user_id, "remove", track_id)
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
                track_count = sum(len(v) for v in disc.values() if isinstance(v, list))
                _evt("discovery", user=user_id or "anon",
                     regions=len(disc), tracks=track_count,
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
            if device_id:
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

            # Step 2: Play the track
            url = "https://api.spotify.com/v1/me/player/play"
            if device_id:
                url += f"?device_id={device_id}"
            req_body = json.dumps({
                "uris": [f"spotify:track:{tid}" for tid in track_ids],
                "position_ms": position_ms,
            }).encode()
            req = urllib.request.Request(url, data=req_body, method="PUT", headers=headers)
            t_play = time.time()
            try:
                resp = urllib.request.urlopen(req, timeout=8)
                play_status = resp.status
                play_ms = int((time.time() - t_play) * 1000)
                # Deterministic start-from-zero. position_ms in the play body is
                # unreliable when the play follows a device transfer (Spotify can
                # inherit the transferred position), so when we asked for 0 we
                # force it with an explicit seek. Best-effort — a seek failure
                # must never fail the play. Converges to 0 even if it races the
                # track switch (seeking old→0 then new loads at 0, or new→0).
                seek_status = None
                if position_ms == 0:
                    try:
                        seek_url = "https://api.spotify.com/v1/me/player/seek?position_ms=0"
                        if device_id:
                            seek_url += f"&device_id={device_id}"
                        seek_req = urllib.request.Request(seek_url, data=b"", method="PUT", headers=headers)
                        seek_status = urllib.request.urlopen(seek_req, timeout=5).status
                    except urllib.error.HTTPError as e:
                        seek_status = e.code
                    except Exception:
                        seek_status = -1
                _evt("transport", action="play", user=user_id, id=track_id,
                     device=device_id or "-", outcome="ok",
                     transfer_ms=transfer_ms, transfer_status=transfer_status,
                     play_ms=play_ms, play_status=play_status, seek_status=seek_status,
                     total_ms=int((time.time() - t_total) * 1000))
                self.send_json({"ok": True, "status": play_status})
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")[:300]
                play_ms = int((time.time() - t_play) * 1000)
                _evt("transport", action="play", user=user_id, id=track_id,
                     device=device_id or "-", outcome="error",
                     transfer_ms=transfer_ms, transfer_status=transfer_status,
                     play_ms=play_ms, play_status=e.code, body=body,
                     total_ms=int((time.time() - t_total) * 1000))
                self.send_json({"error": f"spotify_{e.code}", "detail": body}, e.code if e.code < 500 else 502)
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
                sp.start_playback(device_id=device_id)
                _evt("transport", action="resume", user=user_id, device=device_id or "-", outcome="ok")
                self.send_json({"ok": True})
            except Exception as e:
                _evt("transport", action="resume", user=user_id, device=device_id or "-",
                     outcome="error", err=repr(e)[:160])
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

        # Prevent browsers from caching stale HTML
        if self.path.endswith(".html") or parsed.path == "/":
            filepath = os.path.join(WEB_DIR, self.path.lstrip("/"))
            if os.path.exists(filepath):
                content = open(filepath, "rb").read()
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

        # ── Magic-link sign-in: request a link ───────────────────────────────
        if parsed.path == "/auth/request":
            length = int(self.headers.get("Content-Length", 0))
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except Exception:
                body = {}
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
            length = int(self.headers.get("Content-Length", 0))
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except Exception:
                body = {}
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
            length = int(self.headers.get("Content-Length", 0))
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except Exception:
                body = {}
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
            length = int(self.headers.get("Content-Length", 0))
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except Exception:
                body = {}
            email = (body.get("email") or "").strip().lower()
            if not email:
                self.send_json({"error": "email_required"}, 400)
                return
            execute("UPDATE access_requests SET status='ignored' "
                    "WHERE lower(email)=lower(%s)", (email,))
            _evt("waitlist_ignore", email=email[:60])
            self.send_json({"ok": True})
            return

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
                _evt("history-sync", user="anon", outcome="401")
                self.send_json({"error": "not_authenticated"}, 401)
                return
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                items = json.loads(body.decode())
            except json.JSONDecodeError as e:
                # Mobile networks can truncate large multi-hundred-KB POSTs
                # mid-string. Return a soft 400 instead of crashing the
                # request thread (which then tried to send_json into a
                # half-closed socket and produced cascade BrokenPipe noise).
                _evt("history-sync", user=user_id, ok=False,
                     err="json_decode", bytes_received=len(body),
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
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "127.0.0.1")
    print(f"\n🎵 DIG running at http://{host}:{port}\n")
    server = http.server.HTTPServer((host, port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
