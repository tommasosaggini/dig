"""Shared Spotify rate-limit pre-flight.

Every cron-entry script (discover.py, deep_crawl.py, ingest_mb_artists.py,
crawl_genre_seeds.py, …) calls `pre_flight_or_exit()` at the top of main()
BEFORE making any other API call. The check itself is one cheap search; if
Spotify is in cooldown for our app, we read `Retry-After` from the 429,
record it, and exit 0 — cron logs the no-op cleanly and we don't burn
queued requests against a closed window.

Probe results are cached on disk for 60 s so a single cron tick that
launches multiple scripts doesn't probe each time. After a 429 we cache
the *full* Retry-After window so dependent scripts in the same minute
short-circuit without probing at all.
"""

import json
import os
import time
import urllib.error
import urllib.request
import base64

CACHE_PATH = "/tmp/dig_spotify_health.json"
PROBE_TTL_SECONDS = 60       # how long to trust a 200 probe before re-probing
ABORT_BUFFER_SECONDS = 30    # stay this far away from the cooldown's edge


def _now():
    return int(time.time())


def _load_cache():
    try:
        with open(CACHE_PATH) as f:
            return json.load(f)
    except Exception:
        return None


def _save_cache(d):
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(d, f)
    except Exception:
        pass


def _app_token():
    cid = os.environ.get("SPOTIPY_CLIENT_ID")
    cs = os.environ.get("SPOTIPY_CLIENT_SECRET")
    if not cid or not cs:
        return None
    auth = base64.b64encode(f"{cid}:{cs}".encode()).decode()
    req = urllib.request.Request(
        "https://accounts.spotify.com/api/token",
        data=b"grant_type=client_credentials",
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.loads(r.read())["access_token"]


# A stable, famous artist id. The probe needs SOME endpoint; this one is
# read-only, cacheable on Spotify's side, and — critically — one the pipeline
# actually depends on.
PROBE_URL = "https://api.spotify.com/v1/artists/0TnOYISbd1XYRBk9myaseg"


def probe():
    """Live single-call probe. Returns (ok, retry_after_seconds).
    `ok=True` means Spotify is answering; `ok=False, retry_after=N` means
    rate-limited for N more seconds.

    PROBES /artists/{id}, NOT /search.

    It used to probe /search, and that quietly became a permanent stop sign.
    Spotify's Nov 2024 Dev Mode restrictions rate-limit /search into ~16-22h
    bans while /artists/{id}, /artists/{id}/albums and /albums/{id}/tracks keep
    answering 200 (measured 2026-08-04 from three machines — same countdown on
    all three, so one app-wide counter, not an IP block). With the ingest path
    rebuilt on those working endpoints, a /search probe would report "locked
    out" forever and abort every run in pre-flight without a single real call.

    The rule this encodes: probe what the work needs, not what it used to need.
    """
    try:
        tok = _app_token()
        if not tok:
            return (True, 0)  # no creds → can't probe; let downstream handle
        req = urllib.request.Request(
            PROBE_URL, headers={"Authorization": f"Bearer {tok}"},
        )
        with urllib.request.urlopen(req, timeout=8):
            return (True, 0)
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            try:
                retry = int(exc.headers.get("Retry-After", "60"))
            except Exception:
                retry = 60
            return (False, retry)
        # Other HTTP errors: treat as transient, allow proceed
        return (True, 0)
    except Exception:
        # Network/DNS hiccup: don't block; downstream will see it
        return (True, 0)


def cached_or_probe():
    """Use the cached probe if fresh; else live-probe and update the cache."""
    now = _now()
    cache = _load_cache()
    if cache:
        # If we previously hit 429, respect the recorded cooldown end
        cooldown_until = cache.get("cooldown_until", 0)
        if cooldown_until and now < cooldown_until:
            return (False, cooldown_until - now)
        # If we last probed recently with 200, trust it
        last_probe = cache.get("last_probe_at", 0)
        if cache.get("last_probe_ok") and (now - last_probe) < PROBE_TTL_SECONDS:
            return (True, 0)
    ok, retry = probe()
    new_cache = {"last_probe_at": now, "last_probe_ok": ok}
    if not ok:
        new_cache["cooldown_until"] = now + retry
    _save_cache(new_cache)
    return (ok, retry)


def pre_flight_or_exit(script_name="(unknown)", verbose=True):
    """Probe Spotify; if in cooldown, exit 0 cleanly. Call this at the top
    of every cron-entry script's main() BEFORE making any other Spotify
    request. Exits with status 0 so cron treats the run as a successful
    no-op rather than a failure."""
    ok, retry = cached_or_probe()
    if ok:
        if verbose:
            print(f"[spotify-health] {script_name}: pre-flight OK")
        return True
    if verbose:
        print(
            f"[spotify-health] {script_name}: app rate-limited, "
            f"Retry-After={retry}s — aborting run cleanly (exit 0)"
        )
    raise SystemExit(0)


def record_429(retry_after_seconds: int):
    """Called by call-sites that hit a 429 mid-run; persists the cooldown
    so downstream scripts skip the probe and exit immediately. Use this
    instead of letting the script chew through 50 more requests before
    realising every one will fail."""
    now = _now()
    cache = _load_cache() or {}
    cache["cooldown_until"] = now + max(retry_after_seconds, ABORT_BUFFER_SECONDS)
    cache["last_probe_at"] = now
    cache["last_probe_ok"] = False
    _save_cache(cache)
