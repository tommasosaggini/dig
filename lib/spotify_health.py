"""Shared Spotify rate-limit pre-flight — PER ENDPOINT FAMILY.

Every cron-entry script (discover.py, deep_crawl.py, ingest_mb_artists.py,
crawl_genre_seeds.py, …) calls `pre_flight_or_exit()` at the top of main()
BEFORE making any other API call, naming the endpoint families it needs. If
one of them is in cooldown we exit 0 — cron logs the no-op cleanly and we
don't burn queued requests against a closed window.

**Spotify bans ONE ENDPOINT AT A TIME, not the app.** Measured 2026-08-18
19:13 JST from inside the container, four calls, one app token:

    artists/{id}          200
    artists/{id}/albums   429   Retry-After = 78769   (~21.9h)
    albums/{id}/tracks    200
    search                200

Two bugs followed from modelling that as one app-wide lockout, and both are
what this module is now shaped to prevent:

  * **The probe asked the wrong endpoint.** It probed `/artists/{id}`, which
    is never the endpoint under load, so pre-flight said OK and the run then
    died on its first `artist_albums` call. 552 runs logged "pre-flight OK"
    and 235 of them aborted anyway — a 40% abort rate over 24 days.
  * **One banned endpoint stopped every job.** `record_429` wrote a single
    global `cooldown_until`, so a 22h ban on `artist_albums` also blocked the
    likes sync, whose `me/library` calls were answering 200 the whole time.

The rule, restated so it survives the next rewrite: **probe the endpoint the
run is about to hammer, and quarantine only that endpoint.**

Cache shape is `{"families": {"<family>": {...}}}`. A pre-2026-08-18 file
carrying a bare top-level `cooldown_until` is IGNORED on read: it does not
say which endpoint it belonged to, and honouring it globally is the exact
behaviour being removed. The first probe per family re-derives the truth,
which costs one call and is self-correcting.
"""

import json
import os
import time
import re
import urllib.error
import urllib.parse
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


# ── Endpoint families ───────────────────────────────────────────────────────
# The unit Spotify actually bans. Derived from the request path, most specific
# first: `/artists/{id}/albums` is NOT `/artists/{id}`, and conflating them is
# what let a 22h ban hide behind a green pre-flight.

_FAMILY_RULES = (
    (r"^/v1/artists/[^/]+/albums", "artist_albums"),
    (r"^/v1/artists/[^/]+/top-tracks", "artist_top_tracks"),
    (r"^/v1/artists/[^/]+/related-artists", "artist_related"),
    (r"^/v1/artists", "artists"),
    (r"^/v1/albums/[^/]+/tracks", "album_tracks"),
    (r"^/v1/albums", "albums"),
    (r"^/v1/search", "search"),
    (r"^/v1/me/player", "player"),
    (r"^/v1/me/library", "library"),
    (r"^/v1/me/tracks", "library"),
    (r"^/v1/me/playlists", "playlists"),
    (r"^/v1/me", "me"),
    (r"^/v1/playlists", "playlists"),
    (r"^/v1/tracks", "tracks"),
    (r"^/v1/browse", "browse"),
)


def endpoint_family(url: str) -> str:
    """Which ban-unit a URL belongs to. Never raises: an unknown path gets its
    own bucket named after the first segment, which quarantines it on its own
    rather than pouring it into somebody else's cooldown."""
    try:
        path = urllib.parse.urlsplit(str(url or "")).path or ""
        if not path.startswith("/v1"):
            path = "/v1" + ("" if path.startswith("/") else "/") + path
        for pattern, family in _FAMILY_RULES:
            if re.match(pattern, path):
                return family
        seg = [p for p in path.split("/") if p][1:2]
        return seg[0] if seg else "other"
    except Exception:
        return "other"


# One probe URL per family, and only for families an APP token can reach.
# A family absent from this map is unprobeable (it needs a user token), so it
# is never pre-emptively declared down — only a real 429 can arm its cooldown.
# Probing a user endpoint with an app token returns 401/403, which the old
# single probe would have read as "fine" anyway; naming the gap is better than
# a probe that cannot fail honestly.
_ARTIST = "0TnOYISbd1XYRBk9myaseg"     # stable, famous, cacheable on their side
_ALBUM = "4aawyAB9vmqN3uQ7FjRGTy"

PROBE_URLS = {
    "artists": f"https://api.spotify.com/v1/artists/{_ARTIST}",
    "artist_albums": f"https://api.spotify.com/v1/artists/{_ARTIST}/albums?limit=1",
    "album_tracks": f"https://api.spotify.com/v1/albums/{_ALBUM}/tracks?limit=1",
    "albums": f"https://api.spotify.com/v1/albums/{_ALBUM}",
    "search": "https://api.spotify.com/v1/search?q=a&type=artist&limit=1",
}

# Kept so older imports keep working; it is the `artists` family's probe.
PROBE_URL = PROBE_URLS["artists"]

DEFAULT_FAMILY = "artists"


def probe(family: str = DEFAULT_FAMILY):
    """Live single-call probe OF THE NAMED FAMILY. Returns (ok, retry_after).

    `ok=True` means that endpoint is answering; `ok=False, retry=N` means it is
    rate-limited for N more seconds. A family with no probe URL (user-token
    endpoints) returns (True, 0) — unknown, not healthy; the caller's own 429
    is what arms it.

    It used to probe `/search` and then `/artists/{id}`, app-wide, and both
    were wrong for the same reason: the probe has to ask the question the run
    is about to ask. On 2026-08-18 `/artists/{id}` answered 200 while
    `/artists/{id}/albums` — the endpoint the ingest lives on — was 22 hours
    into a ban.
    """
    url = PROBE_URLS.get(family)
    if not url:
        return (True, 0)
    try:
        tok = _app_token()
        if not tok:
            return (True, 0)  # no creds → can't probe; let downstream handle
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {tok}"},
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


def _family_entry(cache, family):
    try:
        return (cache.get("families") or {}).get(family) or {}
    except Exception:
        return {}


def cooldown_for(family: str) -> int:
    """Seconds left on this family's recorded cooldown (0 if clear). Pure file
    read, never probes. A legacy top-level `cooldown_until` is deliberately not
    consulted — see the module docstring."""
    entry = _family_entry(_load_cache() or {}, family)
    try:
        rem = int(entry.get("cooldown_until", 0)) - _now()
        return rem if rem > 0 else 0
    except Exception:
        return 0


def cached_or_probe(family: str = DEFAULT_FAMILY):
    """Use this family's cached probe if fresh; else live-probe and cache it."""
    now = _now()
    entry = _family_entry(_load_cache() or {}, family)
    cooldown_until = entry.get("cooldown_until", 0)
    if cooldown_until and now < cooldown_until:
        return (False, cooldown_until - now)
    if entry.get("last_probe_ok") and (now - entry.get("last_probe_at", 0)) < PROBE_TTL_SECONDS:
        return (True, 0)
    ok, retry = probe(family)
    _write_family(family, {
        "last_probe_at": now,
        "last_probe_ok": ok,
        **({"cooldown_until": now + retry} if not ok else {}),
    })
    return (ok, retry)


def _write_family(family, fields):
    """Merge `fields` into one family's entry, leaving every other family
    untouched. Read-modify-write on a file several jobs share, so it keeps
    whatever it does not understand rather than rewriting the whole document."""
    cache = _load_cache() or {}
    families = dict(cache.get("families") or {})
    entry = dict(families.get(family) or {})
    entry.update(fields)
    families[family] = entry
    cache["families"] = families
    _save_cache(cache)


def pre_flight_or_exit(script_name="(unknown)", families=None, verbose=True):
    """Probe the endpoint families this script is about to use; if any is in
    cooldown, exit 0 cleanly. Call at the top of every cron-entry script's
    main() BEFORE making any other Spotify request. Exits 0 so cron treats the
    run as a successful no-op rather than a failure.

    `families` is the list of endpoints the run CANNOT WORK WITHOUT. Passing
    none falls back to the artists family, which is almost never what a caller
    wants — name what you call.
    """
    for family in (families or [DEFAULT_FAMILY]):
        ok, retry = cached_or_probe(family)
        if ok:
            continue
        if verbose:
            print(
                f"[spotify-health] {script_name}: {family} rate-limited, "
                f"Retry-After={retry}s — aborting run cleanly (exit 0)"
            )
        raise SystemExit(0)
    if verbose:
        names = ",".join(families or [DEFAULT_FAMILY])
        print(f"[spotify-health] {script_name}: pre-flight OK ({names})")
    return True


def record_429(retry_after_seconds: int, family: str = DEFAULT_FAMILY):
    """Persist a cooldown FOR ONE FAMILY, so other jobs skip the probe and
    exit immediately — without quarantining endpoints that are still serving.
    Use this instead of letting the script chew through 50 more requests
    before realising every one will fail."""
    now = _now()
    _write_family(family, {
        "cooldown_until": now + max(retry_after_seconds, ABORT_BUFFER_SECONDS),
        "last_probe_at": now,
        "last_probe_ok": False,
    })
