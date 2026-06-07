"""
DIG — Bandcamp resolver.

Bandcamp has no public API and its on-page stream URLs are signed + expire in
hours, so we NEVER store a playable URL. Instead we store a stable identity —
`band_id` + `track_id` — and resolve a fresh full-track stream URL at play time
via Bandcamp's mobile API (the same endpoint the official iOS app uses):

    GET /api/mobile/25/tralbum_details?band_id=<b>&tralbum_type=t&tralbum_id=<t>

This returns the track with a fresh `streaming_url["mp3-128"]` (the FULL track,
not a 30s clip — verified) plus duration, title, artist, art_id and tags.

This path costs ZERO Spotify quota — it is entirely independent of the Spotify
drip-lane (see lib/spotify_gate.py / reference_dig_spotify_quota).

Track identity in DIG's pool: `id = "bc:<band_id>:<track_id>"`, `source =
"bandcamp"`. Split the id, call resolve_stream(), play the returned URL through
an HTML5 <audio> element (proven on iPhone incl. lock-screen auto-advance).
"""

import fcntl
import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request

API_BASE = "https://bandcamp.com/api/mobile/25"
# Pretend to be the official app's client — the mobile API is what it uses.
_UA = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"
_TIMEOUT = 20

# ── request safety ───────────────────────────────────────────────────────────
# Bandcamp has no official API and bans on request RATE/bursts, not track count.
# So: never burst, jitter every gap, and back off hard on any pushback. Bulk
# ingest (discover) paces heavily; the per-play resolve call stays light so it
# doesn't add user-facing latency. Both share one cooldown circuit-breaker.
_PACE_PATH = "/tmp/dig_bandcamp_pace.json"
_PACE_LOCK = "/tmp/dig_bandcamp_pace.lock"
_COOLDOWN_PATH = "/tmp/dig_bandcamp_cooldown.json"
INGEST_MIN_INTERVAL = float(os.environ.get("DIG_BANDCAMP_INGEST_INTERVAL", "4.0"))
INGEST_JITTER = 4.0           # → 4–8 s, randomised, between discover calls
RESOLVE_MIN_INTERVAL = 0.3
RESOLVE_JITTER = 0.4
# How long to go quiet after Bandcamp pushes back (undocumented — be generous).
BLOCK_COOLDOWN_SECONDS = int(os.environ.get("DIG_BANDCAMP_COOLDOWN", "7200"))  # 2h


class BandcampBlocked(Exception):
    """Bandcamp signalled pushback (429/403/503/challenge) — stop and back off."""


def cooldown_remaining():
    """Seconds left on a recorded block cooldown (0 if clear). Cheap file read."""
    try:
        with open(_COOLDOWN_PATH) as f:
            rem = int(json.load(f).get("until", 0)) - int(time.time())
        return rem if rem > 0 else 0
    except Exception:
        return 0


def _record_cooldown(seconds=BLOCK_COOLDOWN_SECONDS):
    try:
        with open(_COOLDOWN_PATH, "w") as f:
            json.dump({"until": int(time.time()) + int(seconds)}, f)
    except Exception:
        pass


def _throttle(min_interval, jitter):
    """Cross-process pace with random jitter so calls never look bursty/regular."""
    try:
        lf = open(_PACE_LOCK, "w")
    except Exception:
        time.sleep(min_interval + random.random() * jitter)
        return
    try:
        fcntl.flock(lf, fcntl.LOCK_EX)
        last = 0.0
        try:
            with open(_PACE_PATH) as f:
                last = float(json.load(f).get("last", 0.0))
        except Exception:
            last = 0.0
        target = min_interval + random.random() * jitter
        wait = target - (time.time() - last)
        if wait > 0:
            time.sleep(min(wait, 30.0))
        try:
            with open(_PACE_PATH, "w") as f:
                json.dump({"last": time.time()}, f)
        except Exception:
            pass
    finally:
        try:
            fcntl.flock(lf, fcntl.LOCK_UN)
            lf.close()
        except Exception:
            pass


def _looks_blocked(body):
    head = (body or "")[:2000].lower()
    return ("client challenge" in head or "captcha" in head
            or "unusual traffic" in head or "/cdn-cgi/challenge" in head)


def _fetch(url, ingest=False):
    """One throttled, cooldown-aware Bandcamp request. Raises BandcampBlocked on
    any pushback and records a shared cooldown so the whole fleet backs off."""
    # Both paths respect the cooldown — hammering during a block only prolongs it.
    if cooldown_remaining() > 0:
        raise BandcampBlocked(f"in cooldown {cooldown_remaining()}s")
    if ingest:
        # Bulk scraping: globally serialised + heavily paced (the real ban risk).
        _throttle(INGEST_MIN_INTERVAL, INGEST_JITTER)
    else:
        # Per-play resolve (user playback): light, NON-locking jitter so playback
        # is never serialised behind an ingest run's long paced sleeps.
        time.sleep(random.random() * RESOLVE_JITTER)
    req = urllib.request.Request(url, headers={
        "User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"})
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code in (429, 403, 503):
            _record_cooldown()
            raise BandcampBlocked(f"HTTP {e.code}")
        raise
    if _looks_blocked(body):
        _record_cooldown()
        raise BandcampBlocked("challenge page")
    return body


def _get_json(url, ingest=False):
    return json.loads(_fetch(url, ingest=ingest))


def _get_text(url, ingest=False):
    return _fetch(url, ingest=ingest)


def art_url(art_id, size=10):
    """Bandcamp cover art. size: 10 ~1200px, 16 ~700px, 3 ~100px."""
    if not art_id:
        return ""
    return f"https://f4.bcbits.com/img/a{art_id}_{size}.jpg"


def tralbum_details(band_id, tralbum_id, tralbum_type="a"):
    """Raw mobile-API call. tralbum_type: 't' track, 'a' album. Raises on HTTP
    error; returns the parsed dict (which may carry {'error': True})."""
    qs = urllib.parse.urlencode({
        "band_id": band_id,
        "tralbum_type": tralbum_type,
        "tralbum_id": tralbum_id,
    })
    return _get_json(f"{API_BASE}/tralbum_details?{qs}")


def resolve_stream(band_id, track_id):
    """Resolve a single track to a FRESH full-track stream URL + metadata.

    Returns {ok, url, duration, title, artist, art, tags, streamable} — `ok` is
    False (with an `error` key) on failure or a non-streamable track. The caller
    plays `url` immediately; it expires within hours, so never cache it.
    """
    try:
        d = tralbum_details(band_id, track_id, tralbum_type="t")
    except urllib.error.HTTPError as e:
        return {"ok": False, "error": f"http_{e.code}"}
    except Exception as e:
        return {"ok": False, "error": f"fetch_{type(e).__name__}"}

    if d.get("error"):
        return {"ok": False, "error": d.get("error_message") or "api_error"}

    tracks = d.get("tracks") or []
    if not tracks:
        return {"ok": False, "error": "no_tracks"}
    t = tracks[0]
    url = (t.get("streaming_url") or {}).get("mp3-128")
    if not url:
        # Buy-only / not freely streamable — no full-track URL available.
        return {"ok": False, "error": "not_streamable"}

    tags = [tg.get("name") for tg in (d.get("tags") or []) if tg.get("name")]
    return {
        "ok": True,
        "url": url,
        "duration": t.get("duration") or 0,
        "title": t.get("title") or d.get("title") or "",
        "artist": d.get("tralbum_artist") or (d.get("band") or {}).get("name") or "",
        "art": art_url(t.get("art_id") or d.get("art_id")),
        "tags": tags,
        "streamable": True,
    }


# ── identity helpers ──────────────────────────────────────────────────────────

def make_id(band_id, track_id):
    """DIG pool id for a Bandcamp track."""
    return f"bc:{band_id}:{track_id}"


def parse_id(track_id):
    """Split 'bc:<band_id>:<track_id>' -> (band_id, track_id). Returns
    (None, None) if it isn't a Bandcamp id."""
    m = re.match(r"^bc:(\d+):(\d+)$", track_id or "")
    if not m:
        return None, None
    return m.group(1), m.group(2)


# ── discovery (genre-driven, non-hardcoded) ──────────────────────────────────
# Bandcamp's own discover taxonomy drives breadth — we enumerate genres rather
# than hardcoding artist/genre search strings. /search is bot-walled, but the
# discover endpoint (what the website's "discover" grid uses) is open.

DISCOVER_URL = "https://bandcamp.com/api/discover/3/get_web"

# Top-level Bandcamp genres. Enumerating across all of them is the diversity
# engine: every genre gets a foothold, so the pool isn't Spotify-genre-shaped.
GENRES = [
    "electronic", "rock", "experimental", "hip-hop-rap", "ambient", "punk",
    "metal", "jazz", "folk", "world", "pop", "alternative", "indie", "soul",
    "funk", "reggae", "classical", "country", "blues", "latin", "devotional",
    "spoken-word", "soundtrack", "dub", "techno", "house", "idm",
]

# US state -> country, plus common shorthands, so location_text collapses to a
# country for the discovery region axis (genre is the primary axis; region is
# secondary — kept light on purpose).
_US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming",
}
_COUNTRY_ALIASES = {
    "uk": "United Kingdom", "u.k.": "United Kingdom", "england": "United Kingdom",
    "scotland": "United Kingdom", "wales": "United Kingdom",
    "usa": "United States", "us": "United States", "u.s.": "United States",
    "u.s.a.": "United States",
}


def location_to_country(loc):
    """'Bristol, UK' -> 'United Kingdom'; 'Austin, Texas' -> 'United States'.
    Best-effort; returns '' when nothing usable."""
    if not loc:
        return ""
    last = loc.split(",")[-1].strip()
    low = last.lower()
    if low in _US_STATES:
        return "United States"
    if low in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[low]
    return last


def discover(genre, page=0, sort="top"):
    """One page (~48 releases) of a Bandcamp genre. Returns normalized track
    dicts built from each release's FEATURED track — one signature track per
    artist, which maximises breadth. Only fully-streamable releases are kept
    (a present mp3-128 file == streamable). Costs no second call per release.

    sort: 'top' | 'new' | 'rec' (recommended).
    """
    qs = urllib.parse.urlencode({
        "g": genre, "s": sort, "p": page, "gn": 0, "f": "all", "w": 0,
    })
    data = _get_json(f"{DISCOVER_URL}?{qs}", ingest=True)
    out = []
    for it in data.get("items", []):
        ft = it.get("featured_track") or {}
        tid = ft.get("id")
        # Streamable check: the featured track must expose an mp3-128 file.
        if not tid or not ((ft.get("file") or {}).get("mp3-128")):
            continue
        band_id = it.get("band_id")
        if not band_id:
            continue
        loc = it.get("location_text") or ""
        out.append({
            "id": make_id(band_id, tid),
            "name": ft.get("title") or "",
            "artist": it.get("secondary_text") or "",
            "album": it.get("primary_text") or "",
            "art": art_url(it.get("art_id")),
            "genres": [it.get("genre_text")] if it.get("genre_text") else [],
            "region": location_to_country(loc),
            "location": loc,
            "duration": ft.get("duration") or 0,
            "source": "bandcamp",
        })
    return out
