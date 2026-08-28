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
import unicodedata
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


def _post_json(url, payload, ingest=False):
    """POST variant of _fetch — same throttle, cooldown and UA discipline."""
    if cooldown_remaining() > 0:
        raise BandcampBlocked(f"in cooldown {cooldown_remaining()}s")
    if ingest:
        _throttle(INGEST_MIN_INTERVAL, INGEST_JITTER)
    else:
        time.sleep(random.random() * RESOLVE_JITTER)
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={
        "User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        if e.code in (429, 403, 503):
            _record_cooldown()
        raise


DISCOVER_TAG_URL = "https://bandcamp.com/api/discover/1/discover_web"


def discover_by_tag(tag, cursor=None, slice="top", size=48):
    """One page of tag-filtered discover — the only Bandcamp surface with a
    GEOGRAPHIC lever. The classic feed (discover()) has no location filter,
    which is why the Bandcamp side of the pool grew 52% US/UK/CA: it mirrors
    Bandcamp's user base. Artists tag releases with their country
    ('senegal', 'cambodia', 'mongolia'), so country-tag pages are how digging
    escapes that gravity.

    Returns (tracks, next_cursor, result_count). Track dicts match
    discover()'s shape; year/decade come free from release_date. Genres start
    empty — the play-time resolve backfills the release's full tag set.
    """
    payload = {
        "tag_norm_names": [tag],
        "include_result_types": ["a", "s"],
        "size": size,
        "slice": slice,
    }
    if cursor:
        payload["cursor"] = cursor
    data = _post_json(DISCOVER_TAG_URL, payload, ingest=True)
    out = []
    for it in data.get("results", []):
        ft = it.get("featured_track") or {}
        tid, band_id = ft.get("id"), it.get("band_id")
        # stream_url present == streamable (this API exposes no file dict)
        if not tid or not band_id or not ft.get("stream_url"):
            continue
        loc = it.get("band_location") or ""
        year = str(it.get("release_date") or "")[:4]
        year = year if year.isdigit() and 1900 <= int(year) <= 2100 else ""
        out.append({
            "id": make_id(band_id, tid),
            "name": ft.get("title") or "",
            "artist": it.get("band_name") or "",
            "album": it.get("title") or "",
            "art": art_url((it.get("primary_image") or {}).get("image_id")),
            "genres": [],
            "region": location_to_country(loc),
            "location": loc,
            "year": year,
            "decade": (year[:3] + "0s") if year else "",
            "duration": ft.get("duration") or 0,
            "source": "bandcamp",
        })
    return out, data.get("cursor"), data.get("result_count") or 0


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
        # Artist's declared location — the authoritative signal for dropping a
        # city tag from the genre list during play-time enrichment.
        "location": (d.get("band") or {}).get("location") or "",
        # Release year from the tralbum's unix timestamp — Bandcamp rows were
        # ingested without year (32k of them), and this payload carries it for
        # free on every play.
        "release_year": release_year(d),
        "streamable": True,
    }


def release_year(tralbum: dict) -> str:
    """'1972' from a tralbum payload's release_date (unix ts), '' if absent."""
    ts = tralbum.get("release_date") or tralbum.get("album_release_date")
    try:
        y = time.gmtime(int(ts)).tm_year
    except (TypeError, ValueError, OverflowError, OSError):
        return ""
    return str(y) if 1900 <= y <= 2100 else ""


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
# than hardcoding artist/genre search strings.
#
# CORRECTION 2026-08-05: this comment used to end "/search is bot-walled", and
# that sentence was wrong for long enough to be quoted as a reason not to build
# a search-based resolver at all. What is dead is two OLD endpoint NAMES —
# fuzzysearch/1/autocomplete and nusearch/2/autocomplete both answer 200 with
# {"error":true,"error_message":"bad function"}. The endpoints the site itself
# uses answer normally: fuzzysearch/1/app_autocomplete (GET) and
# bcsearch_public_api/1/autocomplete_elastic (POST). See search_tracks below.

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
    # The raw fallthrough used to return the last comma-token verbatim, which
    # is how 'Ontario', 'Québec' and 'Antarctica' became pool regions.
    # canonical_region maps known cities/subdivisions to their country and
    # passes genuinely new values through so they stay visible.
    from lib.region_norm import canonical_region
    return canonical_region(last)


# ── genre normalization ───────────────────────────────────────────────────────
# Bandcamp tags mix true genres with the artist's CITY (e.g. 'Montreal'), and use
# loose spellings/separators ('hip-hop/rap', 'r&b/soul'). We normalize so they
# co-cluster with the pool's existing (Spotify-derived) genre axis instead of
# sitting in their own raw buckets. Design: drop locations, split slash-joined
# genres, alias a SMALL set of high-confidence variants to their canonical form.
# Broad umbrella terms (rock/pop/jazz/…) are kept as-is — they're valid genres
# and the primary stratification anchor for Bandcamp rows; the canonical map is
# granular (it has 'indie pop','hip hop','r&b' but not bare 'rock'), so we never
# force a broad term to drop just because it isn't a map coordinate.

# Spelling/separator variants -> canonical token. Targets verified to exist in
# the pool's genre axis (hip hop, r&b, …) or to be valid broad genres.
_GENRE_ALIASES = {
    "hip-hop": "hip hop", "hiphop": "hip hop", "hip hop/rap": "hip hop",
    "hip-hop/rap": "hip hop", "rap": "hip hop", "trap": "hip hop",
    "rnb": "r&b", "r & b": "r&b", "r'n'b": "r&b", "r&b/soul": "r&b",
    "dnb": "drum and bass", "d&b": "drum and bass", "drum & bass": "drum and bass",
    "electronica": "electronic", "edm": "electronic",
    "alt": "alternative", "lo fi": "lo-fi", "lofi": "lo-fi",
}

# Place names that show up as tags (countries, US states, common music cities).
# The per-track artist location (band.location) is the authoritative signal and
# is passed in separately; this set is the safety net for rows without it.
_COMMON_CITIES = {
    "montreal", "toronto", "vancouver", "london", "manchester", "bristol",
    "berlin", "hamburg", "cologne", "paris", "lyon", "amsterdam", "rotterdam",
    "brussels", "stockholm", "gothenburg", "oslo", "copenhagen", "helsinki",
    "lisbon", "porto", "madrid", "barcelona", "milan", "rome", "vienna",
    "zurich", "warsaw", "prague", "budapest", "athens", "istanbul", "moscow",
    "kyiv", "kiev", "tokyo", "osaka", "kyoto", "seoul", "beijing", "shanghai",
    "taipei", "bangkok", "jakarta", "manila", "mumbai", "delhi", "bangalore",
    "sydney", "melbourne", "auckland", "wellington", "cape town", "johannesburg",
    "lagos", "nairobi", "accra", "cairo", "tel aviv", "beirut", "dubai",
    "mexico city", "guadalajara", "bogota", "lima", "santiago", "buenos aires",
    "sao paulo", "rio de janeiro", "new york", "brooklyn", "los angeles",
    "san francisco", "oakland", "seattle", "portland", "chicago", "detroit",
    "austin", "nashville", "atlanta", "new orleans", "boston", "philadelphia",
    "washington", "miami", "denver", "minneapolis", "dublin", "glasgow",
    "edinburgh", "leeds", "reykjavik", "tallinn", "riga", "vilnius",
}
_KNOWN_PLACES = (
    {s for s in _US_STATES}
    | set(_COUNTRY_ALIASES.keys())
    | {v.lower() for v in _COUNTRY_ALIASES.values()}
    | _COMMON_CITIES
    | {"united states", "united kingdom", "canada", "australia", "germany",
       "france", "italy", "spain", "portugal", "netherlands", "belgium",
       "sweden", "norway", "denmark", "finland", "iceland", "ireland",
       "poland", "czech republic", "austria", "switzerland", "greece",
       "japan", "china", "south korea", "india", "brazil", "mexico",
       "argentina", "chile", "colombia", "new zealand", "south africa",
       "nigeria", "egypt", "russia", "ukraine", "turkey", "israel"}
)


def location_tokens(loc):
    """Lowercased tokens of an artist location string ('Montreal, Québec' ->
    {'montreal','québec'}) — the authoritative per-track signal for dropping
    a city/region that Bandcamp also listed as a tag."""
    return {p.strip().lower() for p in re.split(r"[,/]", loc or "") if p.strip()}


def normalize_genres(tags, loc_tokens=()):
    """Normalize a list of raw Bandcamp tags into clean genre tokens.

    - drops locations (the artist's own `loc_tokens` + the _KNOWN_PLACES net)
    - splits slash/comma-joined genres ('r&b/soul' -> 'r&b','soul')
    - applies the alias map ('hip-hop/rap' -> 'hip hop')
    - lowercases + collapses whitespace; preserves order, dedups
    Unknown non-location tags are KEPT (likely real niche genres)."""
    loc = set(loc_tokens or ())
    out, seen = [], set()
    for raw in (tags or []):
        for part in re.split(r"[/,]", raw or ""):
            g = re.sub(r"\s+", " ", part.strip().lower())
            if not g:
                continue
            g = _GENRE_ALIASES.get(g, g)            # alias BEFORE place-check
            if g in loc or g in _KNOWN_PLACES:
                continue                            # it's a location, drop it
            if g not in seen:
                seen.add(g)
                out.append(g)
    return out


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
            "genres": normalize_genres([it.get("genre_text")], location_tokens(loc)),
            "region": location_to_country(loc),
            "location": loc,
            "duration": ft.get("duration") or 0,
            "source": "bandcamp",
        })
    return out


# ── search → resolve a named track ───────────────────────────────────────────
#
# Curator ingest starts from prose: an Instagram caption naming "Biosphere —
# Spindrift". Turning that into something playable needs a name→id lookup, and
# for a long time this file said Bandcamp had none (see the correction above).
# It does, and it is the better door than the alternatives: Spotify's /search
# is rate-limited into ~16-22h bans in Dev Mode, and MusicBrainz resolves the
# NAME reliably but only carries a Spotify link for roughly one artist in six.
# Bandcamp is also where this kind of music actually lives, and it is the
# source Dig prefers everywhere.
#
# PRECISION OVER RECALL, deliberately. A wrong match is worse than no match:
# it puts someone else's music in the pool under a curator's name, and nothing
# downstream can tell. Measured on a 12-track sample, a naive "first result
# wins" scored 7/12 — but two of those were a Sinatra bootleg credited to
# "Caball Music" and a Tokischa re-upload credited to "Klean". Requiring the
# BAND NAME to agree with the artist we asked for rejects both, which is why
# the match below is an AND and not a score threshold.

SEARCH_URL = "https://bandcamp.com/api/bcsearch_public_api/1/autocomplete_elastic"


def _post_json(url, payload, ingest=True):
    """POST sibling of _fetch: same cooldown, same pacing, same block detection.

    Written rather than reusing _fetch because urllib treats a request with a
    body as a POST, and _fetch's signature has no room for one — and routing a
    bulk search through the per-play resolve path would serialise nothing and
    pace nothing, which is the shape of request that earns a ban.
    """
    if cooldown_remaining() > 0:
        raise BandcampBlocked(f"in cooldown {cooldown_remaining()}s")
    if ingest:
        _throttle(INGEST_MIN_INTERVAL, INGEST_JITTER)
    else:
        time.sleep(random.random() * RESOLVE_JITTER)
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"),
        headers={"User-Agent": _UA, "Content-Type": "application/json",
                 "Accept-Language": "en-US,en;q=0.9"})
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
    return json.loads(body)


def search_tracks(text, limit=8, ingest=True):
    """Raw Bandcamp track hits for a free-text query. Never raises on a miss."""
    data = _post_json(SEARCH_URL, {
        "search_text": text, "search_filter": "t",   # 't' = tracks only
        "full_page": False, "fan_id": None,
    }, ingest=ingest)
    res = ((data or {}).get("auto") or {}).get("results") or []
    return [r for r in res if r.get("type") == "t"][:limit]


def _norm_tokens(s):
    """Lowercase, accent-stripped, punctuation-flattened token set."""
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    return {t for t in s.lower().split() if len(t) > 1}


def _agree(a, b):
    """Token containment either way — the loose-but-directional test used
    throughout Dig. Handles "Snd" vs "SND", "Rosalía" vs "ROSALÍA", and
    "Spindrift" vs "Spindrift (Remastered)"."""
    ta, tb = _norm_tokens(a), _norm_tokens(b)
    if not ta or not tb:
        return False
    return ta.issubset(tb) or tb.issubset(ta)


def is_same_artist(asked, band_name):
    """Is this Bandcamp account the artist we asked about?

    STRICTER THAN _agree ON PURPOSE. _agree accepts token containment in either
    direction, which is right when a track title is checked too and wrong when
    the artist name is the only anchor. Searching "Bethel Music" matched an
    uploader calling itself "Amanda Cook , Bethel Music", and five hashtag-
    stuffed worship re-uploads went into the pool labelled '2 tone'.

    A hit may be a SHORTER form of the name we asked for — "Cesaria Evora" for
    "Cesária Évora" — but never a longer one carrying extra names, because the
    extra names are somebody else.
    """
    got = _norm_tokens(band_name)
    return bool(got) and got.issubset(_norm_tokens(asked))


def tracks_by_artist(artist, limit=8, ingest=True):
    """Everything Bandcamp has by a NAMED artist, as pool rows.

    The genre-coverage backfill needs this shape rather than resolve_track's:
    there, we know the record we want and ask whether Bandcamp has it. Here we
    only know who to ask about — the artist came out of MusicBrainz, Wikidata
    or Discogs as someone who plays a genre Dig cannot serve — and we want
    whatever they have.

    The band-name agreement rule is kept exactly as resolve_track applies it,
    and it is the only thing standing between "artists who play agbadza" and a
    pool full of records by whoever happens to rank for that word. Without a
    title to check, it is doing all the work alone, so it is applied to every
    hit rather than used to pick one.
    """
    rows, seen = [], set()
    for r in search_tracks(artist, limit=limit, ingest=ingest):
        if not (r.get("id") and r.get("band_id")):
            continue
        if not is_same_artist(artist, r.get("band_name")):
            continue
        tid = make_id(r["band_id"], r["id"])
        if tid in seen:
            continue
        seen.add(tid)
        rows.append({
            "id": tid,
            "name": r.get("name") or "",
            "artist": r.get("band_name") or "",
            "album": r.get("album_name") or "",
            "art": art_url(r.get("art_id")),
            "genres": [],
            "region": "",
            "location": "",
            "duration": 0,
            "source": "bandcamp",
            "bc_url": r.get("item_url_path") or "",
        })
    return rows


# A re-upload announces itself in the TITLE. Every one of these words means
# "somebody else's version of this", and a curator naming "The Cure – Pictures
# of You" did not mean "Pictures Of You (remixed by Dept. Mine)".
#
# `edit` is matched BARE, not only as `re-edit`. That knowingly costs the
# occasional official "Radio Edit", and it is the right side to err on here: an
# edit is a different rendition of the recording either way, and the header's
# rule is that a wrong match is worse than a missing one. "(Mike Tempo Edit)"
# is the case that forced it.
_BOOTLEG_MARK = re.compile(
    r"\b(re-?mix(?:ed)?|re-?work(?:ed)?|re-?edit|edit|bootleg|mash-?up|flip|"
    r"cover(?:ed)?|tribute|karaoke|instrumental|backing\s+track|"
    r"in\s+the\s+style\s+of|as\s+made\s+famous\s+by|8-?bit|slowed|sped\s+up|"
    r"nightcore|reverb(?:ed)?)\b", re.IGNORECASE)


def _gained_a_bootleg_mark(asked_title, got_title):
    """Did the hit's title pick up a re-upload marker the ask did not have?

    Directional on purpose. A curator asking for "Glow - Alternative Master"
    or a genuine "(Remix)" single must still resolve, so the marker only
    disqualifies when it appears on the HIT and not in what was asked for.
    """
    return bool(_BOOTLEG_MARK.search(got_title or "")) and \
        not _BOOTLEG_MARK.search(asked_title or "")


def _title_restates_the_artist(artist, got_title):
    """"Michael Jackson - Baby Be Mine (Mike Tempo Edit)" as a TRACK TITLE.

    A release does not put its own artist in the song title; an uploader
    does, because the upload is filed under an account that is not the
    artist. The separator is required — "Bowie" inside a title is a lyric,
    "Bowie - " in front of one is a credit.
    """
    got = (got_title or "").strip().lower()
    asked = (artist or "").strip().lower()
    if not got or not asked:
        return False
    for sep in (" - ", " – ", " — ", ": ", " | "):
        if got.startswith(asked + sep):
            return True
    return False


def resolve_track(artist, title, limit=8, ingest=True):
    """A named track -> a pool row in the SAME shape discover() produces, or None.

    Both the title and the artist must agree with the hit. Region, genres and
    duration are absent from search results (discover carries them, search does
    not), so they come back empty for the caller to fill from what it already
    knows — a curator's caption often states country and style outright, and
    MusicBrainz supplies them otherwise.

    FOUR gates, because three was not enough. Measured on doubleudiego's 71
    candidates (2026-08-17): 23 resolved and 8 of the 23 were re-uploads —
    a Bowie "ReWork", a Smiths remix, a Beatles cover, two MJ edits, a Cure
    remix, a Michael Jackson mashup. Every one of them is exactly the failure
    the PRECISION OVER RECALL note above says this function exists to prevent,
    and they got through because the artist side used the LOOSE `_agree`:
    "David Bowie - Heroes (Mindsodt-D ReWork)" contains the tokens of "David
    Bowie", so containment-either-way accepted the remixer's account as Bowie.
    `is_same_artist` is the strict form and already existed for exactly this
    reason — it was wired into tracks_by_artist and not into here.
    """
    query = f"{artist} {title}".strip()
    if not query:
        return None
    for r in search_tracks(query, limit=limit, ingest=ingest):
        if not (r.get("id") and r.get("band_id")):
            continue
        if not _agree(title, r.get("name")):
            continue
        # THE RULE THAT DOES THE WORK. Without it a bootleg edit uploaded by a
        # label answers for the artist who never uploaded anything. STRICT
        # (is_same_artist, not _agree): the account name may be a shorter form
        # of who we asked for, never a longer one, because the extra words are
        # somebody else — usually the remixer.
        if not is_same_artist(artist, r.get("band_name")):
            continue
        if _gained_a_bootleg_mark(title, r.get("name")):
            continue
        if _title_restates_the_artist(artist, r.get("name")):
            continue
        return {
            "id": make_id(r["band_id"], r["id"]),
            "name": r.get("name") or "",
            "artist": r.get("band_name") or "",
            "album": r.get("album_name") or "",
            "art": art_url(r.get("art_id")),
            "genres": [],
            "region": "",
            "location": "",
            "duration": 0,
            "source": "bandcamp",
            "bc_url": r.get("item_url_path") or "",
        }
    return None
