"""Artist name → Instagram handle, for the @mention in a post's caption.

WHY THIS IS A LOOKUP AND NEVER A GUESS.

`instagram.com/<name with the spaces taken out>` is right often enough to be
tempting and wrong often enough to be unacceptable: an @mention notifies whoever
owns that handle and puts their name on a post about someone else's record.
There is no un-sending it. So a handle only ever comes from a source that
asserts "this account belongs to this artist" — MusicBrainz url-relations first,
then the Wikidata item MB points at — and anything short of that resolves to
None, which the caption layer reads as "no link on this one".

COVERAGE, measured over the 23 artists in the queue on 2026-08-06:

    MusicBrainz url-rels     9   Clairo, Paramore, Katatonia, NewJeans, …
    + Wikidata P2003         1   Tony Joe White
    nothing                 13   La Lupe, Djeneba Seck, Sarantuya B., 張德蘭, …

The misses are the older and non-Western artists, which is to say precisely
what Dig is for — so the caption has to read well *without* a handle rather
than treat one as the normal case. Bandcamp artist pages list their own socials
and would be the obvious third source for the Terekke/helmuth end of the pool.

Everything is cached in `artist_instagram`, misses included: a name that has
been looked up and found nothing must not cost two MusicBrainz requests again
on the next cron tick. Outages are NOT cached — see resolve_handle.

KNOWN LIMIT: a collaboration credit ("Theodora, Jul") is resolved as one
string, and MusicBrainz's name matching may answer with either party — that
post is tagged @juldetp, not Theodora. Splitting the credit and looking up each
name would fix it and would also break "Earth, Wind & Fire" into three
lookups, two of which would match strangers. Tagging one real collaborator is
the better failure, so the credit stays whole.
"""
import datetime
import re

import requests

from lib.db import execute, fetchone, get_conn
from lib.mb_resolve import (MB_HEADERS, MB_LOOKUP_URL, MB_RATE_LIMIT_S,
                            MB_SEARCH_URL, MIN_MB_SCORE, _get, _name_agrees,
                            _norm)

import time

WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"

# instagram.com/<this> is only a profile for some values of <this>. The rest are
# the site's own furniture, and a post URL carries the poster's handle nowhere
# in the path — so "instagram.com/p/Cxyz" must not resolve to a user called "p".
IG_RESERVED = {
    "p", "reel", "reels", "tv", "stories", "explore", "accounts", "about",
    "developer", "legal", "privacy", "directory", "web", "challenge", "s",
}

# Instagram handles: letters, digits, dot, underscore, max 30.
IG_HANDLE_RE = re.compile(r"^[A-Za-z0-9._]{1,30}$")

# How far clear of the runner-up the best MB match has to be. See pick_artist.
DECISIVE_GAP = 10


def ensure_artist_ig_schema():
    """Idempotent DDL. Same self-provisioning pattern as ensure_ig_schema()."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS artist_instagram (
                    artist_key TEXT PRIMARY KEY,     -- normalised name
                    artist     TEXT NOT NULL,        -- as first seen
                    handle     TEXT,                 -- NULL = looked, found nothing
                    source     TEXT,                 -- musicbrainz | wikidata | manual
                    mbid       TEXT,
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
        conn.commit()
    finally:
        conn.close()


def handle_from_url(url):
    """The handle in an instagram.com URL, or None if it isn't a profile.

    Pure — the reserved-path and charset rules are the whole guard against
    tagging a stranger, so they are unit-tested rather than trusted.
    """
    m = re.search(r"(?:https?://)?(?:www\.)?instagram\.com/([^/?#\s]+)", url or "")
    if not m:
        return None
    handle = m.group(1).strip().rstrip("/").lstrip("@")
    if not handle or handle.lower() in IG_RESERVED:
        return None
    if not IG_HANDLE_RE.match(handle):
        return None
    return handle


def pick_artist(name, candidates):
    """The one MB artist a name unambiguously means, or None.

    Stricter than mb_resolve.resolve_artist's pick, on purpose. There, a wrong
    match costs a neighbouring track by the wrong artist — annoying, fixable,
    invisible to anyone else. Here it costs a public @mention of a stranger who
    happens to share a name, so a real tie is a refusal rather than something
    to settle by MusicBrainz's popularity ordering.

    What makes a tie real is the score gap, not the count. Measured on this
    queue:

        Paramore      100  Paramore (American pop rock band)
                       83  Paramore (indie duo, active 1999-2002)   ← not close
        BUKA          100  Buka (Polish rapper)
                       96  Buka (Croatia)
                       93  BUKA (Brooklyn Experimental)             ← a coin flip

    Refusing anything with a second passing candidate threw away Paramore and
    Leon Bridges, whose runners-up are nowhere near; DECISIVE_GAP keeps those
    and still refuses BUKA, which is five different acts sharing a name.

    Pure, so the rule is testable without the network.
    """
    passing = []
    for a in candidates or []:
        if int(a.get("score") or 0) < MIN_MB_SCORE:
            continue
        if _name_agrees(name, a.get("name") or ""):
            passing.append(a)
            continue
        aliases = [al.get("name") for al in (a.get("aliases") or [])]
        if any(_name_agrees(name, al) for al in aliases if al):
            passing.append(a)
    if not passing:
        return None
    passing.sort(key=lambda a: int(a.get("score") or 0), reverse=True)
    if len(passing) > 1:
        gap = int(passing[0].get("score") or 0) - int(passing[1].get("score") or 0)
        if gap < DECISIVE_GAP:
            return None
    return passing[0]


def _wikidata_handle(qid):
    """P2003 (Instagram username) off a Wikidata item. One request, no key."""
    try:
        r = requests.get(WIKIDATA_ENTITY.format(qid=qid), headers=MB_HEADERS,
                         timeout=20)
        if r.status_code != 200:
            return None
        ent = (r.json().get("entities") or {}).get(qid) or {}
        for claim in (ent.get("claims") or {}).get("P2003") or []:
            val = ((claim.get("mainsnak") or {}).get("datavalue") or {}).get("value")
            # P2003 stores the bare username, but a stray full URL shows up in
            # user-edited data often enough to be worth surviving.
            handle = handle_from_url(val) if "instagram.com" in str(val) else val
            if handle and IG_HANDLE_RE.match(str(handle)) \
                    and str(handle).lower() not in IG_RESERVED:
                return str(handle)
    except Exception:
        return None
    return None


def _cached(artist_key, max_age_days):
    row = fetchone(
        "SELECT handle, source, checked_at FROM artist_instagram "
        "WHERE artist_key = %s", (artist_key,))
    if not row:
        return None
    if max_age_days is not None and row["handle"] is None:
        # Only misses expire. A handle that was found does not stop being that
        # artist's handle, and re-checking it would spend MusicBrainz's budget
        # on a question already answered.
        age = datetime.datetime.now(datetime.timezone.utc) - row["checked_at"]
        if age.days >= max_age_days:
            return None
    return {"handle": row["handle"], "source": row["source"] or "cache"}


def _remember(artist, artist_key, handle, source, mbid=None):
    execute(
        """
        INSERT INTO artist_instagram (artist_key, artist, handle, source, mbid,
                                      checked_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (artist_key) DO UPDATE SET
          artist = EXCLUDED.artist, handle = EXCLUDED.handle,
          source = EXCLUDED.source, mbid = COALESCE(EXCLUDED.mbid, artist_instagram.mbid),
          checked_at = now()
        """,
        (artist_key, artist, handle, source, mbid))


def resolve_handle(artist, *, use_cache=True, recheck_misses_after_days=90):
    """Artist name → {'handle': str|None, 'source': str}.

    Costs at most two MusicBrainz requests and one Wikidata request, and zero
    of anything metered. A miss is a real answer and is cached as one; an
    outage is not, and is not.

    Raises MBRateLimited when MusicBrainz asks for a pause — the caller is a
    batch and is the only one that knows whether to wait or stop.
    """
    artist = (artist or "").strip()
    if len(artist) < 2:
        return {"handle": None, "source": "too-short"}
    key = _norm(artist)

    if use_cache:
        try:
            hit = _cached(key, recheck_misses_after_days)
        except Exception:
            hit = None      # an unreachable cache is a slow path, not a failure
        if hit:
            return hit

    # "MusicBrainz did not answer" is not the same answer as "this artist has
    # no Instagram", and the difference is invisible later: both look like an
    # empty result here, but one is worth writing down for 90 days and the
    # other is a lie that outlives the outage. _get returns None for any
    # non-200 that is not a rate limit, and caching THAT as a miss is what put
    # Leon Bridges and Paramore — both of whom MusicBrainz links — in the
    # never-mention pile on the first backfill run.
    data = _get(MB_SEARCH_URL, {"query": artist, "fmt": "json", "limit": 5})
    time.sleep(MB_RATE_LIMIT_S)
    if data is None:
        return {"handle": None, "source": "mb-unreachable"}

    best = pick_artist(artist, data.get("artists"))
    if not best:
        _safe_remember(artist, key, None, "no-mb-match")
        return {"handle": None, "source": "no-mb-match"}

    full = _get(MB_LOOKUP_URL.format(mbid=best["id"]),
                {"inc": "url-rels", "fmt": "json"})
    time.sleep(MB_RATE_LIMIT_S)
    if full is None:
        return {"handle": None, "source": "mb-unreachable"}

    handle = qid = None
    for rel in full.get("relations") or []:
        res = ((rel.get("url") or {}).get("resource")) or ""
        # MB marks a relation `ended` when the account is gone or the artist
        # moved off it, and it does not sort those last. NewJeans is the case
        # in this queue: MB carries both @newjeans_official (live) and
        # @njz_official (ended), so taking whichever turns up is a coin flip on
        # linking to an account that no longer exists.
        if rel.get("ended"):
            continue
        if "instagram.com" in res and not handle:
            handle = handle_from_url(res)
        m = re.search(r"wikidata\.org/wiki/(Q\d+)", res)
        if m and not qid:
            qid = m.group(1)

    source = "musicbrainz"
    if not handle and qid:
        handle = _wikidata_handle(qid)
        source = "wikidata" if handle else source
    if not handle:
        source = "not-listed"

    _safe_remember(artist, key, handle, source, best.get("id"))
    return {"handle": handle, "source": source}


def _safe_remember(artist, key, handle, source, mbid=None):
    try:
        _remember(artist, key, handle, source, mbid)
    except Exception:
        # Caching is an optimisation; a batch must not die because the cache
        # table was unreachable for one row.
        pass


def set_handle(artist, handle, source="manual"):
    """Record a handle by hand — the escape hatch for the 13 in 23 no database
    knows, and the correction path when a source is wrong."""
    artist = (artist or "").strip()
    handle = (handle or "").strip().lstrip("@") or None
    if handle and not IG_HANDLE_RE.match(handle):
        return {"error": "bad_handle"}
    _remember(artist, _norm(artist), handle, source)
    return {"ok": True, "artist": artist, "handle": handle}
