"""Resolve a free-text artist name to a Spotify artist ID — without /search.

THE PROBLEM THIS EXISTS FOR.

Curator ingest starts from prose: an Instagram caption saying "Mai Thiên Vân —
Ngày Xưa Anh Nói". There is no id in that, and the one endpoint that turned a
string into a Spotify entity is gone: Spotify's November 2024 Dev Mode
restrictions rate-limit /search into ~16-22h bans (measured 2026-08-04 from
three machines, same countdown on all three, so one app-wide counter — see
scripts/ingest_mb_artists.fetch_top_track for the full table). /artists/{id},
/artists/{id}/albums and /albums/{id}/tracks keep answering 200.

So the string has to become an id somewhere that is not Spotify, and
MusicBrainz is already in this stack for exactly that shape of work:
`enumerate_mb_artists.py` browses MB by country and pulls `spotify_id` out of
each artist's url-relations. That is the same lookup, keyed differently — by
name instead of by country — so this module is a second door onto machinery
the project already trusts.

WHY THIS IS BETTER THAN THE SEARCH IT REPLACES, not merely a substitute:

  * MusicBrainz is free and unmetered at 1 req/sec. Spotify's quota is the
    scarcest resource Dig has, and a curator backlog is thousands of names —
    the exact burst that trips the lockout. This spends none of it.
  * MB knows things Spotify does not: country of origin, folksonomy tags,
    disambiguation between two artists of the same name. Dig's region and
    genre layers want all three, and the old path threw the question away.
  * Every resolved artist is written to `mb_artists`, so it joins the queue
    `ingest_mb_artists.py` already drains, and a name seen twice costs nothing
    the second time.

WHAT IT DELIBERATELY DOES NOT DO: match a specific RECORDING. A caption's
track title is a hint, not a key — curators abbreviate, translate and misspell
— and confirming one would cost a second MB search per line for a track the
album walk usually surfaces anyway. The title is instead handed to
`fetch_top_track(prefer_title=…)`, which picks it out of tracklists it was
already going to fetch. Zero extra calls, and a miss costs a neighbouring
track by the right artist rather than a wrong artist entirely.
"""
from __future__ import annotations

import re
import time
import unicodedata

import requests

from lib.db import execute, fetchone

MB_SEARCH_URL = "https://musicbrainz.org/ws/2/artist"
MB_LOOKUP_URL = "https://musicbrainz.org/ws/2/artist/{mbid}"
# Same identity the country enumerator introduces itself with. MB blocks
# unidentified clients, and presenting as two different apps from one project
# is how a shared budget gets throttled for reasons nobody can trace.
MB_HEADERS = {
    "User-Agent": "DIG-MusicDiscovery/1.0 (https://ohdig.com; admin@ohdig.com)"
}
MB_RATE_LIMIT_S = 1.05          # MB asks for 1 req/sec; small buffer

# MusicBrainz `score` is a relevance percentage, and it is generous: a query
# for a name it has never seen still returns the closest thing at 60-70. The
# score alone therefore cannot decide, which is why _name_agrees below is an
# AND rather than a fallback.
MIN_MB_SCORE = 70


def _norm(s: str) -> str:
    """Casefolded, accent-stripped, punctuation-flattened."""
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    return re.sub(r"\s+", " ", s).strip().lower()


def _tokens(s: str) -> set:
    return {t for t in _norm(s).split() if len(t) > 1}


def _name_agrees(query: str, candidate: str) -> bool:
    """Does the MB hit actually carry the name we asked for?

    Token containment either way, because a caption gives short forms ("Terekke"
    for "Terekke"), and MB gives long ones ("Vina Panduwinata" for "Vina"). A
    bare score check passed both of those and also passed genuinely wrong
    artists, which is the failure that matters: a wrong id resolves silently
    and puts someone else's music in the pool under a curator's name.
    """
    q, c = _tokens(query), _tokens(candidate)
    if not q or not c:
        return False
    return q.issubset(c) or c.issubset(q)


class MBRateLimited(Exception):
    """MusicBrainz asked us to back off. Caller decides whether to stop."""


def _get(url: str, params: dict) -> dict | None:
    try:
        r = requests.get(url, params=params, headers=MB_HEADERS, timeout=20)
    except Exception:
        return None
    if r.status_code == 503:
        # MB's documented "you are going too fast". Not retried here: the
        # caller is a long batch and knows better than this function whether
        # to sleep or to stop for the night.
        raise MBRateLimited(url)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None


def _cached(name: str) -> dict | None:
    """A name already resolved into `mb_artists`. Free, and the common case
    once a curator's back catalogue has been walked once."""
    row = fetchone(
        "SELECT mbid, name, country, spotify_id, mb_tags FROM mb_artists "
        "WHERE lower(name) = lower(%s) AND spotify_id IS NOT NULL LIMIT 1",
        (name,))
    return dict(row) if row else None


def _remember(a: dict, spotify_id: str | None, spotify_url: str | None) -> None:
    """Write the artist into the same staging table the country enumerator
    fills, so `ingest_mb_artists.py` picks it up like any other and a second
    caption naming this artist costs nothing."""
    area = (a.get("area") or {}).get("name")
    begin_area = (a.get("begin-area") or {}).get("name")
    tags = [t.get("name") for t in (a.get("tags") or []) if t.get("name")]
    life = a.get("life-span") or {}
    execute(
        """
        INSERT INTO mb_artists (mbid, name, sort_name, country, area,
                                begin_area, type, gender, lifespan_begin,
                                lifespan_end, mb_tags, spotify_url, spotify_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (mbid) DO UPDATE SET
          spotify_url = COALESCE(EXCLUDED.spotify_url, mb_artists.spotify_url),
          spotify_id  = COALESCE(EXCLUDED.spotify_id,  mb_artists.spotify_id),
          mb_tags     = COALESCE(EXCLUDED.mb_tags,     mb_artists.mb_tags)
        """,
        (a.get("id"), a.get("name"), a.get("sort-name"), a.get("country"),
         area, begin_area, a.get("type"), a.get("gender"),
         life.get("begin"), life.get("end"), tags or None,
         spotify_url, spotify_id))


def resolve_artist(name: str, *, use_cache: bool = True) -> dict | None:
    """Free-text artist name → {mbid, name, country, spotify_id, tags, source}.

    Returns None when MB has no confident match, or when it has one but knows
    no Spotify link for it. Those are different outcomes and the caller may
    want to tell them apart, so the reason is on the returned dict when there
    is one: `source='cache'|'musicbrainz'`, and a null `spotify_id` with a
    populated `mbid` means "MB knows this artist, Spotify does not link".

    Costs at most two MB requests (~2.1s) and ZERO Spotify calls.
    """
    name = (name or "").strip()
    if len(name) < 2:
        return None

    if use_cache:
        try:
            hit = _cached(name)
        except Exception:
            hit = None      # an unreachable cache is a slow path, not a failure
        if hit:
            hit["source"] = "cache"
            hit["tags"] = hit.pop("mb_tags", None)
            return hit

    data = _get(MB_SEARCH_URL, {"query": name, "fmt": "json", "limit": 5})
    time.sleep(MB_RATE_LIMIT_S)
    if not data:
        return None

    best = None
    for a in data.get("artists") or []:
        if int(a.get("score") or 0) < MIN_MB_SCORE:
            continue
        if not _name_agrees(name, a.get("name") or ""):
            # Aliases are a real second chance: MB files "Ali Farka Touré"
            # under one name and half the world writes it differently.
            aliases = [al.get("name") for al in (a.get("aliases") or [])]
            if not any(_name_agrees(name, al) for al in aliases if al):
                continue
        best = a
        break
    if not best:
        return None

    # The search response does NOT carry url-relations, so the Spotify link
    # needs the artist's own document. This is the second and last request.
    full = _get(MB_LOOKUP_URL.format(mbid=best["id"]),
                {"inc": "url-rels+tags", "fmt": "json"})
    time.sleep(MB_RATE_LIMIT_S)
    spotify_url = spotify_id = None
    if full:
        best = {**best, **{k: v for k, v in full.items() if v is not None}}
        for rel in full.get("relations") or []:
            res = ((rel.get("url") or {}).get("resource")) or ""
            m = re.search(
                r"open\.spotify\.com(?:/intl-[a-z]{2,4})?/artist/([0-9A-Za-z]+)",
                res)
            if m:
                spotify_url, spotify_id = res, m.group(1)
                break

    try:
        _remember(best, spotify_id, spotify_url)
    except Exception:
        # Caching is an optimisation. A batch must not die because the staging
        # table was unreachable for one row.
        pass

    return {
        "mbid": best.get("id"),
        "name": best.get("name"),
        "country": best.get("country"),
        "spotify_id": spotify_id,
        "tags": [t.get("name") for t in (best.get("tags") or []) if t.get("name")],
        "source": "musicbrainz",
    }
