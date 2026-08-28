"""The list of genres Dig believes exist, and how well it covers each one.

WHY THIS EXISTS. Discovery used to be driven by hardcoded seed lists —
lib.soundcloud.GENRES is nineteen genres, all Western club music — so the pool
could only ever grow in the directions someone had already thought of. Measured
2026-08-07 against MusicBrainz's canonical list of 2,184 world genres:

    any track at all      900   (41%)
    ...of those, under 10 460
    ZERO tracks         1,284   (58%)

Real coverage was about 440 genres, a fifth of recorded music, and the missing
1,284 were not obscure by accident: agbadza, omutibo, balitaw, bubbling — the
non-Western and non-commercial end, which is exactly what Dig is for. Bubbling
had zero tracks while Discogs lists 259 pressed records of it.

So the vocabulary is fetched, never written down here, and coverage is a number
this table carries rather than a thing anyone has to guess at. What is thin is
then visible, and what is visible can be worked through.

SOURCES, and why more than one. MusicBrainz publishes the vocabulary (2,184
genres, one unauthenticated request). It is comprehensive on NAMES. It is not
comprehensive on who plays them, and neither is anything else — measured on the
same day:

    chutney   MB 5 artists    Wikidata none     Discogs 1,778 releases
    mugham    MB 0            Wikidata 10       Discogs 756
    bubbling  MB 3            Wikidata wrong    Discogs 259
    agbadza   MB 0            Wikidata 0        Discogs 72

No single source is a superset of the others, so artist sourcing unions them.
This module owns the vocabulary and the coverage ledger; sourcing lives in the
scripts that read from it.
"""
import datetime
import os
import re
import time
import unicodedata
import urllib.request

from lib.db import execute, fetchall, get_conn

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# A local copy, so a coverage recount never blocks on MusicBrainz being awake.
CACHE_PATH = os.path.join(ROOT, "data", "genres_musicbrainz.txt")

MB_GENRE_URL = "https://musicbrainz.org/ws/2/genre/all?fmt=txt"
MB_HEADERS = {
    "User-Agent": "DIG-MusicDiscovery/1.0 (https://ohdig.com; admin@ohdig.com)"
}

# Spellings that reach the pool from platforms that do not agree with
# MusicBrainz. Matching on the normalised key alone misses these, and a miss
# reads as "no coverage" for a genre that has plenty — which would send a
# backfill run chasing records already in the pool.
# Only genuinely DIFFERENT words belong here. "rai"/"raï" and
# "kayokyoku"/"kayōkyoku" were in this map and did nothing at all: genre_key
# already folds the diacritic away, so both sides produced the same key and the
# alias was dead weight pretending to be coverage logic.
ALIASES = {
    "molam": "mor lam",
    "morlam": "mor lam",
    "bailefunk": "funk carioca",
    "coupedecale": "coupé-décalé",
    "luktung": "luk thung",
}


def genre_key(name):
    """Accent-stripped, punctuation-flattened key for matching.

    'raï' and 'rai', 'coupé-décalé' and 'coupe decale' are the same genre
    written by two platforms that disagree about diacritics.
    """
    s = unicodedata.normalize("NFKD", str(name or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def ensure_genre_schema():
    """Idempotent DDL. Same self-provisioning pattern as ensure_ig_schema()."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS genre_vocabulary (
                    genre       TEXT PRIMARY KEY,   -- canonical name, as the source spells it
                    genre_key   TEXT NOT NULL,      -- normalised, for matching pool labels
                    source      TEXT NOT NULL DEFAULT 'musicbrainz',
                    track_count INTEGER NOT NULL DEFAULT 0,
                    counted_at  TIMESTAMPTZ,
                    added_at    TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS genre_vocab_key_idx "
                        "ON genre_vocabulary (genre_key)")
            # The working index: "what is Dig worst at" is the question this
            # table exists to answer, and it gets asked every discovery run.
            cur.execute("CREATE INDEX IF NOT EXISTS genre_vocab_coverage_idx "
                        "ON genre_vocabulary (track_count)")
        conn.commit()
    finally:
        conn.close()


def _read_cache():
    try:
        with open(CACHE_PATH, encoding="utf-8") as f:
            return [g.strip() for g in f if g.strip()]
    except Exception:
        return []


def fetch_world_genres(timeout=25, max_age_days=30):
    """MusicBrainz's canonical genre list, ~2,184 names. Cached on disk.

    The vocabulary changes on the timescale of MusicBrainz editors arguing
    about subgenres, so re-fetching it on every run buys nothing and costs a
    hard dependency on MB being reachable at that moment. Worse than costs:
    the first version of this blocked for twenty minutes inside urlopen()
    without honouring its own timeout — MB had accepted the connection and
    then sent nothing, which is a state a socket timeout does not catch — and
    the whole coverage ledger sat empty behind it.

    So: a fresh cache is used as-is, a stale one triggers a fetch, and a
    failed fetch falls back to the stale copy rather than to nothing. Only an
    empty cache AND a failed fetch is an error.
    """
    cached = _read_cache()
    if cached and os.path.exists(CACHE_PATH):
        age_days = (time.time() - os.path.getmtime(CACHE_PATH)) / 86400.0
        if age_days < max_age_days:
            return cached
    try:
        req = urllib.request.Request(MB_GENRE_URL, headers=MB_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8")
        genres = [g.strip() for g in body.splitlines() if g.strip()]
        if genres:
            os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                f.write("\n".join(genres) + "\n")
            return genres
    except Exception:
        pass
    if cached:
        return cached      # stale beats empty
    raise RuntimeError("no genre vocabulary: MusicBrainz unreachable and no cache")


def sync_vocabulary(genres=None, source="musicbrainz"):
    """Upsert the world genre list. Returns (added, total).

    Never deletes: a genre that vanishes from MusicBrainz between runs is
    almost certainly an editorial rename, not proof the music stopped
    existing, and dropping the row would silently discard its coverage
    history.
    """
    genres = genres or fetch_world_genres()
    before = fetchall("SELECT count(*) AS c FROM genre_vocabulary")[0]["c"]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                "INSERT INTO genre_vocabulary (genre, genre_key, source) "
                "VALUES %s ON CONFLICT (genre) DO UPDATE SET "
                "genre_key = EXCLUDED.genre_key",
                [(g, genre_key(g), source) for g in genres], page_size=500)
        conn.commit()
    finally:
        conn.close()
    after = fetchall("SELECT count(*) AS c FROM genre_vocabulary")[0]["c"]
    return after - before, after


def _pool_label_counts():
    """Every genre label in the pool, normalised, with its track count."""
    counts = {}
    for r in fetchall("SELECT x AS g, count(*) AS c FROM tracks, unnest(genres) x "
                      "GROUP BY 1"):
        k = genre_key(r["g"])
        counts[k] = counts.get(k, 0) + r["c"]
    return counts


def refresh_coverage():
    """Recount how many pool tracks each vocabulary genre has. Returns a dict.

    Aliases are folded in here rather than at write time, so the pool keeps
    whatever the platform called it and only the ledger has to agree.
    """
    counts = _pool_label_counts()
    rows = fetchall("SELECT genre, genre_key FROM genre_vocabulary")
    now = datetime.datetime.now(datetime.timezone.utc)
    updates = []
    for r in rows:
        k = r["genre_key"]
        n = counts.get(k, 0)
        for alias_key, canonical in ALIASES.items():
            if genre_key(canonical) == k:
                n += counts.get(genre_key(alias_key), 0)
        updates.append((n, now, r["genre"]))
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                "UPDATE genre_vocabulary v SET track_count = d.n, "
                "counted_at = d.at FROM (VALUES %s) AS d(n, at, genre) "
                "WHERE v.genre = d.genre",
                updates, page_size=500)
        conn.commit()
    finally:
        conn.close()
    return {g: n for n, _, g in updates}


def coverage_summary():
    """The one number worth watching, plus the buckets under it."""
    row = fetchall(
        """
        SELECT count(*) AS total,
               count(*) FILTER (WHERE track_count > 0)  AS any_tracks,
               count(*) FILTER (WHERE track_count BETWEEN 1 AND 9) AS thin,
               count(*) FILTER (WHERE track_count = 0)  AS zero,
               count(*) FILTER (WHERE track_count >= 10) AS covered
        FROM genre_vocabulary
        """)[0]
    return dict(row)


def underserved(limit=50, max_tracks=0):
    """The work queue: genres Dig cannot serve, worst first.

    Ordered by name within the zero bucket rather than at random, so a run
    that stops halfway and resumes tomorrow makes forward progress instead of
    re-rolling the dice on which gaps it looks at.
    """
    return fetchall(
        "SELECT genre, genre_key, track_count FROM genre_vocabulary "
        "WHERE track_count <= %s ORDER BY track_count ASC, genre ASC LIMIT %s",
        (max_tracks, limit))
