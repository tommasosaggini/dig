"""Who plays a genre — unioned from every source that knows, and cached.

THE PROBLEM. genre_vocabulary says 1,284 of the world's 2,184 genres have zero
tracks in the pool. Closing that needs artist NAMES, because the one thing we
must not do is type a genre into a consumer search box: "mongolian throat
singing" on YouTube returns stereotype compilations, and whatever came back
would be indistinguishable from discovery while quietly poisoning the pool.
Anchor every platform query on a real artist, or do not make it.

WHY THREE SOURCES. None of them is a superset of the others. Measured
2026-08-07 on the genres Dig had zero coverage of:

    genre     MusicBrainz    Wikidata            Discogs
    chutney   5 artists      not typed a genre   1,778 releases
    mugham    0              10 artists          756
    kuduro    10             9                   263
    bubbling  3              wrong entity        259
    benga     6              4                   0 (style), free-text works
    agbadza   0              0                   72 (free-text)

MusicBrainz has the vocabulary but its artist tagging carries the same Western
bias as Spotify's — it is generous about canterbury scene (42 artists) and
empty on symphonic mugham. Wikidata fills exactly those holes, because it is
fed by Wikipedia in every language the music is actually discussed in. Discogs
is the backstop that reaches the pressed-record tail nothing else indexes.

EACH SOURCE'S TRAP, all found by hitting them:

  * MusicBrainz — artists are a SEARCH (`query=tag:"x"`), not a browse. The
    browse endpoint 400s.
  * Wikidata — a bare label lookup matched an entity meaning "erotic dance"
    for `bubbling`, and P136 returns SONGS as happily as artists. Both need
    type constraints. Labels must be requested in many languages or the
    non-Western artists come back as bare Q-numbers.
  * Discogs — `style=` is its own controlled list (Kuduro is a style, agbadza
    is not); free-text `q=` reaches past it. 60 requests/minute authenticated.

Everything is cached in `genre_artists`, so a genre is never asked twice, and
`genre_sourcing_state` records the attempt — including the empty answers, which
are real answers and must not be re-bought every night.
"""
import json
import os
import re
import time
import unicodedata
import urllib.parse
import urllib.request

from lib.db import fetchall, get_conn
from lib.genre_vocab import genre_key as _genre_key

UA_MB = {"User-Agent": "DIG-MusicDiscovery/1.0 (https://ohdig.com; admin@ohdig.com)"}
UA_DC = {"User-Agent": "DIG-MusicDiscovery/1.0 +https://ohdig.com"}

# Per-source minimum interval. These are the published limits with headroom,
# not guesses: MusicBrainz asks for 1/sec, Discogs allows 60/min authenticated,
# and Wikidata's SPARQL endpoint is a shared service that deserves slack.
# Overrunning any of them costs the whole run, not one request — MB answers a
# burst with hours of 503s.
PACE = {"musicbrainz": 1.3, "wikidata": 1.6, "discogs": 1.2}
_last_call = {}


def _pace(source):
    gap = PACE.get(source, 1.0)
    prev = _last_call.get(source, 0.0)
    wait = gap - (time.time() - prev)
    if wait > 0:
        time.sleep(wait)
    _last_call[source] = time.time()


class SourceUnavailable(Exception):
    """The source did not answer. NOT the same as answering 'nobody'."""


def _jget(url, headers, source, timeout=30):
    _pace(source)
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:
        raise SourceUnavailable(f"{source}: {type(e).__name__}: {str(e)[:120]}")


def norm_name(s):
    """Tidy an artist name WITHOUT flattening it.

    Deliberately does not strip diacritics, unlike genre_key. A genre key is
    for matching two spellings of the same word; this is a name we are about to
    hand to Bandcamp as a search query, and "Cesaria Evora" is not what she is
    called. The platforms do their own accent-insensitive comparison
    (bandcamp._agree normalises both sides), so sending the real name is
    strictly better than sending a mangled one.
    """
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", str(s or ""))).strip()


# A name that is not a name. Discogs release credits carry these constantly and
# they would each become a fake "artist" to go looking for.
_JUNK_NAMES = {
    "various", "various artists", "unknown artist", "unknown", "no artist",
    "traditional", "trad.", "anonymous", "v/a", "soundtrack", "compilation",
}


def _usable(name):
    n = norm_name(name).strip(" -–—")
    if len(n) < 2 or len(n) > 90:
        return None
    if n.lower() in _JUNK_NAMES:
        return None
    # Discogs disambiguates same-named artists with a trailing "(2)".
    n = re.sub(r"\s*\(\d{1,2}\)$", "", n).strip()
    return n or None


# ── schema ────────────────────────────────────────────────────────────────────

def ensure_artist_schema():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS genre_artists (
                    genre       TEXT NOT NULL,
                    artist      TEXT NOT NULL,
                    source      TEXT NOT NULL,      -- musicbrainz|wikidata|discogs
                    external_id TEXT,               -- mbid | QID | discogs id
                    found_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                    ingested_at TIMESTAMPTZ,        -- when we last looked for tracks
                    track_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (genre, artist, source)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS genre_artists_genre_idx "
                        "ON genre_artists (genre)")
            cur.execute("CREATE INDEX IF NOT EXISTS genre_artists_todo_idx "
                        "ON genre_artists (ingested_at) WHERE ingested_at IS NULL")
            # Per (genre, source) attempt log. An empty answer is an answer and
            # is recorded, so tomorrow's run does not re-buy it; a source being
            # DOWN is not recorded, so it is retried.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS genre_sourcing_state (
                    genre       TEXT NOT NULL,
                    source      TEXT NOT NULL,
                    found       INTEGER NOT NULL DEFAULT 0,
                    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (genre, source)
                )
                """
            )
        conn.commit()
    finally:
        conn.close()


# ── sources ───────────────────────────────────────────────────────────────────

def from_musicbrainz(genre, limit=25):
    """Artists tagged with this genre. `query=tag:"x"` — a search, not a browse."""
    q = urllib.parse.quote(f'tag:"{genre}"')
    d = _jget(f"https://musicbrainz.org/ws/2/artist?query={q}&fmt=json&limit={limit}",
              UA_MB, "musicbrainz")
    want = _genre_key(genre)
    out = []
    for a in d.get("artists") or []:
        # MB's score is generous; below 70 it is returning the nearest thing it
        # has rather than a match, exactly as mb_resolve documents.
        if int(a.get("score") or 0) < 70:
            continue
        # Verify rather than trust: the response carries the artist's own tag
        # list, so require the genre to actually be on it. A tag query still
        # scores text matches highly — "Yasunao Tone" ranks for '2 tone' — and
        # this costs no extra request.
        tags = {_genre_key(t.get("name")) for t in (a.get("tags") or [])}
        if tags and want not in tags:
            continue
        n = _usable(a.get("name"))
        if n:
            out.append((n, a.get("id"), "tag"))
    return out


def _wikidata_genre_qid(genre):
    """The QID of a thing that IS a music genre. The type constraint is the
    point: a plain label match for 'bubbling' returns 'erotic dance and music'.
    """
    q = (f'SELECT ?g WHERE {{ ?g rdfs:label|skos:altLabel "{genre}"@en . '
         f'?g wdt:P31/wdt:P279* wd:Q188451 . }} LIMIT 1')
    d = _jget("https://query.wikidata.org/sparql?format=json&query="
              + urllib.parse.quote(q), UA_MB, "wikidata", timeout=60)
    b = d["results"]["bindings"]
    return b[0]["g"]["value"].rsplit("/", 1)[-1] if b else None


def from_wikidata(genre, limit=25):
    """Humans and bands whose P136 is this genre.

    The VALUES clause on ?type is what stops "Danza Kuduro" — a song — being
    returned as a kuduro artist. The language list is what stops the answer
    being a column of bare Q-numbers for precisely the non-Western artists this
    exists to find.
    """
    qid = _wikidata_genre_qid(genre)
    if not qid:
        return []
    q = (f'SELECT DISTINCT ?a ?aLabel WHERE {{ ?a wdt:P136 wd:{qid} . '
         f'?a wdt:P31/wdt:P279* ?type . VALUES ?type {{ wd:Q5 wd:Q215380 wd:Q2088357 }} '
         f'SERVICE wikibase:label {{ bd:serviceParam wikibase:language '
         f'"en,mul,fr,pt,es,ar,ru,sw,az,hi,id,tr,vi,th,ja,zh". }} }} LIMIT {limit}')
    d = _jget("https://query.wikidata.org/sparql?format=json&query="
              + urllib.parse.quote(q), UA_MB, "wikidata", timeout=60)
    out = []
    for b in d["results"]["bindings"]:
        label = b.get("aLabel", {}).get("value", "")
        # Still a Q-number => no label in any language we asked for. Useless as
        # a search term, so it is dropped rather than carried as a fake name.
        if re.fullmatch(r"Q\d+", label):
            continue
        n = _usable(label)
        if n:
            out.append((n, b["a"]["value"].rsplit("/", 1)[-1], "P136"))
    return out


def from_discogs(genre, limit=25):
    """Artists credited on releases of this genre.

    `style=` first because it is Discogs' controlled vocabulary and therefore
    precise; free-text `q=` second because the controlled list stops well short
    of the tail (Kuduro is a style, agbadza is not — but 72 agbadza releases
    exist and free-text finds them).
    """
    key = os.environ.get("DISCOGS_KEY")
    sec = os.environ.get("DISCOGS_SECRET")
    if not key or not sec:
        return []
    base = ("https://api.discogs.com/database/search?type=release&per_page=%d"
            "&key=%s&secret=%s" % (min(limit, 50), key, sec))
    results, method = [], "style"
    for param in ("style", "q"):
        d = _jget(base + f"&{param}={urllib.parse.quote(genre)}", UA_DC, "discogs")
        results = d.get("results") or []
        if results:
            method = param
            break
    # style= is Discogs' curated vocabulary and means "this record IS kuduro".
    # q= means "the word agbadza appears somewhere", which is how an American
    # jam band and a jazz orchestra turned up under Ewe drumming. Both are
    # worth having — the tail exists only in the second — but they are not the
    # same claim, so the caller gets told which one it is holding.
    out, seen = [], set()
    for r in results:
        # Discogs titles are "Artist - Release". Only the part before the first
        # " - " is the credit.
        title = r.get("title") or ""
        artist = title.split(" - ", 1)[0] if " - " in title else ""
        n = _usable(artist)
        if n and n.lower() not in seen:
            seen.add(n.lower())
            out.append((n, str(r.get("id") or ""), method))
    return out


SOURCES = {
    "musicbrainz": from_musicbrainz,
    "wikidata": from_wikidata,
    "discogs": from_discogs,
}


# ── orchestration ─────────────────────────────────────────────────────────────

def already_sourced(genre):
    """Which sources have already answered for this genre."""
    return {r["source"] for r in fetchall(
        "SELECT source FROM genre_sourcing_state WHERE genre = %s", (genre,))}


def sourced_counts():
    """{genre: how many sources have answered} for the WHOLE table, in one query.

    The walker needs this for thousands of genres to decide what is left to do.
    Asking per genre meant one round trip each, and over the SSH tunnel to prod
    that is minutes of latency before the first genre is even looked at — a run
    launched for four hours spent its first fifteen minutes asking the database
    what it already knew.
    """
    return {r["genre"]: r["n"] for r in fetchall(
        "SELECT genre, count(*) AS n FROM genre_sourcing_state GROUP BY genre")}


def source_genre(genre, sources=("musicbrainz", "wikidata", "discogs"),
                 limit=25, skip_done=True):
    """Ask every source who plays `genre`; store and return what they say.

    Returns {"added": n, "by_source": {...}, "unavailable": [...]}. A source
    that raises is reported, NOT recorded — so it is retried next run, while a
    source that genuinely knows nobody is recorded and never re-asked.
    """
    done = already_sourced(genre) if skip_done else set()
    rows, by_source, unavailable = [], {}, []
    for src in sources:
        if src in done:
            continue
        try:
            found = SOURCES[src](genre, limit=limit)
        except SourceUnavailable:
            unavailable.append(src)
            continue
        except Exception:
            unavailable.append(src)
            continue
        by_source[src] = len(found)
        for name, ext, method in found:
            # discogs style= is a curated claim; discogs q= is a word match.
            # Kept apart so ingest can prefer the former and treat the latter
            # as a lead rather than a fact.
            tag = "discogs-text" if (src == "discogs" and method == "q") else src
            rows.append((genre, name, tag, ext))

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if rows:
                from psycopg2.extras import execute_values
                execute_values(
                    cur,
                    "INSERT INTO genre_artists (genre, artist, source, external_id) "
                    "VALUES %s ON CONFLICT (genre, artist, source) DO NOTHING",
                    rows, page_size=200)
            for src, n in by_source.items():
                cur.execute(
                    "INSERT INTO genre_sourcing_state (genre, source, found, checked_at) "
                    "VALUES (%s, %s, %s, now()) ON CONFLICT (genre, source) "
                    "DO UPDATE SET found = EXCLUDED.found, checked_at = now()",
                    (genre, src, n))
        conn.commit()
    finally:
        conn.close()
    return {"added": len(rows), "by_source": by_source, "unavailable": unavailable}


def artists_awaiting_ingest(limit=200, genre=None):
    """Artists we have found but never looked for tracks by."""
    if genre:
        return fetchall(
            "SELECT DISTINCT ON (artist) genre, artist, source, external_id "
            "FROM genre_artists WHERE ingested_at IS NULL AND genre = %s "
            "ORDER BY artist, source LIMIT %s", (genre, limit))
    return fetchall(
        "SELECT DISTINCT ON (artist) genre, artist, source, external_id "
        "FROM genre_artists WHERE ingested_at IS NULL "
        "ORDER BY artist, source LIMIT %s", (limit,))


def mark_ingested(genre, artist, track_count):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE genre_artists SET ingested_at = now(), track_count = %s "
                "WHERE genre = %s AND artist = %s", (track_count, genre, artist))
        conn.commit()
    finally:
        conn.close()


def sourcing_summary():
    row = fetchall(
        """
        SELECT (SELECT count(DISTINCT genre) FROM genre_sourcing_state) AS genres_asked,
               (SELECT count(*) FROM genre_artists)                     AS artists,
               (SELECT count(DISTINCT genre) FROM genre_artists)        AS genres_with_artists,
               (SELECT count(*) FROM genre_artists WHERE ingested_at IS NULL) AS awaiting
        """)[0]
    return dict(row)
