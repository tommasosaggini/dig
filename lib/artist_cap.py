"""
DIG — Hard new-artist cap at every ingest path.

Refuses to add a NEW track to the pool if the track's primary artist
already has `ARTIST_CAP` tracks. Updates to existing tracks (label
backfill, region resolution, etc.) are not gated.

Why this exists: deep_crawl mines an artist's full discography per probe
(no per-artist cap by design — see its header), and search-driven
discovery returns the same canonical names across near-duplicate genre
queries (qawwali / qawwali sufi / qawwali sufi pakistan / …). Together
they produced a long tail of artists with 20-50+ tracks each: Nusrat
Fateh Ali Khan at 56, Bach package at 58, Bad Bunny at 27, Sounds of
Future Siam at 27. That long tail is what makes the player feel like
it's looping the same artist on repeat.

THE CAP COUNTS BY NAME, NOT BY PROVIDER ID. It used to count only
`artist_ids[1]` (primary Spotify ID), and that had two holes big enough
to drive the whole original problem back through:

  * `artist_ids` is EMPTY on every Bandcamp, YouTube and SoundCloud row,
    so `is_over_cap` returned False before counting anything. Bandcamp
    alone is 62% of the pool. Measured over the 14 days to 2026-08-17:
    Spotify put 1 artist over the cap out of 6,862, while Bandcamp put
    2,120 artists over it holding 17,565 tracks. The cap was perfect
    exactly where it was wired and absent everywhere else — AQVARIA
    reached 38 tracks off a single `bandcamp-tag:cayman-islands` scrape.
  * An artist is not one ID. Aphex Twin sits in the pool as 12 YouTube +
    5 Spotify + 4 Bandcamp rows: three different "artists" as far as an
    ID-keyed count is concerned, 21 chances to repeat as far as the
    listener is concerned.

So the count is now `lower(btrim(artist))` OR the primary ID — the name
catches every source and the cross-source case, the ID still catches a
name that got written two different ways under one Spotify identity.
Normalisation stays deliberately shallow (case, surrounding and repeated
whitespace) because anything cleverer collapses artists that really are
distinct; `MIKAN MUKKU [みかんむくっ]` and `MIKAN MUKKU` stay two names,
which costs at most one extra track and never silently merges two acts.

A "Bad Bunny x J Balvin" collab is still a different name from "Bad
Bunny" solo, so collaborations continue not to count against the solo
cap — the property the old ID-keyed count was chosen for survives the
switch for free.

Needs `idx_tracks_artist_key` (see scripts/migrate_artist_key_index.sql)
or the per-track count is a seq scan over 100k rows.

Wiring (one call per ingest path):
  - lib/discovery_lock.py:locked_update — covers discover.py,
    discover_youtube.py, discover_artists.py, and the Bandcamp ingest
  - pipeline/deep_crawl.py — its own INSERT
  - scripts/ingest_mb_artists.py — its own INSERT

NOT wired (intentional):
  - scripts/import_likes.py — user's saved tracks must always import
    (they're real signal and the ledger anchor)
  - pipeline/label_discovery.py — only updates labels on existing rows
"""
import re

ARTIST_CAP = 3

_WS = re.compile(r"\s+")


def artist_key(name: str | None) -> str | None:
    """The string the cap counts on. None when there is no usable name.

    Shallow on purpose — see the module header. Must stay in step with
    the SQL in `is_over_cap` and with idx_tracks_artist_key, which is
    why both sides are lower() + btrim() and nothing else; the inner
    whitespace collapse is applied to the PROBE only, so a stored name
    with a double space still matches its own index entry.
    """
    if not name:
        return None
    key = _WS.sub(" ", str(name)).strip().lower()
    return key or None


def is_over_cap(cur, primary_artist_id: str | None,
                cap: int = ARTIST_CAP,
                artist_name: str | None = None) -> bool:
    """Return True if this artist already has `cap` tracks in the pool.

    Pass `artist_name` — without it this degrades to the old ID-only
    count, which is a no-op for every non-Spotify row. Callers that have
    a track dict should use `is_track_over_cap`.

    Caller passes its own DB cursor — typically the same transaction
    that's about to do the INSERT, so the count is consistent within
    that unit of work. Race window between this read and the subsequent
    INSERT is tiny (microseconds) and is bounded further by the
    pg_advisory_xact_lock that locked_update already takes; for paths
    that don't take that lock (deep_crawl, ingest_mb_artists), the
    worst-case is one extra track for an unlucky artist — acceptable.
    """
    key = artist_key(artist_name)
    if not primary_artist_id and not key:
        return False
    # Two predicates, one scan. Either alone leaves a hole: ID-only misses
    # every source that has no artist_ids, name-only misses a Spotify artist
    # whose name is spelled differently across rows.
    cur.execute(
        "SELECT COUNT(*) AS n FROM tracks "
        "WHERE (%s IS NOT NULL AND artist_ids[1] = %s) "
        "   OR (%s IS NOT NULL AND lower(btrim(artist)) = %s)",
        (primary_artist_id, primary_artist_id, key, key),
    )
    row = cur.fetchone()
    # The caller passes ITS cursor, and callers disagree about the type:
    # locked_update uses a RealDictCursor (rows are dicts), while the plain
    # paths use tuples. `row[0]` works only on the latter and raises
    # KeyError: 0 on the former. That went unnoticed because this function
    # used to return early for tracks with no artist_ids — which was every
    # Bandcamp row — so only a Spotify-sourced locked_update ever reached
    # the line. Now every row reaches it, so both types must work.
    n = row["n"] if isinstance(row, dict) else row[0]
    return n >= cap


def is_track_over_cap(cur, track_dict: dict, cap: int = ARTIST_CAP) -> bool:
    """`is_over_cap` for a track dict shaped like the discovery pipelines
    build — pulls both the primary ID and the name so no caller has to
    remember that passing the name is the part that matters."""
    return is_over_cap(cur, primary_artist_id(track_dict), cap,
                       artist_name=track_dict.get("artist"))


def primary_artist_id(track_dict: dict) -> str | None:
    """Pull the primary Spotify artist ID from a track dict shaped
    like the ones the discovery pipelines build. Returns None if the
    track has no artist_ids (e.g. malformed or YouTube-only)."""
    ids = track_dict.get("artist_ids") or []
    return ids[0] if ids else None
