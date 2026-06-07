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

Gating by `artist_ids[1]` (primary Spotify ID, 1-indexed in PG) means
a "Bad Bunny x J Balvin" collab is a different primary than "Bad Bunny"
solo — collaborations don't count against the solo cap. Tracks with no
artist_ids fall through ungated (rare; only weird ingests).

Wiring (one call per ingest path):
  - lib/discovery_lock.py:locked_update — covers discover.py,
    discover_youtube.py, discover_artists.py
  - pipeline/deep_crawl.py — its own INSERT
  - scripts/ingest_mb_artists.py — its own INSERT

NOT wired (intentional):
  - scripts/import_likes.py — user's saved tracks must always import
    (they're real signal and the ledger anchor)
  - pipeline/label_discovery.py — only updates labels on existing rows
"""

ARTIST_CAP = 3


def is_over_cap(cur, primary_artist_id: str | None,
                cap: int = ARTIST_CAP) -> bool:
    """Return True if this artist already has `cap` tracks in the pool.

    Caller passes its own DB cursor — typically the same transaction
    that's about to do the INSERT, so the count is consistent within
    that unit of work. Race window between this read and the subsequent
    INSERT is tiny (microseconds) and is bounded further by the
    pg_advisory_xact_lock that locked_update already takes; for paths
    that don't take that lock (deep_crawl, ingest_mb_artists), the
    worst-case is one extra track for an unlucky artist — acceptable.
    """
    if not primary_artist_id:
        return False
    cur.execute(
        "SELECT COUNT(*) FROM tracks WHERE artist_ids[1] = %s",
        (primary_artist_id,),
    )
    n = cur.fetchone()[0]
    return n >= cap


def primary_artist_id(track_dict: dict) -> str | None:
    """Pull the primary Spotify artist ID from a track dict shaped
    like the ones the discovery pipelines build. Returns None if the
    track has no artist_ids (e.g. malformed or YouTube-only)."""
    ids = track_dict.get("artist_ids") or []
    return ids[0] if ids else None
