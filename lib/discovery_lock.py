"""
DIG — Discovery pool backed by PostgreSQL.

Replaces the file-based discovery.json + fcntl advisory locking.
Tracks live in the `tracks` table; concurrent cron writes are
serialized via a PostgreSQL session-level advisory lock.

Public API (unchanged from the JSON version):
    load_discovery()          → {region: [track_dict, ...]}
    locked_update(modify_fn)  → {region: [track_dict, ...]}
    save_discovery(data)      → None  (full replace, used by migration)
"""

import os
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.db import get_conn
from lib.artist_cap import is_over_cap, primary_artist_id

# Advisory lock key — any unique int, used to serialize cron runs
_ADVISORY_LOCK = 87654321

# The whole `tracks` table, cached in-process. Reading it is ~700ms of the
# ~1.15s a /discovery request costs, and it is identical for every caller —
# only the per-user exclusion below differs. The ingest cron is the only
# writer, so a short TTL is enough to keep the pool fresh. Set to 0 to disable.
_POOL_TTL = float(os.environ.get("DISCOVERY_POOL_TTL", "60"))
_pool_cache = {"rows": None, "at": 0.0}
_pool_lock = threading.Lock()

# When False, /discovery omits YouTube-ingested rows (Spotify-only listening for now).
INCLUDE_YOUTUBE_IN_DISCOVERY = False


def _is_youtube_row(row):
    src = (row.get("source") or "").strip().lower()
    if src == "youtube":
        return True
    tid = str(row.get("id") or "")
    return tid.startswith("yt:")


def _row_to_track(row):
    """Convert a DB row (dict) back to the track dict format callers expect."""
    t = {
        "id":            row["id"],
        "name":          row["name"],
        "artist":        row["artist"] or "",
        "artist_ids":    list(row["artist_ids"] or []),
        "album":         row["album"] or "",
        "popularity":    row["popularity"] or 0,
        "source":        row["source"] or "spotify",
        # region: discovery bucket / Spotify search-market — kept so cell
        # accounting and downstream consumers don't have to re-look it up.
        "region":        row["region"] or "",
        "decade":        row["decade"] or "",
        "year":          row["year"] or "",
        "query":         row["query"] or "",
        # genres: Spotify artist genres backfilled from the artists table
        "genres":        list(row.get("genres") or []),
        # origin_region: MusicBrainz-derived nationality; None = not yet resolved
        "origin_region": row.get("origin_region") or None,
        # art: stable cover URL (Bandcamp bcbits CDN); '' for Spotify (the
        # player resolves Spotify covers live via the SDK/Connect state).
        "art": row.get("art_url") or "",
    }
    if row.get("label_energy") or row.get("label_mood"):
        t["labels"] = {
            "energy":   row["label_energy"],
            "mood":     row["label_mood"],
            "texture":  row["label_texture"],
            "feel":     row["label_feel"],
            "use_case": row["label_use_case"],
        }
    if row.get("quality_score") is not None:
        t["quality_score"] = row["quality_score"]
    return t


def _ensure_cell(cur, track, region):
    """Emergent catalog: guarantee the cell for this track exists.

    Cells are created lazily as tracks arrive, not via a precomputed grid.
    See lib/cell_accounting.cell_coord for coordinate derivation.
    """
    from lib.cell_accounting import cell_coord
    r, g, d = cell_coord(track, region_override=region)
    cur.execute(
        """
        INSERT INTO catalog_cells (cell_id, region, genre, decade, explored, fetched)
        VALUES (%s, %s, %s, %s, 0, 0)
        ON CONFLICT (cell_id) DO NOTHING
        """,
        (f"{r}|{g}|{d}", r, g, d),
    )


def _upsert_track(cur, track, region):
    """INSERT or UPDATE one track row. Labels and genres are only overwritten when non-null."""
    labels = track.get("labels", {})
    artist_ids = track.get("artist_ids", [])
    genres = track.get("genres") or []
    cur.execute(
        """
        INSERT INTO tracks (
            id, name, artist, artist_ids, album, popularity,
            source, region, decade, year, query, genres, art_url,
            label_energy, label_mood, label_texture, label_feel, label_use_case
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (id) DO UPDATE SET
            name            = EXCLUDED.name,
            artist          = EXCLUDED.artist,
            artist_ids      = EXCLUDED.artist_ids,
            album           = EXCLUDED.album,
            popularity      = EXCLUDED.popularity,
            source          = EXCLUDED.source,
            region          = EXCLUDED.region,
            decade          = EXCLUDED.decade,
            year            = EXCLUDED.year,
            query           = EXCLUDED.query,
            art_url         = COALESCE(EXCLUDED.art_url, tracks.art_url),
            genres          = CASE
                                WHEN array_length(EXCLUDED.genres, 1) > 0 THEN EXCLUDED.genres
                                ELSE tracks.genres
                              END,
            label_energy    = COALESCE(EXCLUDED.label_energy,   tracks.label_energy),
            label_mood      = COALESCE(EXCLUDED.label_mood,     tracks.label_mood),
            label_texture   = COALESCE(EXCLUDED.label_texture,  tracks.label_texture),
            label_feel      = COALESCE(EXCLUDED.label_feel,     tracks.label_feel),
            label_use_case  = COALESCE(EXCLUDED.label_use_case, tracks.label_use_case)
        """,
        (
            track.get("id"),
            track.get("name"),
            track.get("artist"),
            artist_ids,
            track.get("album"),
            track.get("popularity", 0),
            track.get("source", "spotify"),
            region,
            track.get("decade"),
            track.get("year"),
            track.get("query"),
            genres,
            track.get("art") or None,
            labels.get("energy"),
            labels.get("mood"),
            labels.get("texture"),
            labels.get("feel"),
            labels.get("use_case"),
        ),
    )


def _track_key(row):
    """The user_ledger join key for a track row. Must stay byte-identical to
    the SQL it replaces: lower(coalesce(artist,'') || ' - ' || coalesce(name,''))."""
    return ((row.get("artist") or "") + " - " + (row.get("name") or "")).lower()


def _invalidate_pool_cache():
    """Drop the cached pool so the next load_discovery() re-reads.

    Called after every write. The cache exists for the server, where the only
    writer is a different process and 60s of staleness is harmless — but the
    pipeline modules write and then re-read in the SAME process, and they are
    the ones this protects. pipeline/label_discovery.py loads the pool, flushes
    label mutations via locked_update, then re-loads to propagate the new genres
    into artist_db. When that run is fast (nothing to label), the whole sequence
    fits inside the TTL, so PASS 3 would read back the pre-write snapshot and
    silently propagate nothing.
    """
    _pool_cache["rows"] = None
    _pool_cache["at"] = 0.0


def _all_rows():
    """Every row of `tracks`, cached for _POOL_TTL seconds.

    Rows are shared between callers but never mutated — _row_to_track builds a
    fresh dict (and a fresh genres list) per call, so what a caller gets back
    is still its own. Do not hand these rows out directly.
    """
    import psycopg2.extras

    now = time.time()
    if _pool_cache["rows"] is not None and now - _pool_cache["at"] < _POOL_TTL:
        return _pool_cache["rows"]
    with _pool_lock:
        # Another thread may have refreshed while we waited for the lock.
        if _pool_cache["rows"] is not None and time.time() - _pool_cache["at"] < _POOL_TTL:
            return _pool_cache["rows"]
        conn = get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM tracks ORDER BY region, added_at")
                rows = cur.fetchall()
        finally:
            conn.close()
        _pool_cache["rows"] = rows
        _pool_cache["at"] = time.time()
        return rows


def _heard(user_id):
    """(track ids, ledger keys) already seen by this user."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT track_id FROM user_history WHERE user_id = %s", (user_id,))
            ids = {str(r[0]) for r in cur.fetchall()}
            cur.execute("SELECT track_key FROM user_ledger WHERE user_id = %s", (user_id,))
            keys = {r[0] for r in cur.fetchall()}
    finally:
        conn.close()
    return ids, keys


def load_discovery(user_id=None):
    """Return {region: [track_dict, ...]} loaded from the tracks table.

    When `user_id` is given, tracks present in that user's history (any
    status — saved, listened, skipped, disliked) are excluded. The resulting
    pool is the authoritative "never been played" set for that user, and
    every downstream picker (tailored, AI-Mix, journey, normal) inherits
    the guarantee without having to filter again.

    The exclusion used to be an anti-join, which meant re-reading all 40k rows
    per request. The pool is now read once per _POOL_TTL and the exclusion is
    applied in Python against the user's (much smaller) history + ledger.
    """
    rows = _all_rows()
    seen_ids, seen_keys = _heard(user_id) if user_id else (frozenset(), frozenset())

    result = {}
    for row in rows:
        if not INCLUDE_YOUTUBE_IN_DISCOVERY and _is_youtube_row(row):
            continue
        if seen_ids and str(row["id"]) in seen_ids:
            continue
        if seen_keys and _track_key(row) in seen_keys:
            continue
        region = row["region"] or "Unknown"
        result.setdefault(region, []).append(_row_to_track(row))
    return result


def locked_update(modify_fn):
    """
    Atomically update the discovery pool.

    Acquires a PostgreSQL advisory lock, loads the current {region: tracks}
    state, passes it to modify_fn for mutation, then upserts any new or
    changed tracks back. Returns the post-mutation data dict.
    """
    import psycopg2.extras
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (_ADVISORY_LOCK,))

            cur.execute("SELECT * FROM tracks ORDER BY region, added_at")
            rows = cur.fetchall()

            data = {}
            before_labels = {}
            before_genres = set()  # track IDs that already have genres assigned
            existing_ids = set()
            for row in rows:
                region = row["region"] or "Unknown"
                t = _row_to_track(row)
                data.setdefault(region, []).append(t)
                existing_ids.add(row["id"])
                before_labels[row["id"]] = row.get("label_energy")
                if row.get("genres") and len(row["genres"]) > 0:
                    before_genres.add(row["id"])

            modify_fn(data)

            # Upsert tracks that are new, had labels changed, OR had genres assigned.
            # NEW tracks are gated by lib.artist_cap to enforce breadth — if the
            # primary artist already has ARTIST_CAP tracks in the pool, the new
            # track is dropped. Updates (label/genre backfill) bypass the gate.
            capped_skips = 0
            for region, tracks in data.items():
                for t in tracks:
                    tid = t.get("id")
                    if not tid:
                        continue
                    is_new = tid not in existing_ids
                    labels_changed = (
                        t.get("labels") and
                        t["labels"].get("energy") != before_labels.get(tid)
                    )
                    genres_changed = (
                        t.get("genres") and
                        isinstance(t["genres"], list) and
                        len(t["genres"]) > 0 and
                        tid not in before_genres
                    )
                    if is_new and is_over_cap(cur, primary_artist_id(t)):
                        capped_skips += 1
                        continue
                    if is_new or labels_changed or genres_changed:
                        _upsert_track(cur, t, region)
                        if is_new:
                            _ensure_cell(cur, t, region)
            if capped_skips:
                print(f"  [artist_cap] skipped {capped_skips} new tracks "
                      f"(primary artist already at cap)")

        conn.commit()
        _invalidate_pool_cache()
        return data
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save_discovery(data):
    """
    Write a complete {region: [tracks]} dict to the DB, replacing all rows.
    Used by the migration script; not called during normal operation.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE tracks")
            for region, tracks in data.items():
                for t in tracks:
                    if t.get("id"):
                        _upsert_track(cur, t, region)
        conn.commit()
        _invalidate_pool_cache()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
