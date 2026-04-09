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

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.db import get_conn

# Advisory lock key — any unique int, used to serialize cron runs
_ADVISORY_LOCK = 87654321


def _row_to_track(row):
    """Convert a DB row (dict) back to the track dict format callers expect."""
    t = {
        "id":         row["id"],
        "name":       row["name"],
        "artist":     row["artist"] or "",
        "artist_ids": list(row["artist_ids"] or []),
        "album":      row["album"] or "",
        "popularity": row["popularity"] or 0,
        "source":     row["source"] or "spotify",
        "decade":     row["decade"] or "",
        "year":       row["year"] or "",
        "query":      row["query"] or "",
    }
    if row.get("label_energy") or row.get("label_mood"):
        t["labels"] = {
            "energy":   row["label_energy"],
            "mood":     row["label_mood"],
            "texture":  row["label_texture"],
            "feel":     row["label_feel"],
            "use_case": row["label_use_case"],
        }
    return t


def _upsert_track(cur, track, region):
    """INSERT or UPDATE one track row. Labels are only overwritten when non-null."""
    labels = track.get("labels", {})
    artist_ids = track.get("artist_ids", [])
    cur.execute(
        """
        INSERT INTO tracks (
            id, name, artist, artist_ids, album, popularity,
            source, region, decade, year, query,
            label_energy, label_mood, label_texture, label_feel, label_use_case
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
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
            labels.get("energy"),
            labels.get("mood"),
            labels.get("texture"),
            labels.get("feel"),
            labels.get("use_case"),
        ),
    )


def load_discovery():
    """Return {region: [track_dict, ...]} loaded from the tracks table."""
    import psycopg2.extras
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM tracks ORDER BY region, added_at")
            rows = cur.fetchall()
    finally:
        conn.close()

    result = {}
    for row in rows:
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
            existing_ids = set()
            for row in rows:
                region = row["region"] or "Unknown"
                t = _row_to_track(row)
                data.setdefault(region, []).append(t)
                existing_ids.add(row["id"])
                before_labels[row["id"]] = row.get("label_energy")

            modify_fn(data)

            # Upsert only tracks that are new or had labels added/changed
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
                    if is_new or labels_changed:
                        _upsert_track(cur, t, region)

        conn.commit()
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
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
