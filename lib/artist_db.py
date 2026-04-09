"""
DIG — Artist registry backed by PostgreSQL.

Replaces the file-based artist_db.json + fcntl locking.
Artists live in the `artists` table.

Public API (unchanged):
    register_tracks(tracks, region="", source="")
    get_stats() → dict
"""

import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.db import get_conn


def _normalize_key(name):
    return name.strip().lower()


def register_tracks(tracks, region="", source="", genre=""):
    """Register a batch of tracks and their artists into the DB.

    Each track should have at minimum: name, artist, id.
    Optional: genres, decade, year, source (spotify/youtube).
    genre — the search genre that produced this batch (e.g. 'afrobeats').
    """
    if not tracks:
        return

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for t in tracks:
                artist_name = t.get("artist", "").strip()
                if not artist_name:
                    continue

                slug = _normalize_key(artist_name)
                decade = t.get("decade", "")
                track_ref = json.dumps({"id": t.get("id", ""), "name": t.get("name", "")})
                track_source = t.get("source", source) or source
                # Merge genre from: explicit param > track-level genres field
                genres = list(t.get("genres", []))
                if genre and genre not in genres:
                    genres.append(genre)

                cur.execute(
                    """
                    INSERT INTO artists (
                        slug, name, regions, genres, decades, sources,
                        track_count, track_refs, first_seen, last_seen
                    ) VALUES (
                        %s, %s,
                        ARRAY[%s]::TEXT[],
                        %s::TEXT[],
                        ARRAY[%s]::TEXT[],
                        ARRAY[%s]::TEXT[],
                        1, %s::JSONB, %s, %s
                    )
                    ON CONFLICT (slug) DO UPDATE SET
                        name        = EXCLUDED.name,
                        regions     = (
                            SELECT ARRAY(
                                SELECT DISTINCT unnest(artists.regions || EXCLUDED.regions)
                            )
                        ),
                        genres      = (
                            SELECT ARRAY(
                                SELECT DISTINCT unnest(artists.genres || EXCLUDED.genres)
                            )
                        ),
                        decades     = (
                            SELECT ARRAY(
                                SELECT DISTINCT u
                                FROM unnest(artists.decades || EXCLUDED.decades) AS u
                                WHERE u IS NOT NULL AND u <> ''
                            )
                        ),
                        sources     = (
                            SELECT ARRAY(
                                SELECT DISTINCT unnest(artists.sources || EXCLUDED.sources)
                            )
                        ),
                        track_count = artists.track_count + 1,
                        track_refs  = (
                            CASE
                                WHEN jsonb_typeof(artists.track_refs) != 'array'
                                THEN EXCLUDED.track_refs
                                WHEN jsonb_array_length(artists.track_refs) < 50
                                THEN artists.track_refs || EXCLUDED.track_refs->0
                                ELSE artists.track_refs
                            END
                        ),
                        last_seen   = EXCLUDED.last_seen
                    """,
                    (
                        slug,
                        artist_name,
                        region,
                        genres if genres else [],
                        decade,
                        track_source,
                        f"[{track_ref}]",
                        now,
                        now,
                    ),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_stats():
    """Return summary stats about the artist registry."""
    from lib.db import fetchone, fetchall
    total = (fetchone("SELECT COUNT(*) AS n FROM artists") or {}).get("n", 0)
    regions = fetchall(
        "SELECT unnest(regions) AS region, COUNT(*) AS n FROM artists GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
    )
    return {
        "total_artists": total,
        "top_regions": [{"region": r["region"], "count": r["n"]} for r in regions],
    }
