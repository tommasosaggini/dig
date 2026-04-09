"""
DIG — Genre vocabulary backed by PostgreSQL (genres table).

Replaces discovered_genres.json.

Public API:
    load()           → set of all known genre strings
    add(genres, source) → int  (number actually inserted)
"""

import os
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.db import get_conn, fetchall


def load():
    """Return all known genres as a set of lowercase strings."""
    rows = fetchall("SELECT genre FROM genres ORDER BY genre")
    return {r["genre"] for r in rows}


def add(new_genres, source="discovered"):
    """Insert new genres into the genres table (idempotent).

    Returns the number of genres actually inserted (0 for duplicates).
    """
    if not new_genres:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    try:
        inserted = 0
        with conn.cursor() as cur:
            for g in new_genres:
                if not isinstance(g, str):
                    continue
                g = g.strip().lower()
                if len(g) < 3:
                    continue
                cur.execute(
                    """
                    INSERT INTO genres (genre, source, added_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (genre) DO NOTHING
                    """,
                    (g, source, now),
                )
                inserted += cur.rowcount
        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
