"""
DIG — Search query deduplication backed by PostgreSQL.

Replaces the file-based search_history.json.
Tracks which (query, market) pairs have been run to avoid
redundant Spotify searches across cron runs.

Public API (drop-in for the old dict-based approach in discover.py):
    load()                        → {query_key: {count, runs, last}}
    record(query, market, count)  → None
    save(history_dict)            → None  (bulk upsert, called at end of run)
    freshness(query, market)      → int   (number of prior runs)
"""

import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.db import get_conn


def load():
    """Return the full search history as {key: {count, runs, last}} dict."""
    import psycopg2.extras
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT query_key, count, runs, last_searched FROM search_queries")
            rows = cur.fetchall()
    finally:
        conn.close()

    return {
        row["query_key"]: {
            "count": row["count"],
            "runs":  row["runs"],
            "last":  row["last_searched"].isoformat() if row["last_searched"] else None,
        }
        for row in rows
    }


def record(query, market, count):
    """Upsert one search query record (called inline during discovery)."""
    key = f"{query}|{market}"
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO search_queries (query_key, count, runs, last_searched)
                VALUES (%s, %s, 1, %s)
                ON CONFLICT (query_key) DO UPDATE SET
                    count         = EXCLUDED.count,
                    runs          = search_queries.runs + 1,
                    last_searched = EXCLUDED.last_searched
                """,
                (key, count, now),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save(history_dict):
    """Bulk-upsert a {key: {count, runs, last}} dict (backward-compat helper)."""
    if not history_dict:
        return
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for key, v in history_dict.items():
                cur.execute(
                    """
                    INSERT INTO search_queries (query_key, count, runs, last_searched)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (query_key) DO UPDATE SET
                        count         = EXCLUDED.count,
                        runs          = EXCLUDED.runs,
                        last_searched = EXCLUDED.last_searched
                    """,
                    (key, v.get("count", 0), v.get("runs", 0), v.get("last")),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def freshness(query, market):
    """Return the number of prior runs for this (query, market) pair."""
    from lib.db import fetchone
    key = f"{query}|{market}"
    row = fetchone("SELECT runs FROM search_queries WHERE query_key = %s", (key,))
    return row["runs"] if row else 0
