"""
DIG — PostgreSQL connection helper.

All modules import get_conn() and manage their own transactions.
DATABASE_URL is loaded from .env automatically.
"""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_env():
    env_path = os.path.join(ROOT, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def get_conn():
    """Open and return a new psycopg2 connection. Caller is responsible for closing it."""
    _load_env()
    try:
        import psycopg2
    except ImportError:
        raise ImportError("psycopg2 not installed — run: pip install psycopg2-binary")
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set in .env")
    return psycopg2.connect(url)


def fetchall(sql, params=None):
    """Execute a SELECT and return a list of dicts."""
    import psycopg2.extras
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def fetchone(sql, params=None):
    """Execute a SELECT and return one dict, or None."""
    import psycopg2.extras
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def execute(sql, params=None):
    """Execute a statement and commit."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_cell_explored(region, genre, decade, tracks_found=0):
    """Record that a (region × genre × decade) cell has been searched.

    Increments explored count and updates last_scanned. fetched is the
    cumulative count of tracks actually returned by Spotify for this cell.
    Safe to call even if the cell doesn't exist yet (INSERT + DO NOTHING guard).
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    cell_id = f"{region}|{genre}|{decade}"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO catalog_cells (cell_id, region, genre, decade, explored, fetched, last_scanned)
                VALUES (%s, %s, %s, %s, 1, %s, %s)
                ON CONFLICT (cell_id) DO UPDATE SET
                    explored     = catalog_cells.explored + 1,
                    fetched      = catalog_cells.fetched + EXCLUDED.fetched,
                    last_scanned = EXCLUDED.last_scanned
                """,
                (cell_id, region, genre, decade, max(tracks_found, 0), now),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def expand_catalog_for_new_genres(genres):
    """Create catalog_cells for new genres × all known regions × all known decades.

    Safe to call repeatedly — uses ON CONFLICT DO NOTHING.
    Returns the count of new cells actually inserted.
    """
    if not genres:
        return 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT region FROM catalog_cells")
            regions = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT DISTINCT decade FROM catalog_cells")
            decades = [r[0] for r in cur.fetchall()]

        if not regions or not decades:
            return 0

        inserted = 0
        with conn.cursor() as cur:
            for genre in genres:
                for region in regions:
                    for decade in decades:
                        cell_id = f"{region}|{genre}|{decade}"
                        cur.execute(
                            """
                            INSERT INTO catalog_cells (cell_id, region, genre, decade, explored, fetched)
                            VALUES (%s, %s, %s, %s, 0, 0)
                            ON CONFLICT (cell_id) DO NOTHING
                            """,
                            (cell_id, region, genre, decade),
                        )
                        inserted += cur.rowcount
        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_meta(key, default=None):
    """Read a value from catalog_meta by key. Returns the Python object or default."""
    row = fetchone("SELECT value FROM catalog_meta WHERE key = %s", (key,))
    return row["value"] if row else default


def set_meta(key, value):
    """Write a value to catalog_meta (upsert). value must be JSON-serialisable."""
    import json
    execute(
        """
        INSERT INTO catalog_meta (key, value)
        VALUES (%s, %s::JSONB)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """,
        (key, json.dumps(value)),
    )
