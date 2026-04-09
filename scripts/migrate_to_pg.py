#!/usr/bin/env python3
"""
DIG — One-time migration from JSON files to PostgreSQL.

Reads all existing flat JSON files and inserts them into the DB.
Safe to re-run: all inserts use ON CONFLICT DO NOTHING / DO UPDATE.

Usage:
    python3 scripts/migrate_to_pg.py [--dry-run]
"""

import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Load .env before importing lib modules
env_path = os.path.join(ROOT, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from lib.db import get_conn

DRY_RUN = "--dry-run" in sys.argv


def banner(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def path(*parts):
    return os.path.join(ROOT, *parts)


def load_json(filename, default=None):
    p = path(filename)
    if not os.path.exists(p):
        print(f"  (skip — {filename} not found)")
        return default
    with open(p) as f:
        return json.load(f)


# ── Migrate tracks (discovery.json) ──────────────────────────────────────────

def migrate_tracks(conn):
    banner("Migrating discovery.json → tracks")
    data = load_json("discovery.json", {})
    if not data:
        return 0

    total = sum(len(v) for v in data.values() if isinstance(v, list))
    print(f"  Regions: {len(data)}, total tracks: {total}")
    if DRY_RUN:
        return total

    count = 0
    with conn.cursor() as cur:
        for region, tracks in data.items():
            if not isinstance(tracks, list):
                continue
            for t in tracks:
                labels = t.get("labels", {})
                artist_ids = t.get("artist_ids", [])
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
                        label_energy   = COALESCE(EXCLUDED.label_energy,   tracks.label_energy),
                        label_mood     = COALESCE(EXCLUDED.label_mood,     tracks.label_mood),
                        label_texture  = COALESCE(EXCLUDED.label_texture,  tracks.label_texture),
                        label_feel     = COALESCE(EXCLUDED.label_feel,     tracks.label_feel),
                        label_use_case = COALESCE(EXCLUDED.label_use_case, tracks.label_use_case)
                    """,
                    (
                        t.get("id"), t.get("name"), t.get("artist"),
                        artist_ids, t.get("album"), t.get("popularity", 0),
                        t.get("source", "spotify"), region,
                        t.get("decade"), t.get("year"), t.get("query"),
                        labels.get("energy"), labels.get("mood"),
                        labels.get("texture"), labels.get("feel"), labels.get("use_case"),
                    ),
                )
                count += 1
    conn.commit()
    print(f"  ✓ Inserted/updated {count} tracks")
    return count


# ── Migrate artists (artist_db.json) ─────────────────────────────────────────

def migrate_artists(conn):
    banner("Migrating artist_db.json → artists")
    data = load_json("artist_db.json", {})
    if not data:
        return 0

    print(f"  Artists: {len(data)}")
    if DRY_RUN:
        return len(data)

    count = 0
    with conn.cursor() as cur:
        for slug, a in data.items():
            cur.execute(
                """
                INSERT INTO artists (
                    slug, name, regions, genres, decades, sources,
                    track_count, track_refs, first_seen, last_seen
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s::JSONB, %s, %s
                )
                ON CONFLICT (slug) DO UPDATE SET
                    name        = EXCLUDED.name,
                    regions     = EXCLUDED.regions,
                    genres      = EXCLUDED.genres,
                    decades     = EXCLUDED.decades,
                    sources     = EXCLUDED.sources,
                    track_count = EXCLUDED.track_count,
                    track_refs  = EXCLUDED.track_refs,
                    last_seen   = EXCLUDED.last_seen
                """,
                (
                    slug,
                    a.get("name", slug),
                    a.get("regions", []),
                    a.get("genres", []),
                    a.get("decades", []),
                    a.get("sources", []),
                    a.get("track_count", 0),
                    json.dumps(a.get("tracks", [])),
                    a.get("first_seen"),
                    a.get("last_seen"),
                ),
            )
            count += 1
    conn.commit()
    print(f"  ✓ Inserted/updated {count} artists")
    return count


# ── Migrate catalog (catalog.json) ────────────────────────────────────────────

def migrate_catalog(conn):
    banner("Migrating catalog.json → genres + catalog_cells + catalog_scan_queue")
    data = load_json("catalog.json", {})
    if not data:
        return

    genres = data.get("genres", {})
    cells = data.get("cells", {})
    scan_queue = data.get("scan_queue", [])
    version = data.get("version", 2)
    last_scan = data.get("last_scan")

    print(f"  Genres: {len(genres)}, cells: {len(cells)}, queue: {len(scan_queue)}")
    if DRY_RUN:
        return

    with conn.cursor() as cur:
        # Metadata
        cur.execute(
            "INSERT INTO catalog_meta (key, value) VALUES ('version', %s::JSONB)"
            " ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (json.dumps(version),),
        )
        if last_scan:
            cur.execute(
                "INSERT INTO catalog_meta (key, value) VALUES ('last_scan', %s::JSONB)"
                " ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (json.dumps(last_scan),),
            )

        # Genres
        for genre, meta in genres.items():
            cur.execute(
                """
                INSERT INTO genres (genre, source, added_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (genre) DO NOTHING
                """,
                (genre, meta.get("source"), meta.get("added")),
            )

        conn.commit()
        print(f"  ✓ Genres done")

        # Cells (batch for speed)
        batch = []
        for cell_id, c in cells.items():
            batch.append((
                cell_id,
                c.get("region"), c.get("genre"), c.get("decade"),
                c.get("pool_size"), c.get("explored", 0), c.get("fetched", 0),
                c.get("last_scanned"), c.get("last_fetched"),
            ))
            if len(batch) >= 5000:
                cur.executemany(
                    """
                    INSERT INTO catalog_cells
                        (cell_id, region, genre, decade, pool_size, explored, fetched,
                         last_scanned, last_fetched)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cell_id) DO UPDATE SET
                        pool_size    = EXCLUDED.pool_size,
                        explored     = EXCLUDED.explored,
                        fetched      = EXCLUDED.fetched,
                        last_scanned = EXCLUDED.last_scanned,
                        last_fetched = EXCLUDED.last_fetched
                    """,
                    batch,
                )
                conn.commit()
                batch = []

        if batch:
            cur.executemany(
                """
                INSERT INTO catalog_cells
                    (cell_id, region, genre, decade, pool_size, explored, fetched,
                     last_scanned, last_fetched)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cell_id) DO UPDATE SET
                    pool_size    = EXCLUDED.pool_size,
                    explored     = EXCLUDED.explored,
                    fetched      = EXCLUDED.fetched,
                    last_scanned = EXCLUDED.last_scanned,
                    last_fetched = EXCLUDED.last_fetched
                """,
                batch,
            )
            conn.commit()
        print(f"  ✓ Cells done")

        # Scan queue
        cur.execute("TRUNCATE TABLE catalog_scan_queue")
        batch = [(cell_id,) for cell_id in scan_queue if isinstance(cell_id, str)]
        for i in range(0, len(batch), 5000):
            cur.executemany(
                "INSERT INTO catalog_scan_queue (cell_id) VALUES (%s) ON CONFLICT DO NOTHING",
                batch[i:i+5000],
            )
            conn.commit()
        print(f"  ✓ Scan queue done ({len(batch)} items)")


# ── Migrate search history (search_history.json) ─────────────────────────────

def migrate_search_history(conn):
    banner("Migrating search_history.json → search_queries")
    data = load_json("search_history.json", {})
    if not data:
        return

    print(f"  Queries: {len(data)}")
    if DRY_RUN:
        return

    with conn.cursor() as cur:
        for key, v in data.items():
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
    print(f"  ✓ {len(data)} queries migrated")


# ── Migrate users (users/ directory) ─────────────────────────────────────────

def migrate_users(conn):
    banner("Migrating users/ → users + user_tokens + user_history + user_ledger")
    users_dir = path("users")
    if not os.path.isdir(users_dir):
        print("  (skip — users/ not found)")
        return

    user_dirs = [
        d for d in os.listdir(users_dir)
        if os.path.isdir(os.path.join(users_dir, d)) and not d.startswith(".")
    ]
    print(f"  Users found: {len(user_dirs)}")
    if DRY_RUN:
        return

    for uid in user_dirs:
        udir = os.path.join(users_dir, uid)

        # Profile
        profile_path = os.path.join(udir, "profile.json")
        if os.path.exists(profile_path):
            with open(profile_path) as f:
                p = json.load(f)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (id, display_name, email, image_url)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        email        = EXCLUDED.email,
                        image_url    = EXCLUDED.image_url
                    """,
                    (
                        p.get("id", uid),
                        p.get("display_name", uid),
                        p.get("email", ""),
                        p.get("image", ""),
                    ),
                )
            conn.commit()
        else:
            # Create minimal user row so FK constraints work
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (id, display_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (uid, uid),
                )
            conn.commit()

        # Spotify token cache
        token_path = os.path.join(udir, ".spotify_token_cache")
        if os.path.exists(token_path):
            with open(token_path) as f:
                token_data = json.load(f)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_tokens (user_id, token_data, updated_at)
                    VALUES (%s, %s::JSONB, NOW())
                    ON CONFLICT (user_id) DO UPDATE SET
                        token_data = EXCLUDED.token_data,
                        updated_at = NOW()
                    """,
                    (uid, json.dumps(token_data)),
                )
            conn.commit()

        # History
        history_path = os.path.join(udir, "history.json")
        if os.path.exists(history_path):
            with open(history_path) as f:
                history = json.load(f)
            with conn.cursor() as cur:
                cur.execute("DELETE FROM user_history WHERE user_id = %s", (uid,))
                for item in history:
                    cur.execute(
                        """
                        INSERT INTO user_history
                            (user_id, track_id, track_name, artist, region, status, listened_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            uid,
                            item.get("id"),
                            item.get("track"),
                            item.get("artist"),
                            item.get("region"),
                            item.get("status"),
                            item.get("time"),
                        ),
                    )
            conn.commit()

        # Ledger
        ledger_path = os.path.join(udir, "ledger.json")
        if os.path.exists(ledger_path):
            with open(ledger_path) as f:
                ledger = json.load(f)
            with conn.cursor() as cur:
                # known (strings)
                for track_key in ledger.get("known", []):
                    if isinstance(track_key, str):
                        cur.execute(
                            """
                            INSERT INTO user_ledger (user_id, track_key, status)
                            VALUES (%s, %s, 'known')
                            ON CONFLICT (user_id, track_key) DO NOTHING
                            """,
                            (uid, track_key.lower()),
                        )
                # liked (dicts)
                for item in ledger.get("liked", []):
                    if isinstance(item, dict):
                        key = item.get("track", "").lower()
                        vibe = item.get("vibe", [])
                        cur.execute(
                            """
                            INSERT INTO user_ledger (user_id, track_key, status, vibe)
                            VALUES (%s, %s, 'liked', %s)
                            ON CONFLICT (user_id, track_key) DO UPDATE SET
                                status = 'liked', vibe = EXCLUDED.vibe
                            """,
                            (uid, key, vibe),
                        )
                # disliked (dicts)
                for item in ledger.get("disliked", []):
                    if isinstance(item, dict):
                        key = item.get("track", "").lower()
                        reason = item.get("reason", "")
                        cur.execute(
                            """
                            INSERT INTO user_ledger (user_id, track_key, status, reason)
                            VALUES (%s, %s, 'disliked', %s)
                            ON CONFLICT (user_id, track_key) DO UPDATE SET
                                status = 'disliked', reason = EXCLUDED.reason
                            """,
                            (uid, key, reason),
                        )
            conn.commit()

        print(f"  ✓ {uid}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{'#'*60}")
    print(f"  DIG — JSON → PostgreSQL migration")
    if DRY_RUN:
        print(f"  DRY RUN — no data will be written")
    print(f"{'#'*60}")

    conn = get_conn()
    t0 = time.time()

    migrate_tracks(conn)
    migrate_artists(conn)
    migrate_catalog(conn)
    migrate_search_history(conn)
    migrate_users(conn)

    conn.close()

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  Migration complete in {elapsed:.1f}s")
    if DRY_RUN:
        print("  (dry run — nothing was written)")
    print(f"{'='*60}\n")
