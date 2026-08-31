#!/usr/bin/env python3
"""
DIG — Genre-seeded MusicBrainz BFS crawler.

Walks the artist-relationship graph in MusicBrainz starting from the
canonical seed artists in `data/genre_seeds.json`. The point is artist
*breadth* — for every genre on Earth (Wikidata enumerates ~6,500), the
seed map gives us 1-5 internationally-recognized anchors; the BFS walk
expands outward from each anchor by following collaborators, band
members, side projects, similar acts, etc., until we've populated
`mb_artists` with a comprehensive long-tail catalog the existing
ingest pipeline can drain into the `tracks` pool.

Why this exists: search-driven discovery (genre × region × decade →
Spotify search) collapses to the same Anglo-pop attractors. Country-
rotation enumeration (the existing `enumerate_mb_artists.py`) gives us
breadth by geography but misses the cross-border genre lineages
(qawwali artists in Pakistan AND India AND the UK; afrobeat from
Lagos to Brooklyn; etc.). The genre-graph crawler closes that gap.

Architecture:
  data/genre_seeds.json  →  this script  →  mb_artists  →  ingest_mb_artists.py  →  tracks

Resume / restart:
  Crawl progress lives in `mb_crawl_state`. Re-running picks up where
  the previous run left off — already-crawled MBIDs are skipped.

Time budget:
  Default --max-minutes 50 keeps a cron run inside an hourly slot.
  MB rate limit is 1 req/s, so a 50min run does ~3000 artist lookups.

Usage:
  scripts/crawl_genre_seeds.py [--max-minutes 50] [--max-depth 1]
  scripts/crawl_genre_seeds.py --genre "afrobeat"          # one genre
  scripts/crawl_genre_seeds.py --reset                     # clear crawl state
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib.db import fetchone, get_conn

DEFAULT_SEED_PATH = os.path.join(ROOT, "data", "genre_seeds.json")
MB_URL = "https://musicbrainz.org/ws/2/artist"
MB_HEADERS = {
    "User-Agent": "DIG-MusicDiscovery/1.0 (https://ohdig.com; admin@ohdig.com)",
    "Accept": "application/json",
}
MB_RATE_LIMIT_S = 1.05  # MB asks for 1 req/sec; small buffer
MB_MAX_BACKOFF_S = 60   # ceiling on one wait, so a run cannot stall on a 503

# An MBID MusicBrainz does not have is not going to appear because we asked
# again. These end the work item; everything else is worth another attempt.
PERMANENT_ERRORS = {"not_found", "mb_400", "mb_410"}
MAX_ATTEMPTS = 5

# Relationship types that genuinely expand into other artists. MB has
# dozens of artist-rel types but most are noise (engineer-of, producer-
# of, instructed-by, married-to, etc.). These are the ones that
# correspond to musical lineage and yield discovery-worthy neighbors.
WALKABLE_REL_TYPES = {
    "member of band",     # band → members and vice versa
    "collaboration",      # one-off projects
    "supporting musician", # tour/session players
    "is person",          # alias relations
    "subgroup",           # spinoffs
    "founder",            # founders of bands → other projects
    "tribute",            # tribute / cover acts (genre signal)
    "vocal supporting",
    "instrumental supporting",
    "involved with",      # umbrella for collectives, record labels
}


def extract_spotify_id(url: str) -> str | None:
    """Pull a Spotify artist ID out of an open.spotify.com/artist/<id> URL."""
    if not url:
        return None
    import re
    m = re.search(r"open\.spotify\.com(?:/intl-[a-z]{2,4})?/artist/([0-9A-Za-z]+)", url)
    return m.group(1) if m else None


def mb_lookup(mbid: str, retries: int = 3) -> dict | None:
    """Look up one artist on MusicBrainz with relationships and URL rels.

    Returns the parsed JSON, or None on permanent failure. Honors MB's
    1 req/sec rate limit via sleep before the call. Backs off on 503.
    """
    params = {
        "fmt": "json",
        # Both forward and reverse direction artist-rels — MB only returns
        # one direction per query. The two are lopsided in practice
        # (collaboration is bidirectional but member-of-band lists members
        # only on the band side), so we pull artist-rels and walk both.
        "inc": "artist-rels+url-rels+tags+aliases",
    }
    url = f"{MB_URL}/{mbid}"
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=MB_HEADERS, timeout=20)
        except requests.RequestException as e:
            print(f"    network err: {e}; backing off")
            time.sleep(5 * (attempt + 1))
            continue

        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return {"_error": "not_found"}
        if r.status_code in (503, 429):
            # Retry-After is the server's CLAIM, and it is only useful when it
            # asks for more patience than we already planned. MusicBrainz has
            # been observed sending 0 here, and `int(headers.get(k, default))`
            # takes the header whenever the key is present — so the default
            # never applied, `time.sleep(0)` ran, and this loop fired three
            # requests in a few milliseconds at a server that had just said it
            # was overloaded. The logs read "backing off 0s", over and over.
            # Sibling of d7a2787, which fixed the header being ABSENT and left
            # the header being ZERO.
            ours = 5 * (attempt + 1)
            try:
                theirs = int(r.headers.get("Retry-After", 0))
            except (TypeError, ValueError):
                theirs = 0
            wait = min(max(ours, theirs), MB_MAX_BACKOFF_S)
            print(f"    MB {r.status_code}; backing off {wait}s")
            time.sleep(wait)
            continue
        if r.status_code in (400, 410):
            return {"_error": f"mb_{r.status_code}"}
        print(f"    MB {r.status_code}: {r.text[:120]}")
        time.sleep(3)
    return {"_error": "exhausted_retries"}


def upsert_mb_artist(cur, a: dict, source_country: str | None = None) -> bool:
    """Insert/update one mb_artists row from MB JSON.

    Returns True only when this is genuinely a new row (caller-side dedup
    already filters re-crawls of the same seed). UPSERT's rowcount can't
    distinguish INSERT from UPDATE on its own, so we use xmax=0 in
    RETURNING — xmax is the row-version of the displaced tuple, which is
    0 only when no displacement happened (i.e. INSERT)."""
    mbid = a.get("id")
    if not mbid:
        return False
    name = a.get("name") or ""
    sort_name = a.get("sort-name") or ""
    type_ = a.get("type")
    gender = a.get("gender")
    country = a.get("country") or source_country  # MB lookup carries country
    area = (a.get("area") or {}).get("name") if a.get("area") else None
    begin_area = (a.get("begin-area") or {}).get("name") if a.get("begin-area") else None
    life = a.get("life-span") or {}
    lb = life.get("begin")
    le = life.get("end")
    tags = [t.get("name") for t in (a.get("tags") or []) if t.get("name")]

    spotify_url = None
    spotify_id = None
    for rel in a.get("relations") or []:
        if (rel.get("type") in ("streaming", "free streaming")
            and rel.get("url", {}).get("resource", "").startswith(
                ("https://open.spotify.com/", "http://open.spotify.com/"))):
            spotify_url = rel["url"]["resource"]
            spotify_id = extract_spotify_id(spotify_url)
            if spotify_id:
                break

    cur.execute(
        """
        INSERT INTO mb_artists (
          mbid, name, sort_name, country, area, begin_area, type, gender,
          lifespan_begin, lifespan_end, mb_tags, spotify_url, spotify_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (mbid) DO UPDATE SET
          name        = COALESCE(NULLIF(EXCLUDED.name, ''), mb_artists.name),
          sort_name   = COALESCE(NULLIF(EXCLUDED.sort_name, ''), mb_artists.sort_name),
          country     = COALESCE(EXCLUDED.country, mb_artists.country),
          area        = COALESCE(EXCLUDED.area, mb_artists.area),
          begin_area  = COALESCE(EXCLUDED.begin_area, mb_artists.begin_area),
          type        = COALESCE(EXCLUDED.type, mb_artists.type),
          spotify_url = COALESCE(EXCLUDED.spotify_url, mb_artists.spotify_url),
          spotify_id  = COALESCE(EXCLUDED.spotify_id,  mb_artists.spotify_id),
          mb_tags     = CASE
            WHEN array_length(EXCLUDED.mb_tags, 1) > 0
              THEN EXCLUDED.mb_tags
            ELSE mb_artists.mb_tags
          END
        RETURNING (xmax = 0) AS inserted
        """,
        (mbid, name, sort_name, country, area, begin_area, type_, gender,
         lb, le, tags, spotify_url, spotify_id))
    row = cur.fetchone()
    return bool(row and row[0])


def insert_neighbor_stub(cur, mbid: str, name: str | None) -> bool:
    """Pre-populate mb_artists with bare-minimum info for a discovered
    relationship target. Subsequent crawl visits will fill in the rest.
    Returns True if this row is new to mb_artists."""
    cur.execute(
        """
        INSERT INTO mb_artists (mbid, name)
        VALUES (%s, %s)
        ON CONFLICT (mbid) DO NOTHING
        """,
        (mbid, name or ""))
    return cur.rowcount > 0


def ensure_queue_schema(cur) -> None:
    """Make mb_crawl_state a WORK QUEUE, not just a record of what finished.

    The table only ever held completed crawls, so the BFS frontier lived in a
    `deque` inside one process and died with it. Every run rebuilt that deque
    from data/genre_seeds.json — 5,155 MBIDs — and skipped the ones already in
    the table. The crawl therefore finished on 2026-04-28, at 6,645 artists
    (the seeds plus one hop), and every hourly run for the next four months
    walked the same 5,155 skips and wrote nothing. `mb_crawl_state.crawled_at`
    even looked fresh, because the two rows that errored were re-touched every
    hour: the freshness sensor was measuring the failure.

    A neighbour discovered at depth N is now written down as pending work at
    depth N+1, so the walk resumes across runs and --max-depth is a bound on
    the crawl rather than on the process.
    """
    cur.execute("ALTER TABLE mb_crawl_state "
                "ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'done'")
    cur.execute("ALTER TABLE mb_crawl_state "
                "ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE mb_crawl_state "
                "ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ")
    # A pending row has not been crawled, so it has no crawl time. The column
    # defaulted to now(), which would have dated every enqueue as a visit.
    cur.execute("ALTER TABLE mb_crawl_state ALTER COLUMN crawled_at DROP DEFAULT")
    cur.execute("ALTER TABLE mb_crawl_state ALTER COLUMN crawled_at DROP NOT NULL")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS mb_crawl_state_frontier_idx "
        "ON mb_crawl_state (status, depth, next_attempt_at)")

    # Rows written before the queue existed: an error meant "retry me on every
    # future run, for ever" because load_already_crawled() selected
    # `WHERE error IS NULL`. Sort them once into terminal and retryable.
    cur.execute("UPDATE mb_crawl_state SET status = 'failed' "
                "WHERE status = 'done' AND error = ANY(%s)",
                (sorted(PERMANENT_ERRORS),))
    cur.execute("UPDATE mb_crawl_state SET status = 'pending', attempts = 1 "
                "WHERE status = 'done' AND error IS NOT NULL "
                "AND NOT (error = ANY(%s))",
                (sorted(PERMANENT_ERRORS),))


def enqueue(cur, mbid: str, depth: int, seed_genre: str | None,
            seed_qid: str | None) -> bool:
    """Record an MBID as pending work. True if it was not already known."""
    cur.execute(
        """
        INSERT INTO mb_crawl_state (mbid, depth, seed_genre, seed_qid, status)
        VALUES (%s, %s, %s, %s, 'pending')
        ON CONFLICT (mbid) DO NOTHING
        """,
        (mbid, depth, seed_genre, seed_qid))
    return cur.rowcount > 0


def claim_frontier(cur, limit: int, max_depth: int,
                   genre: str | None = None) -> list[tuple]:
    """The next work items: shallowest first, respecting retry backoff.

    `genre` has to be applied HERE, not only when seeds are loaded. With the
    frontier in the database, filtering the seed file alone would have let
    `--genre afrobeat` walk whatever happened to be pending from every other
    genre — the documented single-genre invocation would have quietly ignored
    its own argument.
    """
    cur.execute(
        """
        SELECT mbid, depth, seed_genre, seed_qid
        FROM mb_crawl_state
        WHERE status = 'pending'
          AND depth <= %s
          AND (%s IS NULL OR lower(seed_genre) = lower(%s))
          AND (next_attempt_at IS NULL OR next_attempt_at <= now())
        ORDER BY depth ASC, attempts ASC, mbid
        LIMIT %s
        """,
        (max_depth, genre, genre, limit))
    return cur.fetchall()


def record_done(cur, mbid: str, rels_found: int, new_artists: int) -> None:
    cur.execute(
        """
        UPDATE mb_crawl_state
           SET status = 'done', crawled_at = now(), error = NULL,
               next_attempt_at = NULL,
               rels_found = %s, new_artists = %s
         WHERE mbid = %s
        """,
        (rels_found, new_artists, mbid))


def record_failure(cur, mbid: str, error: str) -> str:
    """Fail one work item. Returns the status it ended in.

    Permanent errors are terminal. Transient ones back off geometrically and
    give up after MAX_ATTEMPTS — the old code had neither notion, so the two
    artists that could not be fetched were re-requested every hour indefinitely.
    """
    if error in PERMANENT_ERRORS:
        cur.execute(
            "UPDATE mb_crawl_state SET status = 'failed', crawled_at = now(), "
            "error = %s, next_attempt_at = NULL WHERE mbid = %s",
            (error, mbid))
        return "failed"
    cur.execute(
        """
        UPDATE mb_crawl_state
           SET attempts = attempts + 1,
               error    = %s,
               status   = CASE WHEN attempts + 1 >= %s THEN 'failed' ELSE 'pending' END,
               crawled_at = CASE WHEN attempts + 1 >= %s THEN now() ELSE crawled_at END,
               next_attempt_at = now() + (interval '5 minutes' * power(3, attempts))
         WHERE mbid = %s
        RETURNING status
        """,
        (error, MAX_ATTEMPTS, MAX_ATTEMPTS, mbid))
    row = cur.fetchone()
    return row[0] if row else "pending"


def load_seeds(path: str, only_genre: str | None = None) -> list[tuple]:
    """Return [(mbid, seed_genre, seed_qid, name), ...] from the seed map.
    Each seed yields one entry. Same MBID across multiple genres dedups
    on first-seen (the BFS will pull each MBID exactly once anyway)."""
    with open(path) as f:
        d = json.load(f)
    seen = set()
    out = []
    for genre, v in d["genres"].items():
        if only_genre and genre.lower() != only_genre.lower():
            continue
        for s in v.get("seeds", []):
            mbid = s.get("mbid")
            if not mbid or mbid in seen:
                continue
            seen.add(mbid)
            out.append((mbid, genre, v.get("qid"), s.get("name", "")))
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--seed-path", default=DEFAULT_SEED_PATH,
                   help="Path to genre_seeds.json")
    p.add_argument("--max-minutes", type=int, default=50,
                   help="Soft time budget for this run (default 50min)")
    p.add_argument("--max-depth", type=int, default=2,
                   help="How far from the seeds to walk (0 = seeds only). "
                        "A bound on the CRAWL, not on one run: neighbours are "
                        "recorded at their true depth whatever this is set to, "
                        "so raising it later just makes them eligible. Was 1, "
                        "which the walk reached on 2026-04-28 and then had "
                        "nothing left to do for four months.")
    p.add_argument("--max-artists", type=int, default=None,
                   help="Hard cap on artists to process this run")
    p.add_argument("--genre", default=None,
                   help="Restrict to one genre name (e.g. 'afrobeat')")
    p.add_argument("--reset", action="store_true",
                   help="Delete all mb_crawl_state rows and start fresh")
    p.add_argument("--rewalk-depth", type=int, default=None,
                   help="One-off repair: re-queue the already-crawled artists AT "
                        "this exact depth, so their neighbours are rediscovered "
                        "and enqueued one level deeper. Needed once, because the "
                        "pre-queue crawl recorded that an artist had been visited "
                        "but never which artists that visit found. Exact, not a "
                        "range: re-walking the seeds would spend 5,155 lookups "
                        "rediscovering the depth-1 artists we already have.")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if not os.path.exists(args.seed_path):
        print(f"FATAL: seed map not found at {args.seed_path}")
        print("Run scripts/build_genre_seeds.py first.")
        sys.exit(1)

    if args.reset:
        if not args.dry_run:
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM mb_crawl_state")
                    n = cur.rowcount
                conn.commit()
                print(f"deleted {n} crawl-state rows")
            finally:
                conn.close()

    seeds = load_seeds(args.seed_path, only_genre=args.genre)
    if not seeds:
        print("No seeds in seed map matching filter; nothing to do.")
        return

    started = datetime.now(timezone.utc)
    deadline = started.timestamp() + args.max_minutes * 60

    processed = 0
    new_mb_artists = 0
    new_neighbors = 0
    errors = 0
    enqueued_seeds = 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            ensure_queue_schema(cur)
            conn.commit()

            # Seeds are a BOOTSTRAP, not the frontier. Ones we have never heard
            # of become depth-0 work; the rest are already in the queue table,
            # done or pending, and are not reconsidered here.
            # One statement, not 5,155. Bootstrap runs on every invocation,
            # and a round-trip per seed is minutes of dead time at the head of
            # an hourly job (worse from a laptop, where the connection is an
            # SSH tunnel to another continent).
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                "INSERT INTO mb_crawl_state (mbid, depth, seed_genre, seed_qid, status) "
                "VALUES %s ON CONFLICT (mbid) DO NOTHING",
                [(mbid, 0, g, qid, "pending") for (mbid, g, qid, _n) in seeds],
                page_size=500)
            enqueued_seeds = cur.rowcount
            conn.commit()
            print(f"Loaded {len(seeds)} seed MBIDs from {args.seed_path}"
                  f" ({enqueued_seeds} new to the queue)")

            if args.rewalk_depth is not None:
                # The edges of the 2026-04 crawl were never persisted — only
                # the fact that each artist had been visited — so the frontier
                # below max-depth cannot be reconstructed from the database and
                # has to be re-fetched once. This is that one-off.
                cur.execute(
                    "UPDATE mb_crawl_state SET status = 'pending', attempts = 0, "
                    "next_attempt_at = NULL "
                    "WHERE status = 'done' AND depth = %s", (args.rewalk_depth,))
                print(f"  re-queued {cur.rowcount:,} crawled artist(s) at "
                      f"depth {args.rewalk_depth} to recover their neighbours")
                conn.commit()

            cur.execute(
                "SELECT count(*) FROM mb_crawl_state WHERE status = 'pending' "
                "AND depth <= %s AND (%s IS NULL OR lower(seed_genre) = lower(%s)) "
                "AND (next_attempt_at IS NULL OR next_attempt_at <= now())",
                (args.max_depth, args.genre, args.genre))
            ready = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM mb_crawl_state WHERE status = 'pending'")
            pending_total = cur.fetchone()[0]
            print(f"  frontier: {ready:,} ready at depth <= {args.max_depth} "
                  f"({pending_total:,} pending overall)")

            while True:
                if time.time() > deadline:
                    print(f"  time budget exhausted ({args.max_minutes}min)")
                    break
                if args.max_artists and processed >= args.max_artists:
                    print(f"  artist budget exhausted ({args.max_artists})")
                    break

                batch = claim_frontier(cur, 50, args.max_depth, args.genre)
                if not batch:
                    print("  frontier empty at this depth — raise --max-depth "
                          "or add seeds")
                    break

                if args.dry_run:
                    for mbid, depth, seed_genre, _qid in batch:
                        print(f"  would crawl: {mbid[:8]} depth={depth} via {seed_genre}")
                    break  # nothing is claimed in a dry run, so do not loop

                for mbid, depth, seed_genre, seed_qid in batch:
                    if time.time() > deadline:
                        break
                    if args.max_artists and processed >= args.max_artists:
                        break

                    doc = mb_lookup(mbid)
                    time.sleep(MB_RATE_LIMIT_S)
                    processed += 1

                    if not doc or "_error" in doc:
                        err = (doc or {}).get("_error", "unknown")
                        record_failure(cur, mbid, err)
                        errors += 1
                        if processed % 25 == 0:
                            conn.commit()
                        continue

                    if upsert_mb_artist(cur, doc):
                        new_mb_artists += 1

                    # Walk relationships
                    walkable = []
                    for rel in doc.get("relations") or []:
                        if rel.get("target-type") != "artist":
                            continue
                        if rel.get("type") not in WALKABLE_REL_TYPES:
                            continue
                        target = rel.get("artist") or {}
                        nbr_mbid = target.get("id")
                        if not nbr_mbid:
                            continue
                        walkable.append((nbr_mbid, target.get("name") or ""))

                    seen_in_rels = set()
                    rels_new = 0
                    for nbr_mbid, nbr_name in walkable:
                        if nbr_mbid in seen_in_rels:
                            continue
                        seen_in_rels.add(nbr_mbid)
                        if insert_neighbor_stub(cur, nbr_mbid, nbr_name):
                            rels_new += 1
                            new_neighbors += 1
                        # Enqueue at its true depth REGARDLESS of --max-depth:
                        # the bound belongs on what we claim, not on what we
                        # remember. Recording it means raising --max-depth
                        # later makes this artist eligible instead of requiring
                        # the whole walk to be redone.
                        enqueue(cur, nbr_mbid, depth + 1, seed_genre, seed_qid)

                    record_done(cur, mbid, len(walkable), rels_new)

                    if processed % 25 == 0:
                        conn.commit()
                        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
                        print(f"  [{processed}] {new_mb_artists} new seeds, "
                              f"{new_neighbors} new neighbors, {errors} errors "
                              f"| {processed / max(elapsed, 1):.2f}/s")
                conn.commit()

        conn.commit()
    finally:
        conn.close()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\n=== DONE ===  processed={processed}, "
          f"new_seeds={new_mb_artists}, new_neighbors={new_neighbors}, "
          f"errors={errors}, elapsed={elapsed / 60:.1f}min "
          f"(rate={processed / max(elapsed, 1):.2f}/s)")

    # Snapshot
    r = fetchone(
        "SELECT COUNT(*) AS total, COUNT(spotify_id) AS sp, "
        "COUNT(ingested_at) AS ing FROM mb_artists")
    print(f"  mb_artists: {r['total']:,} total, {r['sp']:,} with spotify_id, "
          f"{r['ing']:,} ingested")
    r = fetchone(
        "SELECT count(*) FILTER (WHERE status = 'pending') AS pending, "
        "       count(*) FILTER (WHERE status = 'done')    AS done, "
        "       count(*) FILTER (WHERE status = 'failed')  AS failed, "
        "       min(depth) FILTER (WHERE status = 'pending') AS next_depth "
        "FROM mb_crawl_state")
    print(f"  crawl queue: {r['pending']:,} pending "
          f"(shallowest depth {r['next_depth']}), {r['done']:,} done, "
          f"{r['failed']:,} failed")


if __name__ == "__main__":
    main()
