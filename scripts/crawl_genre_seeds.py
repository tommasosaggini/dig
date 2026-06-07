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
from collections import deque
from datetime import datetime, timezone

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

ENV_PATH = os.path.join(ROOT, ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from lib.db import fetchone, get_conn

DEFAULT_SEED_PATH = os.path.join(ROOT, "data", "genre_seeds.json")
MB_URL = "https://musicbrainz.org/ws/2/artist"
MB_HEADERS = {
    "User-Agent": "DIG-MusicDiscovery/1.0 (https://ohdig.com; admin@ohdig.com)",
    "Accept": "application/json",
}
MB_RATE_LIMIT_S = 1.05  # MB asks for 1 req/sec; small buffer

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
            wait = int(r.headers.get("Retry-After", 5 * (attempt + 1)))
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


def record_crawl(cur, mbid: str, depth: int, seed_genre: str | None,
                 seed_qid: str | None, rels_found: int, new_artists: int,
                 error: str | None) -> None:
    cur.execute(
        """
        INSERT INTO mb_crawl_state (
          mbid, depth, seed_genre, seed_qid, rels_found, new_artists, error
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (mbid) DO UPDATE SET
          crawled_at  = NOW(),
          depth       = LEAST(mb_crawl_state.depth, EXCLUDED.depth),
          rels_found  = EXCLUDED.rels_found,
          new_artists = EXCLUDED.new_artists,
          error       = EXCLUDED.error
        """,
        (mbid, depth, seed_genre, seed_qid, rels_found, new_artists, error))


def load_already_crawled() -> set[str]:
    """Pull the full set of successfully-crawled MBIDs into memory.

    Per-item SELECTs would double our latency (DB roundtrip per artist
    on top of the MB request). The set is bounded by mb_crawl_state size
    — even at 1M rows that's ~36MB in memory, fine.
    """
    import psycopg2.extras
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT mbid FROM mb_crawl_state WHERE error IS NULL")
            return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


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
    p.add_argument("--max-depth", type=int, default=1,
                   help="BFS depth from seeds (0 = seeds only, 1 = +1 hop)")
    p.add_argument("--max-artists", type=int, default=None,
                   help="Hard cap on artists to process this run")
    p.add_argument("--genre", default=None,
                   help="Restrict to one genre name (e.g. 'afrobeat')")
    p.add_argument("--reset", action="store_true",
                   help="Delete all mb_crawl_state rows and start fresh")
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

    print(f"Loaded {len(seeds)} unique seed MBIDs from {args.seed_path}")
    if args.genre:
        print(f"  filter: genre = {args.genre!r}")

    # BFS frontier — deque for O(1) popleft. Each entry:
    # (mbid, depth, seed_genre, seed_qid, name).
    frontier = deque((mbid, 0, g, qid, name) for (mbid, g, qid, name) in seeds)
    started = datetime.now(timezone.utc)
    deadline = started.timestamp() + args.max_minutes * 60

    # Pre-load successfully-crawled MBIDs so we don't roundtrip the DB
    # per item to check "already done." Errors are NOT in this set so
    # they can be retried on the next run.
    crawled = load_already_crawled()
    print(f"  resume: {len(crawled)} MBIDs already crawled (will skip)")

    processed = 0
    new_mb_artists = 0
    new_neighbors = 0
    errors = 0
    skipped_already = 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            while frontier:
                if time.time() > deadline:
                    print(f"  time budget exhausted ({args.max_minutes}min)")
                    break
                if args.max_artists and processed >= args.max_artists:
                    print(f"  artist budget exhausted ({args.max_artists})")
                    break

                mbid, depth, seed_genre, seed_qid, name = frontier.popleft()

                if mbid in crawled:
                    skipped_already += 1
                    continue
                crawled.add(mbid)  # claim it now so dup-paths in the BFS don't re-queue

                if args.dry_run:
                    print(f"  [{processed + 1}] would crawl: {name} ({mbid[:8]}) depth={depth} via {seed_genre}")
                    processed += 1
                    continue

                doc = mb_lookup(mbid)
                time.sleep(MB_RATE_LIMIT_S)

                if not doc or "_error" in doc:
                    err = (doc or {}).get("_error", "unknown")
                    record_crawl(cur, mbid, depth, seed_genre, seed_qid, 0, 0, err)
                    errors += 1
                    if processed % 25 == 0:
                        conn.commit()
                    processed += 1
                    continue

                # Upsert the seed itself with full info (country, spotify_id, …)
                if upsert_mb_artist(cur, doc):
                    new_mb_artists += 1

                # Walk relationships
                rels = doc.get("relations") or []
                walkable = []
                for rel in rels:
                    if rel.get("target-type") != "artist":
                        continue
                    if rel.get("type") not in WALKABLE_REL_TYPES:
                        continue
                    target = rel.get("artist") or {}
                    nbr_mbid = target.get("id")
                    if not nbr_mbid:
                        continue
                    walkable.append((nbr_mbid, target.get("name") or ""))

                # Insert neighbor stubs into mb_artists; queue for BFS
                # if depth allows
                seen_in_rels = set()
                rels_new = 0
                for nbr_mbid, nbr_name in walkable:
                    if nbr_mbid in seen_in_rels:
                        continue
                    seen_in_rels.add(nbr_mbid)
                    if insert_neighbor_stub(cur, nbr_mbid, nbr_name):
                        rels_new += 1
                        new_neighbors += 1
                    if depth + 1 <= args.max_depth:
                        frontier.append((nbr_mbid, depth + 1, seed_genre, seed_qid, nbr_name))

                record_crawl(cur, mbid, depth, seed_genre, seed_qid,
                             len(walkable), rels_new, None)
                processed += 1

                if processed % 25 == 0:
                    conn.commit()
                    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
                    rate = processed / max(elapsed, 1)
                    queue = len(frontier)
                    print(f"  [{processed}] {new_mb_artists} new seeds, "
                          f"{new_neighbors} new neighbors, "
                          f"{errors} errors, {skipped_already} pre-crawled "
                          f"| queue={queue} | {rate:.2f}/s")

        conn.commit()
    finally:
        conn.close()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\n=== DONE ===  processed={processed}, "
          f"new_seeds={new_mb_artists}, new_neighbors={new_neighbors}, "
          f"errors={errors}, skipped_pre_crawled={skipped_already}, "
          f"elapsed={elapsed / 60:.1f}min "
          f"(rate={processed / max(elapsed, 1):.2f}/s)")

    # Snapshot
    r = fetchone(
        "SELECT COUNT(*) AS total, COUNT(spotify_id) AS sp, "
        "COUNT(ingested_at) AS ing FROM mb_artists")
    print(f"  mb_artists: {r['total']:,} total, {r['sp']:,} with spotify_id, "
          f"{r['ing']:,} ingested")
    r = fetchone("SELECT COUNT(*) AS n FROM mb_crawl_state")
    print(f"  mb_crawl_state: {r['n']:,} rows")


if __name__ == "__main__":
    main()
