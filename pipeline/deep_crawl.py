#!/usr/bin/env python3
"""
DIG — Deep Discovery Crawler.

Graph-based recursive music discovery. Instead of flat Spotify searches that
return the same popular artists, this crawler builds an artist graph and
explores it depth-first: collaborations → compilations → genre-tag walks →
Claude taxonomy expansion → related artists.

Each cron run adds ~100 NEW unique artists, going deeper into scenes rather
than re-scanning the surface. Per-artist breadth cap enforced via
lib.artist_cap (default 3 tracks per artist NAME, all sources) so a single
`artist:Foo` probe can't dump 25+ tracks for the same artist into the pool.

Phases:
  1. COLLAB PARSE    — mine existing pool for collaboration edges (0 API calls)
  2. COMPILATION     — search for "Various Artists" + genre compilations
  3. GENRE-TAG WALK  — search each unique genre tag to find new artists
  4. RELATED ARTISTS — hop the Spotify artist graph (if dev-mode allows)
  5. CLAUDE TAXONOMY — AI expands under-covered genres with new search terms
  6. INGEST          — pull tracks for newly discovered artists

Usage:  python3 pipeline/deep_crawl.py [--dry-run] [--phase N]
"""
from __future__ import annotations

import json
import os
import random
import re
import sys
import time
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from lib.env import load_env
load_env()
from lib.db import get_conn, fetchall, fetchone
from lib.track_filter import is_trash
from lib.artist_cap import is_over_cap

# ── Spotify client (client-credentials flow — no user auth needed) ────────────
from lib.spotify_gate import make_client
sp = make_client(  # gated: cooldown-guarded + globally paced (lib/spotify_gate.py)
    auth_manager=SpotifyClientCredentials(
        client_id=os.environ.get("SPOTIPY_CLIENT_ID", ""),
        client_secret=os.environ.get("SPOTIPY_CLIENT_SECRET", ""),
    ),
    requests_timeout=15,
)

# ── Config ────────────────────────────────────────────────────────────────────
TRACKS_PER_ARTIST_PER_RUN = 3    # rate limit per run, NOT lifetime cap
MAX_COLLAB_PARSE = 5000          # tracks to scan for collaboration edges (0 API calls)
MAX_COMPILATIONS = 3             # VA albums to mine per run (~6 API calls)
MAX_GENRE_WALKS = 8              # genre-tag searches per run (~8 API calls)
MAX_RELATED_HOPS = 10            # related-artist fetches per run (~10 API calls)
MAX_CLAUDE_EXPANSIONS = 2        # genres to expand via Claude per run (0 Spotify calls)
MAX_INGEST_ARTISTS = 30          # new artists to ingest tracks for per run — priority phase
API_SLEEP = 1.0                  # seconds between Spotify API calls (conservative for dev mode)


# Global API call budget — stop making Spotify calls if we hit the daily limit.
_api_budget_exhausted = False
_api_calls_made = 0
MAX_API_CALLS_PER_RUN = 80  # deep crawler gets priority — main pipeline capped at 40

def safe_call(fn, *args, **kwargs):
    """Call a Spotify API function with budget + rate-limit awareness."""
    global _api_budget_exhausted, _api_calls_made
    if _api_budget_exhausted:
        return None
    if _api_calls_made >= MAX_API_CALLS_PER_RUN:
        print(f"  (API budget reached: {_api_calls_made} calls — stopping API phases)")
        _api_budget_exhausted = True
        return None
    for attempt in range(2):
        try:
            _api_calls_made += 1
            return fn(*args, **kwargs)
        except spotipy.SpotifyException as e:
            if e.http_status == 429:
                wait = int(e.headers.get("Retry-After", 5)) if hasattr(e, "headers") else 5
                if wait > 60:
                    print(f"  (rate limited for {wait}s — daily quota hit, stopping API phases)")
                    _api_budget_exhausted = True
                    return None
                print(f"  (rate limited, waiting {wait}s)")
                time.sleep(wait)
            elif e.http_status == 403:
                print(f"  (403 forbidden — endpoint may be blocked in dev mode)")
                return None
            else:
                print(f"  (Spotify error {e.http_status}: {e})")
                return None
        except Exception as e:
            print(f"  (error: {e})")
            return None
    return None


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_known_artist_ids() -> set:
    """All Spotify artist IDs we already have in the artists table."""
    rows = fetchall("SELECT spotify_id FROM artists WHERE spotify_id IS NOT NULL")
    return {r["spotify_id"] for r in rows if r["spotify_id"]}


def _get_known_artist_names() -> set:
    """Lowercased artist names we already know."""
    rows = fetchall("SELECT name FROM artists WHERE name IS NOT NULL")
    return {(r["name"] or "").lower() for r in rows if r["name"]}


def _register_artist(spotify_id, name, genres=None, region=None, popularity=0,
                     explore_depth=0, discovered_via=None):
    """Insert or update an artist. Idempotent on spotify_id."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artists (spotify_id, name, genres, regions, popularity,
                                     explore_depth, discovered_via, sources)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (spotify_id) DO UPDATE SET
                    genres = CASE WHEN array_length(EXCLUDED.genres, 1) > 0
                                  THEN EXCLUDED.genres ELSE artists.genres END,
                    popularity = GREATEST(artists.popularity, EXCLUDED.popularity)
                """,
                (spotify_id, name, genres or [], [region] if region else [],
                 popularity, explore_depth, discovered_via, ["spotify"]),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        # Might fail on schema mismatch — try simpler insert
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO artists (slug, name, genres, regions, sources, spotify_id,
                                         popularity, explore_depth, discovered_via)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (name.lower().replace(" ", "-")[:100], name, genres or [],
                     [region] if region else [], ["spotify"],
                     spotify_id, popularity, explore_depth, discovered_via),
                )
            conn.commit()
        except Exception:
            conn.rollback()
    finally:
        conn.close()


def _add_edge(from_id, to_id, edge_type):
    """Add an edge to the artist graph. Idempotent."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO artist_edges (from_id, to_id, edge_type) "
                "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (from_id, to_id, edge_type),
            )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()


def _ingest_tracks_for_artist(artist_id, artist_name, max_tracks=TRACKS_PER_ARTIST_PER_RUN):
    """Find tracks by an artist and ingest them into the pool.
    Uses search instead of artist_top_tracks (which is 403'd in dev mode).
    Returns count of tracks added."""
    existing = fetchone(
        "SELECT COUNT(*) AS n FROM tracks WHERE artist_ids @> ARRAY[%s]::text[]",
        (artist_id,),
    )
    existing_count = (existing or {}).get("n", 0)

    # Use search by artist name instead of artist_top_tracks (blocked in dev mode)
    result = safe_call(sp.search, q=f'artist:"{artist_name}"', type="track", limit=10)
    if not result or not result.get("tracks", {}).get("items"):
        return 0

    tracks = result["tracks"]["items"]
    added = 0
    for t in tracks:
        if added >= max_tracks:
            break
        if not t or not t.get("id"):
            continue
        name = t.get("name", "")
        artist_str = ", ".join(a["name"] for a in t.get("artists", []))
        album_name = (t.get("album") or {}).get("name", "")
        if is_trash(name, artist_str, album_name):
            continue
        # Check if we already have this track
        exists = fetchone("SELECT id FROM tracks WHERE id = %s", (t["id"],))
        if exists:
            continue
        # Extract metadata
        album = t.get("album", {})
        release_date = album.get("release_date", "")
        year = release_date[:4] if len(release_date) >= 4 else ""
        decade = (year[:3] + "0s") if year else ""
        artist_ids = [a["id"] for a in t.get("artists", []) if a.get("id")]

        # Insert into tracks table — gated by lib.artist_cap so we don't
        # blow past the per-artist breadth limit. Without this, deep_crawl's
        # design ("no hard cap per artist") repeatedly dumped 25-50 tracks
        # per artist into the pool from one `artist:Foo` probe.
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                primary = artist_ids[0] if artist_ids else None
                if is_over_cap(cur, primary, artist_name=artist_str):
                    continue  # artist already at cap, skip
                cur.execute(
                    """
                    INSERT INTO tracks (id, name, artist, artist_ids, album, popularity,
                                        source, region, decade, year, query)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (t["id"], name, artist_str, artist_ids,
                     album.get("name", ""), t.get("popularity", 0),
                     "spotify", "",  # region will be set by label_discovery
                     decade, year, f"crawl:artist:{artist_id}"),
                )
            conn.commit()
            added += 1
        except Exception:
            conn.rollback()
        finally:
            conn.close()

    if added:
        # Update tracks_ingested counter
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE artists SET tracks_ingested = COALESCE(tracks_ingested, 0) + %s "
                    "WHERE spotify_id = %s",
                    (added, artist_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            conn.close()

    return added


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1: COLLABORATION PARSING (zero API calls)
# ══════════════════════════════════════════════════════════════════════════════

def phase_collab_parse():
    """Mine existing pool for collaboration edges. If track credits "A, B, C",
    and we know A but not B/C, register B and C as new artists."""
    print("\n🔗 PHASE 1: Collaboration parsing")

    known_ids = _get_known_artist_ids()
    known_names = _get_known_artist_names()

    # Fetch tracks with multiple artist_ids
    rows = fetchall(
        """
        SELECT id, artist, artist_ids FROM tracks
        WHERE array_length(artist_ids, 1) > 1
        ORDER BY random()
        LIMIT %s
        """,
        (MAX_COLLAB_PARSE,),
    )

    new_artists = 0
    new_edges = 0
    for r in rows:
        artist_ids = r.get("artist_ids") or []
        artist_names = [p.strip() for p in (r.get("artist") or "").split(",")]
        if len(artist_ids) < 2:
            continue

        # For each pair of collaborating artists, add edges
        for i, aid_a in enumerate(artist_ids):
            for j, aid_b in enumerate(artist_ids):
                if i >= j:
                    continue
                _add_edge(aid_a, aid_b, "collab")
                _add_edge(aid_b, aid_a, "collab")
                new_edges += 1

            # Register unknown artists
            if aid_a not in known_ids:
                name = artist_names[i] if i < len(artist_names) else f"unknown_{aid_a[:8]}"
                _register_artist(aid_a, name, explore_depth=1,
                                 discovered_via=f"collab:{r['id']}")
                known_ids.add(aid_a)
                new_artists += 1

    print(f"  Scanned {len(rows)} tracks → {new_artists} new artists, {new_edges} edges")
    return new_artists


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: COMPILATION MINING
# ══════════════════════════════════════════════════════════════════════════════

def phase_compilation_mine():
    """Search for 'Various Artists' compilation albums in under-covered genres.
    Each compilation is a goldmine of ~15 unique artists."""
    print("\n📀 PHASE 2: Compilation mining")
    if _api_budget_exhausted:
        print("  Skipped — API budget exhausted.")
        return 0

    known_ids = _get_known_artist_ids()

    # Find genres with low artist count
    genre_counts = fetchall(
        """
        SELECT unnest(genres) AS g, COUNT(DISTINCT spotify_id) AS n
        FROM artists WHERE genres IS NOT NULL AND array_length(genres, 1) > 0
        GROUP BY 1 ORDER BY 2 ASC LIMIT 50
        """
    )
    thin_genres = [r["g"] for r in genre_counts if r["n"] < 20]
    if not thin_genres:
        # All genres have decent coverage; pick random ones
        all_genres = fetchall(
            "SELECT DISTINCT unnest(genres) AS g FROM artists ORDER BY random() LIMIT 20"
        )
        thin_genres = [r["g"] for r in all_genres]

    random.shuffle(thin_genres)
    new_artists = 0

    for genre in thin_genres[:MAX_COMPILATIONS]:
        query = f'tag:"{genre}" Various Artists'
        result = safe_call(sp.search, q=query, type="album", limit=5)
        time.sleep(API_SLEEP)
        if not result or not result.get("albums", {}).get("items"):
            continue

        for album in result["albums"]["items"][:2]:
            album_id = album.get("id")
            if not album_id:
                continue
            # Fetch album tracks
            album_tracks = safe_call(sp.album_tracks, album_id, limit=50)
            time.sleep(API_SLEEP)
            if not album_tracks:
                continue

            for t in album_tracks.get("items", []):
                for artist in t.get("artists", []):
                    aid = artist.get("id")
                    if not aid or aid in known_ids:
                        continue
                    _register_artist(aid, artist.get("name", ""),
                                     explore_depth=1,
                                     discovered_via=f"compilation:{album_id}")
                    known_ids.add(aid)
                    new_artists += 1
                    # Add edge from the compilation to each artist
                    _add_edge(f"compilation:{album_id}", aid, "compilation")

        print(f"  '{genre}' → mined compilations → {new_artists} new artists so far")

    print(f"  Total: {new_artists} new artists from compilations")
    return new_artists


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: GENRE-TAG WALKING
# ══════════════════════════════════════════════════════════════════════════════

def phase_genre_walk():
    """For each unique genre tag on known artists, search Spotify for that
    exact genre — finding artists who share the tag but aren't in our graph."""
    print("\n🏷️  PHASE 3: Genre-tag walking")
    if _api_budget_exhausted:
        print("  Skipped — API budget exhausted.")
        return 0

    known_ids = _get_known_artist_ids()

    # Get all distinct genre tags from our artists, prioritize rare ones
    genre_rows = fetchall(
        """
        SELECT unnest(genres) AS g, COUNT(*) AS n
        FROM artists WHERE genres IS NOT NULL
        GROUP BY 1 ORDER BY 2 ASC
        """
    )
    # Mix: 70% rare genres (go deeper), 30% random (breadth)
    rare = [r["g"] for r in genre_rows if r["n"] < 10][:50]
    all_g = [r["g"] for r in genre_rows]
    random.shuffle(rare)
    random.shuffle(all_g)
    targets = rare[:int(MAX_GENRE_WALKS * 0.7)] + all_g[:int(MAX_GENRE_WALKS * 0.3)]
    targets = list(dict.fromkeys(targets))[:MAX_GENRE_WALKS]

    new_artists = 0
    for genre in targets:
        # Search for artists with this genre tag
        # Dev mode doesn't support genre: prefix on artist search.
        # Use track search with the genre as a keyword instead — still finds
        # artists in that genre, just via their tracks.
        result = safe_call(sp.search, q=genre, type="track", limit=10,
                           offset=random.randint(0, 100))
        time.sleep(API_SLEEP)
        if not result:
            continue
        items = result.get("tracks", {}).get("items", [])
        batch_new = 0
        for track in items:
            if not track:
                continue
            for artist in track.get("artists", []):
                aid = artist.get("id")
                if not aid or aid in known_ids:
                    continue
                _register_artist(aid, artist.get("name", ""),
                                 popularity=track.get("popularity", 0),
                                 explore_depth=1, discovered_via=f"genre_walk:{genre}")
                known_ids.add(aid)
                new_artists += 1
                batch_new += 1
        if batch_new:
            print(f"  '{genre}' → {batch_new} new artists")

    print(f"  Total: {new_artists} new artists from genre walks")
    return new_artists


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4: RELATED ARTIST HOPPING
# ══════════════════════════════════════════════════════════════════════════════

def phase_related_hop():
    """For artists we haven't explored yet, fetch their Spotify related artists.
    Each hop adds ~20 new artists one degree deeper into the scene."""
    print("\n🕸️  PHASE 4: Related artist hopping")
    if _api_budget_exhausted:
        print("  Skipped — API budget exhausted.")
        return 0

    known_ids = _get_known_artist_ids()

    # Pick artists to explore: not yet related-fetched, prioritize thin regions
    candidates = fetchall(
        """
        SELECT spotify_id, name, explore_depth
        FROM artists
        WHERE related_fetched = FALSE AND spotify_id IS NOT NULL
        ORDER BY explore_depth ASC, tracks_ingested ASC
        LIMIT %s
        """,
        (MAX_RELATED_HOPS * 2,),  # fetch more than needed in case some fail
    )

    new_artists = 0
    hops_done = 0

    for c in candidates:
        if hops_done >= MAX_RELATED_HOPS:
            break
        aid = c["spotify_id"]
        result = safe_call(sp.artist_related_artists, aid)
        time.sleep(API_SLEEP)

        # Mark as fetched regardless of result
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE artists SET related_fetched = TRUE, last_crawled_at = NOW() "
                    "WHERE spotify_id = %s",
                    (aid,),
                )
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            conn.close()

        if result is None:
            # 403 = endpoint blocked in dev mode. Stop trying.
            print("  Related artists endpoint blocked (403). Skipping phase.")
            break

        hops_done += 1
        related = result.get("artists", [])
        batch_new = 0
        for r_artist in related:
            r_id = r_artist.get("id")
            if not r_id or r_id in known_ids:
                continue
            _register_artist(r_id, r_artist.get("name", ""),
                             genres=r_artist.get("genres", []),
                             popularity=r_artist.get("popularity", 0),
                             explore_depth=c["explore_depth"] + 1,
                             discovered_via=aid)
            _add_edge(aid, r_id, "related")
            known_ids.add(r_id)
            new_artists += 1
            batch_new += 1

        if batch_new:
            print(f"  {c['name']} (depth {c['explore_depth']}) → {batch_new} new related artists")

    print(f"  Total: {new_artists} new artists from {hops_done} hops")
    return new_artists


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5: CLAUDE TAXONOMY EXPANSION
# ══════════════════════════════════════════════════════════════════════════════

def phase_claude_taxonomy():
    """Systematic genre tree walk: instead of randomly expanding 2 genres,
    Claude receives ALL genres in our pool and identifies what major
    traditions, sub-genres, and regional variants are MISSING entirely.
    Then generates search terms to fill those gaps."""
    print("\n🤖 PHASE 5: Claude taxonomy expansion")

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("  No ANTHROPIC_API_KEY — skipping.")
        return 0

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    # Get all genres we have + their track counts
    genre_rows = fetchall(
        """
        SELECT unnest(genres) AS g, COUNT(*) AS n
        FROM tracks WHERE genres IS NOT NULL AND array_length(genres, 1) > 0
        GROUP BY 1 ORDER BY 2 DESC
        """
    )
    genre_summary = {r["g"]: r["n"] for r in genre_rows}

    # Get genres already expanded
    already_expanded = set()
    rows = fetchall("SELECT genre FROM genre_expansions")
    for r in rows:
        already_expanded.add(r["genre"])

    # Send the full genre list to Claude for systematic gap analysis
    # Group by rough count buckets for readability
    over_100 = [g for g, n in genre_summary.items() if n > 100]
    under_10 = [g for g, n in genre_summary.items() if n < 10]
    already_list = list(already_expanded)

    prompt = (
        f"I'm building a music discovery tool with {len(genre_summary)} genres. "
        f"I need to identify MISSING genres — established musical traditions, "
        f"sub-genres, and regional variants that we should have but don't.\n\n"
        f"GENRES WE HAVE (with >100 tracks — well covered):\n{json.dumps(over_100)}\n\n"
        f"GENRES WE HAVE (with <10 tracks — barely covered):\n{json.dumps(under_10[:100])}\n\n"
        f"Total genre count: {len(genre_summary)}\n\n"
        f"ALREADY EXPANDED (don't repeat):\n{json.dumps(already_list)}\n\n"
        f"Your task: identify 5 GENRE FAMILIES where we have significant gaps. "
        f"For each family, provide:\n"
        f"- The family name (e.g. 'Caribbean folk traditions')\n"
        f"- 6-10 specific search terms for sub-genres/variants we're missing\n"
        f"- Focus on established genres with real artists on Spotify, not obscure academic terms\n\n"
        f"Return ONLY a JSON array:\n"
        f"[{{\"family\": \"...\", \"terms\": [\"search term 1\", ...]}}, ...]\n"
    )

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            system=[{"type": "text", "text":
                     "You are a music taxonomy expert. Identify gaps in a genre catalog. "
                     "Focus on real, searchable genres with actual Spotify content. "
                     "Include local-language/script search terms where relevant.",
                     "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text if msg.content else ""
        # Extract JSON
        start = raw.find("[")
        if start >= 0:
            depth = 0; in_str = False; esc = False
            for i in range(start, len(raw)):
                ch = raw[i]
                if in_str:
                    if esc: esc = False
                    elif ch == "\\": esc = True
                    elif ch == '"': in_str = False
                else:
                    if ch == '"': in_str = True
                    elif ch == "[": depth += 1
                    elif ch == "]":
                        depth -= 1
                        if depth == 0:
                            families = json.loads(raw[start:i + 1])
                            break
            else:
                families = []
        else:
            families = []

        new_searches = 0
        for fam in families:
            if not isinstance(fam, dict):
                continue
            family_name = fam.get("family", "unknown")
            terms = [str(t).strip() for t in fam.get("terms", []) if t][:12]
            if not terms:
                continue
            # Save each term set
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO genre_expansions (genre, sub_terms) "
                        "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (family_name, terms),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
            finally:
                conn.close()
            new_searches += len(terms)
            print(f"  '{family_name}' → {len(terms)} terms: {terms[:4]}...")

        print(f"  Total: {new_searches} new search terms across {len(families)} families")
        return new_searches

    except Exception as e:
        print(f"  Claude error: {e}")
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 8: PLAYLIST MINING
# ══════════════════════════════════════════════════════════════════════════════

MAX_PLAYLIST_SEARCHES = 5        # playlist searches per run (~5 API calls + ~5 playlist fetches)

def phase_playlist_mine():
    """Search for user-curated playlists about specific genres/regions.
    A single playlist like "underground Jakarta beats" or "isan folk hidden gems"
    contains 30-100 tracks by artists no search query would ever surface.
    Human curation is the strongest signal for emerging/tiny artists."""
    print("\n📋 PHASE 8: Playlist mining")
    if _api_budget_exhausted:
        print("  Skipped — API budget exhausted.")
        return 0

    known_ids = _get_known_artist_ids()

    # Build search queries: combine under-covered genres/regions with
    # curator-style keywords
    genre_counts = fetchall(
        """
        SELECT unnest(genres) AS g, COUNT(DISTINCT spotify_id) AS n
        FROM artists WHERE genres IS NOT NULL
        GROUP BY 1 ORDER BY 2 ASC LIMIT 30
        """
    )
    thin_genres = [r["g"] for r in genre_counts if r["n"] < 20]

    # Also add region-based playlist searches
    region_counts = fetchall(
        """
        SELECT region, COUNT(*) AS n FROM tracks
        WHERE region IS NOT NULL AND (source != 'youtube' OR source IS NULL)
        GROUP BY 1 ORDER BY 2 ASC LIMIT 20
        """
    )
    thin_regions = [r["region"] for r in region_counts if r["n"] < 100]

    # Build diverse search queries
    curator_keywords = ["hidden gems", "underground", "deep cuts", "essentials",
                        "traditional", "folk", "roots", "emerging", "new",
                        "rare", "obscure", "compilation"]
    queries = []
    for g in thin_genres[:10]:
        kw = random.choice(curator_keywords)
        queries.append(f"{g} {kw}")
    for r in thin_regions[:10]:
        kw = random.choice(curator_keywords)
        queries.append(f"{r} music {kw}")

    random.shuffle(queries)
    queries = queries[:MAX_PLAYLIST_SEARCHES]

    new_artists = 0
    for query in queries:
        result = safe_call(sp.search, q=query, type="playlist", limit=3)
        time.sleep(API_SLEEP)
        if not result or _api_budget_exhausted:
            break

        playlists = result.get("playlists", {}).get("items", [])
        for pl in playlists[:1]:  # just the top playlist per query
            pl_id = pl.get("id")
            if not pl_id:
                continue
            # Fetch playlist tracks
            pl_tracks = safe_call(sp.playlist_tracks, pl_id, limit=100,
                                  fields="items(track(id,name,artists(id,name,genres),popularity))")
            time.sleep(API_SLEEP)
            if not pl_tracks or _api_budget_exhausted:
                break

            batch_new = 0
            for item in pl_tracks.get("items", []):
                track = item.get("track")
                if not track:
                    continue
                for artist in track.get("artists", []):
                    aid = artist.get("id")
                    if not aid or aid in known_ids:
                        continue
                    _register_artist(aid, artist.get("name", ""),
                                     popularity=0,
                                     explore_depth=1,
                                     discovered_via=f"playlist:{pl_id}")
                    _add_edge(f"playlist:{pl_id}", aid, "playlist")
                    known_ids.add(aid)
                    new_artists += 1
                    batch_new += 1

            if batch_new:
                pl_name = (pl.get("name") or "")[:50]
                print(f"  '{query}' → playlist '{pl_name}' → {batch_new} new artists")

    print(f"  Total: {new_artists} new artists from playlists")
    return new_artists


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 9: NEW RELEASE SCANNING
# ══════════════════════════════════════════════════════════════════════════════

MAX_NEW_RELEASE_SEARCHES = 5     # searches per run

def phase_new_releases():
    """Search for recent releases in each genre/region to catch emerging artists
    who literally just dropped their first track. Sorted by recency, these
    surface artists that no graph traversal would find because they have
    no connections yet."""
    print("\n🆕 PHASE 9: New release scanning")
    if _api_budget_exhausted:
        print("  Skipped — API budget exhausted.")
        return 0

    known_ids = _get_known_artist_ids()

    # Build searches: genre + year:2026 (or current year)
    import datetime
    current_year = datetime.datetime.now().year

    # Mix of specific genres and regions
    genre_rows = fetchall(
        "SELECT DISTINCT unnest(genres) AS g FROM artists ORDER BY random() LIMIT 30"
    )
    all_genres = [r["g"] for r in genre_rows]
    random.shuffle(all_genres)

    # Also search by region names
    region_rows = fetchall(
        "SELECT DISTINCT region FROM tracks WHERE region IS NOT NULL ORDER BY random() LIMIT 10"
    )
    region_terms = [r["region"] for r in region_rows]

    queries = []
    for g in all_genres[:8]:
        queries.append(f"{g} year:{current_year}")
    for r in region_terms[:5]:
        queries.append(f"{r} music year:{current_year}")
    # Also try previous year for slightly older but still new artists
    for g in all_genres[8:12]:
        queries.append(f"{g} year:{current_year - 1}")

    random.shuffle(queries)
    queries = queries[:MAX_NEW_RELEASE_SEARCHES]

    new_artists = 0
    for query in queries:
        # Search for tracks (not artists) — track search returns more results for new releases
        result = safe_call(sp.search, q=query, type="track", limit=10,
                           offset=random.randint(0, 50))
        time.sleep(API_SLEEP)
        if not result or _api_budget_exhausted:
            break

        batch_new = 0
        for track in result.get("tracks", {}).get("items", []):
            if not track:
                continue
            for artist in track.get("artists", []):
                aid = artist.get("id")
                if not aid or aid in known_ids:
                    continue
                _register_artist(aid, artist.get("name", ""),
                                 popularity=track.get("popularity", 0),
                                 explore_depth=0,
                                 discovered_via=f"new_release:{query[:50]}")
                known_ids.add(aid)
                new_artists += 1
                batch_new += 1

        if batch_new:
            print(f"  '{query}' → {batch_new} new artists")

    print(f"  Total: {new_artists} new artists from new releases")
    return new_artists


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 6: INGEST TRACKS FOR NEW ARTISTS
# ══════════════════════════════════════════════════════════════════════════════

def phase_ingest():
    """For newly discovered artists (from phases 1-5), fetch their top tracks
    and add to the pool. Rate limited: max TRACKS_PER_ARTIST_PER_RUN per artist."""
    print("\n🎵 PHASE 6: Track ingestion")
    if _api_budget_exhausted:
        print("  Skipped — API budget exhausted.")
        return 0

    # Build a genre-coverage map: how many tracks each genre already has.
    # Artists tagged with under-covered genres get ingested first.
    genre_coverage = {}
    try:
        gc_rows = fetchall(
            """
            SELECT unnest(genres) AS g, COUNT(*) AS n
            FROM tracks WHERE genres IS NOT NULL AND array_length(genres, 1) > 0
            GROUP BY 1
            """
        )
        genre_coverage = {r["g"]: r["n"] for r in gc_rows}
    except Exception:
        pass

    # Pick artists, compute a priority score: lower = ingest first
    # Score = min(track count of their genres) + tracks_ingested * 100 + explore_depth * 10
    # Artists whose genres have fewer pool tracks get priority.
    candidates = fetchall(
        """
        SELECT spotify_id, name, genres, explore_depth, COALESCE(tracks_ingested, 0) AS ti
        FROM artists
        WHERE spotify_id IS NOT NULL
        ORDER BY
            COALESCE(tracks_ingested, 0) ASC,
            explore_depth ASC,
            random()
        LIMIT %s
        """,
        (MAX_INGEST_ARTISTS * 3,),  # fetch more, then sort by genre priority
    )
    # Re-sort by genre coverage priority
    def _genre_priority(artist_row):
        gs = artist_row.get("genres") or []
        if not gs:
            return 50  # no genres = medium priority
        # Use the MINIMUM coverage of any of their genres
        min_cov = min((genre_coverage.get(g, 0) for g in gs), default=50)
        return min_cov + (artist_row["ti"] * 100) + (artist_row["explore_depth"] * 10)

    candidates = sorted(candidates, key=_genre_priority)[:MAX_INGEST_ARTISTS]
    if candidates:
        sample = candidates[:3]
        for s in sample:
            gs = s.get("genres") or []
            min_g = min((genre_coverage.get(g, 0) for g in gs), default=0) if gs else 0
            print(f"  priority sample: {s['name'][:30]} genres={gs[:2]} min_coverage={min_g} ingested={s['ti']}")

    total_added = 0
    artists_with_tracks = 0

    for c in candidates:
        added = _ingest_tracks_for_artist(c["spotify_id"], c["name"])
        if added > 0:
            artists_with_tracks += 1
            total_added += added
            print(f"  {c['name']} (depth {c['explore_depth']}) → +{added} tracks")
        time.sleep(API_SLEEP)

    print(f"  Total: {total_added} tracks from {artists_with_tracks} artists")
    return total_added


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 7: SEARCH EXPANDED TERMS (from Claude taxonomy)
# ══════════════════════════════════════════════════════════════════════════════

def phase_search_expanded():
    """Search Spotify using the terms Claude suggested in Phase 5.
    Finds artists that no standard genre search would reach."""
    print("\n🔍 PHASE 7: Searching expanded terms")
    if _api_budget_exhausted:
        print("  Skipped — API budget exhausted.")
        return 0

    known_ids = _get_known_artist_ids()

    # Get unexplored terms
    rows = fetchall(
        "SELECT id, genre, sub_terms, explored FROM genre_expansions"
    )

    new_artists = 0
    for row in rows:
        explored = set(row["explored"] or [])
        unexplored = [t for t in (row["sub_terms"] or []) if t not in explored]
        if not unexplored:
            continue

        for term in unexplored[:3]:  # max 3 terms per genre per run
            result = safe_call(sp.search, q=term, type="track", limit=10)
            time.sleep(API_SLEEP)
            if not result:
                continue

            batch_new = 0
            for track in result.get("tracks", {}).get("items", []):
                if not track:
                    continue
                for artist in track.get("artists", []):
                    aid = artist.get("id")
                    if not aid or aid in known_ids:
                        continue
                    _register_artist(aid, artist.get("name", ""),
                                     popularity=track.get("popularity", 0),
                                     explore_depth=1,
                                     discovered_via=f"claude_term:{term}")
                    _add_edge(f"genre:{row['genre']}", aid, "claude_suggested")
                    known_ids.add(aid)
                    new_artists += 1
                    batch_new += 1

            # Mark term as explored
            explored.add(term)
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE genre_expansions SET explored = %s WHERE id = %s",
                        (list(explored), row["id"]),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
            finally:
                conn.close()

            if batch_new:
                print(f"  '{term}' ({row['genre']}) → {batch_new} new artists")

    print(f"  Total: {new_artists} new artists from expanded terms")
    return new_artists


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--phase", type=int, default=0, help="Run only this phase (1-7)")
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — no writes")
        return

    # Pre-flight: bail before the first Spotify call if the app key is in cooldown.
    from lib.spotify_health import pre_flight_or_exit
    # deep_crawl fans out over several endpoints and degrades phase by phase
    # (it already skips phases when the budget is spent), so it only refuses to
    # start if the artist lookup itself is banned.
    pre_flight_or_exit("deep_crawl", families=["artists"])

    print("\n🌍 DIG — DEEP DISCOVERY CRAWLER\n")

    # Pre-stats
    pre = fetchone("SELECT COUNT(*) AS tracks, COUNT(DISTINCT LOWER(SPLIT_PART(artist, ',', 1))) AS artists FROM tracks WHERE source != 'youtube' OR source IS NULL")
    pre_artist_table = fetchone("SELECT COUNT(*) AS n FROM artists")
    print(f"  Pool: {pre['tracks']} tracks, {pre['artists']} distinct artists")
    print(f"  Artists table: {pre_artist_table['n']} entries")

    started = time.time()

    if args.phase == 0 or args.phase == 1:
        phase_collab_parse()           # 0 API calls
    if args.phase == 0 or args.phase == 5:
        # Costs Anthropic, not Spotify — but its ONLY consumer is
        # phase_search_expanded, which needs the `search` family. When search
        # is locked out we would pay Sonnet for 50 search terms and then skip
        # every phase that could use them. Measured 2026-08-27: every one of
        # the last 15 runs hit the lockout, so all 8 runs a day were buying
        # taxonomy nobody could spend. An explicit --phase 5 still forces it.
        from lib.spotify_health import cooldown_for
        _search_locked = cooldown_for("search") > 0
        if args.phase == 5 or not _search_locked:
            phase_claude_taxonomy()    # 0 Spotify calls (Anthropic only)
        else:
            print(f"\n🤖 PHASE 5: Claude taxonomy expansion\n  Skipped — "
                  f"`search` is in cooldown for another "
                  f"{cooldown_for('search') // 60} min, so nothing could "
                  f"search the terms it would generate.")
    if args.phase == 0 or args.phase == 2:
        phase_compilation_mine()       # ~4 API calls
    if args.phase == 0 or args.phase == 3:
        phase_genre_walk()             # ~8 API calls
    if args.phase == 0 or args.phase == 6:
        phase_ingest()                 # ~10 API calls — runs EARLY so new artists get tracks
    if args.phase == 0 or args.phase == 7:
        phase_search_expanded()        # ~3 API calls
    if args.phase == 0 or args.phase == 8:
        phase_playlist_mine()          # ~5 API calls
    if args.phase == 0 or args.phase == 9:
        phase_new_releases()           # ~5 API calls
    if args.phase == 0 or args.phase == 4:
        phase_related_hop()            # ~10 API calls (if not 403'd)

    elapsed = time.time() - started

    # Post-stats
    post = fetchone("SELECT COUNT(*) AS tracks, COUNT(DISTINCT LOWER(SPLIT_PART(artist, ',', 1))) AS artists FROM tracks WHERE source != 'youtube' OR source IS NULL")
    post_artist_table = fetchone("SELECT COUNT(*) AS n FROM artists")
    print(f"\n{'='*60}")
    print(f"  Elapsed: {elapsed:.0f}s")
    print(f"  Pool: {pre['tracks']} → {post['tracks']} tracks (+{post['tracks'] - pre['tracks']})")
    print(f"  Pool artists: {pre['artists']} → {post['artists']} (+{post['artists'] - pre['artists']})")
    print(f"  Artists table: {pre_artist_table['n']} → {post_artist_table['n']} (+{post_artist_table['n'] - pre_artist_table['n']})")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
