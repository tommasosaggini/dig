#!/usr/bin/env python3
"""
DIG — Curator ingest.

Takes a list of candidate tracks scraped from a curator (Spotify playlists,
YouTube playlists, …), resolves each to a REAL Spotify track via the search
endpoint (the one still available in Development Mode), and ingests first-class
rows into the pool — identical shape to discover.py output. The curator's
artists then become expansion seeds for deep_crawl automatically.

Input:  a JSON file of candidates: [{"raw": "<title>", "source": "<tag>",
        "region": "<optional>"}].  `raw` is a messy "Artist - Track" string
        (YouTube video title, etc.); it gets cleaned and fuzzy-matched.
Output: rows inserted into `tracks`, artists registered, dedup against the pool.

Resumable: processed `raw` strings are recorded in a state file so re-runs skip
them. Bounded per invocation by --limit and a self-imposed call budget so it
never starves the live crons' shared Spotify budget.

Usage:
  python scripts/ingest_curator.py --in candidates.json --handle stirred.blessings [--dry-run] [--limit 200]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ── load .env ──
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from lib.env import load_env
load_env()
from lib.track_filter import is_trash
from lib.discovery_lock import locked_update
from lib.artist_db import register_tracks
from lib.db import fetchall

from lib.spotify_gate import make_client
sp = make_client()  # gated: cooldown-guarded + globally paced (lib/spotify_gate.py)

STATE_PATH = os.path.join(ROOT, ".curator_ingest_state.json")


# ── text helpers ──
def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").lower()
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_title(raw: str) -> str:
    """Strip the cruft DJs/labels add to video titles, leaving ~'Artist - Track'."""
    t = raw
    t = re.sub(r"\[[^\]]*\]", " ", t)                       # [LABEL123], [PREMIERE]
    t = re.sub(r"\([^)]*\b(out now|free dl|free download|premiere|official|video|audio|hd)\b[^)]*\)", " ", t, flags=re.I)
    t = re.sub(r"(?i)\bpremiere\b\s*[:\-]?", " ", t)
    t = re.sub(r"(?i)^\s*(full\s+album|album|ep|lp)\s*[:\-]\s*", "", t)
    t = re.sub(r"^\s*[A-Da-d]?\s?\d{1,2}\s*[\.\)]\s*", "", t)  # leading A1.  / 03)
    t = t.replace("｜", "|")
    t = re.sub(r"\s*\|\s*.*$", "", t)                       # drop "| Label / Series" tail
    t = re.sub(r"\s+", " ", t).strip(" -–—|·•")
    return t


def tokens(s: str) -> set:
    return set(t for t in norm(s).split() if len(t) >= 2)


def _cov(a: set, b: set) -> float:
    return len(a & b) / len(a) if a else 0.0


def _side(q_side: set, cand_side: set) -> float:
    """How well one side (artist or title) matches — best of both directions,
    so trailing label/catalog junk on either side doesn't tank a real match."""
    if not q_side:
        return 0.0
    return max(_cov(q_side, cand_side), _cov(cand_side, q_side))


def match_score(query: str, artist: str, name: str) -> float:
    """Strict match: require BOTH the artist side and the title side to line up.
    Handles "Artist - Track" and the reversed "Track - Artist" by trying both
    orientations and taking the best. min() across sides kills title-only
    coincidences (wrong artist, same title)."""
    A, T = tokens(artist), tokens(name)
    parts = re.split(r"\s[-–—]\s", query, maxsplit=1)
    if len(parts) == 2:
        p1, p2 = tokens(parts[0]), tokens(parts[1])
        i1 = min(_side(p1, A), _side(p2, T))   # p1=artist, p2=title
        i2 = min(_side(p2, A), _side(p1, T))   # reversed
        return max(i1, i2)
    # no dash: only accept a strong full-string match against artist+title
    q = tokens(query)
    return _cov(q, A | T) if len(q) >= 3 else 0.0


# ── Spotify search → real track ──
_calls = 0
_rate_limited = False
_PACE = 1.5


def search_track(query: str):
    global _calls, _rate_limited
    if _rate_limited:
        return None
    try:
        r = sp.search(q=query, type="track", limit=5)
        _calls += 1
        time.sleep(_PACE)  # polite pacing for Dev Mode's shared quota
    except spotipy.SpotifyException as e:
        # In Dev Mode any 429 means the shared app quota is spent — stop the whole
        # run immediately (don't sleep-and-retry; the lockout is ~24h). The
        # Retry-After is often only in the message, not the headers.
        if e.http_status == 429 or "rate" in str(e).lower():
            print(f"  (rate limited — stopping run: {str(e)[:90]})")
            _rate_limited = True
            return None
        return None
    except Exception as e:
        if "rate" in str(e).lower() or "429" in str(e):
            print(f"  (rate limited — stopping run: {str(e)[:90]})")
            _rate_limited = True
        return None
    items = (r.get("tracks") or {}).get("items", [])
    return items or None


def extract(t: dict, query_tag: str, region: str) -> dict:
    artists = t.get("artists", [])
    album = t.get("album", {})
    rd = album.get("release_date", "") or ""
    year = rd[:4] if len(rd) >= 4 else ""
    decade = year[:3] + "0s" if year else ""
    return {
        "name": t.get("name", ""),
        "artist": ", ".join(a["name"] for a in artists),
        "artist_ids": [a["id"] for a in artists if a.get("id")],
        "id": t["id"],
        "album": album.get("name", ""),
        "popularity": t.get("popularity", 0),
        "source": "spotify",
        "region": region or "",
        "decade": decade,
        "year": year,
        "query": query_tag,
        "genres": [],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True)
    ap.add_argument("--handle", required=True)
    ap.add_argument("--limit", type=int, default=250)
    ap.add_argument("--call-budget", type=int, default=90, help="max Spotify searches this run (keep low — shared dev-mode quota)")
    ap.add_argument("--pace", type=float, default=1.5, help="seconds between Spotify searches")
    ap.add_argument("--min-score", type=float, default=0.6)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    global _PACE
    _PACE = args.pace

    cands = json.load(open(args.infile))
    state = {"done": []}
    if os.path.exists(STATE_PATH):
        try:
            state = json.load(open(STATE_PATH))
        except Exception:
            pass
    done = set(state.get("done", []))

    existing_ids = set(r["id"] for r in fetchall("SELECT id FROM tracks"))
    print(f"  pool has {len(existing_ids)} tracks | {len(cands)} candidates | {len(done)} already processed\n")

    to_insert = {}   # region -> [track dicts]   (dedup by id within run)
    seen_ids = set()
    matched = skipped_nomatch = skipped_trash = dup = 0
    processed = 0

    for c in cands:
        if _rate_limited or processed >= args.limit or _calls >= args.call_budget:
            break
        raw = c["raw"]
        if raw in done:
            continue
        q = clean_title(raw)
        if len(norm(q)) < 3:
            done.add(raw); continue
        processed += 1
        items = search_track(q)
        if _rate_limited:
            # quota hit mid-run — do NOT mark this one done; retry next run
            processed -= 1
            break
        done.add(raw)
        if not items:
            skipped_nomatch += 1
            continue
        best = max(items, key=lambda t: match_score(q, ", ".join(a["name"] for a in t.get("artists", [])), t.get("name", "")))
        sc = match_score(q, ", ".join(a["name"] for a in best.get("artists", [])), best.get("name", ""))
        if sc < args.min_score:
            skipped_nomatch += 1
            if args.dry_run:
                print(f"  ✗ {sc:.2f} {q[:50]!r} -> {best['artists'][0]['name']} - {best['name']}")
            continue
        tr = extract(best, f"curator:{args.handle}", c.get("region", ""))
        if tr["id"] in existing_ids or tr["id"] in seen_ids:
            dup += 1
            continue
        if is_trash(tr["name"], tr["artist"], tr["album"]):
            skipped_trash += 1
            continue
        seen_ids.add(tr["id"])
        to_insert.setdefault(tr["region"] or "Unknown", []).append(tr)
        matched += 1
        if args.dry_run and matched <= 40:
            print(f"  ✓ {sc:.2f} {q[:46]!r:48} -> {tr['artist'][:34]} - {tr['name'][:34]} ({tr['year'] or '?'})")

    total_new = sum(len(v) for v in to_insert.values())
    print(f"\n  processed={processed} calls={_calls} | matched={matched} nomatch={skipped_nomatch} trash={skipped_trash} dup={dup}")
    print(f"  NEW tracks to insert: {total_new}")

    if args.dry_run:
        print("\n  DRY RUN — nothing written.")
        return

    if to_insert:
        def _merge(disk):
            for region, rows in to_insert.items():
                ex = disk.get(region, [])
                ex_ids = set(t["id"] for t in ex)
                for t in rows:
                    if t["id"] not in ex_ids:
                        ex.append(t); ex_ids.add(t["id"])
                disk[region] = ex
        locked_update(_merge)
        for region, rows in to_insert.items():
            register_tracks(rows, region=region)
        print(f"  inserted {total_new} tracks across {len(to_insert)} region buckets")

    state["done"] = list(done)
    json.dump(state, open(STATE_PATH, "w"), ensure_ascii=False)
    print(f"  state saved ({len(done)} processed total)")


if __name__ == "__main__":
    main()
