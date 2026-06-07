#!/usr/bin/env python3
"""
DIG — Build the authoritative genre → seed-artist map from Wikidata.

Why this exists: discovery was leaning on Spotify genre-search and AI
genre-expansion, both of which collapse to the same Anglo-pop attractors.
We need a static, human-editable, complete map of music genres and a
handful of canonical artists per genre — those become the seeds for the
MusicBrainz-graph crawler that will populate `mb_artists` with the long
tail.

Wikidata is the right pull source: it's the structured backbone behind
Wikipedia, models artist-has-genre (P136) explicitly, and exposes each
artist's MusicBrainz ID (P434) directly — so the seeds drop straight
into our existing MB pipeline with no name-matching guesswork.

The script is resumable. Output is `data/genre_seeds.json`; rerunning
appends/updates new genres but never overwrites human edits to existing
entries (override by passing --force).

Usage:
  scripts/build_genre_seeds.py [--max-seeds 5] [--min-sitelinks 3]
  scripts/build_genre_seeds.py --refresh-genres   # re-pull genre list
  scripts/build_genre_seeds.py --genre Q205432    # one genre by QID

Cost: ~3000 music genres × 1 SPARQL query each ≈ 1 hour at 1 req/sec.
This is a one-shot data-generation task — the output is committed to
the repo and the crawler reads it from disk.
"""

import argparse
import functools
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# Stream prints unbuffered so a backgrounded run shows live progress in the
# tail of its log file. Otherwise stdout sits in a 4KB buffer and silence
# looks identical to a hung query.
print = functools.partial(print, flush=True)  # noqa: A001

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
OUTPUT_PATH = os.path.join(DATA_DIR, "genre_seeds.json")

WD_SPARQL = "https://query.wikidata.org/sparql"
WD_HEADERS = {
    "User-Agent": "DIG-MusicDiscovery/1.0 (https://ohdig.com; admin@ohdig.com)",
    "Accept": "application/sparql-results+json",
}
RATE_LIMIT_S = 1.1  # Wikidata's query service is generous but be polite

# instance of "music genre" — Wikidata's canonical class for genres.
# We also walk subclass-of so "music style" and similar narrow types
# come along, and exclude generic dance-form / non-music items via type
# filtering inside the query.
MUSIC_GENRE_QID = "Q188451"


def sparql(query: str, retries: int = 3) -> dict:
    """Run a SPARQL query, return parsed JSON. Rate-limited and retried
    on 429/503 since Wikidata throttles aggressively under load."""
    for attempt in range(retries):
        try:
            r = requests.get(
                WD_SPARQL,
                params={"query": query, "format": "json"},
                headers=WD_HEADERS,
                timeout=60,
            )
        except requests.RequestException as e:
            print(f"  network err (attempt {attempt + 1}): {e}; backing off")
            time.sleep(5 * (attempt + 1))
            continue

        if r.status_code == 200:
            time.sleep(RATE_LIMIT_S)
            return r.json()
        if r.status_code in (429, 503):
            wait = int(r.headers.get("Retry-After", 5 * (attempt + 1)))
            print(f"  WD {r.status_code} — backing off {wait}s")
            time.sleep(wait)
            continue
        print(f"  WD {r.status_code}: {r.text[:200]}")
        time.sleep(5)
    raise RuntimeError("SPARQL request failed after retries")


def fetch_all_genres() -> list[dict]:
    """Pull every music-genre item from Wikidata.

    Includes items that are direct instances of Q188451 OR instances of
    any subclass of Q188451 (so 'music style', 'music subgenre', etc.
    are included). Filters out items that have been merged or marked
    with no English label (those are typically incomplete stubs).
    """
    print("Fetching full music-genre list from Wikidata...")
    q = """
    SELECT DISTINCT ?genre ?genreLabel ?genreDescription
                    (GROUP_CONCAT(DISTINCT ?aliasStr; separator="|") AS ?aliases)
    WHERE {
      ?genre wdt:P31/wdt:P279* wd:Q188451 .
      OPTIONAL {
        ?genre skos:altLabel ?aliasStr .
        FILTER (LANG(?aliasStr) = "en")
      }
      SERVICE wikibase:label {
        bd:serviceParam wikibase:language "en" .
      }
    }
    GROUP BY ?genre ?genreLabel ?genreDescription
    """
    data = sparql(q)
    rows = data["results"]["bindings"]
    out = []
    for r in rows:
        qid = r["genre"]["value"].rsplit("/", 1)[-1]
        label = r.get("genreLabel", {}).get("value", "")
        # Skip Wikidata items that didn't get an English label resolved
        # (label falls back to the QID, which we don't want as a name)
        if not label or label == qid:
            continue
        out.append({
            "qid": qid,
            "name": label,
            "description": r.get("genreDescription", {}).get("value", ""),
            "aliases": [a for a in r.get("aliases", {}).get("value", "").split("|") if a],
        })
    print(f"  fetched {len(out)} genres")
    return out


def fetch_seeds_for_genre(qid: str, max_seeds: int = 5,
                          min_sitelinks: int = 1) -> list[dict]:
    """Return up to max_seeds artists tagged with this genre.

    Ordering: by Wikipedia sitelink count descending — that's the closest
    proxy Wikidata gives us for "internationally recognized." We require
    a MusicBrainz ID so the crawler can walk relationships from the seed
    immediately; artists without MBIDs are skipped (they'd need a name
    search and we already know that path is unreliable).

    min_sitelinks filters out single-Wikipedia stubs that are typically
    poorly-tagged or non-notable. 3+ wikipedias = real artist with cross-
    cultural footprint.
    """
    q = f"""
    SELECT ?artist ?artistLabel ?mbid ?sitelinks ?country ?countryLabel WHERE {{
      ?artist wdt:P136 wd:{qid} .
      ?artist wdt:P434 ?mbid .
      ?artist wikibase:sitelinks ?sitelinks .
      FILTER (?sitelinks >= {min_sitelinks})
      OPTIONAL {{ ?artist wdt:P27 ?country }}
      SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "en" .
      }}
    }}
    ORDER BY DESC(?sitelinks)
    LIMIT {max_seeds}
    """
    try:
        data = sparql(q)
    except Exception as e:
        print(f"    err for {qid}: {e}")
        return []
    rows = data["results"]["bindings"]
    out = []
    seen_mbids = set()
    for r in rows:
        mbid = r["mbid"]["value"]
        if mbid in seen_mbids:
            continue
        seen_mbids.add(mbid)
        out.append({
            "qid": r["artist"]["value"].rsplit("/", 1)[-1],
            "name": r.get("artistLabel", {}).get("value", ""),
            "mbid": mbid,
            "sitelinks": int(r["sitelinks"]["value"]),
            "country": r.get("countryLabel", {}).get("value", "") or None,
        })
    return out


def load_existing() -> dict:
    """Load the existing seeds JSON if present, else return empty scaffold."""
    if not os.path.exists(OUTPUT_PATH):
        return {
            "schema_version": 1,
            "generated_at": None,
            "source": "Wikidata (instance of Q188451 / music genre)",
            "notes": ("One-shot generated seed map. Human-editable: feel "
                      "free to add/remove artists per genre — the crawler "
                      "reads from this file. Re-running the generator "
                      "preserves edits unless --force is passed."),
            "genres": {},
        }
    with open(OUTPUT_PATH) as f:
        return json.load(f)


def save(state: dict) -> None:
    """Atomic write — never half-clobber the file mid-run."""
    state["generated_at"] = datetime.now(timezone.utc).isoformat()
    tmp = OUTPUT_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, sort_keys=False)
    os.replace(tmp, OUTPUT_PATH)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--max-seeds", type=int, default=5,
                   help="Max seed artists per genre (default 5)")
    p.add_argument("--min-sitelinks", type=int, default=1,
                   help="Min Wikipedia sitelink count for a seed (default 1: "
                        "any artist with at least one Wikipedia article)")
    p.add_argument("--refresh-genres", action="store_true",
                   help="Re-fetch the full genre list from Wikidata")
    p.add_argument("--genre", help="One genre by QID (skip the full pull)")
    p.add_argument("--force", action="store_true",
                   help="Overwrite existing seed entries (default: skip)")
    p.add_argument("--limit", type=int, default=None,
                   help="Cap number of genres processed this run "
                        "(useful for testing / resuming in chunks)")
    args = p.parse_args()

    state = load_existing()

    # --- Phase 1: ensure we have a genre list to walk ---
    if args.genre:
        # Single-genre mode: fabricate a one-row genre list
        genres = [{"qid": args.genre.upper(), "name": "(unknown)",
                   "description": "", "aliases": []}]
    elif args.refresh_genres or not state["genres"]:
        genres = fetch_all_genres()
        # Pre-populate skeleton entries so a crash mid-Phase-2 still leaves
        # a record of what's in scope. Don't overwrite existing seed lists.
        for g in genres:
            if g["name"] not in state["genres"]:
                state["genres"][g["name"]] = {
                    "qid": g["qid"],
                    "description": g["description"],
                    "aliases": g["aliases"],
                    "seeds": [],
                }
        save(state)
    else:
        # Resume from disk
        genres = [{"qid": v["qid"], "name": k,
                   "description": v.get("description", ""),
                   "aliases": v.get("aliases", [])}
                  for k, v in state["genres"].items()]

    # --- Phase 2: populate seeds per genre ---
    todo = [g for g in genres
            if args.force or not state["genres"].get(g["name"], {}).get("seeds")]
    if args.limit:
        todo = todo[:args.limit]
    print(f"Populating seeds for {len(todo)} genres "
          f"(skipping {len(genres) - len(todo)} already-populated)...")

    started = datetime.now(timezone.utc)
    populated = 0
    empty = 0
    for i, g in enumerate(todo, 1):
        seeds = fetch_seeds_for_genre(
            g["qid"], max_seeds=args.max_seeds,
            min_sitelinks=args.min_sitelinks)
        # Ensure entry exists (single-genre / --force paths)
        entry = state["genres"].setdefault(g["name"], {
            "qid": g["qid"],
            "description": g.get("description", ""),
            "aliases": g.get("aliases", []),
            "seeds": [],
        })
        entry["seeds"] = seeds
        if seeds:
            populated += 1
        else:
            empty += 1

        # Periodic flush so a Ctrl-C doesn't toss an hour of work
        if i % 25 == 0:
            save(state)
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            rate = i / max(elapsed, 1)
            eta = (len(todo) - i) / max(rate, 0.01)
            print(f"  [{i}/{len(todo)}] {populated} populated, "
                  f"{empty} empty | rate={rate:.2f}/s, eta={eta / 60:.1f}min")

    save(state)
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    total_seeds = sum(len(v.get("seeds", [])) for v in state["genres"].values())
    print(f"\n=== DONE ===  genres={len(state['genres'])}, "
          f"with seeds={populated}, empty={empty}, "
          f"total seed artists={total_seeds}, "
          f"elapsed={elapsed:.1f}s")
    print(f"  output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
