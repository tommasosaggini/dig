#!/usr/bin/env python3
"""Resolve artist ORIGIN for rows whose country claim isn't about the artist.

`scripts/backfill_origin_source.py` marks every row with where its country came
from. This script takes the rows marked `market` / `inherited` / `uploader` /
`unknown` and tries to find out where the artist is ACTUALLY from, cheapest and
most exact source first.

The ladder, with the hit rates measured on dig's own unresolved set (2026-08-27)
rather than assumed:

  1. mb_artists, exact Spotify-ID join   6,185 rows   free, instant, exact
  2. mb_artists, unambiguous name join   2,057 rows   free, instant
  3. Wikidata P1902 (Spotify artist ID)  17.7% of ids ~150 ids per SPARQL call
  4. Wikidata music-entity name match    23% of names namesake-guarded
  5. Bandcamp artist page (band_location) 58% of bc rows, 1 call each
  6. MusicBrainz /url?resource=spotify   ~27%, 1 req/s — the slow tail

Two sources were tested and REJECTED, and should not be re-added:

  * YouTube channel country — that is the UPLOADER. 'Sahel Sounds' reports US
    and reissues Nigerien music; 'Spinnin' Records' reports NL and releases the
    world. Adopting it would rebuild exactly the bug we are fixing.
  * Discogs — the artist object carries no structured country (only free-text
    `profile`), and a release's `country` is the PRESSING country: Discogs
    lists INFERNAL EXECRATOR, a Singaporean band, under US, Norway and Brazil.

Usage:
    python3 scripts/resolve_origin.py --stage offline
    python3 scripts/resolve_origin.py --stage wikidata --limit 4000
    python3 scripts/resolve_origin.py --stage bandcamp --limit 200
    python3 scripts/resolve_origin.py --stage musicbrainz --limit 300
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env                       # noqa: E402
load_env()
from lib.db import fetchall, get_conn              # noqa: E402
from lib.region_norm import ISO2, canonical_region  # noqa: E402

UNRESOLVED = ("(origin_source IS NULL OR origin_source IN "
              "('market','inherited','uploader','unknown'))")

WD_UA = {"User-Agent": "dig/1.0 region-provenance (tommaso@trustbuild.it)",
         "Accept": "application/sparql-results+json"}
MB_UA = {"User-Agent": "dig/1.0 region-provenance ( tommaso@trustbuild.it )"}


# ── writing ──────────────────────────────────────────────────────────────────

def _stamp(pairs):
    """pairs = [(track_id, country, source)] → write origin_region+source."""
    if not pairs:
        return 0
    import psycopg2.extras
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                UPDATE tracks SET origin_region = %s, origin_source = %s,
                       origin_checked_at = now() WHERE id = %s
            """, [(c, s, i) for i, c, s in pairs], page_size=1000)
        conn.commit()
    finally:
        conn.close()
    return len(pairs)


def _mark_checked(track_ids, stage=None):
    """Record that we looked and found nothing, so the next run moves on.

    `stage` is the rung of the ladder that asked. It is what makes "the next
    run moves on" true: one shared flag cannot answer six independent
    questions, and when it was the only bookkeeping, two of the three network
    stages ignored it and re-read the same rows every run for ever. See
    scripts/migrate_origin_stage_cursor.sql for the measurement.

    Passing no stage keeps the old behaviour — the timestamp only — which is
    right for callers that mean "this row is settled" rather than "this
    particular source had nothing".
    """
    if not track_ids:
        return
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if stage:
                cur.execute(
                    "UPDATE tracks SET origin_checked_at = now(), "
                    "  origin_stages_tried = "
                    "    array_append(coalesce(origin_stages_tried, '{}'), %s) "
                    "WHERE id = ANY(%s) "
                    "  AND NOT (%s = ANY(coalesce(origin_stages_tried, '{}')))",
                    (stage, list(track_ids), stage))
            else:
                cur.execute("UPDATE tracks SET origin_checked_at = now() "
                            "WHERE id = ANY(%s)", (list(track_ids),))
        conn.commit()
    finally:
        conn.close()


# Every network stage adds this to its WHERE, with its own name. A row leaves
# a stage's backlog when THAT stage has asked it, and no sooner — so a cheap
# 23%-yield rung can no longer strip the pool before the 58% one sees it.
NOT_YET_TRIED = ("NOT (%s = ANY(coalesce(origin_stages_tried, '{}')))")


# ── stage 1+2: our own mb_artists table, no network at all ───────────────────

def stage_offline():
    print("loading mb_artists…")
    mb = fetchall("SELECT lower(btrim(name)) k, country, spotify_id "
                  "FROM mb_artists WHERE country IS NOT NULL AND country <> ''")
    by_sp, by_name = {}, defaultdict(set)
    for r in mb:
        c = canonical_region(r["country"]) or r["country"]
        if r["spotify_id"]:
            by_sp[r["spotify_id"]] = c
        by_name[r["k"]].add(c)
    # A name that MusicBrainz files under two countries is a namesake pair, and
    # picking either one is a coin flip dressed up as a fact. Drop it.
    uniq = {k: next(iter(v)) for k, v in by_name.items() if len(v) == 1}
    print(f"  {len(by_sp)} spotify-id keys, {len(uniq)} unambiguous names")

    rows = fetchall(f"SELECT id, artist, artist_ids FROM tracks WHERE {UNRESOLVED}")
    print(f"  {len(rows)} unresolved rows")

    out = []
    for r in rows:
        hit = None
        for sid in (r["artist_ids"] or []):
            if sid in by_sp:
                hit = (r["id"], by_sp[sid], "mb_artists_spotify_id")
                break
        if not hit:
            prim = (r["artist"] or "").split(",")[0].strip().lower()
            if prim in uniq:
                hit = (r["id"], uniq[prim], "mb_artists_name")
        if hit:
            out.append(hit)
    by_src = defaultdict(int)
    for _, _, s in out:
        by_src[s] += 1
    print(f"  resolved {len(out)}: " + ", ".join(f"{k}={v}" for k, v in by_src.items()))
    return _stamp(out)


# ── stage 3+4: Wikidata ──────────────────────────────────────────────────────

def _sparql(query, tries=3):
    url = "https://query.wikidata.org/sparql?" + urllib.parse.urlencode(
        {"query": query, "format": "json"})
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers=WD_UA)
            return json.load(urllib.request.urlopen(req, timeout=120))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(30 * (attempt + 1))
                continue
            raise
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(5 * (attempt + 1))
    return None


# P495 country of origin > P740 formation location's country > P27 citizenship.
# That order matters: Drake is a Canadian artist with US citizenship, and only
# the first two are statements about the ACT.
_WD_SPOTIFY = """SELECT ?sid ?oiso ?fiso ?ciso WHERE {
  VALUES ?sid { %s }
  ?a wdt:P1902 ?sid .
  OPTIONAL { ?a wdt:P495 ?o . ?o wdt:P297 ?oiso }
  OPTIONAL { ?a wdt:P740/wdt:P17 ?f . ?f wdt:P297 ?fiso }
  OPTIONAL { ?a wdt:P27 ?c . ?c wdt:P297 ?ciso }
}"""

# Constrained to musicians and musical groups. Without the constraint a name
# match lands on a footballer as happily as on a band.
_WD_NAME = """SELECT ?nm ?oiso ?fiso ?ciso WHERE {
  VALUES ?nm { %s }
  ?a rdfs:label|skos:altLabel ?nm .
  { ?a wdt:P31 wd:Q5 ; wdt:P106 ?occ .
    VALUES ?occ { wd:Q177220 wd:Q639669 wd:Q36834 wd:Q855091 wd:Q130857 } }
  UNION { ?a wdt:P31/wdt:P279* wd:Q215380 }
  OPTIONAL { ?a wdt:P495 ?o . ?o wdt:P297 ?oiso }
  OPTIONAL { ?a wdt:P740/wdt:P17 ?f . ?f wdt:P297 ?fiso }
  OPTIONAL { ?a wdt:P27 ?c . ?c wdt:P297 ?ciso }
}"""


def _pick(binding):
    for k in ("oiso", "fiso", "ciso"):
        if k in binding:
            iso = binding[k]["value"].upper()
            if iso in ISO2:
                return ISO2[iso]
    return None


def _name_is_safe(name):
    """Namesake guard for the name lane.

    A one-word, short handle ('Pocket', 'Giga', 'EXPRESS') is exactly the shape
    that collides, and Wikidata having only one such entity is not evidence
    that OUR artist is that entity. Require either a multi-word name or a long
    single token.
    """
    n = (name or "").strip()
    if len(n) < 4:
        return False
    return " " in n or len(n) >= 9


def stage_wikidata(limit):
    rows = fetchall(f"""SELECT id, artist, artist_ids FROM tracks
        WHERE {UNRESOLVED} AND {NOT_YET_TRIED} LIMIT %s""", ("wikidata", limit))
    if not rows:
        print("nothing unchecked left for wikidata")
        return 0
    print(f"{len(rows)} rows to try")

    ids, names = set(), set()
    for r in rows:
        for s in (r["artist_ids"] or []):
            ids.add(s)
        p = (r["artist"] or "").split(",")[0].strip()
        if _name_is_safe(p):
            names.add(p)

    # ── by Spotify ID (exact) ──
    id_country = {}
    ids = sorted(ids)
    for i in range(0, len(ids), 150):
        chunk = ids[i:i + 150]
        d = _sparql(_WD_SPOTIFY % " ".join(f'"{x}"' for x in chunk))
        if not d:
            continue
        for b in d["results"]["bindings"]:
            c = _pick(b)
            if c:
                id_country.setdefault(b["sid"]["value"], c)
        time.sleep(1)
    print(f"  wikidata by spotify-id: {len(id_country)}/{len(ids)} ids")

    # ── by name (guarded) ──
    name_country = defaultdict(set)
    names = sorted(names)
    for i in range(0, len(names), 60):
        chunk = names[i:i + 60]
        vals = " ".join('"%s"@en' % x.replace("\\", "").replace('"', '\\"')
                        for x in chunk)
        try:
            d = _sparql(_WD_NAME % vals)
        except Exception as e:
            print(f"  ! name chunk: {type(e).__name__}")
            continue
        if not d:
            continue
        for b in d["results"]["bindings"]:
            c = _pick(b)
            if c:
                name_country[b["nm"]["value"]].add(c)
        time.sleep(1)
    # Two countries for one name is a namesake collision, not a dual passport
    # we can resolve — drop it rather than guess.
    uniq_name = {k: next(iter(v)) for k, v in name_country.items() if len(v) == 1}
    print(f"  wikidata by name: {len(uniq_name)} unambiguous "
          f"({len(name_country) - len(uniq_name)} namesake collisions dropped)")

    out, checked = [], []
    for r in rows:
        hit = None
        for s in (r["artist_ids"] or []):
            if s in id_country:
                hit = (r["id"], id_country[s], "wikidata_spotify_id")
                break
        if not hit:
            p = (r["artist"] or "").split(",")[0].strip()
            if p in uniq_name:
                hit = (r["id"], uniq_name[p], "wikidata_name")
        if hit:
            out.append(hit)
        else:
            checked.append(r["id"])
    _mark_checked(checked, "wikidata")
    print(f"  resolved {len(out)}, marked-checked {len(checked)}")
    return _stamp(out)


# ── stage 5: Bandcamp artist pages ───────────────────────────────────────────

def stage_bandcamp(limit):
    # NOTE the doubled %% in the LIKE below. psycopg2 does its own parameter
    # interpolation, so a literal % inside the SQL is read as the start of a
    # placeholder — 'bc:%' plus the LIMIT %s made two placeholders for one
    # argument and the whole stage died with `IndexError: tuple index out of
    # range` on every run.
    from lib import bandcamp
    rows = fetchall(f"""SELECT id, artist FROM tracks
        WHERE {UNRESOLVED} AND source='bandcamp'
          AND id LIKE 'bc:%%' AND {NOT_YET_TRIED} LIMIT %s""",
                    ("bandcamp", limit))
    print(f"{len(rows)} bandcamp rows to try")
    out, checked = [], []
    for r in rows:
        try:
            band_id, track_id = bandcamp.parse_id(r["id"])
            d = bandcamp.resolve_stream(band_id, track_id)
        except bandcamp.BandcampBlocked as e:
            print(f"  ! blocked — stopping: {e}")
            break
        except Exception:
            checked.append(r["id"])
            time.sleep(1.5)
            continue
        loc = (d or {}).get("location") or ""
        c = canonical_region(bandcamp.location_to_country(loc)) if loc else ""
        if c:
            out.append((r["id"], c, "bandcamp_page"))
        else:
            checked.append(r["id"])
        time.sleep(1.5)
    _mark_checked(checked, "bandcamp")
    print(f"  resolved {len(out)}, checked-no-answer {len(checked)}")
    return _stamp(out)


# ── stage 6: MusicBrainz URL relationship (the slow, exact tail) ─────────────

def stage_musicbrainz(limit):
    rows = fetchall(f"""SELECT id, artist, artist_ids FROM tracks
        WHERE {UNRESOLVED} AND artist_ids IS NOT NULL
          AND array_length(artist_ids,1) > 0
          AND {NOT_YET_TRIED} LIMIT %s""", ("musicbrainz", limit))
    print(f"{len(rows)} rows to try via MB /url")
    seen, out, checked = {}, [], []
    for r in rows:
        sid = (r["artist_ids"] or [None])[0]
        if not sid:
            continue
        if sid in seen:
            if seen[sid]:
                out.append((r["id"], seen[sid], "musicbrainz"))
            continue
        u = "https://musicbrainz.org/ws/2/url?" + urllib.parse.urlencode({
            "resource": f"https://open.spotify.com/artist/{sid}",
            "inc": "artist-rels", "fmt": "json"})
        country = None
        try:
            d = json.load(urllib.request.urlopen(
                urllib.request.Request(u, headers=MB_UA), timeout=25))
            for rel in (d.get("relations") or []):
                a = rel.get("artist") or {}
                if a.get("country"):
                    country = canonical_region(a["country"])
                    break
        except urllib.error.HTTPError as e:
            if e.code in (503, 429):
                print("  ! MB pushing back — stopping run cleanly")
                break
        except Exception:
            pass
        seen[sid] = country
        if country:
            out.append((r["id"], country, "musicbrainz"))
        else:
            checked.append(r["id"])
        time.sleep(1.1)
    _mark_checked(checked, "musicbrainz")
    print(f"  resolved {len(out)}")
    return _stamp(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True,
                    choices=["offline", "wikidata", "bandcamp", "musicbrainz"])
    ap.add_argument("--limit", type=int, default=2000)
    args = ap.parse_args()

    n = {"offline": lambda: stage_offline(),
         "wikidata": lambda: stage_wikidata(args.limit),
         "bandcamp": lambda: stage_bandcamp(args.limit),
         "musicbrainz": lambda: stage_musicbrainz(args.limit)}[args.stage]()

    row = fetchall("""SELECT count(*) tot,
        count(*) FILTER (WHERE origin_source IN
          ('musicbrainz','bandcamp_location','mb_artists_spotify_id',
           'wikidata_spotify_id','bandcamp_page','mb_artists_name',
           'wikidata_name')) trusted FROM tracks""")[0]
    print(f"\nwrote {n} | pool trusted origin: "
          f"{row['trusted']}/{row['tot']} = {row['trusted']/row['tot']:.1%}")


if __name__ == "__main__":
    main()
