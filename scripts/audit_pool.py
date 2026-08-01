#!/usr/bin/env python3
"""
scripts/audit_pool.py

Independent audit of the discovery pool. Samples N random tracks, asks Claude
Haiku for ground-truth country / genre / decade / vibes (ignoring DB labels
entirely), then scores the pool on depth, diversity, range, and measures how
wrong the stored labels are.

Usage:
    python scripts/audit_pool.py [--n 150] [--out audit_report.json]
"""

import argparse
import json
import math
import os
import random
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()

from lib.db import fetchall


BATCH = 25
MODEL = "claude-haiku-4-5-20251001"


AUDIT_PROMPT = """You are auditing a music discovery pool. For each track below, return what you actually know — NOT what the DB says. Use your own knowledge of artist and track.

For each track, output a JSON object with:
  - "country": country of origin of the primary artist, as a plain name (e.g. "Japan", "Brazil", "United States", "Democratic Republic of the Congo", "Mongolia"). Use "Unknown" if you cannot identify with reasonable confidence.
  - "genre": the single most specific genre that best describes this track (e.g. "qawwali", "shoegaze", "rebetiko", "drill", "chamber pop"). Not a multi-word free-form tag. Use "Unknown" if unsure.
  - "genre_family": broader family (e.g. "religious", "indie rock", "folk", "hip-hop", "electronic"). Use "Unknown" if unsure.
  - "decade": release decade as e.g. "1970s", "2010s". Use "Unknown" if unsure.
  - "vibes": array of 3 concise single-word vibes (e.g. ["melancholic","acoustic","slow"]). Always provide 3.
  - "confidence": "high" | "medium" | "low" — how sure you are about country+genre+decade.

Return a JSON array with one object per track in the same order. Return ONLY the JSON array, no prose, no markdown fences.

Tracks:
"""


def haiku_audit(client, batch):
    """Send a batch of tracks to Haiku, return parsed audit list."""
    lines = []
    for i, t in enumerate(batch):
        line = f"{i+1}. {t['artist']} — {t['name']}"
        if t.get("album"):
            line += f"  [album: {t['album']}]"
        if t.get("year"):
            line += f"  ({t['year']})"
        lines.append(line)
    prompt = AUDIT_PROMPT + "\n".join(lines)

    resp = client.messages.create(
        model=MODEL,
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )
    text = resp.content[0].text
    start = text.find("[")
    end = text.rfind("]") + 1
    if start < 0 or end <= start:
        raise ValueError(f"No JSON array in response: {text[:200]}")
    parsed = json.loads(text[start:end])
    if len(parsed) != len(batch):
        print(f"  ! mismatch: asked about {len(batch)}, got {len(parsed)} — truncating/padding")
        while len(parsed) < len(batch):
            parsed.append({"country": "Unknown", "genre": "Unknown",
                           "genre_family": "Unknown", "decade": "Unknown",
                           "vibes": [], "confidence": "low"})
    return parsed[:len(batch)]


def gini(counts):
    """Concentration index [0, 1]. 0 = perfectly flat, ~1 = one thing dominates."""
    if not counts or sum(counts) == 0:
        return 0.0
    values = sorted(counts)
    n = len(values)
    cum = sum((i + 1) * v for i, v in enumerate(values))
    return (2 * cum) / (n * sum(values)) - (n + 1) / n


def entropy(counter):
    """Shannon entropy in bits; higher = more diverse."""
    total = sum(counter.values())
    if total == 0:
        return 0.0
    return -sum((c / total) * math.log2(c / total) for c in counter.values() if c > 0)


def pct(n, d):
    return f"{100 * n / d:.1f}%" if d else "n/a"


def main():
    parser = argparse.ArgumentParser(description="Audit the discovery pool")
    parser.add_argument("--n", type=int, default=150, help="Sample size")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    parser.add_argument("--out", default="audit_report.json", help="Write full report to this JSON file")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    print(f"🔎 DIG — POOL AUDIT (n={args.n})\n")

    # ── Sample ──────────────────────────────────────────────────────────
    sample = fetchall(
        """
        SELECT id, name, artist, album, region, origin_region, genres,
               decade, year, popularity, source, query,
               label_energy, label_mood, label_texture, label_feel, label_use_case
        FROM tracks
        TABLESAMPLE SYSTEM (5)
        ORDER BY random()
        LIMIT %s
        """,
        (args.n,),
    )
    if len(sample) < args.n:
        # TABLESAMPLE can under-fill small tables — fallback to plain random
        sample = fetchall(
            """
            SELECT id, name, artist, album, region, origin_region, genres,
                   decade, year, popularity, source, query,
                   label_energy, label_mood, label_texture, label_feel, label_use_case
            FROM tracks ORDER BY random() LIMIT %s
            """,
            (args.n,),
        )
    print(f"  Sampled {len(sample)} tracks from the pool\n")

    # ── Run Haiku audit in batches ──────────────────────────────────────
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    truths = []
    for i in range(0, len(sample), BATCH):
        batch = sample[i:i + BATCH]
        print(f"  Auditing batch {i // BATCH + 1}/{(len(sample) + BATCH - 1) // BATCH} …", end=" ", flush=True)
        try:
            parsed = haiku_audit(client, batch)
            truths.extend(parsed)
            print(f"{len(parsed)} results")
        except Exception as exc:
            print(f"failed: {exc}")
            truths.extend([{"country": "Unknown", "genre": "Unknown",
                            "genre_family": "Unknown", "decade": "Unknown",
                            "vibes": [], "confidence": "low"}] * len(batch))

    # Pair sample with Haiku truth
    items = []
    for db, tr in zip(sample, truths):
        items.append({"db": db, "truth": tr})

    # ── Distributions (from ground truth) ───────────────────────────────
    c_country = Counter(it["truth"].get("country", "Unknown") or "Unknown" for it in items)
    c_genre   = Counter(it["truth"].get("genre", "Unknown") or "Unknown" for it in items)
    c_family  = Counter(it["truth"].get("genre_family", "Unknown") or "Unknown" for it in items)
    c_decade  = Counter(it["truth"].get("decade", "Unknown") or "Unknown" for it in items)
    c_artist  = Counter((it["db"]["artist"] or "").lower() for it in items)
    c_conf    = Counter(it["truth"].get("confidence", "low") for it in items)
    c_vibes   = Counter()
    for it in items:
        for v in it["truth"].get("vibes", []) or []:
            c_vibes[str(v).lower().strip()] += 1

    # ── Mislabeling: stored vs actual ───────────────────────────────────
    # Country vs origin_region / region (very loose match — substring both ways)
    def loose_match(a, b):
        if not a or not b:
            return False
        a, b = a.lower(), b.lower()
        return a in b or b in a

    mism_origin = 0
    mism_region = 0
    has_origin = 0
    for it in items:
        true_country = (it["truth"].get("country") or "").strip()
        if not true_country or true_country.lower() == "unknown":
            continue
        if it["db"].get("origin_region"):
            has_origin += 1
            if not loose_match(it["db"]["origin_region"], true_country):
                mism_origin += 1
        if it["db"].get("region"):
            if not loose_match(it["db"]["region"], true_country):
                mism_region += 1

    # Decade vs stored decade
    mism_decade = 0
    have_decade = 0
    for it in items:
        td = (it["truth"].get("decade") or "").strip()
        if not td or td.lower() == "unknown":
            continue
        sd = (it["db"].get("decade") or "").strip()
        if sd:
            have_decade += 1
            if sd != td:
                mism_decade += 1

    # Genre vs stored genres array
    mism_genre = 0
    have_genre = 0
    for it in items:
        tg = (it["truth"].get("genre") or "").lower().strip()
        if not tg or tg == "unknown":
            continue
        sg = [str(g).lower() for g in (it["db"].get("genres") or [])]
        if sg:
            have_genre += 1
            if not any(tg in s or s in tg for s in sg):
                mism_genre += 1

    # ── Print report ────────────────────────────────────────────────────
    n = len(items)
    print("\n" + "=" * 72)
    print("POOL AUDIT REPORT")
    print("=" * 72)

    print(f"\nSample: {n} tracks")
    print(f"Confidence of Haiku assessments: high={c_conf['high']}  med={c_conf['medium']}  low={c_conf['low']}")

    print(f"\n── DEPTH ──")
    print(f"  Unique artists in sample: {len(c_artist)} / {n}  ({pct(len(c_artist), n)} unique)")
    top_artists = c_artist.most_common(5)
    for a, k in top_artists:
        if k > 1:
            print(f"    · {a}: {k} tracks")

    print(f"\n── DIVERSITY (from independent Haiku assessment) ──")
    print(f"  Unique countries of origin: {len(c_country)}")
    print(f"  Unique genres (specific):   {len(c_genre)}")
    print(f"  Unique genre families:      {len(c_family)}")
    print(f"  Unique decades:             {len(c_decade)}")
    print(f"  Unique vibe words:          {len(c_vibes)}")

    print(f"\n  Country entropy:   {entropy(c_country):.2f} bits  (gini={gini(list(c_country.values())):.2f})")
    print(f"  Genre entropy:     {entropy(c_genre):.2f} bits  (gini={gini(list(c_genre.values())):.2f})")
    print(f"  Decade entropy:    {entropy(c_decade):.2f} bits  (gini={gini(list(c_decade.values())):.2f})")

    print(f"\n── RANGE ──")
    print(f"  Countries top 5:  {c_country.most_common(5)}")
    print(f"  Genre families top 5: {c_family.most_common(5)}")
    print(f"  Decades:          {sorted(c_decade.items())}")
    print(f"  Vibes top 10:     {c_vibes.most_common(10)}")

    print(f"\n── STORED LABEL CORRECTNESS (stored vs Haiku ground truth) ──")
    if has_origin:
        print(f"  origin_region mismatch: {mism_origin}/{has_origin}  ({pct(mism_origin, has_origin)})")
    else:
        print("  origin_region: not yet backfilled on this sample")
    print(f"  region (discovery bucket) mismatch: {mism_region}/{n}  ({pct(mism_region, n)})")
    if have_decade:
        print(f"  decade mismatch: {mism_decade}/{have_decade}  ({pct(mism_decade, have_decade)})")
    if have_genre:
        print(f"  genre mismatch (stored genres[] vs truth): {mism_genre}/{have_genre}  ({pct(mism_genre, have_genre)})")

    # ── Bright-line concentration flags ─────────────────────────────────
    print(f"\n── CONCENTRATION FLAGS ──")
    for a, k in c_artist.most_common(10):
        if k >= 3:
            print(f"  ⚠ artist '{a}' accounts for {k}/{n} tracks ({pct(k, n)})")
    for country, k in c_country.most_common(5):
        if k / n > 0.15:
            print(f"  ⚠ country '{country}' is {pct(k, n)} of the sample")
    for genre_f, k in c_family.most_common(3):
        if k / n > 0.30:
            print(f"  ⚠ genre family '{genre_f}' is {pct(k, n)} of the sample")

    # Dump full report
    report = {
        "n": n,
        "confidence": dict(c_conf),
        "countries": c_country.most_common(40),
        "genres": c_genre.most_common(60),
        "genre_families": c_family.most_common(30),
        "decades": dict(c_decade),
        "artists_top": c_artist.most_common(20),
        "vibes_top": c_vibes.most_common(30),
        "mismatch": {
            "origin_region": {"n_mismatch": mism_origin, "n_checked": has_origin},
            "region": {"n_mismatch": mism_region, "n_checked": n},
            "decade": {"n_mismatch": mism_decade, "n_checked": have_decade},
            "genre":  {"n_mismatch": mism_genre, "n_checked": have_genre},
        },
        "items": [
            {
                "artist": it["db"]["artist"],
                "track": it["db"]["name"],
                "album": it["db"].get("album"),
                "db_region": it["db"].get("region"),
                "db_origin_region": it["db"].get("origin_region"),
                "db_decade": it["db"].get("decade"),
                "db_genres": list(it["db"].get("genres") or []),
                "truth": it["truth"],
            }
            for it in items
        ],
    }
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n  Full report written to {args.out}")


if __name__ == "__main__":
    main()
