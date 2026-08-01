#!/usr/bin/env python3
"""
Render sample abstract covers from real liked tracks, deliberately picking
label combinations that are far apart — so the range is visible, not five
variations of the same picture.

    python3 scripts/ig_card_samples.py [-n 8] [-o media/ig_samples]
"""
import argparse
import io
import os
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

for line in open(os.path.join(ROOT, ".env")):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

import psycopg2
import psycopg2.extras
from PIL import Image

from lib import ig_artwork

_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"

# One row per visual family we want to see, chosen so the transforms differ.
WANTED = [
    ("deep bass",         "very high", "aggressive"),
    ("airy vocals",       "low",       "dreamy"),
    ("punchy drums",      "high",      "euphoric"),
    ("acoustic wood",     "very low",  "melancholic"),
    ("shimmering synths", "moderate",  "mysterious"),
    ("raw distorted",     "very high", "rebellious"),
    ("warm analog",       "moderate",  "nostalgic"),
    ("crisp digital",     "high",      "playful"),
]


def fetch_art(url):
    if not url:
        return None
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=25) as r:
            return Image.open(io.BytesIO(r.read())).convert("RGB")
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=8)
    ap.add_argument("-o", default=os.path.join(ROOT, "media", "ig_samples"))
    ap.add_argument("--size", type=int, default=1080)
    args = ap.parse_args()
    os.makedirs(args.o, exist_ok=True)

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # One track per texture family, and within each, the most extreme energy
    # available — the point is to show the spread, not the average.
    picked, seen = [], set()
    for texture, energy, mood in WANTED[: args.n]:
        cur.execute(
            """
            SELECT t.id, t.name, t.artist, t.art_url,
                   t.label_energy, t.label_mood, t.label_texture, t.label_feel
            FROM user_history h JOIN tracks t ON t.id = h.track_id
            WHERE h.status = 'saved' AND t.art_url IS NOT NULL
              AND t.label_texture ILIKE %s AND NOT (t.id = ANY(%s))
            ORDER BY (t.label_energy = %s) DESC, (t.label_mood = %s) DESC,
                     t.quality_score DESC NULLS LAST
            LIMIT 1
            """,
            ("%" + texture + "%", list(seen) or [""], energy, mood),
        )
        row = cur.fetchone()
        if row:
            seen.add(row["id"])
            picked.append(row)

    print(f"{len(picked)} tracks picked\n")
    for i, r in enumerate(picked, 1):
        labels = {
            "energy": r["label_energy"], "mood": r["label_mood"],
            "texture": r["label_texture"], "feel": r["label_feel"],
        }
        art = fetch_art(r["art_url"])
        img, used = ig_artwork.generate(art, (args.size, args.size), labels, r["id"])
        safe = "".join(ch if ch.isalnum() else "_" for ch in r["name"])[:34]
        dest = os.path.join(args.o, f"{i:02d}_{safe}.png")
        img.save(dest, "PNG")
        print(f"{i:02d}  {r['name'][:38]:38.38s} — {r['artist'][:22]:22.22s}")
        print(f"    {labels['energy']} · {labels['mood']} · {labels['texture']}")
        print(f"    feel={labels['feel']}  transforms={used}  → {dest}\n")


if __name__ == "__main__":
    main()
