#!/usr/bin/env python3
"""
DIG — Backfill decade/year on existing tracks.

Uses Claude Haiku to estimate release years for tracks that don't have them.
Runs once to fill the gap, then future discover.py runs capture year from
Spotify's album release_date automatically.
"""

import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR = ROOT
ENV_PATH = os.path.join(ROOT, ".env")

if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
if not ANTHROPIC_API_KEY:
    print("No ANTHROPIC_API_KEY — cannot backfill.")
    sys.exit(1)

import anthropic
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

print("\n📅 DIG — BACKFILLING DECADES\n")

out_path = os.path.join(DIR, "discovery.json")
with open(out_path) as f:
    discovery = json.load(f)

# Collect tracks missing decade/year
need_backfill = []
total = 0
for region, tracks in discovery.items():
    for i, t in enumerate(tracks):
        total += 1
        if not t.get("decade") and not t.get("year"):
            need_backfill.append((region, i, t))

already = total - len(need_backfill)
print(f"  Total tracks: {total}")
print(f"  Already have year: {already}")
print(f"  Need backfill: {len(need_backfill)}")

if not need_backfill:
    print("\n  All tracks have decade info.")
    sys.exit(0)

# Process in batches of 40
BATCH_SIZE = 40
filled = 0

for batch_start in range(0, len(need_backfill), BATCH_SIZE):
    batch = need_backfill[batch_start:batch_start + BATCH_SIZE]

    lines = []
    for _, _, t in batch:
        artist = t.get("artist", "?")
        name = t.get("name", "?")
        album = t.get("album", "")
        line = f"{t['id']} | {artist} — {name}"
        if album:
            line += f" [{album}]"
        lines.append(line)

    prompt = f"""For each track, estimate the release year (or decade if unsure).
Use your knowledge of the artist/song. If completely unknown, estimate from genre/style clues.

Return ONLY valid JSON: {{"track_id": "YYYY", ...}}
Use 4-digit years. If only decade known, use middle year (e.g. "1975" for 1970s).

Tracks:
{chr(10).join(lines)}"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            years = json.loads(text[start:end])
            batch_filled = 0
            for region, idx, track in batch:
                year = years.get(track["id"], "")
                if year and len(str(year)) == 4:
                    year = str(year)
                    discovery[region][idx]["year"] = year
                    discovery[region][idx]["decade"] = year[:3] + "0s"
                    batch_filled += 1
            filled += batch_filled
            batch_num = batch_start // BATCH_SIZE + 1
            total_batches = (len(need_backfill) + BATCH_SIZE - 1) // BATCH_SIZE
            print(f"  Batch {batch_num}/{total_batches}: {batch_filled}/{len(batch)} filled")
    except Exception as e:
        print(f"  (batch failed: {e})")

    # Checkpoint every 10 batches
    if (batch_start // BATCH_SIZE + 1) % 10 == 0:
        with open(out_path, "w") as f:
            json.dump(discovery, f)
        print(f"  (checkpoint saved)")

    time.sleep(0.3)

with open(out_path, "w") as f:
    json.dump(discovery, f)

print(f"\n✓ Done. {filled}/{len(need_backfill)} tracks now have decade info.")
