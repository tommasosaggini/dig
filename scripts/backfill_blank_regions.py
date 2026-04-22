#!/usr/bin/env python3
"""
DIG — backfill blank regions on tracks.

Prioritizes tracks the user has saved (they matter most for the taste
profile), then falls through to any remaining blank-region track. Uses
gpt-5-mini to infer region from artist/track metadata.

One-shot script — run manually. Idempotent: only touches rows with blank
region.
"""

import os
import sys
import json
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.db import get_conn, fetchall

ENV_PATH = os.path.join(ROOT, ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

import openai

MODEL = os.environ.get("DIG_LABEL_MODEL", "gpt-5-mini")
client = openai.OpenAI(
    api_key=os.environ["OPENAI_API_KEY"],
    max_retries=1,
    timeout=90.0,
)

BATCH = 30
MAX_FAILURES = 20


def infer_regions(tracks):
    """Return {track_id: region} from gpt-5-mini. UNSURE → skipped."""
    lines = []
    for t in tracks:
        line = f"{t['id']} | {t['artist']} — {t['name']}"
        if t.get("album"):
            line += f" [{t['album']}]"
        lines.append(line)

    prompt = f"""You are a music metadata expert. For each track, identify the artist's region of origin (their actual nationality or music scene).

Use simple country or macro region names. Examples: "USA", "UK", "Brazil", "Japan", "South Korea", "France", "Germany", "Nordic", "West Africa", "Southern Africa", "Caribbean", "Mexico", "Italy", "Spain", "Thailand", "India", "Middle East", "Eastern Europe". Do not invent compound names.

If you don't know the artist well enough to be confident, return "UNSURE". Be conservative — only return a region when you're confident.

Return ONLY valid JSON in this format:
{{"track_id_1": "USA", "track_id_2": "UNSURE", ...}}

Tracks:
{chr(10).join(lines)}"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.choices[0].message.content or ""
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            result = json.loads(text[start:end])
            out = {}
            for tid, region in result.items():
                if isinstance(region, str):
                    r = region.strip()
                    if r and r.upper() not in ("UNSURE", "UNKNOWN", "?"):
                        out[tid] = r
            return out
    except json.JSONDecodeError as e:
        print(f"  (JSON parse error: {e})")
    except openai.RateLimitError:
        print("  (rate limited — waiting 30s)")
        time.sleep(30)
    except Exception as e:
        print(f"  ({MODEL} error: {e})")
    return None


def main():
    print("\n🌍 DIG — blank region backfill\n")

    # Prioritize saved tracks, then everything else
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT t.id, t.artist, t.name, t.album,
                       COALESCE(h.is_saved, FALSE) AS is_saved
                FROM tracks t
                LEFT JOIN (
                    SELECT DISTINCT track_id, TRUE AS is_saved
                    FROM user_history WHERE status = 'saved'
                ) h ON h.track_id = t.id
                WHERE (t.region IS NULL OR t.region = '')
                  AND t.artist IS NOT NULL AND t.name IS NOT NULL
                ORDER BY is_saved DESC, t.id
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    total = len(rows)
    saved_count = sum(1 for r in rows if r[4])
    print(f"  Blank-region tracks: {total}")
    print(f"  Of which saved by user: {saved_count}")
    if not rows:
        print("  Nothing to do.")
        return

    tracks = [{"id": r[0], "artist": r[1], "name": r[2], "album": r[3] or ""} for r in rows]

    updated = 0
    failures = 0
    for i in range(0, len(tracks), BATCH):
        if failures >= MAX_FAILURES:
            print(f"\n  Too many failures ({MAX_FAILURES}), stopping.")
            break

        chunk = tracks[i:i + BATCH]
        result = infer_regions(chunk)
        if result is None:
            failures += 1
            time.sleep(min(5 + failures * 2, 30))
            continue

        if not result:
            batch_num = i // BATCH + 1
            total_b = (len(tracks) + BATCH - 1) // BATCH
            print(f"  Batch {batch_num}/{total_b}: 0 confident (all UNSURE)")
            time.sleep(0.3)
            continue

        # Apply updates
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for tid, region in result.items():
                    cur.execute(
                        "UPDATE tracks SET region = %s WHERE id = %s AND (region IS NULL OR region = '')",
                        (region, tid),
                    )
            conn.commit()
        finally:
            conn.close()

        updated += len(result)
        batch_num = i // BATCH + 1
        total_b = (len(tracks) + BATCH - 1) // BATCH
        print(f"  Batch {batch_num}/{total_b}: {len(result)}/{len(chunk)} filled (total: {updated})")
        time.sleep(0.3)

    print(f"\n✓ Region backfill done. {updated}/{total} tracks updated.")


if __name__ == "__main__":
    main()
