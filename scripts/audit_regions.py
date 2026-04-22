#!/usr/bin/env python3
"""
DIG — Region-tag audit.

Samples N random tracks from the pool and asks Claude Sonnet whether the
assigned region is plausible given the artist + genres. Reports the mismatch
rate and the worst examples.

Usage:  python3 scripts/audit_regions.py [--n 500] [--batch 50]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Load .env
ENV_PATH = os.path.join(ROOT, ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from lib.db import fetchall  # noqa: E402

MODEL = "claude-sonnet-4-6"

_SYSTEM = """You are auditing the region tags on a music discovery pool.

For each row given, judge whether the assigned region is PLAUSIBLE for that artist. Use your training knowledge AND the genre tags as evidence.

A region tag is OK if:
  • It matches the artist's nationality / origin
  • It matches the regional scene the artist belongs to (e.g. a Bristol producer tagged "UK" is fine)
  • For multi-national groups, the primary scene is acceptable

A region tag is WRONG if:
  • The artist is clearly from a different macro region (e.g. UK rapper tagged "Central Africa")
  • **The genre tags are unambiguously tied to a specific region that contradicts the assignment.** This is the most important rule. Examples:
      - "harmonica blues", "delta blues", "chicago blues", "country", "americana", "bluegrass", "old-time", "appalachian", "cool jazz", "hard bop", "synthwave", "chillwave" → American/Western, NEVER Asian/African/Middle Eastern
      - "fado" → Portugal
      - "rebetiko", "laïkó" → Greece
      - "raï", "chaabi" → Algeria/North Africa
      - "gnawa" → Morocco
      - "mor lam", "luk thung" → Thailand/Laos
      - "qawwali", "ghazal", "sufi devotional" → Pakistan/India (South Asia)
      - "samba", "bossa nova", "MPB" → Brazil
      - "cumbia", "vallenato" → Colombia
      - "highlife", "afrobeats", "naija pop" → West Africa (Nigeria/Ghana)
      - "amapiano", "kwaito", "gqom", "sungura" → Southern Africa
      - "j-pop", "city pop", "kayōkyoku", "shibuya-kei" → Japan
      - "k-pop", "trot" → South Korea
      - "mandopop", "cantopop" → mainland China / Hong Kong / Taiwan
      - "shoegaze", "britpop", "grunge", "alt-country", "indie folk", "bedroom pop", "indie rock" → primarily Western (US/UK)
      - "klezmer" → Eastern European Jewish; "polka" → Central/Eastern Europe
    If you see one of these genre tags AND the region is on the wrong side of the world, mark WRONG even if you've never heard of the artist.
  • The artist's name is in a script that contradicts the region (e.g. Japanese kana with "USA" region) AND no genre signal supports the assignment

A region tag is UNSURE only if:
  • Genre tags are absent or generic (e.g. just "pop", "rock", "electronic")
  • You don't recognize the artist AND the genres don't pin a region
  • The artist could plausibly be from multiple regions AND genres don't help

Default to WRONG (with a region guess) when genre signals contradict region — this is more useful than UNSURE.

Output ONLY a JSON array, one object per row, in the same order:
[
  {"id": "<the id>", "verdict": "OK|WRONG|UNSURE", "actual_region_guess": "<your best guess of the right region or empty>"},
  ...
]"""


def fetch_sample(n: int) -> list[dict]:
    if n <= 0:
        # n=0 → return ALL tracks (full pool audit)
        rows = fetchall(
            "SELECT id, name, artist, region, genres "
            "FROM tracks "
            "WHERE region IS NOT NULL AND region != '' "
            "  AND (source != 'youtube' OR source IS NULL)"
        )
    else:
        rows = fetchall(
            "SELECT id, name, artist, region, genres "
            "FROM tracks "
            "WHERE region IS NOT NULL AND region != '' "
            "  AND (source != 'youtube' OR source IS NULL) "
            "ORDER BY random() LIMIT %s",
            (n,),
        )
    return rows


def audit_batch(batch: list[dict]) -> list[dict]:
    import anthropic
    client = anthropic.Anthropic()
    payload = [
        {
            "id": r["id"],
            "artist": (r["artist"] or "")[:80],
            "track": (r["name"] or "")[:60],
            "region": r["region"],
            "genres": (r["genres"] or [])[:4],
        }
        for r in batch
    ]
    msg = client.messages.create(
        model=MODEL,
        max_tokens=4000,
        system=[{"type": "text", "text": _SYSTEM, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content":
                   f"Audit these {len(payload)} tracks. Return ONLY the JSON array.\n\n"
                   f"{json.dumps(payload, ensure_ascii=False)}"}],
    )
    raw = (msg.content[0].text if msg.content else "").strip()
    # Strip code fences if any
    if raw.startswith("```"):
        nl = raw.find("\n")
        if nl >= 0:
            raw = raw[nl + 1:]
        if raw.endswith("```"):
            raw = raw[: -3].strip()
    # Locate first JSON array
    start = raw.find("[")
    if start >= 0:
        depth = 0
        in_str = False
        esc = False
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
                        try:
                            return json.loads(raw[start: i + 1])
                        except Exception:
                            return []
    return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=500, help="0 = full pool")
    parser.add_argument("--batch", type=int, default=50)
    parser.add_argument("--threads", type=int, default=1, help="parallel batches via ThreadPoolExecutor")
    parser.add_argument("--out", type=str, default="", help="write verdicts JSON to this path")
    args = parser.parse_args()

    sample = fetch_sample(args.n)
    print(f"Sampled {len(sample)} tracks from pool. Auditing in batches of {args.batch} "
          f"with {args.threads} thread(s)...", flush=True)

    by_id = {r["id"]: r for r in sample}
    batches = [sample[i: i + args.batch] for i in range(0, len(sample), args.batch)]
    total_batches = len(batches)
    verdicts = []

    if args.threads <= 1:
        for i, batch in enumerate(batches):
            result = audit_batch(batch)
            verdicts.extend(result)
            print(f"  batch {i+1}/{total_batches}: {len(result)} verdicts", flush=True)
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        done = 0
        with ThreadPoolExecutor(max_workers=args.threads) as ex:
            futures = {ex.submit(audit_batch, b): i for i, b in enumerate(batches)}
            for fut in as_completed(futures):
                i = futures[fut]
                try:
                    result = fut.result()
                except Exception as e:
                    print(f"  batch {i+1}/{total_batches}: ERROR {e}", flush=True)
                    continue
                verdicts.extend(result)
                done += 1
                print(f"  batch {i+1}/{total_batches} done ({done}/{total_batches} complete): "
                      f"{len(result)} verdicts", flush=True)

    # Tally
    counts = Counter(v.get("verdict", "?") for v in verdicts)
    total = len(verdicts)
    print()
    print("=== VERDICT TALLY ===")
    for verd, n in counts.most_common():
        print(f"  {verd:8} {n:4}  ({100 * n / max(total, 1):.1f}%)")

    # Mismatch breakdown by current region
    wrong_by_region = Counter()
    wrong_by_actual = Counter()
    examples_by_region = defaultdict(list)
    for v in verdicts:
        if v.get("verdict") != "WRONG":
            continue
        track = by_id.get(v.get("id"))
        if not track:
            continue
        wrong_by_region[track["region"]] += 1
        actual = (v.get("actual_region_guess") or "?").strip() or "?"
        wrong_by_actual[actual] += 1
        if len(examples_by_region[track["region"]]) < 4:
            examples_by_region[track["region"]].append(
                f"  {track['artist']} — {track['name']}  "
                f"(genres={track['genres']})  → guess: {actual}"
            )

    if wrong_by_region:
        print()
        print("=== TOP MISTAGGED REGIONS (which region tags are wrong most often) ===")
        for region, n in wrong_by_region.most_common(15):
            print(f"  {region:25} {n:3} wrong")
            for ex in examples_by_region[region]:
                print(ex)
            print()

    if wrong_by_actual:
        print("=== WHERE THEY ACTUALLY BELONG (Claude's best guess) ===")
        for actual, n in wrong_by_actual.most_common(15):
            print(f"  {actual:25} {n:3}")

    # Estimated total mistagging across whole pool
    if total:
        wrong_pct = 100 * counts.get("WRONG", 0) / total
        unsure_pct = 100 * counts.get("UNSURE", 0) / total
        print()
        print(f"Extrapolated to ~16k pool: ~{int(16000 * wrong_pct / 100)} confidently mistagged "
              f"+ ~{int(16000 * unsure_pct / 100)} uncertain")

    # Persist verdicts (with full track context) for the relabel script
    if args.out:
        out_payload = []
        for v in verdicts:
            tid = v.get("id")
            t = by_id.get(tid)
            if not t:
                continue
            out_payload.append({
                "id": tid,
                "artist": t["artist"],
                "track": t["name"],
                "current_region": t["region"],
                "genres": t["genres"] or [],
                "verdict": v.get("verdict"),
                "actual_region_guess": v.get("actual_region_guess", ""),
            })
        with open(args.out, "w") as f:
            json.dump(out_payload, f, ensure_ascii=False)
        print(f"\nWrote {len(out_payload)} verdicts to {args.out}")


if __name__ == "__main__":
    main()
