#!/usr/bin/env python3
"""
DIG — Genre-tag audit.

Sends batches of (artist, track, region, assigned genres) to Claude Sonnet
and asks: "are these genres plausible for this artist?" Flags hallucinations
like Kendrick Lamar tagged "mor lam" or Ennio Morricone tagged "mor lam".

For WRONG verdicts, Claude suggests correct genres. Results saved to JSON
for the fix script to apply.

Usage:  python3 scripts/audit_genres.py [--n 0] [--batch 50] [--threads 8] [--out audit_genres.json]
        --n 0 = full pool
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()

from lib.db import fetchall

MODEL = "claude-sonnet-4-6"

_SYSTEM = """You are auditing genre tags on a music discovery pool.

For each row, judge whether the assigned genres are PLAUSIBLE for that artist.

A genre tag is OK if:
  • It matches the artist's actual musical style
  • It's a reasonable sub-genre or parent genre of their actual style

A genre tag is WRONG if:
  • The genre is clearly from a completely different musical tradition than the artist
  • Examples of WRONG: Kendrick Lamar tagged "mor lam", Ennio Morricone tagged "mor lam",
    a Brazilian samba singer tagged "k-pop", a baroque composer tagged "grime"
  • Regional genres (mor lam, fado, gnawa, raï, gamelan, etc.) assigned to artists
    from the wrong region are almost always WRONG

A genre tag is UNSURE if:
  • You don't recognize the artist well enough to judge
  • The genre is vaguely plausible but not the best fit

When marking WRONG, suggest 1-3 correct genres for the artist.

Output ONLY a JSON array, one object per row:
[
  {"id": "<track id>", "verdict": "OK|WRONG|UNSURE", "correct_genres": ["genre1", "genre2"]},
  ...
]

For OK and UNSURE, leave correct_genres as an empty array."""


def audit_batch(batch):
    import anthropic
    client = anthropic.Anthropic()
    payload = [
        {
            "id": r["id"],
            "artist": (r["artist"] or "")[:80],
            "track": (r["name"] or "")[:60],
            "region": r.get("region") or r.get("origin_region") or "",
            "genres": (r["genres"] or [])[:4],
        }
        for r in batch
    ]
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=4000,
            system=[{"type": "text", "text": _SYSTEM, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content":
                       f"Audit these {len(payload)} tracks. Return ONLY the JSON array.\n\n"
                       f"{json.dumps(payload, ensure_ascii=False)}"}],
        )
        raw = (msg.content[0].text if msg.content else "").strip()
        if raw.startswith("```"):
            nl = raw.find("\n")
            if nl >= 0: raw = raw[nl + 1:]
            if raw.endswith("```"): raw = raw[:-3].rstrip()
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
                            try: return json.loads(raw[start:i+1])
                            except: return []
        return []
    except Exception as e:
        print(f"  (batch error: {e})")
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=0, help="0 = full pool")
    parser.add_argument("--batch", type=int, default=50)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--out", type=str, default="/opt/dig/audit_genres.json")
    args = parser.parse_args()

    # Only audit tracks that HAVE genres (no point auditing empty ones)
    if args.n <= 0:
        sql = ("SELECT id, name, artist, region, origin_region, genres FROM tracks "
               "WHERE genres IS NOT NULL AND array_length(genres, 1) > 0 "
               "AND (source != 'youtube' OR source IS NULL)")
    else:
        sql = ("SELECT id, name, artist, region, origin_region, genres FROM tracks "
               "WHERE genres IS NOT NULL AND array_length(genres, 1) > 0 "
               "AND (source != 'youtube' OR source IS NULL) "
               f"ORDER BY random() LIMIT {args.n}")

    sample = fetchall(sql)
    print(f"Auditing {len(sample)} tracks with genres. Batches of {args.batch}, {args.threads} threads...", flush=True)

    by_id = {r["id"]: r for r in sample}
    batches = [sample[i:i + args.batch] for i in range(0, len(sample), args.batch)]
    total_batches = len(batches)
    verdicts = []
    done = 0

    if args.threads <= 1:
        for i, batch in enumerate(batches):
            result = audit_batch(batch)
            verdicts.extend(result)
            done += 1
            print(f"  batch {i+1}/{total_batches}: {len(result)} verdicts", flush=True)
    else:
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
                print(f"  batch {i+1}/{total_batches} done ({done}/{total_batches}): {len(result)} verdicts", flush=True)

    # Tally
    total = len(verdicts)
    counts = Counter(v.get("verdict", "?") for v in verdicts)
    print(f"\n=== VERDICT TALLY ===")
    for verd, n in counts.most_common():
        print(f"  {verd:8} {n:5}  ({100*n/max(total,1):.1f}%)")

    # Show sample WRONG verdicts
    wrong = [v for v in verdicts if v.get("verdict") == "WRONG"]
    if wrong:
        print(f"\n=== SAMPLE WRONG ({len(wrong)} total) ===")
        for w in wrong[:20]:
            t = by_id.get(w.get("id"), {})
            print(f"  {(t.get('artist') or '')[:35]:35} old={t.get('genres',[])}  →  fix={w.get('correct_genres',[])}")

    # Save
    if args.out:
        out_payload = []
        for v in verdicts:
            tid = v.get("id")
            t = by_id.get(tid)
            if not t: continue
            out_payload.append({
                "id": tid,
                "artist": t["artist"],
                "track": t["name"],
                "current_genres": t["genres"] or [],
                "verdict": v.get("verdict"),
                "correct_genres": v.get("correct_genres", []),
            })
        with open(args.out, "w") as f:
            json.dump(out_payload, f, ensure_ascii=False)
        print(f"\nWrote {len(out_payload)} verdicts to {args.out}")


if __name__ == "__main__":
    main()
