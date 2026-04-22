#!/usr/bin/env python3
"""
DIG — Apply genre corrections from audit_genres.py results.

Usage:  python3 scripts/fix_genres.py [--audit /path/to/audit_genres.json] [--apply]
"""
import argparse
import json
import os
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

ENV_PATH = os.path.join(ROOT, ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from lib.db import get_conn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit", default="/opt/dig/audit_genres.json")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with open(args.audit) as f:
        verdicts = json.load(f)
    print(f"Loaded {len(verdicts)} verdicts")

    corrections = []
    for v in verdicts:
        if v.get("verdict") != "WRONG":
            continue
        new_genres = v.get("correct_genres") or []
        if not new_genres:
            continue
        corrections.append({
            "id": v["id"],
            "artist": (v.get("artist") or "")[:40],
            "old": v.get("current_genres", [])[:3],
            "new": new_genres[:3],
        })

    print(f"  WRONG with corrections: {len(corrections)}")
    print()
    print("Sample (first 20):")
    for c in corrections[:20]:
        print(f"  {c['artist']:40} {c['old']}  →  {c['new']}")

    if not args.apply:
        print("\n--dry-run. Pass --apply to write.")
        return

    print(f"\nApplying {len(corrections)} corrections...")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for i, c in enumerate(corrections):
                cur.execute("UPDATE tracks SET genres = %s WHERE id = %s", (c["new"], c["id"]))
                if (i + 1) % 500 == 0:
                    print(f"  wrote {i+1}/{len(corrections)}")
        conn.commit()
        print(f"DONE — corrected {len(corrections)} tracks.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
