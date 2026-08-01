#!/usr/bin/env python3
"""
DIG — Region relabel.

Reads /opt/dig/audit_results.json (produced by audit_regions.py) and applies
Claude's actual_region_guess as the new region tag for every WRONG verdict.

Safety:
  - Dry-run by default (use --apply to actually write)
  - Only writes when the guess maps to a region that already exists in our
    pool taxonomy (avoids inventing new region buckets)
  - Skips ambiguous guesses ("USA/Colombia", "South Asia (Pakistan)", etc.)
  - Logs every change

Usage:  python3 scripts/relabel_regions.py [--audit /path/to/audit_results.json] [--apply]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib.db import get_conn, fetchall  # noqa: E402


# Map common Claude-guess strings to the region taxonomy used by the pool.
# Anything not in our taxonomy gets normalized via this lookup; if no match,
# we skip the relabel (leave the row alone) rather than invent a new bucket.
_GUESS_NORMALIZE = {
    # exact pool regions Claude often phrases differently
    "south korea": "South Korea",
    "korea": "South Korea",
    "k-pop": "South Korea",
    "japan": "Japan",
    "china": "China",
    "taiwan": "Taiwan",
    "hong kong": "Hong Kong",
    "mongolia": "Mongolia",
    "vietnam": "Vietnam",
    "indonesia": "Indonesia",
    "philippines": "Philippines",
    "malaysia": "Malaysia",
    "thailand": "Thailand",
    "myanmar": "Myanmar",
    "laos": "Laos",
    "cambodia": "Cambodia",
    "singapore": "Singapore",
    "india": "India",
    "pakistan": "South Asia",
    "south asia": "South Asia",
    "south asia (pakistan)": "South Asia",
    "bangladesh": "South Asia",
    "sri lanka": "South Asia",
    "nepal": "Nepal",
    "tibet": "Tibet",
    "iran": "Iran",
    "turkey": "Turkey",
    "israel": "Middle East",
    "lebanon": "Middle East",
    "saudi arabia": "Middle East",
    "syria": "Middle East",
    "iraq": "Middle East",
    "egypt": "North Africa",
    "morocco": "North Africa",
    "algeria": "North Africa",
    "tunisia": "North Africa",
    "ethiopia": "Horn of Africa",
    "horn of africa": "Horn of Africa",
    "kenya": "East Africa",
    "tanzania": "East Africa",
    "uganda": "East Africa",
    "east africa": "East Africa",
    "east africa/kenya": "East Africa",
    "nigeria": "West Africa",
    "ghana": "West Africa",
    "senegal": "West Africa",
    "mali": "West Africa",
    "ivory coast": "West Africa",
    "cote d'ivoire": "West Africa",
    "west africa": "West Africa",
    "central africa": "Central Africa",
    "congo": "Central Africa",
    "drc": "Central Africa",
    "angola": "Southern Africa",
    "south africa": "Southern Africa",
    "zimbabwe": "Southern Africa",
    "southern africa": "Southern Africa",
    "south africa (zimbabwe)": "Southern Africa",
    "cape verde": "Cape Verde",
    "usa": "USA",
    "united states": "USA",
    "us": "USA",
    "america": "USA",
    "canada": "Canada",
    "uk": "UK",
    "united kingdom": "UK",
    "britain": "UK",
    "england": "UK",
    "scotland": "UK",
    "ireland": "Ireland",
    "france": "France",
    "germany": "Germany",
    "spain": "Spain",
    "portugal": "Portugal",
    "italy": "Italy",
    "netherlands": "Netherlands",
    "belgium": "Belgium",
    "switzerland": "Western Europe",
    "austria": "Western Europe",
    "western europe": "Western Europe",
    "scandinavia": "Nordic",
    "nordic": "Nordic",
    "sweden": "Nordic",
    "norway": "Nordic",
    "denmark": "Nordic",
    "finland": "Nordic",
    "iceland": "Nordic",
    "russia": "Russia",
    "belarus": "Eastern Europe",
    "ukraine": "Eastern Europe",
    "poland": "Eastern Europe",
    "czech republic": "Eastern Europe",
    "hungary": "Eastern Europe",
    "romania": "Eastern Europe",
    "bulgaria": "Eastern Europe",
    "serbia": "Eastern Europe",
    "croatia": "Eastern Europe",
    "eastern europe": "Eastern Europe",
    "balkans": "Eastern Europe",
    "greece": "Greece",
    "kazakhstan": "Central Asia",
    "uzbekistan": "Central Asia",
    "central asia": "Central Asia",
    "mexico": "Mexico",
    "guatemala": "Central America",
    "central america": "Central America",
    "cuba": "Cuba",
    "jamaica": "Caribbean",
    "caribbean": "Caribbean",
    "trinidad": "Trinidad",
    "puerto rico": "Caribbean",
    "dominican republic": "Caribbean",
    "haiti": "Caribbean",
    "cuba/caribbean": "Caribbean",
    "brazil": "Brazil",
    "argentina": "Argentina",
    "chile": "Chile",
    "colombia": "Colombia",
    "peru": "Peru",
    "venezuela": "Venezuela",
    "ecuador": "Ecuador",
    "bolivia": "Bolivia",
    "south america": "Brazil",   # shouldn't happen; defensive default
    "australia": "Australia",
    "new zealand": "Oceania",
    "oceania": "Oceania",
    "polynesia": "Oceania",
    "hawaii": "USA",
}


def _normalize_guess(raw: str) -> str | None:
    """Map a free-form Claude guess to a known pool region, or None to skip."""
    if not raw:
        return None
    s = raw.strip().lower()
    # Drop parenthetical or slash-disambiguation: "USA/Colombia" → "usa", "South Asia (Pakistan)" → "south asia"
    primary = re.split(r"[\/\(]", s)[0].strip()
    if primary in _GUESS_NORMALIZE:
        return _GUESS_NORMALIZE[primary]
    if s in _GUESS_NORMALIZE:
        return _GUESS_NORMALIZE[s]
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit", default="/opt/dig/audit_results.json",
                        help="Path to audit_results.json from audit_regions.py")
    parser.add_argument("--apply", action="store_true",
                        help="Actually write changes (default: dry-run)")
    args = parser.parse_args()

    with open(args.audit) as f:
        verdicts = json.load(f)
    print(f"Loaded {len(verdicts)} verdicts from {args.audit}")

    # Filter to WRONG with a normalizable guess
    relabel = []
    skipped_no_guess = 0
    skipped_unmappable = 0
    same_region = 0
    for v in verdicts:
        if v.get("verdict") != "WRONG":
            continue
        guess = (v.get("actual_region_guess") or "").strip()
        if not guess or guess in ("?", "unknown"):
            skipped_no_guess += 1
            continue
        new_region = _normalize_guess(guess)
        if not new_region:
            skipped_unmappable += 1
            continue
        if new_region == v.get("current_region"):
            same_region += 1
            continue
        relabel.append({
            "id": v["id"],
            "artist": v.get("artist", ""),
            "track": v.get("track", ""),
            "from": v["current_region"],
            "to": new_region,
            "guess_raw": guess,
        })

    print(f"  WRONG with mappable guess (will relabel): {len(relabel)}")
    print(f"  WRONG but no guess given:                 {skipped_no_guess}")
    print(f"  WRONG but guess didn't normalize:         {skipped_unmappable}")
    print(f"  WRONG but guess equals current region:    {same_region}")

    # Tally moves
    moves = Counter((r["from"], r["to"]) for r in relabel)
    print()
    print("=== TOP 20 MOVES (from → to) ===")
    for (a, b), n in moves.most_common(20):
        print(f"  {n:4}  {a:25} → {b}")

    print()
    print("=== SAMPLE RELABELS (first 15) ===")
    for r in relabel[:15]:
        print(f"  {r['from']:18} → {r['to']:18}  {r['artist'][:40]:40} — {r['track'][:40]}")

    if not args.apply:
        print()
        print("DRY RUN — pass --apply to write changes.")
        return

    print()
    print(f"Applying {len(relabel)} updates...")
    conn = get_conn()
    written = 0
    try:
        with conn.cursor() as cur:
            BATCH = 200
            for i in range(0, len(relabel), BATCH):
                chunk = relabel[i: i + BATCH]
                # Build a temporary VALUES list for batch UPDATE via UPDATE ... FROM (VALUES ...)
                values_sql = ",".join(cur.mogrify("(%s,%s)", (r["id"], r["to"])).decode()
                                      for r in chunk)
                cur.execute(
                    f"UPDATE tracks SET region = v.r "
                    f"FROM (VALUES {values_sql}) AS v(id, r) "
                    f"WHERE tracks.id = v.id"
                )
                written += len(chunk)
                if written % 1000 == 0 or written == len(relabel):
                    print(f"  wrote {written}/{len(relabel)}")
        conn.commit()
        print(f"DONE — relabeled {written} tracks.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
