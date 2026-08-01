#!/usr/bin/env python3
"""
DIG — Fix track genres by cross-referencing with the artists table.

The genre backfill (Haiku) hallucinated genres for many tracks — e.g.,
Kendrick Lamar tagged "mor lam". The artists table has more reliable genre
data (from Spotify's own classification). This script overwrites track
genres with the artist-table genres when available.

Usage:  python3 scripts/fix_genres_from_artists.py [--dry-run]
"""
import os
import sys
import argparse
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib.db import get_conn, fetchall


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Build artist_id → genres lookup from artists table
    # Only use artists that have genres AND a spotify_id (more likely to be real Spotify genres)
    print("Loading artist genres from artists table...")
    artist_rows = fetchall(
        "SELECT spotify_id, name, genres FROM artists "
        "WHERE spotify_id IS NOT NULL AND genres IS NOT NULL AND array_length(genres, 1) > 0"
    )
    artist_genres = {}
    for r in artist_rows:
        if r["spotify_id"] and r["genres"]:
            artist_genres[r["spotify_id"]] = r["genres"]
    print(f"  {len(artist_genres)} artists with genres available for cross-reference")

    # Find tracks whose primary artist has genres in the artists table
    print("Scanning tracks for genre corrections...")
    tracks = fetchall(
        "SELECT id, name, artist, artist_ids, genres FROM tracks "
        "WHERE artist_ids IS NOT NULL AND array_length(artist_ids, 1) > 0"
    )

    corrections = []
    no_match = 0
    already_correct = 0
    for t in tracks:
        # Find the first artist_id that has genres in our lookup
        new_genres = None
        for aid in (t["artist_ids"] or []):
            if aid in artist_genres:
                new_genres = artist_genres[aid]
                break
        if not new_genres:
            no_match += 1
            continue
        # Check if current genres differ
        current = t.get("genres") or []
        if set(current) == set(new_genres):
            already_correct += 1
            continue
        corrections.append({
            "id": t["id"],
            "artist": (t["artist"] or "")[:40],
            "old": current[:3],
            "new": new_genres[:3],
        })

    print(f"  Tracks scanned: {len(tracks)}")
    print(f"  No artist match: {no_match}")
    print(f"  Already correct: {already_correct}")
    print(f"  Need correction: {len(corrections)}")

    if corrections:
        print()
        print("Sample corrections (first 20):")
        for c in corrections[:20]:
            print(f"  {c['artist']:40} old={c['old']}  →  new={c['new']}")

    if args.dry_run:
        print("\n--dry-run — no changes written.")
        return

    if not corrections:
        print("Nothing to fix.")
        return

    print(f"\nApplying {len(corrections)} genre corrections...")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            BATCH = 200
            written = 0
            for i in range(0, len(corrections), BATCH):
                chunk = corrections[i:i + BATCH]
                for c in chunk:
                    cur.execute(
                        "UPDATE tracks SET genres = %s WHERE id = %s",
                        (c["new"], c["id"]),
                    )
                written += len(chunk)
                if written % 1000 == 0 or written == len(corrections):
                    print(f"  wrote {written}/{len(corrections)}")
        conn.commit()
        print(f"DONE — corrected genres on {len(corrections)} tracks.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
