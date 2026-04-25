#!/usr/bin/env python3
"""Blacklist an artist (one-off stock act) and retroactively delete their tracks.

Usage:
  scripts/blacklist_artist.py "Artist Name" "reason"
  scripts/blacklist_artist.py --remove "artist key"
  scripts/blacklist_artist.py --list
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.db import fetchall, get_conn


def normalize(artist: str) -> str:
    """Primary artist = first comma-segment, lowercased, stripped."""
    return ((artist or "").split(",")[0] or "").strip().lower()


def add(artist: str, reason: str = "") -> None:
    key = normalize(artist)
    if not key:
        print("error: empty artist key after normalisation", file=sys.stderr)
        sys.exit(1)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO artist_blacklist "
                "(artist_key, artist_display, reason) VALUES (%s, %s, %s) "
                "ON CONFLICT (artist_key) DO UPDATE SET "
                "  artist_display = EXCLUDED.artist_display, "
                "  reason = EXCLUDED.reason",
                (key, artist, reason),
            )
            # Retroactive delete: any track whose primary artist matches
            cur.execute(
                "DELETE FROM tracks WHERE "
                "lower(split_part(coalesce(artist,''), ',', 1)) = %s",
                (key,))
            deleted = cur.rowcount
        conn.commit()
        print(f"blacklisted: {key!r} (display: {artist!r})")
        print(f"deleted {deleted} existing tracks")
    finally:
        conn.close()


def remove(artist: str) -> None:
    key = normalize(artist)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM artist_blacklist WHERE artist_key = %s",
                (key,))
            n = cur.rowcount
        conn.commit()
        print(f"removed {n} entry/entries for {key!r}")
    finally:
        conn.close()


def show() -> None:
    rows = fetchall(
        "SELECT artist_key, artist_display, reason, added_at, added_by "
        "FROM artist_blacklist ORDER BY added_at DESC")
    print(f"{len(rows)} blacklisted artists")
    for r in rows:
        date = r["added_at"].strftime("%Y-%m-%d") if r["added_at"] else "?"
        disp = r["artist_display"] or r["artist_key"]
        reason = r["reason"] or "—"
        print(f"  {date}  {disp[:30]:30}  ({r['added_by']})  {reason}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("artist", nargs="?")
    p.add_argument("reason", nargs="?", default="")
    p.add_argument("--remove", action="store_true")
    p.add_argument("--list", action="store_true")
    args = p.parse_args()

    if args.list:
        show()
        return
    if args.remove:
        if not args.artist:
            print("error: --remove needs an artist", file=sys.stderr)
            sys.exit(1)
        remove(args.artist)
        return
    if not args.artist:
        p.print_help()
        sys.exit(1)
    add(args.artist, args.reason)


if __name__ == "__main__":
    main()
