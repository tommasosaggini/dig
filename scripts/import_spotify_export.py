#!/usr/bin/env python3
"""
DIG — import a Spotify privacy export into the listening ledger.

WHY THIS EXISTS
---------------
DIG is meant to be the ledger of what its listener heard, and the Web API
cannot supply the past. /me/player/recently-played returns FIFTY items and
nothing else: paging back with the `before` cursor returns an empty page
immediately (measured 2026-08-03 on this account — 50 items, ~2 days, then a
wall). There is no other endpoint. Everything before that window is only
obtainable from Spotify's own data export.

  Request it at: spotify.com → Account → Privacy settings
    * "Account data"               ~5 days   — last 12 months, NO track ids
    * "Extended streaming history" ~30 days  — everything, WITH track ids

Ask for the extended one. The other is supported here because a listener who
already requested it shouldn't have to wait another month, but it identifies
tracks only by name, which is a weaker join.

WHAT A "PLAY" BECOMES
---------------------
user_history is one row per track — the client's model since forever, and now
the table's key. An export has hundreds of plays per track, so plays collapse:
the row's date is the MOST RECENT play, and its status is 'listened' if ANY
play passed 30 seconds (Spotify's own threshold for counting a stream) and
'skipped' otherwise. The strongest evidence across all plays wins, which is
the only reading that doesn't let one accidental skip erase a hundred listens.

played_pct is left NULL. The export gives ms_played but not track duration, so
a percentage would have to be invented, and the completeness signal that
quality scoring reads is not a place to invent things.

    python3 scripts/import_spotify_export.py --user <id> --dir ~/Downloads/my_spotify_data
    python3 scripts/import_spotify_export.py --user <id> --dir <path> --apply
"""
import argparse
import datetime
import glob
import hashlib
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()

from lib.db import get_conn, fetchall  # noqa: E402
from lib.spotify_sync import iso_to_ms, upsert_history  # noqa: E402

# Spotify counts a play as a stream at 30s. Reusing their threshold rather than
# inventing one keeps DIG's 'listened' meaning the same thing as Spotify's.
STREAM_MS = 30_000


def parse_play(raw):
    """One export record → {ts_ms, track_id, name, artist, ms_played}, or None.

    Handles both export shapes. Pure, so every field mapping below is testable
    without a 400 MB download:

      extended : ts / ms_played / master_metadata_* / spotify_track_uri
      account  : endTime / msPlayed / artistName / trackName

    Podcast rows are dropped: they carry spotify_episode_uri and null track
    metadata, and a music ledger is not the place for them.
    """
    if not isinstance(raw, dict):
        return None

    # ── Extended streaming history ──
    if "ms_played" in raw or "ts" in raw:
        name = raw.get("master_metadata_track_name")
        artist = raw.get("master_metadata_album_artist_name")
        if not name or not artist:
            return None                      # podcast episode, or a corrupt row
        uri = raw.get("spotify_track_uri") or ""
        track_id = uri.rsplit(":", 1)[-1] if uri.startswith("spotify:track:") else None
        ts = iso_to_ms(raw.get("ts"))
        if ts is None:
            return None
        return {"ts_ms": ts, "track_id": track_id, "name": name,
                "artist": artist, "ms_played": int(raw.get("ms_played") or 0)}

    # ── Account data ("StreamingHistory_music_N.json") ──
    if "endTime" in raw:
        name = raw.get("trackName")
        artist = raw.get("artistName")
        if not name or not artist:
            return None
        # "2024-01-15 20:31" — minute precision, no timezone. Read as UTC; the
        # export has no offset to apply and guessing one would shift every
        # timestamp in the file by a whole number of hours.
        try:
            dt = datetime.datetime.strptime(raw["endTime"], "%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            return None
        ts = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
        return {"ts_ms": ts, "track_id": None, "name": name,
                "artist": artist, "ms_played": int(raw.get("msPlayed") or 0)}

    return None


def synthetic_id(artist, name):
    """A stable id for a play the export could not tie to a Spotify track.

    The row still belongs in the ledger — it records something that was heard —
    but it needs a key, because (user_id, track_id) is what makes the write a
    merge instead of a duplicate on every re-import. Prefixed so nothing
    mistakes it for a Spotify id and tries to fetch it.
    """
    digest = hashlib.sha1(f"{artist} - {name}".lower().encode("utf-8")).hexdigest()
    return f"ext:{digest[:20]}"


def collapse_plays(plays, resolve=None):
    """Many plays → one history row per track. Pure.

    `resolve` maps 'artist - track' (lowercased) to a known track id, for the
    account-data export that carries no ids.
    """
    resolve = resolve or {}
    by_id = {}
    for p in plays:
        track_id = p["track_id"]
        if not track_id:
            track_id = resolve.get(f"{p['artist']} - {p['name']}".lower())
        synthetic = not track_id
        if synthetic:
            track_id = synthetic_id(p["artist"], p["name"])
        row = by_id.get(track_id)
        heard = p["ms_played"] >= STREAM_MS
        if row is None:
            by_id[track_id] = {
                "id": track_id, "track": p["name"], "artist": p["artist"],
                "region": "", "status": "listened" if heard else "skipped",
                "listened_at": p["ts_ms"], "played_pct": None,
                "mode": "external",
                # Synthetic ids must not look like Spotify ids downstream —
                # the album-art prefetch would POST them to /v1/tracks.
                "source": "external" if synthetic else "spotify",
                "plays": 1,
            }
            continue
        row["plays"] += 1
        # Most recent play dates the row…
        if p["ts_ms"] > row["listened_at"]:
            row["listened_at"] = p["ts_ms"]
        # …but the strongest evidence across ALL plays sets the status, so one
        # accidental skip cannot erase a hundred real listens.
        if heard:
            row["status"] = "listened"
    return list(by_id.values())


def load_export(directory):
    """Every play in every export JSON under `directory`. Returns (plays, files)."""
    paths = sorted(glob.glob(os.path.join(directory, "**", "*.json"), recursive=True))
    plays, used = [], []
    for path in paths:
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            continue
        if not isinstance(data, list):
            continue                       # Playlist1.json, Userdata.json, …
        parsed = [p for p in (parse_play(r) for r in data) if p]
        if parsed:
            plays.extend(parsed)
            used.append((os.path.basename(path), len(parsed)))
    return plays, used


def known_track_ids():
    """{'artist - track': id} for everything DIG already knows, so an export
    without ids still lands on the SAME row as the pool's copy."""
    out = {}
    for r in fetchall("SELECT id, name, artist FROM tracks WHERE name IS NOT NULL"):
        if r.get("artist"):
            out[f"{r['artist']} - {r['name']}".lower()] = r["id"]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", required=True)
    ap.add_argument("--dir", required=True, help="unzipped Spotify export folder")
    ap.add_argument("--apply", action="store_true",
                    help="write the rows (default is a dry run)")
    args = ap.parse_args()

    if not os.path.isdir(args.dir):
        print(f"Not a directory: {args.dir}")
        sys.exit(1)

    plays, files = load_export(args.dir)
    if not plays:
        print(f"No streaming-history records found under {args.dir}.\n"
              "Expected files named Streaming_History_Audio_*.json (extended) "
              "or StreamingHistory_music_*.json (account data).")
        sys.exit(1)

    print(f"Files with plays ({len(files)}):")
    for name, n in files[:12]:
        print(f"   {n:8,}  {name}")
    if len(files) > 12:
        print(f"   … and {len(files) - 12} more")

    rows = collapse_plays(plays, resolve=known_track_ids())
    synthetic = [r for r in rows if r["source"] == "external"]
    heard = [r for r in rows if r["status"] == "listened"]
    span = (min(p["ts_ms"] for p in plays), max(p["ts_ms"] for p in plays))
    fmt = lambda ms: datetime.datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d")

    print(f"\n  plays            : {len(plays):,}")
    print(f"  distinct tracks  : {len(rows):,}")
    print(f"  heard (>=30s)    : {len(heard):,}")
    print(f"  skipped only     : {len(rows) - len(heard):,}")
    print(f"  unmatched to a Spotify id: {len(synthetic):,}"
          f"  (recorded under ext: ids)")
    print(f"  range            : {fmt(span[0])} .. {fmt(span[1])}")

    if not args.apply:
        print("\nDry run. Re-run with --apply to write.")
        return

    for r in rows:
        r.pop("plays", None)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            written = upsert_history(cur, args.user, rows)
        conn.commit()
        print(f"\nMerged {written:,} tracks into the ledger for {args.user}.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
