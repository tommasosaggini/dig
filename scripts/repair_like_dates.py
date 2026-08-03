#!/usr/bin/env python3
"""
DIG — restore the real dates on likes that an import stamped with now().

THE DAMAGE
----------
scripts/import_likes.py wrote `listened_at = int(time.time() * 1000)` for every
Liked Song it inserted, discarding Spotify's `added_at`. On this account that
put 559 saves on 2026-04-21 — the day the import first ran — across just 14
distinct millisecond values, one per API page. The ledger says the listener
liked 559 songs in one day. They didn't; they liked them over years.

It only matters because DIG is meant to be a ledger of what its listener likes
and hears. A ledger with fabricated dates is worse than one with gaps, because
nothing about it looks wrong.

HOW A MACHINE STAMP IS IDENTIFIED
---------------------------------
Not by hardcoding the date this account happens to have. A human cannot start
twenty tracks in the same millisecond, so any `listened_at` value shared by
`--min-cluster` rows or more is an import artifact by construction. That rule
finds the damage on any account and, just as importantly, cannot mistake real
listening for it.

WHERE THE TRUE DATE COMES FROM
------------------------------
  1. Spotify Liked Songs — `added_at` per track. Authoritative.
  2. The user's DIG playlist — `added_at` per track, for saves DIG mirrored up
     rather than putting in Liked Songs (the default save destination).

A row no source can date is LEFT ALONE and counted in the report. Guessing
would just be the original bug with a different constant.

    python3 scripts/repair_like_dates.py --user <id>            # dry run
    python3 scripts/repair_like_dates.py --user <id> --apply
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()

from lib.db import get_conn, fetchall  # noqa: E402
from lib.spotify_sync import iso_to_ms  # noqa: E402


def machine_stamps(user_id, min_cluster):
    """listened_at values shared by >= min_cluster rows — i.e. written by a
    loop, not by a person listening."""
    rows = fetchall(
        """
        SELECT listened_at, COUNT(*) AS n
          FROM user_history
         WHERE user_id = %s AND listened_at IS NOT NULL
         GROUP BY listened_at
        HAVING COUNT(*) >= %s
         ORDER BY n DESC
        """,
        (user_id, min_cluster),
    )
    return [(r["listened_at"], r["n"]) for r in rows]


def true_dates(sp, user_id):
    """{track_id: added_at_ms} from Liked Songs, then the DIG playlist.

    Liked Songs wins where both have the track: that is where the listener
    themself pressed the heart, and the DIG playlist entry may be a later
    mirror of it.
    """
    dates = {}

    def _walk(fetch_page, label):
        offset, seen = 0, 0
        while True:
            resp = fetch_page(offset)
            items = (resp or {}).get("items") or []
            if not items:
                break
            for item in items:
                track = (item or {}).get("track") or {}
                tid = track.get("id")
                added = iso_to_ms((item or {}).get("added_at"))
                if tid and added and tid not in dates:
                    dates[tid] = added
            seen += len(items)
            offset += len(items)
            if len(items) < 50 or not resp.get("next"):
                break
        print(f"  {label}: {seen} tracks")

    _walk(lambda o: sp.current_user_saved_tracks(limit=50, offset=o), "Liked Songs")

    playlist_id = None
    row = fetchall("SELECT dig_playlist_id FROM users WHERE id = %s", (user_id,))
    if row and row[0].get("dig_playlist_id"):
        playlist_id = row[0]["dig_playlist_id"]
    if playlist_id:
        # Ask only for what we use. The default field set on playlist items is
        # enormous and this walk is already the most expensive part of the run.
        _walk(lambda o: sp.playlist_items(
            playlist_id, limit=50, offset=o,
            fields="next,items(added_at,track(id))"), "DIG playlist")
    else:
        print("  DIG playlist: none recorded for this user")
    return dates


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", required=True)
    ap.add_argument("--apply", action="store_true",
                    help="write the repairs (default is a dry run)")
    ap.add_argument("--min-cluster", type=int, default=20,
                    help="rows sharing one listened_at before it counts as a "
                         "machine stamp (default 20)")
    args = ap.parse_args()

    stamps = machine_stamps(args.user, args.min_cluster)
    if not stamps:
        print(f"No machine stamps found for {args.user} "
              f"(no listened_at shared by {args.min_cluster}+ rows). Nothing to repair.")
        return
    total = sum(n for _, n in stamps)
    print(f"Machine stamps: {len(stamps)} distinct values covering {total} rows")

    affected = fetchall(
        """
        SELECT track_id, track_name, artist, status, listened_at
          FROM user_history
         WHERE user_id = %s AND listened_at = ANY(%s)
        """,
        (args.user, [s for s, _ in stamps]),
    )

    from server import _user_token_or_refresh
    from lib.spotify_sync import make_user_client
    sp = make_user_client(_user_token_or_refresh(args.user))
    if sp is None:
        print("No usable Spotify token for this user — cannot date anything.")
        return
    print("Fetching true dates:")
    dates = true_dates(sp, args.user)

    repairs, unknown = [], []
    for row in affected:
        true_ms = dates.get(row["track_id"])
        if true_ms is None:
            unknown.append(row)
        elif true_ms != row["listened_at"]:
            repairs.append((true_ms, row))

    print(f"\n  datable and wrong : {len(repairs)}")
    print(f"  no source date    : {len(unknown)}  (left untouched)")
    for true_ms, row in repairs[:5]:
        print(f"    {row['artist'][:28]:30} {row['track_name'][:30]:32} "
              f"{row['listened_at']} -> {true_ms}")
    if len(repairs) > 5:
        print(f"    … and {len(repairs) - 5} more")

    if not args.apply:
        print("\nDry run. Re-run with --apply to write.")
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for true_ms, row in repairs:
                cur.execute(
                    "UPDATE user_history SET listened_at = %s "
                    " WHERE user_id = %s AND track_id = %s",
                    (true_ms, args.user, row["track_id"]),
                )
        conn.commit()
        print(f"\nRepaired {len(repairs)} rows.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
