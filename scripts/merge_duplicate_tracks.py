#!/usr/bin/env python3
"""Merge same-source duplicate tracks — one row per (artist, name, source).

The pool holds ~871 groups where the SAME source carries the same song twice
(Spotify: the single and the album edition under different ids; Bandcamp: the
same song re-crawled from a different release). Each extra row is a slot the
picker can serve twice and a split in play-history accounting.

Deliberately NOT merged: cross-source pairs (spotify + bandcamp copies of one
song). Those are playability tiers, not drift — guests can only play the
Bandcamp copy, approved listeners get the Spotify one natively.

For each group:
  keeper = most-played row (any user's history), then richest metadata
           (labels/genres/year/art), then popularity, then stable id order;
  merge  = keeper inherits any field it is missing from the losers;
  remap  = user_history, ig_post_queue, mb_artists.ingested_track_id move
           their pointers to the keeper (history rows are precious — a heard
           track must stay heard, or the picker re-serves it);
  delete = the losers.

Re-duplication is prevented at write time by the same-source name-key guard
in lib/discovery_lock._upsert_track (added together with this script).

Usage:
    python3 scripts/merge_duplicate_tracks.py            # dry run
    python3 scripts/merge_duplicate_tracks.py --apply
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import psycopg2.extras                     # noqa: E402
from lib.db import get_conn                # noqa: E402

META_FIELDS = ("genres", "label_energy", "label_mood", "label_texture",
               "label_feel", "label_use_case", "year", "decade", "art_url",
               "origin_region")


def richness(row):
    score = 0
    if row["label_energy"]: score += 3
    score += min(len(row["genres"] or []), 5)
    if (row["year"] or ""): score += 2
    if (row["art_url"] or ""): score += 1
    if (row["origin_region"] or ""): score += 1
    return score


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT lower(artist || ' - ' || name) AS k, source,
               array_agg(id ORDER BY id) AS ids
        FROM tracks
        GROUP BY 1, 2 HAVING count(*) > 1
        ORDER BY 1
    """)
    groups = cur.fetchall()
    print(f"{len(groups)} same-source duplicate groups")

    cur.execute("""
        SELECT track_id, count(*) AS n FROM user_history
        WHERE track_id IS NOT NULL GROUP BY 1
    """)
    plays = {r["track_id"]: r["n"] for r in cur.fetchall()}

    merged = removed = remapped_hist = filled = 0
    for g in groups:
        cur.execute(
            "SELECT * FROM tracks WHERE id = ANY(%s)", (list(g["ids"]),))
        rows = cur.fetchall()
        rows.sort(key=lambda r: (-plays.get(r["id"], 0), -richness(r),
                                 -(r["popularity"] or 0), r["id"]))
        keeper, losers = rows[0], rows[1:]

        fills = {}
        for f in META_FIELDS:
            empty = (keeper[f] is None or keeper[f] == "" or
                     (isinstance(keeper[f], list) and not keeper[f]))
            if not empty:
                continue
            for lo in losers:
                v = lo[f]
                if v not in (None, "", []):
                    fills[f] = v
                    break

        loser_ids = [lo["id"] for lo in losers]
        if not args.apply:
            merged += 1
            removed += len(loser_ids)
            filled += len(fills)
            remapped_hist += sum(plays.get(i, 0) for i in loser_ids)
            if merged <= 8:
                print(f"  keep {keeper['id'][:22]:22s} (plays={plays.get(keeper['id'],0)}, "
                      f"rich={richness(keeper)}) drop {len(loser_ids)} "
                      f"fill {sorted(fills)}  | {g['k'][:44]}")
            continue

        if fills:
            sets = ", ".join(f"{f} = %s" for f in fills)
            cur.execute(f"UPDATE tracks SET {sets} WHERE id = %s",
                        (*fills.values(), keeper["id"]))
        # user_history is UNIQUE(user_id, track_id), so remapping collides
        # when a listener heard BOTH twins. Resolve before remapping:
        # a loser row's 'saved' upgrades the keeper row (a save must never be
        # lost), colliding loser rows are dropped, and losers colliding with
        # EACH OTHER (same user heard two losers) are deduped to one.
        cur.execute(
            """UPDATE user_history k SET status = 'saved'
               FROM user_history l
               WHERE l.track_id = ANY(%s) AND k.track_id = %s
                 AND k.user_id = l.user_id AND l.status = 'saved'
                 AND k.status IS DISTINCT FROM 'saved'""",
            (loser_ids, keeper["id"]))
        cur.execute(
            """DELETE FROM user_history l USING user_history k
               WHERE l.track_id = ANY(%s) AND k.track_id = %s
                 AND k.user_id = l.user_id""",
            (loser_ids, keeper["id"]))
        cur.execute(
            """DELETE FROM user_history a USING user_history b
               WHERE a.track_id = ANY(%s) AND b.track_id = ANY(%s)
                 AND a.user_id = b.user_id AND a.id > b.id""",
            (loser_ids, loser_ids))
        cur.execute("UPDATE user_history SET track_id = %s WHERE track_id = ANY(%s)",
                    (keeper["id"], loser_ids))
        remapped_hist += cur.rowcount
        # ig_post_queue has a partial UNIQUE on active track_ids. If both
        # twins are actively queued, the duplicate post is exactly what
        # should not happen — skip the losers' active rows, then remap.
        cur.execute(
            """UPDATE ig_post_queue a SET status = 'skipped'
               WHERE a.track_id = ANY(%s)
                 AND a.status NOT IN ('skipped','failed')
                 AND (EXISTS (SELECT 1 FROM ig_post_queue k
                              WHERE k.track_id = %s
                                AND k.status NOT IN ('skipped','failed'))
                      OR EXISTS (SELECT 1 FROM ig_post_queue b
                                 WHERE b.track_id = ANY(%s)
                                   AND b.status NOT IN ('skipped','failed')
                                   AND b.id < a.id))""",
            (loser_ids, keeper["id"], loser_ids))
        cur.execute("UPDATE ig_post_queue SET track_id = %s WHERE track_id = ANY(%s)",
                    (keeper["id"], loser_ids))
        cur.execute("UPDATE mb_artists SET ingested_track_id = %s "
                    "WHERE ingested_track_id = ANY(%s)",
                    (keeper["id"], loser_ids))
        cur.execute("DELETE FROM tracks WHERE id = ANY(%s)", (loser_ids,))
        merged += 1
        removed += len(loser_ids)
        filled += len(fills)

    if args.apply:
        conn.commit()
        print(f"APPLIED: {merged} groups merged, {removed} rows removed, "
              f"{remapped_hist} history rows remapped, {filled} fields filled")
    else:
        conn.rollback()
        print(f"DRY RUN: would merge {merged} groups, remove {removed} rows, "
              f"remap {remapped_hist} history rows, fill {filled} fields")
    conn.close()


if __name__ == "__main__":
    main()
