#!/usr/bin/env python3
"""
Re-label specific tracks with the current labelling prompt, and show the diff.

Built to check whether a prompt change actually improves labels before paying
to re-label a 40k-track pool. Prints old → new per field and only writes with
--write.

    python3 scripts/relabel_tracks.py --queued            # everything in the IG queue
    python3 scripts/relabel_tracks.py --id <track_id>...
    python3 scripts/relabel_tracks.py --queued --write
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib.db import fetchall, execute

FIELDS = ("energy", "mood", "texture", "feel", "use_case")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queued", action="store_true",
                    help="re-label every track currently in the IG queue")
    ap.add_argument("--id", action="append", default=[])
    ap.add_argument("--write", action="store_true", help="persist the new labels")
    args = ap.parse_args()

    if args.queued:
        rows = fetchall("""
            SELECT DISTINCT t.id, t.name, t.artist, t.album, t.region, t.genres,
                   t.label_energy, t.label_mood, t.label_texture,
                   t.label_feel, t.label_use_case
            FROM ig_post_queue q JOIN tracks t ON t.id = q.track_id
            WHERE q.status NOT IN ('skipped','published')
        """)
    elif args.id:
        rows = fetchall("""
            SELECT id, name, artist, album, region, genres, label_energy,
                   label_mood, label_texture, label_feel, label_use_case
            FROM tracks WHERE id = ANY(%s)
        """, (args.id,))
    else:
        ap.error("pass --queued or --id")

    if not rows:
        print("no tracks matched.")
        return

    from pipeline.label_discovery import label_batch
    batch = [{"id": r["id"], "name": r["name"], "artist": r["artist"],
              "album": r.get("album") or "", "region": r.get("region") or ""}
             for r in rows]
    out = label_batch(batch) or {}

    changed = 0
    for r in rows:
        new = out.get(r["id"])
        if not new:
            print(f"  (no label returned) {r['name']}")
            continue
        diffs = [(f, r.get(f"label_{f}"), new.get(f))
                 for f in FIELDS if (r.get(f"label_{f}") or "") != (new.get(f) or "")]
        head = f"{r['name'][:34]} — {r['artist'][:22]}"
        if not diffs:
            print(f"  =  {head}")
            continue
        changed += 1
        print(f"  ~  {head}   {', '.join(r.get('genres') or [])}")
        for f, old, nv in diffs:
            print(f"       {f:<9} {str(old):<32} → {nv}")
        if args.write:
            execute(
                "UPDATE tracks SET label_energy=%s, label_mood=%s, "
                "label_texture=%s, label_feel=%s, label_use_case=%s WHERE id=%s",
                tuple(new.get(f) or r.get(f"label_{f}") for f in FIELDS) + (r["id"],))

    print(f"\n{changed}/{len(rows)} changed." + ("" if args.write else "  (dry run — pass --write)"))


if __name__ == "__main__":
    main()
