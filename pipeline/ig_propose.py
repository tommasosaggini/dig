#!/usr/bin/env python3
"""
DIG → Instagram: candidate proposer.

Suggests tracks from the admin's Spotify Liked Songs into the queue (status
'suggested'), spread across genres and avoiding recently-posted artists. The
admin then approves or skips each in the dashboard — nothing publishes from
here. Re-runnable: never re-suggests a track that's already in the queue.

Usage:
    python3 pipeline/ig_propose.py            # top up to the target backlog
    python3 pipeline/ig_propose.py --n 5      # force-add 5 suggestions
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import ig_queue, ig_caption
from lib.db import fetchone

# Keep this many un-acted-on suggestions waiting, so there's always a choice.
TARGET_SUGGESTED = int(os.environ.get("IG_TARGET_SUGGESTED", "5"))


def _suggested_count():
    """How many posts are already waiting on Tommaso.

    Counts every unreviewed item, not just status='suggested'. The pipeline
    now advances suggestions automatically (resolve → clip → render) so that
    the dashboard shows finished posts rather than buttons — which means an
    item leaves 'suggested' within a minute or two. Counting only that status
    made the top-up believe the queue was empty on every pass, and with a
    2-minute cron that proposes 5 tracks an hour into thousands.
    """
    row = fetchone(
        "SELECT count(*) AS n FROM ig_post_queue "
        "WHERE status NOT IN ('published','skipped','publishing','scheduled')")
    return row["n"] if row else 0


def propose(n=None):
    admin = os.environ.get("ADMIN_UID", "")
    if not admin:
        print("ADMIN_UID not set — cannot read the admin's likes. Aborting.")
        return 0
    if n is None:
        deficit = max(0, TARGET_SUGGESTED - _suggested_count())
        n = deficit
    if n <= 0:
        print(f"already at target ({TARGET_SUGGESTED} suggested). Nothing to do.")
        return 0

    cands = ig_queue.pick_candidates(admin, n=n)
    if not cands:
        print("no eligible candidates (all liked tracks queued, or no likes imported).")
        return 0

    added = 0
    for t in cands:
        genres = list(t.get("genres") or [])
        labels = {"energy": t.get("label_energy"), "mood": t.get("label_mood"),
                  "texture": t.get("label_texture"), "feel": t.get("label_feel")}
        caption = ig_caption.template_caption(t["name"], t["artist"], genres, labels)
        new_id = ig_queue.add_item(
            track_id=t["id"], track_name=t["name"], artist=t["artist"],
            status="suggested", caption=caption)
        if new_id:
            added += 1
            print(f"  + suggested #{new_id}: {t['name']} — {t['artist']}"
                  f"  [{genres[0] if genres else 'unknown'}]")
    print(f"proposed {added} track(s).")
    return added


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, help="force-add exactly N (else top up to target)")
    args = ap.parse_args()
    propose(args.n)


if __name__ == "__main__":
    main()
