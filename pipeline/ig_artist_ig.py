#!/usr/bin/env python3
"""
Put the song, the artist and — where one exists — the artist's Instagram on the
first line of every queued caption.

Two jobs that are really one: resolve the handle (lib/artist_ig.py, MusicBrainz
then Wikidata, cached including misses) and rewrite the opening line of the
caption to match (lib/ig_caption.ensure_headline). Only the first line is
touched, so a caption the admin has written or edited keeps its body.

Runs after propose in the cron, which is early enough that the dashboard shows
the finished caption while the post is still being reviewed. Published posts are
never rewritten: their captions are already on Instagram, and changing the row
would only make our records disagree with what people can see.

Rate limits are MusicBrainz's, ~1 request/sec, two per uncached artist. A 503
means "you are going too fast" and stops the batch rather than poisoning the
cache with misses that were really timeouts — the next tick picks up where this
one stopped.

    python3 pipeline/ig_artist_ig.py [--limit N] [--force] [--dry-run]
    python3 pipeline/ig_artist_ig.py --set-handle "La Lupe=lalupeoficial"
"""
import argparse
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import artist_ig, ig_caption
from lib.db import execute, fetchall
from lib.mb_resolve import MBRateLimited

# resolve_handle already sleeps MusicBrainz's 1/sec between its own two
# requests. This is the gap between artists, and it is only paid on a cache
# miss. A flat 1/sec tripped MB's limiter twice while backfilling 27 artists —
# the documented rate is an average, not a licence to sit exactly on it.
EXTRA_PACE_S = 1.0

# A 503 is "slow down", not "this artist has no Instagram", so it must never be
# written to the cache as a miss. Waiting it out costs a minute; giving up
# costs the rest of the batch, and on a nightly cron a batch that always dies
# two-thirds of the way through never finishes at all.
BACKOFF_S = [20, 45, 90]

_GAVE_UP = object()


def _resolve_with_backoff(artist, *, use_cache):
    for wait in [0] + BACKOFF_S:
        if wait:
            print(f"  …  musicbrainz asked us to back off — waiting {wait}s")
            time.sleep(wait)
        try:
            handle = artist_ig.resolve_handle(artist, use_cache=use_cache).get("handle")
            time.sleep(EXTRA_PACE_S)
            return handle
        except MBRateLimited:
            continue
    return _GAVE_UP


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--force", action="store_true",
                    help="re-resolve handles instead of trusting the cache")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the new first lines, write nothing")
    ap.add_argument("--set-handle", metavar="ARTIST=HANDLE", action="append",
                    default=[], help="record a handle by hand, then exit")
    args = ap.parse_args()

    artist_ig.ensure_artist_ig_schema()

    if args.set_handle:
        for pair in args.set_handle:
            name, _, handle = pair.partition("=")
            res = artist_ig.set_handle(name.strip(), handle.strip())
            print(f"  {name.strip()} → @{handle.strip() or '—'}  {res}")
        return

    # The genres and labels are only needed for a post that has no caption at
    # all — a manual add from the dashboard, which goes in with none. They come
    # along on the same query rather than a second one per row.
    rows = fetchall("""
        SELECT q.id, q.track_name, q.artist, q.caption, t.genres,
               t.label_energy, t.label_mood, t.label_texture, t.label_feel
        FROM ig_post_queue q
        LEFT JOIN tracks t ON t.id = q.track_id
        WHERE q.status NOT IN ('published', 'skipped')
        ORDER BY COALESCE(q.scheduled_at, 'infinity'::timestamptz), q.queue_order
        LIMIT %s
    """, (args.limit,))
    if not rows:
        print("  nothing queued.")
        return

    handles = {}        # artist → handle, so a repeated artist costs one lookup
    linked = changed = 0
    for r in rows:
        artist = r["artist"]
        if artist not in handles:
            got = _resolve_with_backoff(artist, use_cache=not args.force)
            if got is _GAVE_UP:
                print("  musicbrainz is still asking us to back off — stopping "
                      f"here, {len(handles)} artists resolved this run. The "
                      "next cron tick resumes from the cache.")
                break
            handles[artist] = got
        handle = handles[artist]
        if handle:
            linked += 1

        labels = {"energy": r["label_energy"], "mood": r["label_mood"],
                  "texture": r["label_texture"], "feel": r["label_feel"]}
        if (r["caption"] or "").strip():
            # Two retirements, then the headline. drop_generated_extras needs
            # this track's own labels: it removes the vibe line only when it
            # matches what we would have written for THIS track, so a sentence
            # Tommaso typed himself survives the sweep.
            new_caption = ig_caption.ensure_headline(
                ig_caption.drop_generated_extras(
                    ig_caption.drop_bio_line(r["caption"]), labels),
                r["track_name"], artist, handle)
        else:
            # A post added by hand from the dashboard arrives with no caption
            # at all — /admin/ig/add never writes one, unlike propose.
            new_caption = ig_caption.template_caption(
                r["track_name"], artist, list(r["genres"] or []), labels, handle)
        if new_caption == (r["caption"] or "").strip():
            continue
        changed += 1
        first = new_caption.split("\n")[0]
        if args.dry_run:
            print(f"  ·  #{r['id']} {first}")
            continue
        execute("UPDATE ig_post_queue SET caption = %s WHERE id = %s",
                (new_caption, r["id"]))
        print(f"  ✓  #{r['id']} {first}")

    verb = "would change" if args.dry_run else "rewritten"
    print(f"  {changed} caption(s) {verb}; {linked}/{len(rows)} carry an "
          f"@mention.")


if __name__ == "__main__":
    main()
