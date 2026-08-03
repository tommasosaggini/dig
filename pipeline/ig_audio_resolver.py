#!/usr/bin/env python3
"""
DIG → Instagram: audio resolver.

For every queue item in 'needs_audio', acquire full audio (Bandcamp → yt-dlp;
see lib/ig_audio) and advance it to 'needs_clip'. On total failure the item
stays in 'needs_audio' with an error, and the dashboard offers manual upload.

Usage:
    python3 pipeline/ig_audio_resolver.py
    python3 pipeline/ig_audio_resolver.py --id 12
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import ig_queue
from lib.ig_audio import resolve_audio, AudioResolveError


def _next_candidate(item):
    """Which upload to fetch: 0 normally, or the one after whatever we used.

    An item back in needs_audio that already carries an audio_source is a
    rejected source, not a fresh one — advance past it rather than downloading
    the same file the admin just threw away.
    """
    src = str(item.get("audio_source") or "")
    if not src.startswith("youtube"):
        return 0
    return int(src.split("#")[1]) if "#" in src else 1


def resolve_one(item):
    try:
        r = resolve_audio(item, skip=_next_candidate(item))
        ig_queue.set_audio(item["id"], r["source"], r["path"],
                           r["duration_ms"], r.get("artwork_url"))
        print(f"  audio #{item['id']} via {r['source']}: {item['track_name']} "
              f"({r['duration_ms']//1000}s)")
        return True
    except AudioResolveError as e:
        ig_queue.set_audio_failed(item["id"], str(e))
        print(f"  no audio #{item['id']} ({item['track_name']}): {e} → needs manual upload")
        return False
    except Exception as e:
        ig_queue.set_audio_failed(item["id"], repr(e))
        print(f"  ERROR #{item['id']}: {e!r}")
        return False


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int)
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()

    if args.id:
        item = ig_queue.get_item(args.id)
        items = [item] if item else []
    else:
        items = ig_queue.items_needing_audio(args.limit)

    if not items:
        print("nothing needs audio.")
        return
    ok = sum(resolve_one(it) for it in items)
    print(f"resolved {ok}/{len(items)}.")


if __name__ == "__main__":
    main()
