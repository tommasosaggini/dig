#!/usr/bin/env python3
"""
Pick a sensible default 30s window for queued items that don't have one.

A fixed offset lands on intros and outros. Instead, decode the track cheaply
and take the window with the highest sustained loudness — in practice that is
the chorus or the drop, which is what you'd want a stranger to hear first.

The dashboard's waveform scrubber still overrides it; this only decides where
the handle starts.

    python3 pipeline/ig_autoclip.py [--force]
"""
import argparse
import os
import subprocess
import sys

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

for line in open(os.path.join(ROOT, ".env")):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

from lib import ig_queue

SR = 8000          # plenty for a loudness envelope
HEAD_GUARD = 8.0   # never start in the first 8s — intros mislead the metric


def loudest_window(path, dur_ms, sr=SR):
    """Start offset (ms) of the loudest `dur_ms` window in `path`."""
    proc = subprocess.run(
        ["ffmpeg", "-v", "quiet", "-i", path, "-ac", "1", "-ar", str(sr),
         "-f", "s16le", "-"],
        capture_output=True, check=True)
    x = np.frombuffer(proc.stdout, dtype=np.int16).astype(np.float32) / 32768.0
    if x.size < sr * 5:
        return 0

    win = int(dur_ms / 1000.0 * sr)
    if x.size <= win:
        return 0

    # RMS via a cumulative sum of squares — one pass, no python loop.
    sq = np.concatenate([[0.0], np.cumsum(x.astype(np.float64) ** 2)])
    starts = np.arange(0, x.size - win, sr // 2)          # every 0.5s
    energy = (sq[starts + win] - sq[starts]) / win

    guard = int(HEAD_GUARD * sr)
    ok = starts >= guard
    if ok.any():
        starts, energy = starts[ok], energy[ok]

    # Prefer the loudest, but pull back slightly so the window opens just
    # *before* the peak rather than in the middle of it.
    best = int(starts[int(np.argmax(energy))])
    best = max(0, best - int(2.0 * sr))
    return int(best / sr * 1000)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true",
                    help="re-pick even where a window is already set")
    args = ap.parse_args()

    done = 0
    for item in ig_queue.list_queue():
        if item.get("clip_start_ms") is not None and not args.force:
            continue
        path = item.get("audio_path")
        if not path or not os.path.exists(path):
            continue
        dur = item.get("clip_duration_ms") or 30000
        try:
            start = loudest_window(path, dur)
        except Exception as e:
            print(f"  ! #{item['id']} {item['track_name'][:30]}: {e}")
            continue
        ig_queue.update_item(item["id"], clip_start_ms=start,
                             status="needs_clip")
        print(f"  #{item['id']:>2} {item['track_name'][:34]:34.34s} "
              f"→ {start // 60000}:{start // 1000 % 60:02d}")
        done += 1
    print(f"picked {done} window(s).")


if __name__ == "__main__":
    main()
