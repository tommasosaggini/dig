#!/usr/bin/env python3
"""
Measure tracks from their audio, then throw the audio away.

The pool labeller reads "NewJeans — Ditto" and reaches for what k-pop usually
is; measured across 30k tracks that bias is stark. Labelling from the actual
recording fixes it — but keeping 42k downloads would be ~126 GB, so this
downloads one track, measures it, stores ~200 bytes of numbers, and deletes
the file before moving on. Disk stays flat regardless of pool size.

Analysis is deliberately separated from labelling: the download is the slow,
rate-limited part, and the LLM pass is cheap and batchable. Features persist in
`tracks.audio_features`, so labelling can run later, be re-run with a better
prompt, or use a different model, without re-downloading anything.

Resumable — it only looks at tracks with no features yet, so it can be killed
and restarted freely.

    python3 pipeline/audio_analyze.py --scope saves           # the IG candidate pool
    python3 pipeline/audio_analyze.py --scope all --limit 500 # chip away at the pool
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import audio_features
from lib.db import execute, fetchall

# Enough audio to characterise a track; a 90s slice measures essentially the
# same as the full song and downloads far faster.
CLIP_SECONDS = 90
BACKOFF_START = 20      # seconds, doubled on each consecutive failure
BACKOFF_MAX = 900


def ensure_schema():
    execute("ALTER TABLE tracks ADD COLUMN IF NOT EXISTS audio_features JSONB")
    execute("ALTER TABLE tracks ADD COLUMN IF NOT EXISTS audio_analyzed_at TIMESTAMPTZ")
    # Partial index: the worker's only query is "what still needs analysing?"
    execute("CREATE INDEX IF NOT EXISTS tracks_needs_audio_idx "
            "ON tracks (quality_score DESC NULLS LAST) "
            "WHERE audio_analyzed_at IS NULL")


def pick(scope, limit, admin):
    if scope == "saves":
        return fetchall("""
            SELECT DISTINCT t.id, t.name, t.artist
            FROM user_history h JOIN tracks t ON t.id = h.track_id
            WHERE h.user_id = %s AND h.status = 'saved'
              AND t.audio_analyzed_at IS NULL
            ORDER BY t.id LIMIT %s
        """, (admin, limit))
    # Whole pool, best tracks first — if this never finishes, the tracks most
    # likely to be surfaced are the ones that got done.
    return fetchall("""
        SELECT id, name, artist FROM tracks
        WHERE audio_analyzed_at IS NULL
        ORDER BY quality_score DESC NULLS LAST LIMIT %s
    """, (limit,))


def fetch_audio(track, dest_dir):
    """Download a short slice of the track. Returns a path, or None."""
    query = f"{track['artist']} {track['name']}"
    out = os.path.join(dest_dir, "a.%(ext)s")
    # Deliberately NOT --download-sections: it makes yt-dlp pull byte ranges
    # through ffmpeg, and YouTube 403s those (verified — every attempt failed).
    # Fetching the whole track at a low bitrate is slower per file but actually
    # works, and the file is deleted straight after analysis either way.
    # `python3 -m yt_dlp`, not the `yt-dlp` binary: the binary on PATH is an old
    # install under Python 3.9 whose YouTube extractor is stale enough to fail
    # every download with "The page needs to be reloaded". Same package, current
    # version, and it tracks whatever this interpreter has installed.
    cmd = [
        sys.executable, "-m", "yt_dlp", "-q", "--no-warnings", "--no-playlist",
        "--extract-audio", "--audio-format", "mp3", "--audio-quality", "7",
        "-o", out, f"ytsearch1:{query}",
    ]
    proc = subprocess.run(cmd, capture_output=True, timeout=300, text=True)
    if proc.returncode != 0:
        # Surface yt-dlp's own message. Echoing the command instead (the obvious
        # thing) hides the one line that says what actually went wrong.
        err = (proc.stderr or proc.stdout or "").strip().splitlines()
        raise RuntimeError(err[-1] if err else f"yt-dlp exited {proc.returncode}")
    for f in os.listdir(dest_dir):
        if f.endswith(".mp3"):
            return os.path.join(dest_dir, f)
    return None


# Failures that will repeat for every track in the queue. Detected by message
# because they surface from yt-dlp's own stderr, not as typed exceptions.
_ENV_FAULTS = ("ffmpeg not found", "ffprobe and ffmpeg not found",
               "postprocessing: ffprobe", "no such file or directory: 'ffmpeg'")


def _environment_fault(msg):
    low = msg.lower()
    return any(f in low for f in _ENV_FAULTS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", choices=("saves", "all"), default="saves")
    ap.add_argument("--limit", type=int, default=100000)
    args = ap.parse_args()

    # Fail before touching a single row rather than one track at a time.
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        sys.exit("FATAL: ffmpeg/ffprobe not on PATH. Under cron, PATH excludes "
                 "Homebrew — export PATH=/usr/local/bin:/opt/homebrew/bin:$PATH")

    ensure_schema()
    admin = os.environ.get("ADMIN_UID", "")
    tracks = pick(args.scope, args.limit, admin)
    if not tracks:
        print("nothing left to analyse.")
        return

    print(f"analysing {len(tracks)} track(s), scope={args.scope}", flush=True)
    done = failed = 0
    backoff = BACKOFF_START
    t0 = time.time()

    for i, t in enumerate(tracks, 1):
        tmp = tempfile.mkdtemp(prefix="digaudio-")
        try:
            path = fetch_audio(t, tmp)
            feats = audio_features.analyse(path) if path else None
            if not feats:
                raise RuntimeError("no audio / analysis failed")
            execute("UPDATE tracks SET audio_features = %s, "
                    "audio_analyzed_at = now() WHERE id = %s",
                    (json.dumps(feats), t["id"]))
            done += 1
            backoff = BACKOFF_START
            if done % 25 == 0 or i == len(tracks):
                rate = done / max(1e-9, (time.time() - t0)) * 3600
                print(f"  {i}/{len(tracks)}  ok={done} failed={failed}  "
                      f"~{rate:.0f}/hr", flush=True)
        except Exception as e:
            failed += 1
            msg = str(e)[:90]
            # An environment fault is not the track's fault. Marking it analysed
            # would burn it permanently — the row is never revisited — so a
            # missing ffmpeg under cron silently destroyed 148 tracks' worth of
            # queue before the backoff stalled it. Stop instead: every remaining
            # track would fail the same way.
            if _environment_fault(msg):
                print(f"  FATAL: {msg}", flush=True)
                print("    environment problem, not a track problem — stopping "
                      "so the rest of the queue is not burned.", flush=True)
                break
            # Mark it attempted so a permanently-unfindable track doesn't make
            # every future run retry it forever and stall the queue.
            execute("UPDATE tracks SET audio_analyzed_at = now() WHERE id = %s",
                    (t["id"],))
            print(f"  ! {t['name'][:32]}: {msg}", flush=True)
            # Consecutive failures usually mean rate limiting, not bad tracks.
            if failed and done == 0 or "429" in msg or "Sign in" in msg:
                print(f"    backing off {backoff}s", flush=True)
                time.sleep(backoff)
                backoff = min(BACKOFF_MAX, backoff * 2)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    print(f"done: {done} analysed, {failed} failed, "
          f"{time.time() - t0:.0f}s elapsed")


if __name__ == "__main__":
    main()
