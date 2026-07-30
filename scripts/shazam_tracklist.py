#!/usr/bin/env python3
"""
DIG — identify the tracks in a DJ set / mixtape via Shazam.

Downloads the audio (yt-dlp) or takes a local file, slides a short window across
it, fingerprints each window with Shazam, then collapses consecutive identical
matches into a timestamped tracklist. Outputs Markdown + an .srt you can drop
onto the video.

Great for turning a 'gem' mix into diggable artist/track names. NOTE: a DJ's own
UNRELEASED edits/bootlegs won't be in Shazam's DB — those windows come back as
"— ID —" (Shazam may instead match the original the edit is built on).

Deps (local utility, not prod): pip install shazamio yt-dlp  (+ ffmpeg).

Usage:
  python scripts/shazam_tracklist.py "https://youtu.be/VIDEO" --step 30
  python scripts/shazam_tracklist.py mix.mp3 --start 0 --end 1800 --out mix
"""
import argparse
import asyncio
import os
import subprocess
import sys
import tempfile


def _download(url, dest_dir):
    out = os.path.join(dest_dir, "audio.%(ext)s")
    subprocess.run([sys.executable, "-m", "yt_dlp", "--no-warnings",
                    "-f", "bestaudio", "-x", "--audio-format", "mp3",
                    "-o", out, url], check=True)
    return os.path.join(dest_dir, "audio.mp3")


def _duration(path):
    out = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                          "format=duration", "-of",
                          "default=noprint_wrappers=1:nokey=1", path],
                         capture_output=True, text=True)
    return float(out.stdout.strip() or 0)


def _slice(src, start, window, dest):
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", str(start),
                    "-t", str(window), "-i", src, "-ar", "44100", "-ac", "1", dest],
                   check=True)


def _ts(sec):
    sec = int(sec)
    return f"{sec//60:02d}:{sec%60:02d}"


def _srt_ts(sec):
    h = int(sec // 3600); m = int((sec % 3600) // 60); s = int(sec % 60)
    return f"{h:02d}:{m:02d}:{s:02d},000"


async def _recognize(shazam, path):
    try:
        out = await shazam.recognize(path)
    except AttributeError:                       # older shazamio
        out = await shazam.recognize_song(path)
    tr = (out or {}).get("track")
    if not tr:
        return None
    return f"{tr.get('subtitle')} — {tr.get('title')}"


async def scan(audio, start, end, step, window, pause):
    from shazamio import Shazam
    shazam = Shazam()
    end = end or _duration(audio)
    points = []
    tmp = tempfile.mkdtemp()
    t = start
    while t < end:
        w = os.path.join(tmp, "w.mp3")
        _slice(audio, t, window, w)
        try:
            name = await _recognize(shazam, w)
        except Exception as e:
            name = None
            print(f"  {_ts(t)}  (err {type(e).__name__})", file=sys.stderr)
        points.append((t, name))
        print(f"  {_ts(t)}  {name or '— ID —'}")
        t += step
        await asyncio.sleep(pause)               # be gentle on Shazam's API
    return points


def collapse(points, end):
    """Merge consecutive windows with the same match into segments."""
    segs = []
    for t, name in points:
        if segs and segs[-1]["name"] == name:
            continue
        segs.append({"start": t, "name": name})
    for i, s in enumerate(segs):
        s["end"] = segs[i + 1]["start"] if i + 1 < len(segs) else end
    return segs


def write_outputs(segs, out_base, source):
    md = [f"# Tracklist — {source}", ""]
    for s in segs:
        label = s["name"] or "— ID — (unreleased edit / not in Shazam)"
        md.append(f"- **{_ts(s['start'])}** {label}")
    with open(out_base + ".md", "w") as f:
        f.write("\n".join(md) + "\n")

    with open(out_base + ".srt", "w") as f:
        for i, s in enumerate(segs, 1):
            label = s["name"] or "ID (unknown)"
            f.write(f"{i}\n{_srt_ts(s['start'])} --> {_srt_ts(s['end'])}\n{label}\n\n")
    print(f"\nwrote {out_base}.md and {out_base}.srt  ({len(segs)} segments)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="YouTube URL or local audio file")
    ap.add_argument("--step", type=int, default=30, help="seconds between probes")
    ap.add_argument("--window", type=int, default=12, help="probe length (s)")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--end", type=int, default=0, help="0 = whole file")
    ap.add_argument("--out", default="tracklist", help="output basename")
    args = ap.parse_args()

    workdir = tempfile.mkdtemp()
    if args.input.startswith("http"):
        print("downloading audio…")
        audio = _download(args.input, workdir)
    else:
        audio = args.input
    dur = _duration(audio)
    print(f"audio: {audio}  ({_ts(dur)})\nscanning…")

    points = asyncio.run(scan(audio, args.start, args.end, args.step,
                              args.window, pause=1.0))
    segs = collapse(points, args.end or dur)
    write_outputs(segs, args.out, args.input)


if __name__ == "__main__":
    main()
