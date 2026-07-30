"""
DIG → Instagram: acquire FULL audio for a queued track.

Order of preference:
  1. Bandcamp  — when the pool id is already a 'bc:<band>:<track>' (cleanest,
     artist-sanctioned, full MP3-128). Reuses lib/bandcamp.resolve_stream.
  2. yt-dlp    — universal fallback (YouTube etc.). Searches 'artist name',
     downloads best audio, transcodes to MP3 via ffmpeg.
  3. manual    — if both fail, the item stays in needs_audio and the dashboard
     prompts the admin to upload a file (handled in the server, not here).

Bandcamp ToS allows full-track streaming; yt-dlp + re-publishing carries the
small-account copyright risk accepted per-track at the approval step. SoundCloud
is intentionally NOT used here — its API ToS forbids downloading/storing audio.

Requires `yt-dlp` (pip) and `ffmpeg` (system) for the fallback path.
"""
import os
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.ig_queue import item_dir

_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"


class AudioResolveError(Exception):
    pass


def probe_duration_ms(path):
    """Best-effort audio duration via ffprobe. Returns 0 if unavailable — the
    dashboard derives duration client-side from the decoded audio anyway."""
    from shutil import which
    if not which("ffprobe") or not os.path.exists(path):
        return 0
    import subprocess
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=30)
        return int(float(out.stdout.strip()) * 1000)
    except Exception:
        return 0


def resolve_audio(item):
    """Download full audio for a queue item (dict). Returns
    {source, path, duration_ms, artwork_url}. Raises AudioResolveError on total
    failure (caller leaves the item in needs_audio for manual upload)."""
    out_dir = item_dir(item["id"])
    os.makedirs(out_dir, exist_ok=True)
    track_id = item.get("track_id") or ""

    if track_id.startswith("bc:"):
        try:
            return _from_bandcamp(track_id, out_dir)
        except Exception as e:
            print(f"  [ig_audio] bandcamp failed for {track_id}: {e!r}; trying yt-dlp")

    query = f'{item.get("artist", "")} {item.get("track_name", "")}'.strip()
    if not query:
        raise AudioResolveError("no query (missing artist/title)")
    return _from_ytdlp(query, out_dir)


def _from_bandcamp(track_id, out_dir):
    from lib import bandcamp
    band, tid = bandcamp.parse_id(track_id)
    if not band:
        raise AudioResolveError("bad bandcamp id")
    r = bandcamp.resolve_stream(band, tid)
    if not r.get("ok"):
        raise AudioResolveError(f"bandcamp resolve: {r.get('error')}")
    dest = os.path.join(out_dir, "source.mp3")
    req = urllib.request.Request(r["url"], headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=60) as resp, open(dest, "wb") as f:
        f.write(resp.read())
    return {
        "source": "bandcamp",
        "path": dest,
        "duration_ms": int((r.get("duration") or 0) * 1000),
        "artwork_url": r.get("art") or None,
    }


def _from_ytdlp(query, out_dir):
    try:
        import yt_dlp
    except ImportError:
        raise AudioResolveError("yt-dlp not installed (pip install yt-dlp)")

    out_tmpl = os.path.join(out_dir, "source.%(ext)s")
    opts = {
        "format": "bestaudio/best",
        "outtmpl": out_tmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "default_search": "ytsearch1",
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(query, download=True)
    except Exception as e:
        raise AudioResolveError(f"yt-dlp: {e}")
    # ytsearch returns a playlist-like dict; take the first entry.
    if info.get("entries"):
        info = info["entries"][0]
    dest = os.path.join(out_dir, "source.mp3")
    if not os.path.exists(dest):
        raise AudioResolveError("yt-dlp produced no mp3 (ffmpeg missing?)")
    return {
        "source": "youtube",
        "path": dest,
        "duration_ms": int((info.get("duration") or 0) * 1000),
        "artwork_url": info.get("thumbnail") or None,
    }
