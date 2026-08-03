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


def resolve_audio(item, skip=0):
    """Download full audio for a queue item (dict). Returns
    {source, path, duration_ms, artwork_url}. Raises AudioResolveError on total
    failure (caller leaves the item in needs_audio for manual upload).

    `skip` selects a lower-ranked YouTube upload — the escape hatch for a
    source whose own audio is damaged.
    """
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
    # The released track length, used to reject live takes, edits and
    # compilations before anything is downloaded. Best-effort: no reference
    # just means the ranking falls back to bitrate and title heuristics.
    want_ms = None
    try:
        from lib import cover_art
        want_ms = cover_art.lookup(item.get("artist", ""),
                                   item.get("track_name", ""))["duration_ms"]
    except Exception:
        pass
    return _from_ytdlp(query, out_dir, skip=skip, want_ms=want_ms)


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


def _score(entry, want_ms):
    """Rank a YouTube search result as a source for a known recording.

    Taking the first hit is how you end up with a live take, a sped-up edit, a
    fan video with a compressed rip, or an upload carrying a defect of its own
    — and the artefact that started this was exactly that: damage present in
    YouTube's copy, not introduced anywhere in our pipeline.

    Duration against the released length is the strongest signal available: a
    proper upload lands within a couple of seconds, while remixes, live
    versions and hour-long compilations do not. Bitrate breaks ties.
    """
    dur_ms = int((entry.get("duration") or 0) * 1000)
    if not dur_ms:
        return -1e9
    score = 0.0
    if want_ms:
        delta = abs(dur_ms - want_ms) / 1000.0
        if delta > 25:
            return -1e9              # a different recording, not a worse copy
        score -= delta * 10
    else:
        # No reference length: at least reject things that cannot be the song.
        if dur_ms < 45_000 or dur_ms > 15 * 60_000:
            return -1e9
    score += min((entry.get("abr") or 0), 192) / 10.0
    title = (entry.get("title") or "").lower()
    for bad in ("live", "cover", "karaoke", "remix", "sped up", "slowed",
                "nightcore", "reaction", "8d audio"):
        if bad in title:
            score -= 40
    return score


def _from_ytdlp(query, out_dir, skip=0, want_ms=None):
    """Download the best YouTube upload for `query`.

    `skip` walks down the ranked list — so a source that turns out to be
    audibly damaged can be rejected and the next-best tried, rather than the
    pipeline handing back the same bad file forever.
    """
    try:
        import yt_dlp
    except ImportError:
        raise AudioResolveError("yt-dlp not installed (pip install yt-dlp)")

    out_tmpl = os.path.join(out_dir, "source.%(ext)s")
    # Keep whatever YouTube actually serves; do NOT transcode on the way in.
    #
    # This used to run FFmpegExtractAudio to mp3 192k, which meant YouTube's
    # Opus was decoded and re-encoded before anything had even been cut — one
    # entire lossy generation spent to arrive at a worse codec than the one we
    # started with. Preferring the native m4a (AAC) stream costs nothing and
    # arrives untouched; opus/webm is the fallback and ffmpeg reads both
    # happily. The single remaining encode is the AAC written into the mp4.
    opts = {
        "format": "bestaudio[ext=m4a]/bestaudio/best",
        "outtmpl": out_tmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "default_search": "ytsearch8",
    }
    # Two passes: list the candidates without downloading, rank them, then
    # fetch only the one we chose. Downloading eight files to discard seven
    # would be absurd.
    try:
        with yt_dlp.YoutubeDL({**opts, "skip_download": True,
                               "extract_flat": False}) as ydl:
            listing = ydl.extract_info(query, download=False)
    except Exception as e:
        raise AudioResolveError(f"yt-dlp search: {e}")
    entries = [e for e in (listing.get("entries") or [listing]) if e]
    ranked = sorted(entries, key=lambda e: _score(e, want_ms), reverse=True)
    ranked = [e for e in ranked if _score(e, want_ms) > -1e8]
    if not ranked:
        raise AudioResolveError("no usable YouTube result for this track")
    if skip >= len(ranked):
        raise AudioResolveError(
            f"no further sources (tried all {len(ranked)} candidates)")
    chosen = ranked[skip]

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(chosen.get("webpage_url") or chosen["id"],
                                    download=True)
    except Exception as e:
        raise AudioResolveError(f"yt-dlp: {e}")
    if info.get("entries"):
        info = info["entries"][0]
    # The extension now follows the stream we were given, so find it rather
    # than assuming .mp3.
    dest = None
    for f in sorted(os.listdir(out_dir)):
        if f.startswith("source.") and not f.endswith((".part", ".ytdl")):
            dest = os.path.join(out_dir, f)
            break
    if not dest:
        raise AudioResolveError("yt-dlp produced no audio file")
    return {
        # Record WHICH candidate this was, so "try another source" knows where
        # it got to and does not re-download the same rejected upload.
        "source": "youtube" if not skip else f"youtube#{skip + 1}",
        "path": dest,
        "duration_ms": int((info.get("duration") or 0) * 1000),
        "artwork_url": info.get("thumbnail") or None,
    }
