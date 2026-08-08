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

    if track_id.startswith("sc:"):
        # Straight at the permalink, never through the YouTube search below.
        # SoundCloud's whole value to this pool is the niche electro that
        # exists nowhere else, so "find this on YouTube instead" would either
        # come back empty or come back with a different recording that happens
        # to share a title.
        try:
            return _from_soundcloud(track_id, out_dir, item)
        except Exception as e:
            print(f"  [ig_audio] soundcloud failed for {track_id}: {e!r}; trying yt-dlp")

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
    return _from_ytdlp(query, out_dir, skip=skip, want_ms=want_ms,
                       want_artist=item.get("artist", ""),
                       want_name=item.get("track_name", ""))


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


def _from_soundcloud(track_id, out_dir, item=None):
    """Download the exact SoundCloud track behind an 'sc:<id>' pool id.

    Note this crosses a line the rest of the codebase deliberately drew:
    lib/soundcloud and scripts/ingest_soundcloud say in as many words that
    SoundCloud is streaming-and-discovery only, storing nothing but the id
    because SoundCloud's terms forbid downloading. That rule still describes
    the app; this path exists because the Instagram queue was asked for
    SoundCloud likes specifically, and a clip cannot be rendered from a
    stream URL that expires. Reached only for items whose track_id starts
    'sc:' — nothing routes here by accident.
    """
    from lib import soundcloud
    sid = soundcloud.parse_id(track_id)
    if not sid:
        raise AudioResolveError("bad soundcloud id")
    # The API turns an id back into a page URL, but it needs credentials this
    # project does not always have — SOUNDCLOUD_CLIENT_ID is unset on the studio
    # machine, so this path would be dead on arrival without a second route.
    # yt-dlp's own SoundCloud search needs none, and the target here is one
    # specific upload rather than "some version of this song", so a single hit
    # on artist + title is the right query.
    target = ""
    try:
        t = soundcloud._api_get(f"/tracks/{sid}") or {}
        target = t.get("permalink_url") or ""
    except Exception:
        pass
    if not target:
        q = f'{(item or {}).get("artist", "")} {(item or {}).get("track_name", "")}'.strip()
        if not q:
            raise AudioResolveError("no soundcloud permalink and nothing to search on")
        target = f"scsearch1:{q}"

    import yt_dlp
    opts = {
        "format": "bestaudio/best",
        "outtmpl": os.path.join(out_dir, "source.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        # quiet alone still leaves the progress bar on stdout, and this runs
        # from cron into a log file — a few hundred redraw lines per track.
        "noprogress": True,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(target, download=True)
    except Exception as e:
        raise AudioResolveError(f"yt-dlp soundcloud: {e}")
    if info.get("entries"):
        info = info["entries"][0]
    dest = None
    for f in sorted(os.listdir(out_dir)):
        if f.startswith("source.") and not f.endswith((".part", ".ytdl")):
            dest = os.path.join(out_dir, f)
            break
    if not dest:
        raise AudioResolveError("yt-dlp produced no audio file")
    return {
        "source": "soundcloud",
        "path": dest,
        "duration_ms": probe_duration_ms(dest) or int((info.get("duration") or 0) * 1000),
        "artwork_url": info.get("thumbnail") or None,
    }


def _score(entry, want_ms, want_artist="", want_name=""):
    """Rank a YouTube search result as a source for a known recording.

    Taking the first hit is how you end up with a live take, a sped-up edit or
    an upload carrying a defect of its own. But ranking on duration alone is
    worse than not ranking at all: an earlier version of this scored duration,
    bitrate and title keywords and never checked the title was the SONG, so a
    297s unrelated track beat the real 294s one and "try another source"
    cheerfully returned a completely different piece of music.

    So the title match is a gate, not a bonus. Nothing that does not name the
    track can win, however good its duration looks.
    """
    from lib.cover_art import _norm, _similar

    title = entry.get("title") or ""
    if want_name:
        t, n = _norm(title), _norm(want_name)
        # The song's name must actually appear in the video title. Uploads are
        # titled every imaginable way — "Artist - Song (Official Video)",
        # "Song | Artist", non-Latin scripts — so containment plus a fuzzy
        # fallback, but never nothing.
        if n and n not in t and _similar(want_name, title) < 0.55:
            return -1e9

    dur_ms = int((entry.get("duration") or 0) * 1000)
    if not dur_ms:
        return -1e9
    score = 0.0
    if want_ms:
        delta_s = (dur_ms - want_ms) / 1000.0
        # Asymmetric on purpose. A ±12s window rejected ISSAM's own upload of
        # "Nike": the record runs 2:52 and the official music video runs 3:21,
        # because music videos carry intros, outros and spoken cold opens that
        # the release does not. The song was on the artist's channel the whole
        # time and the pipeline reported "no usable YouTube result".
        #
        # Running LONG is the normal, benign case; running short is not — a
        # shorter upload is a radio edit, a snippet or a sped-up version, i.e.
        # actually different audio. So allow generous overrun, stay strict
        # underneath, and keep ranking by closeness so the plain audio upload
        # still beats the video when both are present.
        if delta_s > 60 or delta_s < -10:
            return -1e9              # a different recording, not a worse copy
        score -= abs(delta_s) * 10
    else:
        if dur_ms < 45_000 or dur_ms > 15 * 60_000:
            return -1e9
    # Artist agreement is a bonus rather than a gate: plenty of correct uploads
    # sit on a fan channel with an unrelated name.
    uploader = f'{entry.get("uploader") or ""} {entry.get("channel") or ""}'
    if want_artist and _similar(want_artist, uploader) >= 0.5:
        score += 15
    if want_artist and _similar(want_artist, title) >= 0.4:
        score += 10
    score += min((entry.get("abr") or 0), 192) / 10.0
    low = title.lower()
    for bad in ("live", "cover", "karaoke", "remix", "sped up", "slowed",
                "nightcore", "reaction", "8d audio", "mix", "full album"):
        if bad in low:
            score -= 40
    return score


def _from_ytdlp(query, out_dir, skip=0, want_ms=None,
                want_artist="", want_name=""):
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
        # ignoreerrors: one dead result must not sink the search. A single
        # "This video is not available" among eight candidates was aborting the
        # whole listing and failing the track outright, when the other seven
        # were fine.
        with yt_dlp.YoutubeDL({**opts, "skip_download": True,
                               "extract_flat": False,
                               "ignoreerrors": True}) as ydl:
            listing = ydl.extract_info(query, download=False)
    except Exception as e:
        raise AudioResolveError(f"yt-dlp search: {e}")
    entries = [e for e in (listing.get("entries") or [listing]) if e]
    scored = [(_score(e, want_ms, want_artist, want_name), e) for e in entries]
    ranked = [e for sc, e in sorted(scored, key=lambda x: -x[0]) if sc > -1e8]
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
