#!/usr/bin/env python3
"""
DIG — your SoundCloud likes, into the pool and (optionally) the Instagram queue.

WHY THIS EXISTS. The IG queue is fed from Spotify Liked Songs, and that skews
the genre balance: the electronic and club end of the taste lives on SoundCloud,
where a lot of it exists nowhere else. This walks the likes on a SoundCloud
profile and turns them into ordinary pool tracks, so a queued SoundCloud post is
a track someone can actually go and find in Dig — not a post advertising a tool
that has never heard of the song.

TWO NOTES ON WHAT THIS CROSSES.

  * lib/soundcloud and scripts/ingest_soundcloud both say SoundCloud is
    streaming-only here, because SoundCloud's terms forbid downloading, and
    that is why SoundCloud has never fed the clip pipeline. Queueing a like
    for Instagram means downloading it (lib/ig_audio._from_soundcloud). That
    is a deliberate exception, not an oversight — the rest of the app still
    stores nothing but 'sc:<id>' and resolves a fresh stream at play time.
  * Enumeration needs no credentials at all: a profile's likes are public, and
    yt-dlp reads the listing. The SoundCloud API key is used only to enrich
    each track with genre/artwork, and the script degrades to yt-dlp's own
    metadata without it.

    python3 scripts/ingest_soundcloud_likes.py --user <soundcloud-handle>
    python3 scripts/ingest_soundcloud_likes.py --user tommaso --limit 30 --queue
    python3 scripts/ingest_soundcloud_likes.py --user tommaso --dry-run

The handle is the one in your profile URL: soundcloud.com/<handle>. Set
SOUNDCLOUD_LIKES_USER in .env to skip --user.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import ig_queue, soundcloud
from lib.artist_db import register_tracks
from lib.db import fetchall, get_conn
from lib.discovery_lock import locked_update
from lib.track_filter import is_trash


def list_likes(user, limit):
    """The liked tracks on a public SoundCloud profile, newest first.

    extract_flat: the listing alone is what is wanted here — resolving each
    track properly would fetch every stream URL for a page we are only reading
    titles off.
    """
    import yt_dlp
    opts = {"quiet": True, "no_warnings": True, "extract_flat": True,
            "skip_download": True, "playlistend": limit, "ignoreerrors": True}
    url = f"https://soundcloud.com/{user}/likes"
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
    return [e for e in ((info or {}).get("entries") or []) if e]


# Promo furniture SoundCloud titles carry, none of which is part of the song.
# Leading: "PREMIERE:", "BANGKIT PREMIERE:", "[ST014]". Trailing: the label in
# brackets, the free-download shout, the catalogue number.
_LEAD_NOISE = re.compile(
    r"^\s*(?:\[[^\]]{1,20}\]\s*|(?:\w[\w'&.]*\s+)?(?:premiere|premier|exclusive|free\s*dl)\s*[:\-–]\s*)+",
    re.I)
_TAIL_NOISE = re.compile(
    # A trailing [bracket] on SoundCloud is the label or the catalogue number
    # essentially always, so it goes wholesale. Parentheses do NOT: "(Original
    # Mix)", "(feat. …)" and "(MJF REMIX)" are part of what the track is.
    r"(?:\s*\[[^\]]{0,60}\]|\s*\([^\)]{0,40}(?:free\s*(?:dl|download)|out\s+now)"
    r"[^\)]{0,40}\)|\s*\|\s*free\s*dl\.?)\s*$", re.I)


def clean_title(title):
    """Strip the promo wrapper off a SoundCloud upload title.

    "PREMIERE: Closed Paradise - Secrets [Better Listen Records]" is one song
    with three pieces of marketing stapled to it, and all three end up in the
    caption headline and in the artist@ lookup if they are left on.
    """
    out = _LEAD_NOISE.sub("", title or "").strip()
    prev = None
    while out != prev:              # a title can carry two trailing tags
        prev = out
        out = _TAIL_NOISE.sub("", out).strip()
    return out or (title or "").strip()


def uploader_from_url(url):
    """soundcloud.com/<handle>/<slug> → '<handle>'.

    Flat playlist entries carry no uploader field at all — only id, title and
    url — so without this every like whose title is not "Artist - Song" was
    landing in the pool credited to "Unknown".
    """
    m = re.match(r"https?://(?:www\.)?soundcloud\.com/([^/?#]+)/", url or "")
    return m.group(1).replace("-", " ").strip() if m else ""


def to_pool_track(entry):
    """A yt-dlp likes entry → DIG's pool-track shape.

    The SoundCloud API is asked first because it knows the genre, the real
    cover and the duration, and the pool's genre buckets are the whole reason
    for doing this. Without credentials it falls back to what the listing
    carries, which is title and URL and nothing else.
    """
    sid = str(entry.get("id") or "").strip()
    if not sid.isdigit():
        return None
    try:
        t = soundcloud._api_get(f"/tracks/{sid}")
        if t and t.get("id"):
            return soundcloud._track_to_pool(t)
    except Exception:
        pass
    title = clean_title(entry.get("title"))
    artist = ""
    # "Artist - Title" is the house style for edits and remixes, and there the
    # uploader is the editor rather than whoever made the record.
    if " - " in title:
        head, _, tail = title.partition(" - ")
        if head.strip() and tail.strip():
            artist, title = head.strip(), tail.strip()
    if not artist:
        artist = uploader_from_url(entry.get("url"))
    if not title:
        return None
    return {
        "id": soundcloud.make_id(sid),
        "name": title,
        "artist": artist or "Unknown",
        "album": "",
        "art": entry.get("thumbnail") or "",
        "genres": [],
        "duration": int((entry.get("duration") or 0) * 1000),
        "permalink": entry.get("url") or "",
        "source": "soundcloud",
    }


def _mark_liked(tracks):
    """Record the likes as likes, in user_history.

    Being in `tracks` is not enough to be usable: the add-track picker and the
    proposer both read the admin's LIKED songs (user_history, status='saved'),
    which is a Spotify-shaped table. A SoundCloud like that only reached the
    pool is invisible to both — the exact opposite of the genre balance this
    script exists for. Writing it here is also true: it IS a like, just liked
    somewhere else.

    listened_at walks backwards from now so the picker's newest-first order
    matches the order they were liked in, rather than collapsing to one
    timestamp.
    """
    admin = os.environ.get("ADMIN_UID", "")
    if not admin:
        return 0
    have = {r["track_id"] for r in fetchall(
        "SELECT track_id FROM user_history WHERE user_id = %s AND track_id = ANY(%s)",
        (admin, [t["id"] for t in tracks]))}
    now_ms = int(time.time() * 1000)
    rows = [(admin, t["id"], t["name"], t["artist"], "Unknown",
             now_ms - i * 1000)
            for i, t in enumerate(tracks) if t["id"] not in have]
    if not rows:
        return 0
    # One connection for the batch. lib.db.execute opens and closes its own, and
    # the database is on the far end of an SSH tunnel — 39 round trips that way
    # took over two minutes and timed out halfway through.
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO user_history (user_id, track_id, track_name, "
                "artist, region, status, listened_at, source) "
                "VALUES (%s, %s, %s, %s, %s, 'saved', %s, 'soundcloud')", rows)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", default=os.environ.get("SOUNDCLOUD_LIKES_USER", ""),
                    help="soundcloud.com/<handle>")
    ap.add_argument("--limit", type=int, default=50, help="how many likes to read")
    ap.add_argument("--queue", action="store_true",
                    help="also add the new tracks to the Instagram queue")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.user:
        print("  no --user and no SOUNDCLOUD_LIKES_USER in .env — nothing to read.")
        print("  it is the handle in your profile URL: soundcloud.com/<handle>")
        return

    print(f"  reading soundcloud.com/{args.user}/likes (first {args.limit})…")
    try:
        entries = list_likes(args.user, args.limit)
    except Exception as e:
        print(f"  ! could not read that profile: {e}")
        print("    (likes have to be public for this to see them)")
        return
    if not entries:
        print("  no likes visible on that profile.")
        return

    existing = {r["id"] for r in fetchall(
        "SELECT id FROM tracks WHERE id LIKE 'sc:%%'")}
    queued = ig_queue._already_queued_track_ids()

    fresh, dup, trash, bad = [], 0, 0, 0
    for e in entries:
        t = to_pool_track(e)
        if not t:
            bad += 1
            continue
        if t["id"] in existing or any(f["id"] == t["id"] for f in fresh):
            dup += 1
            continue
        if is_trash(t["name"], t["artist"], t.get("album", "")):
            trash += 1
            continue
        t["query"] = f"soundcloud:likes:{args.user}"
        fresh.append(t)

    print(f"  {len(entries)} likes read → {len(fresh)} new "
          f"(already in pool: {dup}, filtered: {trash}, unreadable: {bad})")
    for t in fresh[:40]:
        print(f"    ✓ {t['artist'][:26]:28} - {t['name'][:34]:36} "
              f"{'/'.join(t['genres']) or '—'}")

    if args.dry_run:
        print("\n  DRY RUN — nothing written.")
        return
    if not fresh:
        return

    # Same two writes as scripts/ingest_soundcloud: the region-keyed discovery
    # file under its lock, then the artist/track registry. Region is unknown
    # for a like — SoundCloud does not carry one — and "Unknown" is the bucket
    # the rest of the pipeline already expects for that.
    def _merge(disk):
        ex = disk.get("Unknown", [])
        ex_ids = {t["id"] for t in ex}
        for t in fresh:
            if t["id"] not in ex_ids:
                ex.append(t)
                ex_ids.add(t["id"])
        disk["Unknown"] = ex
    locked_update(_merge)
    register_tracks(fresh, region="Unknown", source="soundcloud")
    print(f"  inserted {len(fresh)} track(s) into the pool")

    saved = _mark_liked(fresh)
    if saved:
        print(f"  marked {saved} as liked — they now show in the add-track "
              f"picker and can be suggested like any other like")

    if not args.queue:
        print("  (pass --queue to also put them in the Instagram queue)")
        return

    added = 0
    for t in fresh:
        if t["id"] in queued:
            continue
        # needs_audio, like a manual add: these are chosen, not suggested, so
        # they skip the approve step and go straight to the resolver — which
        # downloads them from the permalink, not via a YouTube search.
        new_id = ig_queue.add_item(track_id=t["id"], track_name=t["name"],
                                   artist=t["artist"], status="needs_audio")
        if new_id:
            added += 1
            print(f"    + queued #{new_id}: {t['name'][:34]} — {t['artist'][:24]}")
    print(f"  queued {added} track(s). The next ig_cron run fetches their audio "
          f"and writes their captions.")


if __name__ == "__main__":
    main()
