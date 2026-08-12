#!/usr/bin/env python3
"""
DIG — your YouTube likes, into the pool and the Instagram funnel.

The IG proposer reads the admin's likes out of user_history, which already
carries Spotify Liked Songs and SoundCloud likes. YouTube is the third place
the same taste lives — and the only auth it needs is the .yt_cookies.txt file
the resolve stage already maintains (scripts/export_yt_cookies.py), because a
logged-in session can read its own Liked Videos playlist (list=LL).

Two realities this script is shaped around:

  * MANY YOUTUBE LIKES ARE ALSO SPOTIFY/SOUNDCLOUD LIKES. So a like whose
    "artist - title" already matches a pool track (same normalized key the
    dedup pass uses) does NOT become a new 'yt:' row — the EXISTING track is
    marked liked instead. Twins converge; the proposer suggests the copy with
    the richer metadata; nothing is posted twice.
  * ~1% OF LIKES ARE NOT SONGS. A duration window (45s–15min, the same bounds
    lib/ig_audio trusts for an unverified upload) plus a non-music keyword
    filter drops the obvious ones. Whatever slips through still only ever
    lands as a SUGGESTION the admin approves by hand — a false positive costs
    one click in the dashboard, so the filter aims for obvious, not clever.

Tracks enter the pool as 'yt:<videoid>' (the discover_youtube.py convention),
which /discovery already excludes from listener-facing serving — this feeds
the IG funnel, not the app. The resolver downloads a 'yt:' id's EXACT video,
never a search result.

    python3 scripts/ingest_youtube_likes.py --dry-run
    python3 scripts/ingest_youtube_likes.py --limit 50
    python3 scripts/ingest_youtube_likes.py --queue   # skip the approve step
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
from lib import ig_queue
from lib.artist_db import register_tracks
from lib.db import fetchall, get_conn
from lib.discovery_lock import locked_update
from lib.ig_audio import COOKIE_FILE
from lib.pool_search import _norm_key
from lib.track_filter import is_trash

LIKES_URL = "https://www.youtube.com/playlist?list=LL"

# The ~1% that isn't a song. Matched against title AND channel, lowercased.
# Obvious-only on purpose: everything here still needs a dashboard approval,
# so a missed vlog costs one click while an over-eager filter silently eats
# a song called "Interview" forever.
NON_MUSIC = (
    "podcast", "interview", "full episode", "tutorial", "how to", "review",
    "documentary", "gameplay", "walkthrough", "lecture", "ted talk", "tedx",
    "trailer", "reaction", "asmr", "stand-up", "standup", "highlights",
    "full match", "full movie", "compilation", "vlog",
)

# Upload-title furniture: "(Official Music Video)", "[Audio]", "Official MV
# (ENG/CHN)". Stripped from the SONG half only, repeatedly — a title can
# carry two tags.
_TAG = (r"(?:official|music|lyric[s]?|audio|video|visuali[sz]er|mv|m/v"
        r"|videoclip|clip officiel|hd|hq|4k|eng|chn|sub(?:s|bed)?|/| )+")
_TAIL_NOISE = re.compile(r"\s*(?:[\(\[]" + _TAG + r"[\)\]]|official\s+mv"
                         r"|official\s+(?:music\s+)?video|m/v)\s*$", re.I)


def clean_name(name):
    out = (name or "").strip()
    prev = None
    while out != prev:
        prev = out
        out = _TAIL_NOISE.sub("", out).strip()
    # "'ONLY' Official MV" leaves "'ONLY'" behind once the tag goes.
    if len(out) > 2 and out[0] == out[-1] and out[0] in "'\"“”‘’":
        out = out[1:-1].strip()
    return out or (name or "").strip()


def is_probably_music(entry):
    # Keywords only, no duration window — likes include long mixes and short
    # interludes that are absolutely songs (Tommaso, 2026-08-12). If a long
    # non-song slips through, the dashboard approval is the backstop.
    hay = f'{entry.get("title") or ""} {entry.get("channel") or ""}'.lower()
    return not any(k in hay for k in NON_MUSIC)


def _split_dash(title):
    """The first " - " OUTSIDE brackets, or (None, None).

    "Snoop Dogg X Dr Dre (Del - 30's ...)" must not split inside the
    parenthesis — the dash there is part of an annotation, not the
    artist/title seam.
    """
    depth = 0
    for i, ch in enumerate(title):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        elif depth == 0 and title[i:i + 3] == " - ":
            head, tail = title[:i], title[i + 3:]
            if head.strip() and tail.strip():
                return head.strip(), tail.strip()
    return None, None


def parse_entry(entry):
    """A liked-videos playlist entry → (artist, name), best effort.

    Priority: a "- Topic" channel is YouTube's own auto-generated music
    upload (channel IS the artist, title IS the song); then the near-universal
    "Artist - Title" convention (also right when a label channel like
    Majestic Casual uploads someone else's song); else the channel is the
    artist and the title is the song ("Daddy Issues" on The Neighbourhood's
    own channel).
    """
    title = (entry.get("title") or "").strip()
    channel = (entry.get("channel") or entry.get("uploader") or "").strip()
    if not title:
        return None
    # A trailing "| <channel>" is the uploader signing their own title.
    if channel and title.lower().endswith(f"| {channel}".lower()):
        title = title[: -len(channel) - 1].rstrip(" |").strip()
    if channel.endswith(" - Topic"):
        return channel[: -len(" - Topic")].strip(), clean_name(title)
    head, tail = _split_dash(title)
    if head:
        return head, clean_name(tail)
    if not channel:
        return None
    name = clean_name(title)
    # "IVAN VALEEV — NOVELLA" on IVAN VALEEV's own channel: the artist is
    # already the channel, so a name that repeats it is carrying a separator
    # the " - " split above didn't recognize (em dash, colon).
    if name.lower().startswith(channel.lower()) and len(name) > len(channel):
        rest = name[len(channel):].lstrip(" -—–:|·").strip()
        if rest:
            name = rest
    return channel, name


def list_likes(limit):
    if not os.path.exists(COOKIE_FILE):
        raise SystemExit(
            "  no .yt_cookies.txt — run scripts/export_yt_cookies.py once "
            "(interactively; it is the only script allowed to ask Keychain)")
    import yt_dlp
    opts = {"quiet": True, "no_warnings": True, "extract_flat": True,
            "skip_download": True, "playlistend": limit, "ignoreerrors": True,
            "cookiefile": COOKIE_FILE}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(LIKES_URL, download=False)
    return [e for e in ((info or {}).get("entries") or []) if e]


def _mark_liked(rows):
    """Record likes as likes in user_history — same rationale as the
    SoundCloud ingest: the proposer and the add-track picker read the admin's
    saved rows, and a YouTube like IS a like, just liked somewhere else.
    `rows` = (track_id, name, artist, region) tuples, newest first."""
    admin = os.environ.get("ADMIN_UID", "")
    if not admin or not rows:
        return 0
    have = {r["track_id"] for r in fetchall(
        "SELECT track_id FROM user_history WHERE user_id = %s AND track_id = ANY(%s)",
        (admin, [r[0] for r in rows]))}
    now_ms = int(time.time() * 1000)
    todo = [(admin, tid, name, artist, region or "Unknown", now_ms - i * 1000)
            for i, (tid, name, artist, region) in enumerate(rows)
            if tid not in have]
    if not todo:
        return 0
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO user_history (user_id, track_id, track_name, "
                "artist, region, status, listened_at, source) "
                "VALUES (%s, %s, %s, %s, %s, 'saved', %s, 'youtube')", todo)
        conn.commit()
    finally:
        conn.close()
    return len(todo)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50, help="how many likes to read")
    ap.add_argument("--queue", action="store_true",
                    help="add straight to the IG queue instead of waiting for the proposer")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"  reading YouTube liked videos (first {args.limit})…")
    entries = list_likes(args.limit)
    if not entries:
        print("  no likes visible — is the cookie session still valid?")
        return

    # The whole pool's name keys: Spotify and SoundCloud likes all live in
    # tracks, so this one map is what makes cross-platform twins CONVERGE
    # (existing row gets the like) instead of multiplying.
    known = {}
    for r in fetchall("SELECT id, name, artist, region FROM tracks"):
        known.setdefault(_norm_key(r["artist"], r["name"]), r)
    have_yt = {r["id"] for r in fetchall(
        "SELECT id FROM tracks WHERE id LIKE 'yt:%%'")}

    fresh, converged, dup, skipped = [], [], 0, 0
    seen_keys = set()
    for e in entries:
        if not is_probably_music(e):
            skipped += 1
            continue
        parsed = parse_entry(e)
        if not parsed:
            skipped += 1
            continue
        artist, name = parsed
        if is_trash(name, artist, ""):
            skipped += 1
            continue
        tid = f'yt:{e.get("id")}'
        key = _norm_key(artist, name)
        if tid in have_yt or key in seen_keys:
            dup += 1
            continue
        seen_keys.add(key)
        match = known.get(key)
        if match:
            converged.append((match["id"], match["name"], match["artist"],
                              match["region"]))
            continue
        fresh.append({
            "id": tid, "name": name, "artist": artist, "album": "",
            "art": "", "genres": [],
            "duration": int((e.get("duration") or 0) * 1000),
            "query": "youtube:likes", "source": "youtube",
        })

    print(f"  {len(entries)} likes read → {len(fresh)} new, "
          f"{len(converged)} matched existing pool tracks "
          f"(already ingested: {dup}, filtered non-music/trash: {skipped})")
    for t in fresh[:40]:
        print(f"    ✓ {t['artist'][:26]:28} - {t['name'][:40]}")
    for tid, name, artist, _ in converged[:20]:
        print(f"    = {artist[:26]:28} - {name[:34]:36} ({tid[:24]})")

    if args.dry_run:
        print("\n  DRY RUN — nothing written.")
        return

    if fresh:
        def _merge(disk):
            ex = disk.get("Unknown", [])
            ex_ids = {t["id"] for t in ex}
            for t in fresh:
                if t["id"] not in ex_ids:
                    ex.append(t)
                    ex_ids.add(t["id"])
            disk["Unknown"] = ex
        locked_update(_merge)
        register_tracks(fresh, region="Unknown", source="youtube")
        print(f"  inserted {len(fresh)} track(s) into the pool")

    liked_rows = ([(t["id"], t["name"], t["artist"], "Unknown") for t in fresh]
                  + converged)
    saved = _mark_liked(liked_rows)
    if saved:
        print(f"  marked {saved} as liked — the proposer can now suggest them")

    if not args.queue:
        print("  (they reach Instagram via the normal propose → approve flow; "
              "pass --queue to skip the approve step)")
        return

    queued = ig_queue._already_queued_track_ids()
    added = 0
    for t in fresh:
        if t["id"] in queued:
            continue
        new_id = ig_queue.add_item(track_id=t["id"], track_name=t["name"],
                                   artist=t["artist"], status="needs_audio")
        if new_id:
            added += 1
            print(f"    + queued #{new_id}: {t['name'][:34]} — {t['artist'][:24]}")
    print(f"  queued {added} track(s).")


if __name__ == "__main__":
    main()
