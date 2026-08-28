#!/usr/bin/env python3
"""DIG — pull a curator's played tracks from their NTS Radio residency.

WHY THIS EXISTS, AND WHY IT IS NOT THE INSTAGRAM SCRAPER. Some curators do not
write track names anywhere. @rotational___ (Rotational / Karim, 273 posts) is
the case that prompted this: he photographs physical media and writes prose
about where a sound comes from — "Ethio Reggae", "Rai Beats", "80's Telugu
Electro" — so scrape_ig_curator got 0 candidates out of 36 captions, correctly.
His Instagram is a genre map, not a tracklist.

The tracks are on NTS, and they are STRUCTURED. Every archived episode carries
a tracklist with artist, title, and — for most entries — a Deezer track id and
an ISRC. That is a better starting point than any caption parser: no fuzzy
extraction, and the ISRC is an exact key into MusicBrainz for whoever wants to
use it later. Measured on `rotational` 2026-08: 9 episodes, ~14-17 tracks each.

Output is the SAME candidate shape scrape_ig_curator writes, so
resolve_curator_bandcamp.py consumes it with no changes:

    {"raw": "Artist - Track", "artist": …, "track": …, "style": …,
     "country": None, "year": None, "label": None, "source": "nts:<show>"}

Deliberately NOT filled: `country`. NTS episodes carry a location, but it is
the studio's (London, Manchester), not the music's — writing that into the
field Dig stratifies regions on would be worse than leaving it empty. The
episode's genre tag IS the music's, so that goes to `style`.

THE TRAP THIS GUARDS. When NTS throttles a client it does not fail: it returns
the episode JSON with the `tracklist` key REMOVED. An episode that answered
with 17 tracks came back indistinguishable from "this show has no tracklist"
after ~10 quick requests, and stayed that way for minutes, while the same
episode still showed all its tracks from another address. Scraping through that
would silently record a whole residency as empty and look like a clean run — so
a missing key stops the run rather than counting as zero. (A tracklist key that
is present but empty is treated as a genuinely untracklisted episode; that case
has not been observed, only reasoned about.)

    python3 scripts/scrape_nts_show.py rotational
    python3 scripts/scrape_nts_show.py rotational --out nts_rotational.json
    python3 scripts/scrape_nts_show.py rotational --limit 3 --pace 6
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

API = "https://www.nts.live/api/v2"
HEADERS = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36")}
# Slower than feels necessary, on purpose: the throttle above costs minutes to
# clear and is invisible while it lasts, so the cheap fix is to not trip it.
PACE_S = 4.0


class Throttled(Exception):
    """NTS answered without the tracklist it normally carries."""


def _get(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def episodes(show, limit=None, pace=PACE_S):
    """Every archived episode of a show, newest first."""
    out, offset = [], 0
    while True:
        page = _get(f"{API}/shows/{show}/episodes?offset={offset}&limit=12")
        got = page.get("results") or []
        out += got
        if len(got) < 12 or (limit and len(out) >= limit):
            break
        offset += 12
        time.sleep(pace)
    return out[:limit] if limit else out


def tracklist(show, alias):
    """The episode's tracks. Raises Throttled when NTS drops the key."""
    d = _get(f"{API}/shows/{show}/episodes/{alias}")
    if "tracklist" not in d:
        raise Throttled(alias)
    return ((d.get("tracklist") or {}).get("results")) or []


def episode_style(ep):
    """The episode's genre tag — the music's, unlike its location."""
    genres = [g.get("value") for g in (ep.get("genres") or []) if g.get("value")]
    return ", ".join(genres) or None


def to_candidate(track, style, show):
    """One tracklist entry → the candidate shape resolve_curator_bandcamp reads.

    The ISRC and Deezer id are carried through even though nothing consumes
    them yet: they are exact identifiers, and the alternative to keeping them
    is scraping the show again to get them back.
    """
    artist = (track.get("artist") or "").strip()
    title = (track.get("title") or "").strip()
    if not artist or not title:
        return None
    return {
        "raw": f"{artist} - {title}",
        "artist": artist,
        "track": title,
        "label": None,
        "year": None,
        "country": None,          # see module docstring — NTS's is the studio's
        "style": style,
        "isrc": track.get("isrc_id"),
        "deezer_track_id": track.get("deezer_track_id"),
        "source": f"nts:{show}",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("shows", nargs="+", help="NTS show alias, e.g. 'rotational'")
    ap.add_argument("--limit", type=int, help="max episodes per show (newest first)")
    ap.add_argument("--pace", type=float, default=PACE_S,
                    help="seconds between requests (the throttle is silent — do not rush)")
    ap.add_argument("--out", help="write candidates JSON (otherwise dry-run only)")
    args = ap.parse_args()

    all_rows, styles, throttled = [], {}, False
    for show in args.shows:
        print(f"\n=== nts.live/shows/{show} ===")
        try:
            eps = episodes(show, limit=args.limit, pace=args.pace)
        except urllib.error.HTTPError as e:
            print(f"  FAILED: HTTP {e.code} — is '{show}' the right alias?")
            continue
        if not eps:
            print("  no episodes.")
            continue
        print(f"  {len(eps)} episode(s)")

        rows, blank = [], 0
        for ep in eps:
            time.sleep(args.pace)
            alias = ep.get("episode_alias")
            style = episode_style(ep)
            try:
                tl = tracklist(show, alias)
            except Throttled:
                throttled = True
                print(f"\n  !! NTS dropped the tracklist for {alias} — that is "
                      f"the throttle, not an empty episode. Stopping with "
                      f"{len(rows)} track(s) kept; re-run later (and raise "
                      f"--pace) to finish. !!")
                break
            except urllib.error.HTTPError as e:
                print(f"  ERR  {alias}: HTTP {e.code}")
                continue
            if not tl:
                blank += 1
            for t in tl:
                c = to_candidate(t, style, show)
                if c:
                    rows.append(c)
            print(f"    {ep.get('broadcast', '')[:10]}  {len(tl):>3} tracks  "
                  f"{(style or '—')[:34]:36} {(ep.get('name') or '')[:40]}")
            if style:
                styles[style] = styles.get(style, 0) + 1

        with_isrc = sum(1 for r in rows if r["isrc"])
        print(f"\n  {len(rows)} candidates"
              + (f", {blank} episode(s) genuinely tracklist-free" if blank else "")
              + f" — {with_isrc} carry an ISRC")
        for r in rows[:8]:
            print(f"    {r['artist'][:30]:32} — {r['track'][:34]:36} "
                  f"[{(r['style'] or '—')[:22]}]")
        all_rows.extend(rows)

    uniq = {r["raw"].lower(): r for r in all_rows}
    print(f"\nTOTAL candidates: {len(all_rows)}  unique: {len(uniq)}")
    if styles:
        top = sorted(styles.items(), key=lambda kv: -kv[1])
        print("genres across the residency: "
              + ", ".join(f"{s}×{n}" for s, n in top))

    if not args.out:
        print("\n(dry run — nothing written; pass --out to save)")
        return
    if throttled:
        # The guard above stops the SCRAPE, but writing what it managed to get
        # is how the silent failure comes back: an empty (or half) candidates
        # file is indistinguishable from a residency with nothing in it, and
        # the resolver downstream would report a clean run over nothing. A run
        # that was cut short does not get to produce an artefact.
        print(f"\nNOT writing {args.out} — the run was cut short by the "
              f"throttle, and a partial file reads exactly like a complete "
              f"one. Wait for it to clear, raise --pace, and re-run.")
        return
    with open(args.out, "w") as f:
        json.dump(list(uniq.values()), f, ensure_ascii=False, indent=1)
    print(f"\nwrote {len(uniq)} candidates → {args.out}")
    print(f"next: python3 scripts/resolve_curator_bandcamp.py --in {args.out} --limit 60")


if __name__ == "__main__":
    main()
