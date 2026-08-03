"""
Find a record's actual cover art.

The Instagram cards were showing YouTube video thumbnails, because that is what
yt-dlp hands back and nothing else was ever asked. For a good share of uploads
the thumbnail happens to BE the sleeve, which is why it went unnoticed — but for
the rest it is a screen-grab from the video, complete with burnt-in titles and
production credits. All 24 items in the first real queue were video stills.

The pool's own `tracks.art_url` only covers ~36% of tracks and was empty for
every one of them, so it cannot be the answer on its own. iTunes Search can:
free, unauthenticated, no quota to burn (unlike Spotify, whose dev quota locks
this project out for a day if leaned on), square artwork up to 1000px, and it
resolves obscure catalogue — Mongolian, Vietnamese, Cantonese and Malian tracks
all matched on the first try.

A WRONG cover is worse than a video still, so a result is only accepted when
the artist or the title genuinely corresponds. Everything else falls back.
"""
import difflib
import json
import re
import unicodedata
import urllib.parse
import urllib.request

SEARCH = "https://itunes.apple.com/search"
TIMEOUT = 15


def _norm(s):
    """Casefold, strip accents and punctuation — so 'Edicion Especial' matches
    'Edición Especial' and '(Album Version)' does not sink an otherwise
    perfect title match."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\(.*?\)|\[.*?\]", " ", s.lower())
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _similar(a, b):
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return 0.0
    if a in b or b in a:
        return 1.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def _plausible(want_artist, want_name, got_artist, got_name):
    """Accept only a match a person would agree with.

    Either the artist lines up (different pressings, remasters, live takes all
    still carry the right sleeve) or the title is a near-exact hit. Requiring
    both would drop transliterations like 張德蘭 → Teresa Cheung, where the
    artist field cannot possibly match but the title is identical.
    """
    return _similar(want_artist, got_artist) >= 0.62 or \
        _similar(want_name, got_name) >= 0.85


def _upsize(url, px=1000):
    """iTunes returns a 100px thumbnail; the same asset exists at any size."""
    return re.sub(r"/\d+x\d+bb\.(jpg|png)", f"/{px}x{px}bb.jpg", url or "")


def lookup(artist, name, timeout=TIMEOUT):
    """Everything iTunes knows about this recording that we can use.

    Returns {"art": url|None, "duration_ms": int|None} — never raises, because
    the callers are a render pipeline and a downloader, and a failed lookup
    should degrade the result, not the run.

    duration_ms matters as much as the artwork: it is the canonical length of
    the released track, which is the one reliable way to tell a proper upload
    from a live take, a sped-up edit, a remix or an hour-long compilation when
    picking between YouTube search results.
    """
    out = {"art": None, "duration_ms": None}
    term = urllib.parse.quote(f"{artist} {name}".strip())
    if not term:
        return out
    url = f"{SEARCH}?term={term}&entity=song&limit=5"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            results = json.load(r).get("results") or []
    except Exception:
        return out
    for res in results:
        if not _plausible(artist, name, res.get("artistName", ""),
                          res.get("trackName", "")):
            continue
        art = res.get("artworkUrl100") or res.get("artworkUrl60")
        if art and not out["art"]:
            out["art"] = _upsize(art)
        if res.get("trackTimeMillis") and not out["duration_ms"]:
            out["duration_ms"] = int(res["trackTimeMillis"])
        if out["art"] and out["duration_ms"]:
            break
    if not out["duration_ms"]:
        # Duration drives YouTube source selection, so a miss here is worth a
        # second call — it is what stops a live take or an edit being chosen.
        out["duration_ms"] = _from_deezer(artist, name, timeout)["duration_ms"]
    return out


def _from_deezer(artist, name, timeout=TIMEOUT):
    """Second opinion when iTunes has never heard of the record.

    Apple's catalogue is broad but not universal — small European and net-label
    releases fall straight through it. Deezer's search is free, unauthenticated
    and indexes a different long tail, and it carried both tracks iTunes missed.
    Same plausibility gate: a wrong cover is worse than no cover.
    """
    out = {"art": None, "duration_ms": None}
    q = urllib.parse.quote(f"{artist} {name}".strip())
    if not q:
        return out
    try:
        with urllib.request.urlopen(
                f"https://api.deezer.com/search?q={q}&limit=5", timeout=timeout) as r:
            results = json.load(r).get("data") or []
    except Exception:
        return out
    for res in results:
        if not _plausible(artist, name, (res.get("artist") or {}).get("name", ""),
                          res.get("title", "")):
            continue
        alb = res.get("album") or {}
        art = alb.get("cover_xl") or alb.get("cover_big") or alb.get("cover_medium")
        if art and not out["art"]:
            out["art"] = art
        if res.get("duration") and not out["duration_ms"]:
            out["duration_ms"] = int(res["duration"]) * 1000
        if out["art"] and out["duration_ms"]:
            break
    return out


def find(artist, name, timeout=TIMEOUT):
    """Just the cover-art URL, or None. iTunes first, then Deezer."""
    hit = lookup(artist, name, timeout)
    if hit["art"]:
        return hit["art"]
    return _from_deezer(artist, name, timeout)["art"]
