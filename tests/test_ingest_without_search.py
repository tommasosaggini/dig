"""Regression lock for "Spotify keeps locking us out and scraping stops".

Diagnosed 2026-08-04. It was never bursting, never total volume, and never the
server's IP — three wrong theories, the last of which was mine and nearly cost a
machine setup. What settled it was running the SAME call from three machines
minutes apart:

    /search   Tokyo laptop 429 RA=57775 | Milan Mac mini 429 | Hetzner 429 RA=57797

One countdown, three continents: a single app-wide counter. Meanwhile, from the
same machines and the same credentials:

    /artists/{id}          200      top-tracks           403
    /artists/{id}/albums   200      artists?ids= (batch) 403
    /albums/{id}/tracks    200      tracks?ids=  (batch) 403
                                    related-artists      403
                                    audio-features       403
                                    recommendations      404   playlists 404

That is Spotify's November 2024 Development Mode restriction, not a rate limit
we can pace our way around. `lib/spotify_gate`'s founding comment — "the 24h
lockouts are caused by BURSTING, not by total volume" — is wrong, and pacing at
0.5 calls/s never helped because the dial was not connected to anything.

ingest_mb_artists resolved every artist by NAME through /search, so it died with
it. It now walks albums → tracks from the spotify_id already stored on the row
(27,253 of 119,571 queued artists have one). Verified on prod the same day:
ingested=11/15, zero search errors, zero 429s.

AMENDED 2026-08-18: "a single app-wide counter" was over-read from that
evidence. Four calls on one token the same second — artists 200, artists/albums
429 RA=78769, albums/tracks 200, **search 200** — say the ban is PER ENDPOINT
and rotates. The Nov-2024 restriction is real for the 403/404 endpoints above;
the 429s are not app-wide, and treating them as such quarantined services that
were serving. See tests/test_spotify_cooldown_is_per_endpoint.py.

    python3 tests/test_ingest_without_search.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402


def _src(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def _code_only(src: str) -> str:
    """Strip comments and docstrings — both quote the banned endpoints by name."""
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    src = re.sub(r"(?m)^\s*#.*$", "", src)
    return src


BANNED = {
    "search": "429, ~16h app-wide bans (Nov 2024 Dev Mode)",
    "top-tracks": "403 — this was the ORIGINAL endpoint, blocked before search",
    "related-artists": "403",
    "audio-features": "403",
    "recommendations": "404",
}


def test_the_ingest_path_never_calls_search():
    """The defect: one banned endpoint took the whole scraper down."""
    code = _code_only(_src("scripts/ingest_mb_artists.py"))
    for pat in ("sp.search(", "/search", "type=\"track\"", "type='track'"):
        assert pat not in code, (
            f"ingest must not use {pat!r} — /search is 429 for ~16h at a time "
            f"and cannot be paced around")


def test_the_ingest_path_uses_only_endpoints_that_still_answer():
    code = _code_only(_src("scripts/ingest_mb_artists.py"))
    # Matched on the METHOD, not on how the client is spelled. The client was
    # a module-level `sp` until it had to be made lazy — building it at import
    # meant a clean checkout could not load the module at all — and pinning the
    # receiver here turned that fix into a test failure about nothing.
    assert ".artist_albums(" in code, "albums is the entry point that works"
    assert ".album_tracks(" in code, "album tracks is where the tracks come from"
    for banned, why in BANNED.items():
        assert banned.replace("-", "_") not in code.replace("-", "_") or banned == "search", (
            f"{banned} is unusable: {why}")


def test_the_album_walk_is_bounded():
    """This replaced ONE search call per artist. Unbounded, it would become a
    catalogue crawl per artist and reintroduce the volume problem for real."""
    code = _src("scripts/ingest_mb_artists.py")
    scan = re.search(r"ALBUM_SCAN\s*=\s*(\d+)", code)
    page = re.search(r"ALBUM_PAGE\s*=\s*(\d+)", code)
    assert scan and page, "the bounds must be explicit and named"
    assert 0 < int(scan.group(1)) <= 5, (
        "at most 1 + ALBUM_SCAN calls per artist; keep it single digits")
    assert 0 < int(page.group(1)) <= 50


def test_a_dead_end_artist_leaves_the_queue():
    """Without this the queue never advances.

    Measured 2026-07-31: the same 200 artists re-selected every 30 minutes
    forever, ~9,600 wasted searches a day, while 26,335 never-tried artists
    queued behind them. The new failure modes need the same treatment.
    """
    code = _src("scripts/ingest_mb_artists.py")
    terminal = code[code.index("TERMINAL_ERRORS = ("):]
    terminal = terminal[:terminal.index(")")]
    for verdict in ("artist_has_no_albums", "all_album_tracks_were_trash",
                    "no_spotify_id"):
        assert verdict in terminal, (
            f"{verdict} is a verdict, not an accident — it must retire the row")


def test_a_transient_failure_does_NOT_leave_the_queue():
    """The mirror of the rule above: one network blip must not retire an artist
    permanently."""
    code = _src("scripts/ingest_mb_artists.py")
    terminal = code[code.index("TERMINAL_ERRORS = ("):]
    terminal = terminal[:terminal.index(")")]
    # Exact entries, not substrings: `no_spotify_id` contains "spotify_" and a
    # substring check failed this test against correct code.
    entries = set(re.findall(r'"([^"]+)"', terminal))
    for transient in ("network", "spotify_429_brief", "spotify_5xx",
                      "spotify_500", "spotify_502", "spotify_503"):
        assert transient not in entries, (
            f"{transient!r} is transient and must stay retryable — one network "
            f"blip would otherwise retire an artist permanently")
    assert entries, "TERMINAL_ERRORS should not be empty"


def test_the_health_probe_asks_the_endpoint_the_run_is_about_to_use():
    """Probe what the work needs, not what it used to need.

    This test used to read "no /search anywhere in spotify_health" — right
    answer, wrong invariant, and it went on passing while the SAME mistake
    reappeared one endpoint over: the probe asked `/artists/{id}` on behalf of
    a run whose every call was `/artists/{id}/albums`. Spotify bans one
    endpoint at a time (measured 2026-08-18: albums 429 for 21.9h while
    artists, album_tracks and search all answered 200), so what must hold is
    that each family probes ITS OWN path and no run probes a family it never
    calls.
    """
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from lib import spotify_health as H
    for family, url in H.PROBE_URLS.items():
        assert H.endpoint_family(url) == family, (
            f"{family}'s probe URL actually belongs to "
            f"{H.endpoint_family(url)} — the 2026-08-18 bug, one door along")
    ingest = _code_only(_src("scripts/ingest_mb_artists.py"))
    families = re.search(r"pre_flight_or_exit\([^)]*\)", ingest, re.S).group(0)
    assert "artist_albums" in families and "album_tracks" in families, (
        "the ingest must pre-flight the two endpoints it cannot work without")
    assert "search" not in families, (
        "a /search probe on a run that never calls /search is a stop sign "
        "that blocks work Spotify is happily serving")


def test_a_429_still_ends_the_run_rather_than_retrying():
    """Retrying reset Spotify's counter and kept us perpetually locked out."""
    code = _code_only(_src("scripts/ingest_mb_artists.py"))
    assert "record_429" in code and "SystemExit(0)" in code, (
        "a real lockout must be persisted and the run ended, not retried")
    sites = re.findall(r"_abort_if_locked_out\(e,\s*\"([a-z_]+)\"\)", code)
    assert len(sites) >= 3, (
        "every call site (both album pages and the track walk) must honour a "
        "lockout; one that does not would keep calling straight through a ban")
    assert set(sites) == {"artist_albums", "album_tracks"}, (
        f"each site must name the endpoint that got banned, got {sites} — "
        "recording the wrong family quarantines an endpoint that is serving")


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except AssertionError as exc:
                failed += 1
                print(f"FAIL {name}: {exc}")
            except Exception as exc:                       # noqa: BLE001
                failed += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    if failed:
        print(f"\n{failed} failed")
        sys.exit(1)
    print("all search-free-ingest checks passed")
