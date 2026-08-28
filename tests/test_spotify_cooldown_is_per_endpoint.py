"""A ban on one Spotify endpoint must not stop the endpoints that still work.

Measured 2026-08-18 19:13 JST, one app token, four calls, same second:

    artists/{id}          200
    artists/{id}/albums   429   Retry-After = 78769   (~21.9h)
    albums/{id}/tracks    200
    search                200

Before this, `record_429` wrote ONE global `cooldown_until` and the pre-flight
probe asked `/artists/{id}` — an endpoint that is never the one under load. Two
consequences, both visible in 24 days of production log: 552 runs printed
"pre-flight OK" and 235 of them died on their first `artist_albums` call (a 40%
abort rate), and every 22h album ban also silenced the likes sync, whose
`me/library` calls were answering 200 throughout.

    python3 tests/test_spotify_cooldown_is_per_endpoint.py
    pytest tests/test_spotify_cooldown_is_per_endpoint.py
"""
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import spotify_health as H  # noqa: E402


def _fresh_cache():
    """Point the module at an empty cache file for the duration of one test."""
    fd, path = tempfile.mkstemp(prefix="dig_spotify_health_test_", suffix=".json")
    os.close(fd)
    os.unlink(path)
    H.CACHE_PATH = path
    return path


def test_the_album_walk_is_a_different_family_from_the_artist_lookup():
    # The whole bug in one assertion: these two are not the same endpoint, and
    # a probe of the second says nothing about the first.
    assert H.endpoint_family(
        "https://api.spotify.com/v1/artists/0TnO/albums?limit=10") == "artist_albums"
    assert H.endpoint_family(
        "https://api.spotify.com/v1/artists/0TnO") == "artists"
    assert H.endpoint_family(
        "https://api.spotify.com/v1/albums/4aaw/tracks?limit=50") == "album_tracks"
    assert H.endpoint_family(
        "https://api.spotify.com/v1/search?q=a&type=artist") == "search"
    assert H.endpoint_family(
        "https://api.spotify.com/v1/me/library/contains?uris=x") == "library"
    assert H.endpoint_family(
        "https://api.spotify.com/v1/me/player/play") == "player"


def test_an_unknown_path_gets_its_own_bucket_not_somebody_elses():
    # An endpoint we have never seen must not inherit another family's ban,
    # and must not become a second global cooldown either.
    fam = H.endpoint_family("https://api.spotify.com/v1/audiobooks/123/chapters")
    assert fam not in ("artists", "artist_albums", "album_tracks", "other")
    assert H.endpoint_family("") == "other"
    assert H.endpoint_family(None) == "other"


def test_banning_one_family_leaves_the_others_serving():
    _fresh_cache()
    H.record_429(78769, "artist_albums")
    assert H.cooldown_for("artist_albums") > 78000
    # The three that were measured at 200 during that exact ban:
    assert H.cooldown_for("album_tracks") == 0
    assert H.cooldown_for("artists") == 0
    assert H.cooldown_for("library") == 0, (
        "a 22h album ban used to silence the likes sync for the full window")


def test_recording_one_family_does_not_erase_another():
    _fresh_cache()
    H.record_429(600, "search")
    H.record_429(78769, "artist_albums")
    assert H.cooldown_for("search") > 0
    assert H.cooldown_for("artist_albums") > 0


def test_a_legacy_global_cooldown_is_ignored_rather_than_applied_to_everything():
    # Files written before 2026-08-18 carry a bare `cooldown_until` with no
    # record of WHICH endpoint earned it. Honouring it globally is precisely
    # the behaviour being removed, so it is dropped and re-derived per family.
    path = _fresh_cache()
    with open(path, "w") as f:
        json.dump({"cooldown_until": int(time.time()) + 80000,
                   "last_probe_ok": False}, f)
    assert H.cooldown_for("artist_albums") == 0
    assert H.cooldown_for("library") == 0


def test_pre_flight_probes_the_families_the_run_actually_needs():
    _fresh_cache()
    asked = []

    real_probe = H.probe
    H.probe = lambda family=H.DEFAULT_FAMILY: (asked.append(family), (True, 0))[1]
    try:
        H.pre_flight_or_exit("ingest_mb_artists",
                             families=["artist_albums", "album_tracks"],
                             verbose=False)
    finally:
        H.probe = real_probe
    assert asked == ["artist_albums", "album_tracks"], asked
    assert "artists" not in asked, (
        "probing artists/{id} is what made the green pre-flight meaningless")


def test_pre_flight_exits_zero_when_a_needed_family_is_banned():
    _fresh_cache()
    H.record_429(78769, "artist_albums")
    try:
        H.pre_flight_or_exit("ingest_mb_artists",
                             families=["artist_albums", "album_tracks"],
                             verbose=False)
    except SystemExit as e:
        assert e.code == 0, "cron must read a lockout as a no-op, not a failure"
    else:
        raise AssertionError("a banned work endpoint must stop the run")


def test_a_user_token_family_is_never_declared_down_by_an_app_token_probe():
    # `me/library` and `me/player` cannot be probed with client credentials.
    # Unprobeable must mean "unknown, let the real call decide" — never
    # "healthy" by omission and never "banned" by a 401 the probe misread.
    _fresh_cache()
    assert "library" not in H.PROBE_URLS
    assert H.probe("library") == (True, 0)


def test_the_gate_reads_the_family_off_the_url_it_is_about_to_call():
    from lib.spotify_gate import cooldown_remaining
    _fresh_cache()
    H.record_429(78769, "artist_albums")
    assert cooldown_remaining("artist_albums") > 0
    assert cooldown_remaining("album_tracks") == 0


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: per-endpoint cooldown passed ({len(fns)} tests)")
