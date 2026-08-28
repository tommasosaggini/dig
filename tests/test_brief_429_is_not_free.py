"""A 429 with no Retry-After is still a 429, and a run may not absorb many.

WHAT THIS GUARDS, AND WHAT IT COST

`_abort_if_locked_out` handles two kinds of 429. A long one (Retry-After above
a minute) is recorded and ends the run — that half was always right. A short
one used to be handled like this:

    print(f"  rate limited, waiting {wait}s once")
    time.sleep(wait)
    return {"_error": "spotify_429_brief"}

Spotify sends its soft warning with **no Retry-After header at all**, so `wait`
is 0, so that `time.sleep(0)` is not a backoff — it is a no-op. A run that met
the warning answered it by asking again immediately, at network speed, with
nothing counting how often.

Measured, in mb_ingest.log at line 82258:

    === DONE ===  ingested=0, errors={'spotify_429_brief': 181}, elapsed=112.0s
    [spotify-health] ingest_mb_artists: app rate-limited, Retry-After=79200s

181 brief 429s in 112 seconds, nothing ingested, and the first app-wide 22-hour
ban this project had ever taken. The regime change is visible either side of
that line: every run before it succeeded hourly with no cooldown; every run
after has managed one success per ~11-12 hourly slots. One run of hammering
cost the lane roughly an order of magnitude of throughput, and it did not
recover on its own.

So two properties, and neither is optional:

  1. A brief 429 sleeps for a REAL interval even when Spotify names no number.
  2. A run that keeps meeting them stops, because a run that is mostly 429s is
     not making progress — it is only deepening the hole it is in.

    python3 tests/test_brief_429_is_not_free.py
    pytest tests/test_brief_429_is_not_free.py
"""
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _module():
    """Load the script fresh, so the per-run 429 counter starts at zero."""
    spec = importlib.util.spec_from_file_location(
        "_ingest_mb_artists_under_test",
        os.path.join(ROOT, "scripts", "ingest_mb_artists.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class _Exc:
    """A 429 as Spotify actually sends it — headers optional."""

    def __init__(self, headers=None):
        self.headers = headers if headers is not None else {}


def _instrument(m):
    """Replace the two side effects with recorders. Returns (naps, recorded)."""
    naps, recorded = [], []
    m.time.sleep = lambda s: naps.append(s)

    import lib.spotify_health as H
    H.record_429 = lambda wait, family=None: recorded.append((wait, family))
    return naps, recorded


def test_a_brief_429_without_a_header_still_backs_off():
    """`Retry-After: 0` meant sleep(0), which is not a wait at all."""
    m = _module()
    naps, _ = _instrument(m)

    out = m._abort_if_locked_out(_Exc(), "artist_albums")

    assert out == {"_error": "spotify_429_brief"}, out
    assert naps, "a brief 429 slept for nothing at all"
    assert naps[0] >= m.BRIEF_429_MIN_SLEEP, (
        f"slept {naps[0]}s, floor is {m.BRIEF_429_MIN_SLEEP}s")


def test_repeated_brief_429s_back_off_further_each_time():
    """Asking again at the same rate is what escalates a warning to a ban."""
    m = _module()
    naps, _ = _instrument(m)

    for _ in range(4):
        m._abort_if_locked_out(_Exc(), "artist_albums")

    assert naps == sorted(naps), f"backoff did not grow: {naps}"
    assert naps[-1] > naps[0], f"backoff was flat: {naps}"
    assert max(naps) <= m.BRIEF_429_MAX_SLEEP, (
        f"backoff blew past its ceiling: {naps}")


def test_a_run_stops_instead_of_absorbing_181_of_them():
    """The measured failure: 181 in 112s, then a 22-hour ban."""
    m = _module()
    _, recorded = _instrument(m)

    stopped_after = None
    for i in range(1, 200):
        try:
            m._abort_if_locked_out(_Exc(), "artist_albums")
        except SystemExit as e:
            assert e.code == 0, f"a stand-down must exit clean, got {e.code}"
            stopped_after = i
            break

    assert stopped_after is not None, (
        "the run absorbed 199 brief 429s without stopping — this is exactly "
        "the loop that bought a 22-hour ban")
    assert stopped_after <= m.BRIEF_429_BUDGET + 1, (
        f"stopped only after {stopped_after}, budget is {m.BRIEF_429_BUDGET}")
    assert recorded, "stood down without telling the other jobs to hold off"
    assert recorded[-1][0] >= m.BRIEF_429_STAND_DOWN, recorded
    assert recorded[-1][1] == "artist_albums", (
        "recorded the cooldown against the wrong endpoint family, which "
        f"quarantines a service that is answering fine: {recorded[-1]}")


def test_a_long_retry_after_still_ends_the_run_immediately():
    """The half that was already right must stay right — no budget, no sleep."""
    m = _module()
    naps, recorded = _instrument(m)

    try:
        m._abort_if_locked_out(_Exc({"Retry-After": "79200"}), "artist_albums")
    except SystemExit as e:
        assert e.code == 0, e.code
    else:
        raise AssertionError("a 22-hour cooldown did not end the run")

    assert recorded == [(79200, "artist_albums")], recorded
    assert not naps, "slept while holding a 22-hour cooldown instead of exiting"


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"ok   {name}")
        except AssertionError as e:
            print(f"FAIL {name}: {e}")
            failed += 1
    print("all brief-429 checks passed" if not failed else f"\n{failed} failed")
    sys.exit(1 if failed else 0)
