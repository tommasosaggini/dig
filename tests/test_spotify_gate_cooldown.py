"""The Spotify cooldown must reflect the LONGEST window Spotify quoted.

Spotify hands out per-endpoint rate-limit windows. The health probe asks a
cheap endpoint (/v1/artists/{id}); the caller that just got 429'd may have been
using a different one. Measured 2026-08-07: a real call reported

    Your application has reached a rate/request limit.
    Retry will occur after: 11672 s

while the probe endpoint reported 273. Recording the probe's number let the
gate declare itself clear over three hours early, so the next run walked into
another 429 and re-armed the lockout — the opposite of what the gate is for.

    python3 tests/test_spotify_gate_cooldown.py
    pytest tests/test_spotify_gate_cooldown.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.spotify_gate import _retry_after_from_exception  # noqa: E402


class _Exc:
    """Stands in for spotipy.SpotifyException without importing spotipy."""

    def __init__(self, msg="", headers=None):
        self.msg = msg
        self.headers = headers or {}

    def __str__(self):
        return self.msg


def test_reads_the_number_out_of_the_message_when_the_message_has_one():
    # Kept because some spotipy paths DO build this text — but note it is not
    # the path that fires on a lockout. See the sniffer tests below: this test
    # passing is exactly why the real bug stayed invisible, because it asserts
    # a message shape the RetryError path never produces.
    e = _Exc("Your application has reached a rate/request limit. "
             "Retry will occur after: 11672 s")
    assert _retry_after_from_exception(e) == 11672


def test_prefers_the_header_when_there_is_one():
    assert _retry_after_from_exception(_Exc("whatever", {"Retry-After": "900"})) == 900
    # Header casing varies by proxy.
    assert _retry_after_from_exception(_Exc("x", {"retry-after": "42"})) == 42


def test_returns_zero_when_spotify_said_nothing():
    # "Max Retries reached" carries no number; the caller then falls back to
    # the probe and the floor, rather than recording a bogus 0-second cooldown.
    assert _retry_after_from_exception(_Exc("Max Retries reached")) == 0
    assert _retry_after_from_exception(_Exc("")) == 0


def test_never_raises_out_of_the_handler():
    # It runs inside the except block that records the lockout; throwing there
    # would lose the cooldown entirely and let the next run burst again.
    class Hostile:
        @property
        def headers(self):
            raise RuntimeError("boom")

        def __str__(self):
            raise RuntimeError("boom")

    assert _retry_after_from_exception(Hostile()) == 0
    assert _retry_after_from_exception(None) == 0


# ── the log sniffer ─────────────────────────────────────────────────────────
# THE REAL LOCKOUT PATH. With retries=0 urllib3 raises MaxRetryError on the
# first 429, discarding the response; requests wraps it as RetryError; spotipy
# raises SpotifyException(429, -1, "<path>:\n Max Retries", reason=…) with no
# headers and no number. Both branches above then return 0 and record_429
# falls to its 300s floor. Measured 2026-08-17: Spotify asked for 45,599s, the
# gate armed 300s, declared itself clear in five minutes, and the next caller
# re-armed the lockout. The number survives only in spotipy's log line.


def test_the_sniffer_catches_what_spotipy_logs():
    import logging
    from lib.spotify_gate import _SNIFFER
    logging.getLogger("spotipy.util").warning(
        "Your application has reached a rate/request limit. "
        "Retry will occur after: 45599 s")
    assert _SNIFFER.recent() == 45599


def test_the_real_lockout_exception_carries_nothing():
    # Verbatim shape of what spotipy raises on the RetryError path. If this
    # ever starts returning a number, spotipy changed and the sniffer can go.
    e = _Exc("/v1/search?q=x&limit=1&offset=0&type=track:\n Max Retries")
    assert _retry_after_from_exception(e) == 0


def test_a_stale_sniffed_number_is_not_reused():
    # The handler is process-global and lives for the life of the process, so
    # a number logged an hour ago must not be attributed to today's 429 — that
    # would arm a cooldown from an unrelated call.
    from lib.spotify_gate import _SNIFFER
    _SNIFFER.seconds, _SNIFFER.at = 9999, 0.0     # logged at the epoch
    assert _SNIFFER.recent() == 0


def test_the_sniffer_never_raises_into_spotipy():
    # It is a logging handler firing during a lockout; an exception here would
    # break the very call it is observing.
    from lib.spotify_gate import _SNIFFER

    class Hostile:
        levelno = 30

        def getMessage(self):
            raise RuntimeError("boom")

    _SNIFFER.emit(Hostile())        # must not raise
    _SNIFFER.seconds, _SNIFFER.at = 0, 0.0


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: spotify cooldown parsing passed ({len(fns)} tests)")
