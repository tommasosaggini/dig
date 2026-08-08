"""Regression lock for "the song ended and nothing played next" — the 502 half.

`PUT /me/player/play` answers Spotify's own

    {"error": {"status": 502, "message": "Bad gateway."}}

a few times a week on this account. The server relayed it and stopped. Measured
over the five days to 2026-08-07, seven of them, every one returning in
250-270ms — a gateway refusing the command, not a device thinking about it.

The damage is worst behind a transfer, because the transfer sends `play:false`
and has therefore already PAUSED the device. 2026-08-07 11:58:02 is the whole
shape in three lines of the container log:

    11:57:22  play ok            transfer 1045ms → 204, play 204
    11:58:02  play FAILED        transfer 2893ms → 204, play 502 in 257ms
    11:59:41  play ok            — 99 seconds later, a different track

Nothing between: the device sat paused until the listener picked the phone up.
The client does believe these are transient and retries once, but it defers
that retry while the page is hidden — which is exactly the backgrounded phone
that cannot rescue itself.

    python3 tests/test_play_retries_spotify_5xx.py
    pytest tests/test_play_retries_spotify_5xx.py
"""
import os
import sys
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import server  # noqa: E402

retry = server._issue_with_transient_retry


def _http_error(code):
    return urllib.error.HTTPError("https://api.spotify.com/v1/me/player/play",
                                  code, "boom", {}, None)


class _Issue:
    """A play call that fails with `codes` in order, then succeeds."""

    def __init__(self, *codes):
        self.codes = list(codes)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self.codes:
            raise _http_error(self.codes.pop(0))
        return "played"


def _no_sleep(_seconds):
    pass


def test_a_transient_502_is_retried_and_the_music_starts():
    issue = _Issue(502)
    assert retry(issue, sleep=_no_sleep) == "played"
    assert issue.calls == 2, "the 502 must cost a retry, not the song"


def test_500_and_503_count_as_transient_too():
    for code in (500, 503):
        issue = _Issue(code)
        assert retry(issue, sleep=_no_sleep) == "played"
        assert issue.calls == 2


def test_a_404_is_an_answer_and_is_never_retried():
    # "No active device found" is handled by the caller's own wake-and-reissue
    # recovery. Retrying it here would burn the round-trip before that runs.
    issue = _Issue(404)
    try:
        retry(issue, sleep=_no_sleep)
    except urllib.error.HTTPError as e:
        assert e.code == 404
    else:
        raise AssertionError("a 404 must propagate")
    assert issue.calls == 1


def test_a_403_or_429_is_a_verdict_not_a_blip():
    for code in (403, 429):
        issue = _Issue(code)
        try:
            retry(issue, sleep=_no_sleep)
        except urllib.error.HTTPError as e:
            assert e.code == code
        else:
            raise AssertionError(f"{code} must propagate")
        assert issue.calls == 1, f"{code} must not be retried"


def test_it_gives_up_rather_than_hammering_spotify():
    # A gateway that is properly down must not turn one play into a storm.
    issue = _Issue(502, 502, 502, 502)
    try:
        retry(issue, sleep=_no_sleep)
    except urllib.error.HTTPError as e:
        assert e.code == 502
    else:
        raise AssertionError("a persistent 502 must still surface")
    assert issue.calls == server._PLAY_5XX_ATTEMPTS == 2


def test_the_retry_is_announced():
    seen = []
    issue = _Issue(502)
    retry(issue, on_retry=lambda attempt, code: seen.append((attempt, code)),
          sleep=_no_sleep)
    # A night that sounded fine BECAUSE of the retry has to be tellable from one
    # that never needed it; silence here would hide the upstream getting worse.
    assert seen == [(1, 502)]


def test_it_waits_before_reissuing():
    slept = []
    retry(_Issue(502), sleep=slept.append)
    assert slept == [server._PLAY_5XX_BACKOFF_S]
    assert 0 < server._PLAY_5XX_BACKOFF_S <= 1, (
        "the caller is a listener waiting in silence — the pause has to be short"
    )


def test_a_clean_play_costs_nothing():
    issue = _Issue()
    assert retry(issue, sleep=_no_sleep) == "played"
    assert issue.calls == 1


def test_the_play_handler_actually_uses_it():
    # The helper being right is worthless if /api/play still calls _issue_play
    # directly. Both call sites — the first attempt and the post-404 reissue —
    # have to go through the retry.
    with open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "server.py"), encoding="utf-8") as fh:
        src = fh.read()
    body = src[src.index('if parsed.path == "/api/play":'):]
    body = body[:body.index('if parsed.path == "/api/queue":')]
    code = "\n".join(ln for ln in body.splitlines() if not ln.lstrip().startswith("#"))
    assert "_issue_with_transient_retry" in code, "the handler must use the retry"
    direct = [ln.strip() for ln in code.splitlines()
              if "_issue_play(" in ln and "def _issue_play" not in ln
              and "lambda" not in ln]
    assert not direct, f"these call _issue_play without the retry: {direct}"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all good")
