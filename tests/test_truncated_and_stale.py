"""Two failures that reported themselves as server crashes but were not.

Both were found in the platform health digest on 2026-07-31, filed as HTTP 500s
against dig, and both were already half-handled in the code:

  * `/history` guards a truncated POST body and says so in a comment — "Mobile
    networks can truncate large multi-hundred-KB POSTs mid-string" — but caught
    only json.JSONDecodeError. A cut lands mid-CHARACTER as often as
    mid-string, and then body.decode() raises UnicodeDecodeError first, before
    json sees anything. A 1.9 MB sync died at byte 1899900 with an unhandled
    500 and a stack trace: precisely the crash the guard exists to prevent.

  * `/api/resume` flattened every exception to 500, including Spotify's 404
    "Device not found" — a stale device id, which is the client's situation to
    handle and indistinguishable, in a health report, from a crash.

    python3 tests/test_truncated_and_stale.py
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SERVER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "server.py")
SRC = open(SERVER, encoding="utf-8").read()


# ── the truncation itself ─────────────────────────────────────────────────────

def test_a_mid_character_cut_raises_unicode_not_json():
    """The premise. If this ever stops holding, the guard can be narrowed.

    ensure_ascii=False matters: the client posts real UTF-8 (DIG's catalogue is
    full of it — the track that exposed this was Sơn Tùng M-TP), so the bytes on
    the wire are multi-byte and a cut can land inside one. With the default
    escaping there would be nothing but ASCII and no way to reproduce it.
    """
    body = json.dumps([{"t": "Sơn Tùng 東京"}], ensure_ascii=False).encode()
    assert max(body) > 127, "payload must contain real multi-byte characters"
    cut = body[:-4]                      # lands inside the trailing 京
    try:
        cut.decode()
    except UnicodeDecodeError as e:
        assert "unexpected end of data" in str(e)
    else:
        raise AssertionError("expected a UnicodeDecodeError from a mid-char cut")


def test_a_mid_string_cut_raises_json():
    body = json.dumps([{"t": "plain ascii"}]).encode()
    try:
        json.loads(body[:-3].decode())
    except json.JSONDecodeError:
        pass
    else:
        raise AssertionError("expected a JSONDecodeError from a mid-string cut")


# ── /history catches both ─────────────────────────────────────────────────────

def _history_guard() -> str:
    # Anchored on the POST handler's own marker: there are TWO "/history"
    # routes (a GET at ~1524 and this POST), and a plain .index() finds the
    # GET — which would have made every assertion below pass or fail for
    # reasons having nothing to do with the guard.
    i = SRC.index('_evt("history-sync", user="anon"')
    return SRC[i:i + 2500]


# The except CLAUSE, not the word — which also appears in the comment above it.
# Greping for the bare name passed against deliberately reverted code when this
# was mutation-checked, i.e. it was testing the documentation, not the handler.
_CLAUSE = re.compile(r"except \(\s*UnicodeDecodeError\s*,\s*json\.JSONDecodeError\s*\)")


def test_history_catches_the_unicode_case():
    assert _CLAUSE.search(_history_guard()), (
        "a truncated body fails in body.decode() before json.loads sees it — "
        "catching only JSONDecodeError leaves the 500 this block prevents"
    )


def test_history_still_catches_the_json_case():
    assert _CLAUSE.search(_history_guard()), (
        "the original guard must survive the widening, not be replaced by it"
    )


def test_history_answers_400_not_500():
    guard = _history_guard()
    assert '"malformed_payload"' in guard and ", 400)" in guard


def test_the_two_are_caught_together():
    """One except clause, not two paths that can drift apart."""
    assert re.search(r"except \(UnicodeDecodeError, json\.JSONDecodeError\)", SRC)


# ── /api/resume reports the real status ───────────────────────────────────────

def _resume_handler() -> str:
    i = SRC.index('if parsed.path == "/api/resume"')
    return SRC[i:i + 2200]


def test_resume_propagates_spotifys_status():
    h = _resume_handler()
    assert "spotipy.SpotifyException" in h, (
        "a device that has gone away is a 404 from Spotify, not a fault here"
    )
    assert "e.http_status" in h


def test_resume_marks_a_missing_device_the_way_play_does():
    """The client already knows what no_device means — /api/play sends it."""
    assert '"no_device"' in _resume_handler()
    play = SRC[SRC.index('if parsed.path == "/api/play"'):]
    play = play[:play.index('if parsed.path == "/api/resume"')]
    assert '"no_device"' in play, "the signal resume now reuses must exist in play"


def test_resume_keeps_a_real_500_for_a_real_fault():
    h = _resume_handler()
    assert "except Exception as e:" in h and ", 500)" in h, (
        "only Spotify's own 4xx should stop being a 500 — an actual bug here "
        "must still report as one"
    )


def test_a_5xx_from_spotify_is_not_reported_as_a_client_error():
    h = _resume_handler()
    assert "400 <=" in h and "< 500" in h, (
        "Spotify having an outage is not the caller's bad request"
    )


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("ok   %s" % name)
            except AssertionError as e:
                print("FAIL %s\n     %s" % (name, e))
                fails += 1
    sys.exit(1 if fails else 0)
