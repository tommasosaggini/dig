"""A track Bandcamp took down is not dig's server breaking.

`/api/bandcamp/resolve` answered 502 for EVERY failure, so a track that had
simply been removed came back as a gateway error — retryable-looking, and filed
in the TrustBuild health digest as six sev-4 rows against `dig`. All three ids
in those rows resolve to the same thing, checked against the live API on
2026-08-15:

    bc:1246633546:1275395674  -> {'ok': False, 'error': 'no_tracks'}
    bc:4279892106:2482947114  -> {'ok': False, 'error': 'no_tracks'}
    bc:419397733:2001009658   -> {'ok': False, 'error': 'no_tracks'}

`no_tracks` is Bandcamp answering: the identity is fine, the music is gone.
No retry changes that, and 502 says the opposite.

The player never read the status — it branches on `ok` and advances the queue —
so this changes what the server SAYS, not what playback does.

    python3 tests/test_resolve_status_is_honest.py
    pytest tests/test_resolve_status_is_honest.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SERVER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "server.py")

# What Bandcamp says when the track is gone for good vs when the road there is
# blocked. lib/bandcamp.resolve_stream is the only producer of these strings.
PERMANENT = ("no_tracks", "not_streamable", "http_404", "http_403", "http_410")
TRANSIENT = ("api_error", "fetch_URLError", "fetch_TimeoutError", "http_500", "http_502")


def _handler_source(marker: str, end: str) -> str:
    with open(SERVER, encoding="utf-8") as fh:
        src = fh.read()
    body = src[src.index(marker):]
    return body[:body.index(end)]


def test_permanent_failures_are_named_in_the_handler():
    code = _handler_source('if parsed.path == "/api/bandcamp/resolve":',
                           'if parsed.path == "/api/soundcloud/resolve":')
    assert "404" in code, "a removed track must not answer 502"
    for err in PERMANENT:
        assert err in code, f"{err} is permanent — it belongs in the 404 set"


def test_transient_failures_keep_502():
    """502 has to stay for the case it was written for, or a real outage goes
    quiet in the digest — the opposite mistake, and the more expensive one."""
    code = _handler_source('if parsed.path == "/api/bandcamp/resolve":',
                           'if parsed.path == "/api/soundcloud/resolve":')
    assert "502" in code
    for err in TRANSIENT:
        assert err not in code, f"{err} may pass on its own; it must not be a 404"


def test_soundcloud_says_the_same_thing():
    code = _handler_source('if parsed.path == "/api/soundcloud/resolve":',
                           "# ── Static data files")
    assert "no_hls_url" in code and "404" in code, "same shape, same honesty"


def test_the_pool_identity_is_still_a_400():
    """A malformed id is the CALLER being wrong, and stays 400 — the three
    codes have to keep meaning three different things."""
    code = _handler_source('if parsed.path == "/api/bandcamp/resolve":',
                           'if parsed.path == "/api/soundcloud/resolve":')
    assert '"bad_id"' in code and "400" in code


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all good")
