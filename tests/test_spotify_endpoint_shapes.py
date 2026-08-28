"""The Spotify endpoints DIG mirrors saves through, pinned by name.

Every save DIG ever made failed to reach Spotify, silently, for months. Not one
playlist was created across 3,830 accounts. The cause was not a scope, a token,
a quota or Development Mode — all of which were proposed and all of which were
wrong. It was five URLs that Spotify has replaced:

    retired (403 Forbidden)                     current
    POST   /users/{id}/playlists                POST   /me/playlists
    POST   /playlists/{id}/tracks {"uris":[…]}  POST   /playlists/{id}/items ["uri"]
    DELETE /playlists/{id}/tracks {"tracks":…}  DELETE /playlists/{id}/items {"items":…}
    PUT    /me/tracks {"ids":[…]}               PUT    /me/library?uris=…
    GET    /me/tracks/contains?ids=             GET    /me/library/contains?uris=

Three renames, applied consistently: /me/tracks → /me/library, /tracks →
/items, ids → uris.

WHY A TEST AND NOT JUST A FIX. The retired endpoints answer `403 Forbidden`
with an empty message, which is indistinguishable from a missing scope — and
`mirror_save` logs failures and swallows them ON PURPOSE, because a Spotify
hiccup must never surface as a failed like. So the only thing standing between
a wrong URL and another silent months-long outage is this file.

It asserts on source text rather than making live calls: the live check needs a
user token and would write to a real Spotify account. The names are the part
that rots.

    python3 tests/test_spotify_endpoint_shapes.py
    pytest tests/test_spotify_endpoint_shapes.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _server_source():
    with open(os.path.join(ROOT, "server.py"), encoding="utf-8") as fh:
        return fh.read()


def _code_only(src):
    """`src` with comments stripped — the comment block above
    _spotify_library_call names every retired endpoint in order to explain it,
    and a bare substring search would find the explanation and fail on it."""
    return re.sub(r"(?m)^\s*#.*$", "", src)


# (needle, what it is, what went wrong when it was the other one)
RETIRED = [
    ('v1/users/{', 'POST /users/{id}/playlists',
     'the DIG playlist could never be created — 20 of 20 attempts 403'),
    ('/tracks"', 'a /playlists/{id}/tracks URL',
     'adding a saved track to the playlist 403d'),
    ('v1/me/tracks"', 'PUT|DELETE /me/tracks',
     'saving to Liked Songs 403d'),
    ('me/tracks/contains', 'GET /me/tracks/contains',
     'checking whether a track is saved 403d'),
]


def test_no_retired_endpoint_comes_back():
    code = _code_only(_server_source())
    for needle, what, consequence in RETIRED:
        assert needle not in code, (
            f"server.py is calling {what} again. Spotify retired it: {consequence}. "
            "The failure is SILENT — mirror_save swallows it — so nothing will tell "
            "you but this test."
        )


def test_the_current_endpoints_are_the_ones_used():
    code = _code_only(_server_source())
    for needle, what in (
        ("v1/me/playlists", "POST /me/playlists (create the DIG playlist)"),
        ("/items", "/playlists/{id}/items (add and remove)"),
        ("v1/me/library", "PUT|DELETE /me/library (Liked Songs)"),
    ):
        assert needle in code, f"server.py no longer calls {what}"


def test_the_two_item_verbs_keep_their_different_bodies():
    """POST takes a bare array, DELETE takes {"items": [...]}. Sending one
    shape to both verbs is a 400 that looks like every other mirror failure."""
    code = _code_only(_server_source())
    assert re.search(r"json\.dumps\(\[uri\]\)", code), (
        'adding an item posts a BARE ARRAY — ["spotify:track:…"] — not '
        '{"uris": [...]}, which is the retired body'
    )
    assert re.search(r'json\.dumps\(\{"items":', code), (
        'removing an item sends {"items": [{"uri": …}]}, not the retired '
        '{"tracks": [{"uri": …}]}'
    )


def test_the_library_uri_is_a_uri_and_is_escaped():
    """/me/library takes `uris=spotify:track:<id>` in the QUERY STRING — not a
    body, and not a bare id. The colons must survive quoting."""
    code = _code_only(_server_source())
    assert "me/library?uris=" in code, "the uri goes in the query string"
    assert re.search(r'quote\(f?"spotify:track:', code), (
        "the uri must be percent-escaped into the query string"
    )


def main():
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    print("\nall good" if not failures else f"\n{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
