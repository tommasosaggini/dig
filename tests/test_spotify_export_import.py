"""The past can only come from Spotify's export, so the parser has to be right.

/me/player/recently-played returns FIFTY items and nothing else — paging back
with `before` returns an empty page immediately (measured 2026-08-03). Every
play older than that window exists in exactly one place: the privacy export.
There is no second chance to re-read it and no API to check it against, so the
field mapping is verified here against both documented shapes rather than
discovered when a 400 MB download lands.

    python3 tests/test_spotify_export_import.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import importlib   # noqa: E402
E = importlib.import_module("scripts.import_spotify_export")


EXTENDED = {
    "ts": "2024-01-15T20:31:00Z",
    "platform": "ios",
    "ms_played": 214000,
    "master_metadata_track_name": "Hey Baby",
    "master_metadata_album_artist_name": "Stephen Marley",
    "master_metadata_album_album_name": "Mind Control",
    "spotify_track_uri": "spotify:track:4uLU6hMCjMI75M1A2tKUQC",
    "reason_end": "trackdone",
}

ACCOUNT = {
    "endTime": "2024-01-15 20:31",
    "artistName": "Stephen Marley",
    "trackName": "Hey Baby",
    "msPlayed": 214000,
}


def test_the_extended_export_yields_a_real_spotify_id():
    """The extended export is the one worth waiting a month for, precisely
    because spotify_track_uri joins straight onto DIG's existing rows."""
    p = E.parse_play(EXTENDED)
    assert p["track_id"] == "4uLU6hMCjMI75M1A2tKUQC"
    assert p["name"] == "Hey Baby" and p["artist"] == "Stephen Marley"
    assert p["ms_played"] == 214000
    assert p["ts_ms"] == 1705350660000


def test_the_account_export_is_read_at_minute_precision_as_utc():
    """'2024-01-15 20:31' carries no offset. Guessing one would shift every
    timestamp in the file by whole hours; UTC at least is stated, not invented."""
    p = E.parse_play(ACCOUNT)
    assert p["track_id"] is None
    assert p["ts_ms"] == 1705350660000
    assert p["ms_played"] == 214000


def test_podcast_rows_are_dropped():
    """Episodes carry spotify_episode_uri and null track metadata. A music
    ledger that silently absorbed them would report listening that never
    happened to any song."""
    assert E.parse_play({
        "ts": "2024-01-15T20:31:00Z", "ms_played": 900000,
        "master_metadata_track_name": None,
        "master_metadata_album_artist_name": None,
        "spotify_episode_uri": "spotify:episode:abc",
    }) is None
    assert E.parse_play({}) is None
    assert E.parse_play(None) is None
    assert E.parse_play({"endTime": "not-a-date", "artistName": "X",
                         "trackName": "Y", "msPlayed": 1}) is None


def test_a_local_file_play_keeps_its_name_but_gets_no_spotify_id():
    """Local files appear with a null uri. Inventing an id would tie the play
    to whatever track happened to hash the same."""
    p = E.parse_play(dict(EXTENDED, spotify_track_uri=None))
    assert p is not None and p["track_id"] is None


def test_plays_collapse_to_one_row_per_track():
    """user_history is one row per track, and the unique key makes that a hard
    constraint: a batch carrying the same id twice aborts entirely."""
    rows = E.collapse_plays([
        {"ts_ms": 100, "track_id": "t1", "name": "A", "artist": "X", "ms_played": 200000},
        {"ts_ms": 300, "track_id": "t1", "name": "A", "artist": "X", "ms_played": 200000},
        {"ts_ms": 200, "track_id": "t2", "name": "B", "artist": "Y", "ms_played": 200000},
    ])
    assert len(rows) == 2
    t1 = next(r for r in rows if r["id"] == "t1")
    assert t1["listened_at"] == 300, "the row is dated by its most recent play"
    assert t1["plays"] == 3 - 1


def test_one_skip_cannot_erase_a_hundred_listens():
    """Status is the strongest evidence across all plays, not the last one."""
    rows = E.collapse_plays([
        {"ts_ms": 100, "track_id": "t1", "name": "A", "artist": "X", "ms_played": 200000},
        {"ts_ms": 900, "track_id": "t1", "name": "A", "artist": "X", "ms_played": 4000},
    ])
    assert rows[0]["status"] == "listened"
    assert rows[0]["listened_at"] == 900, "…while still being dated by the latest play"


def test_a_track_never_played_past_30s_is_a_skip():
    """Spotify's own stream threshold, reused so 'listened' means the same
    thing in DIG as it does in the export."""
    rows = E.collapse_plays([
        {"ts_ms": 100, "track_id": "t1", "name": "A", "artist": "X", "ms_played": 29999},
    ])
    assert rows[0]["status"] == "skipped"


def test_an_id_less_play_lands_on_the_pools_row_when_the_pool_knows_it():
    """The account-data export has no ids. Without this join every track DIG
    already has would be duplicated under a synthetic id."""
    rows = E.collapse_plays(
        [{"ts_ms": 100, "track_id": None, "name": "Hey Baby",
          "artist": "Stephen Marley", "ms_played": 200000}],
        resolve={"stephen marley - hey baby": "REALID"},
    )
    assert rows[0]["id"] == "REALID"
    assert rows[0]["source"] == "spotify"


def test_an_unresolvable_play_is_still_recorded_under_a_stable_key():
    """It was still heard, so it belongs in the ledger — but it needs a key, or
    every re-import duplicates it instead of merging."""
    play = {"ts_ms": 100, "track_id": None, "name": "Obscure",
            "artist": "Nobody", "ms_played": 200000}
    a = E.collapse_plays([dict(play)])
    b = E.collapse_plays([dict(play)])
    assert a[0]["id"] == b[0]["id"], "the id must be stable across runs"
    assert a[0]["id"].startswith("ext:")
    assert a[0]["source"] == "external", (
        "must not look like a Spotify id downstream — the album-art prefetch "
        "would POST it to /v1/tracks"
    )


def test_played_pct_is_never_invented():
    """The export gives ms_played but not track duration."""
    rows = E.collapse_plays([
        {"ts_ms": 100, "track_id": "t1", "name": "A", "artist": "X", "ms_played": 200000},
    ])
    assert rows[0]["played_pct"] is None


def test_a_directory_of_mixed_export_files_reads_only_the_play_files():
    """An export folder is mostly things that are not streaming history —
    Playlist1.json, Userdata.json, Marquee.json. A parser that choked on them,
    or absorbed them, would be discovered only after the month-long wait."""
    import json
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "Streaming_History_Audio_2024_0.json"), "w") as fh:
            json.dump([EXTENDED, EXTENDED], fh)
        with open(os.path.join(d, "Userdata.json"), "w") as fh:
            json.dump({"username": "x", "email": "y"}, fh)     # a dict, not a list
        with open(os.path.join(d, "Playlist1.json"), "w") as fh:
            json.dump({"playlists": []}, fh)
        with open(os.path.join(d, "broken.json"), "w") as fh:
            fh.write("{not json")
        plays, files = E.load_export(d)
    assert len(plays) == 2
    assert [name for name, _ in files] == ["Streaming_History_Audio_2024_0.json"]


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
    if failed:
        print(f"\n{failed} failed")
        sys.exit(1)
    print("all export-import checks passed")
