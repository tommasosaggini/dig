"""Unit tests for the NTS residency scraper (no network).

The fixture below is the real shape of an `/api/v2/shows/{show}/episodes/{alias}`
response, taken from `rotational-6th-july-2026` — entries carry an artist and
title, usually a deezer_track_id and an isrc_id, and sometimes only estimates
for offset/duration.

The case worth guarding is the last one: NTS answers a throttled client by
REMOVING the tracklist key rather than erroring, which reads exactly like an
episode nobody tracklisted. A scrape that treats those as zero records a whole
residency as empty and looks like it worked.

    python3 tests/test_nts_scrape.py
    pytest tests/test_nts_scrape.py
"""
import importlib.util
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_SPEC = importlib.util.spec_from_file_location(
    "scrape_nts_show", os.path.join(os.path.dirname(_HERE), "scripts",
                                    "scrape_nts_show.py"))
nts = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(nts)


EPISODE = {
    "episode_alias": "rotational-6th-july-2026",
    "broadcast": "2026-07-06T20:00:00Z",
    "name": "Archived w/ Rotational - Street Kuduro 2",
    "location_short": "LDN",
    "genres": [{"id": "kuduro", "value": "Kuduro"}],
    "tracklist": {
        "metadata": {"resultset": {"count": 3, "offset": 0, "limit": 3}},
        "results": [
            {"artist": "Game Walla", "title": "The Game é Carga",
             "offset": 20, "duration": 205, "acr_id": "6c070c2b552d",
             "deezer_track_id": 1039479662, "isrc_id": "QZHZ62031283",
             "musicbrainz_track_id": None},
            {"artist": "MN", "title": "Chama Chaleno",
             "offset": None, "duration": None, "offset_estimate": 225,
             "duration_estimate": 217, "acr_id": None,
             "deezer_track_id": None, "isrc_id": None,
             "musicbrainz_track_id": None},
            {"artist": "", "title": "", "deezer_track_id": None,
             "isrc_id": None},          # unidentified entry — IDs but no names
        ],
    },
}


def _stub_get(payload):
    """Point the module's only network call at a fixture."""
    nts._get = lambda url: payload


def test_tracklist_reads_the_results():
    _stub_get(EPISODE)
    assert len(nts.tracklist("rotational", "rotational-6th-july-2026")) == 3


def test_a_missing_tracklist_key_is_the_throttle_not_an_empty_show():
    # The whole point of the guard. Same episode, key gone: this must stop the
    # run, never be recorded as "0 tracks".
    stripped = {k: v for k, v in EPISODE.items() if k != "tracklist"}
    _stub_get(stripped)
    try:
        nts.tracklist("rotational", "rotational-6th-july-2026")
    except nts.Throttled:
        return
    raise AssertionError("a missing tracklist key must raise Throttled")


def test_an_empty_tracklist_is_an_untracklisted_episode():
    # Key present, no rows: that is a real answer and the scrape carries on.
    _stub_get({**EPISODE, "tracklist": {"results": []}})
    assert nts.tracklist("rotational", "rotational-6th-july-2026") == []


def test_episode_style_takes_the_genre_not_the_location():
    # NTS's location is the studio's, not the music's — "Kuduro" is the signal,
    # "LDN" would be a lie in the field Dig stratifies regions on.
    assert nts.episode_style(EPISODE) == "Kuduro"
    assert nts.episode_style({"genres": []}) is None


def test_to_candidate_matches_the_shape_the_resolver_reads():
    c = nts.to_candidate(EPISODE["tracklist"]["results"][0], "Kuduro", "rotational")
    assert c["raw"] == "Game Walla - The Game é Carga"
    assert c["artist"] == "Game Walla" and c["track"] == "The Game é Carga"
    assert c["style"] == "Kuduro"
    assert c["country"] is None          # never the studio's location
    assert c["source"] == "nts:rotational"
    assert c["isrc"] == "QZHZ62031283" and c["deezer_track_id"] == 1039479662


def test_to_candidate_drops_unidentified_entries():
    # ACR fingerprinting leaves rows with timing but no names; they would
    # resolve to " - " and match whatever Bandcamp felt like returning.
    assert nts.to_candidate(EPISODE["tracklist"]["results"][2], "Kuduro", "x") is None
    assert nts.to_candidate({"artist": "MN", "title": ""}, None, "x") is None


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: nts scraper passed ({len(fns)} tests)")
