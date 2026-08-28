"""The artist cap must count every source, and read either cursor shape.

Two bugs live here, both of which let the pool loop the same artist:

1. CURSOR SHAPE. Callers disagree: locked_update opens a RealDictCursor
   (rows are dicts keyed by column name) while the plain discovery paths
   use tuple cursors. The original `cur.fetchone()[0]` only worked on
   tuples and raised KeyError: 0 on dicts.

2. WHAT THE CAP COUNTS. It counted `artist_ids[1]` and returned early
   when there were none — which is every Bandcamp, YouTube and SoundCloud
   row, 65% of the pool. That early return is also what hid bug 1 for so
   long. AQVARIA reached 38 tracks through the hole; in the 14 days to
   2026-08-17 Bandcamp put 2,120 artists over the cap while Spotify put
   one. The cap now counts `lower(btrim(artist))` as well, so a track
   with no artist_ids must still be checked — the exact opposite of the
   old short-circuit, which is why that test is inverted below.

    python3 tests/test_artist_cap_cursor.py
    pytest tests/test_artist_cap_cursor.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.artist_cap import (ARTIST_CAP, artist_key, is_over_cap,  # noqa: E402
                            is_track_over_cap, primary_artist_id)


class _Cur:
    """Minimal cursor stub. `shape` picks what fetchone() hands back."""

    def __init__(self, count, shape):
        self.count, self.shape = count, shape
        self.params = None

    def execute(self, sql, params=None):
        assert "COUNT(*) AS n" in sql, "the alias is what makes dict rows readable"
        self.params = params

    def fetchone(self):
        return {"n": self.count} if self.shape == "dict" else (self.count,)


def test_reads_the_count_from_a_dict_cursor():
    # RealDictCursor — the shape locked_update actually uses.
    assert is_over_cap(_Cur(ARTIST_CAP, "dict"), "spid") is True
    assert is_over_cap(_Cur(0, "dict"), "spid") is False


def test_reads_the_count_from_a_tuple_cursor():
    assert is_over_cap(_Cur(ARTIST_CAP, "tuple"), "spid") is True
    assert is_over_cap(_Cur(0, "tuple"), "spid") is False


def test_a_track_with_no_artist_ids_is_still_capped():
    # THE regression. Bandcamp/YouTube rows carry no artist_ids, and used
    # to short-circuit to False before any query ran — an uncapped 65% of
    # the pool. They must now reach the count via the name.
    bc = {"id": "bc:1:2", "artist": "AQVARIA", "artist_ids": []}
    assert primary_artist_id(bc) is None
    assert is_track_over_cap(_Cur(ARTIST_CAP, "dict"), bc) is True
    assert is_track_over_cap(_Cur(0, "dict"), bc) is False


def test_both_predicates_are_sent():
    # Name-only would miss a Spotify artist spelled two ways under one id;
    # id-only misses every source without ids. The query carries both, and
    # the name arrives normalised so 'AQVARIA' and ' aqvaria ' are one key.
    cur = _Cur(0, "dict")
    is_track_over_cap(cur, {"artist": "  AQVARIA  ", "artist_ids": ["spid"]})
    assert cur.params == ("spid", "spid", "aqvaria", "aqvaria")


def test_nothing_to_key_on_means_no_query():
    class Boom:
        def execute(self, *a, **k):
            raise AssertionError("must not query without an id or a name")

    assert is_over_cap(Boom(), None) is False
    assert is_track_over_cap(Boom(), {"artist": "   ", "artist_ids": []}) is False
    assert primary_artist_id({"id": "bc:1:2"}) is None
    assert primary_artist_id({"artist_ids": ["abc", "def"]}) == "abc"


def test_artist_key_normalises_shallowly():
    # Case, surrounding and repeated whitespace — and nothing else. Anything
    # cleverer merges acts that really are distinct, so the bracketed and
    # bare forms of a name stay two keys on purpose.
    assert artist_key("  Blur   Records ") == "blur records"
    assert artist_key("MIKAN MUKKU [みかんむくっ]") != artist_key("MIKAN MUKKU")
    assert artist_key("") is None
    assert artist_key(None) is None


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: artist cap passed ({len(fns)} tests)")
