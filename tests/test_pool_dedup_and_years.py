"""Duplicates stay merged and Bandcamp rows get years — the guards.

2026-08-11 pool-quality pass, part two. 872 same-source duplicate groups
(the single and the album edition under different Spotify ids; the same song
re-crawled from another Bandcamp release) were merged with history remapped —
and every one of them would drift back in on rediscovery, because
ON CONFLICT(id) can't see a duplicate that arrives under a NEW id. The
write-time guard in _upsert_track is what makes the merge stick.

Separately: all 32k Bandcamp rows were ingested without a year, which is
most of why a third of the pool had no decade. The tralbum payload carries
release_date for free, so play-time resolves backfill it and a cron walks
the backlog.

    python3 tests/test_pool_dedup_and_years.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.bandcamp import release_year  # noqa: E402


def _src(rel: str) -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, rel), encoding="utf-8") as fh:
        return fh.read()


def test_upsert_refuses_a_same_source_twin():
    src = _src("lib/discovery_lock.py")
    i = src.index("def _upsert_track")
    body = src[i:i + 2000]
    assert "lower(artist || ' - ' || name) = lower(%s)" in body, (
        "without the name-key guard every merged duplicate drifts back in "
        "under a fresh id on rediscovery")
    assert "AND source = %s" in body, (
        "the guard must be SAME-SOURCE only — a spotify + bandcamp pair is "
        "playability tiers (guests can only play the Bandcamp copy), not drift")


def test_release_year_parses_the_tralbum_timestamp():
    ts = int(time.mktime((1972, 6, 1, 0, 0, 0, 0, 0, 0)))
    assert release_year({"release_date": ts}) == "1972"
    assert release_year({"release_date": str(ts)}) == "1972"
    assert release_year({"album_release_date": ts}) == "1972"


def test_release_year_rejects_garbage():
    assert release_year({}) == ""
    assert release_year({"release_date": None}) == ""
    assert release_year({"release_date": "soon"}) == ""
    assert release_year({"release_date": -99999999999}) == ""


def test_playtime_resolve_carries_the_year():
    src = _src("lib/bandcamp.py")
    i = src.index("def resolve_stream")
    assert '"release_year"' in src[i:i + 2200], (
        "resolve_stream must ship release_year so every play backfills a "
        "year at zero extra Bandcamp calls")


def test_playtime_backfill_never_overwrites():
    src = _src("server.py")
    i = src.index("def _bandcamp_backfill_genres")
    body = src[i:i + 4000]
    assert "coalesce(year, '') = ''" in body, (
        "the play-time year backfill must only fill EMPTY years — a stored "
        "year may be the true original release, the resolve's may be a "
        "reissue date")


def test_merge_script_protects_history():
    src = _src("scripts/merge_duplicate_tracks.py")
    assert "status = 'saved'" in src and "IS DISTINCT FROM 'saved'" in src, (
        "when both twins are in a listener's history, the loser's SAVE must "
        "upgrade the keeper row — a save silently lost is the worst outcome "
        "a cleanup script can produce")


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok  {name}")
            except AssertionError as e:
                failed += 1
                print(f"FAIL  {name}: {e}")
    sys.exit(1 if failed else 0)
