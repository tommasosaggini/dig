"""Unknown regions heal from evidence, never from guesses.

Two recovery paths for the 5,754 Unknown-region tracks (2026-08-11):
MusicBrainz drip (scripts/backfill_unknown_regions.py) and the play-time
Bandcamp band-location backfill in server.py. Both must only ever touch rows
that are STILL Unknown, and the drip must yield to MB's rate limit — the
genre crawler owns most of this box's MB budget, and a drip that retries
into a 503 turns one polite consumer into two rude ones.

    python3 tests/test_unknown_region_backfill.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402


def _src(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def test_drip_stops_on_rate_limit_keeping_progress():
    src = _src("scripts/backfill_unknown_regions.py")
    assert "except MBRateLimited:" in src and "break" in src.split(
        "except MBRateLimited:")[1][:120], (
        "the drip must exit cleanly when MB pushes back — retrying into a "
        "503 makes the shared budget worse for every MB consumer on the box")


def test_drip_updates_only_still_unknown_rows():
    src = _src("scripts/backfill_unknown_regions.py")
    i = src.index("UPDATE tracks SET origin_region")
    # The guard is spelled via the shared {UNKNOWN} f-string predicate — the
    # same one that SELECTS the candidates, so the two can never drift apart.
    assert "{UNKNOWN}" in src[i:i + 300], (
        "the artist-wide UPDATE must be guarded on still-Unknown — an artist "
        "whose other tracks already carry a resolved origin must not be "
        "overwritten by a namesake match")


def test_playtime_region_heal_never_overrides_proven_origin():
    """Same intent as before, now spelled in provenance rather than in a string.

    The guard used to read `... = 'Unknown'`, which asked whether the CURRENT
    LABEL looked empty. Since lib/origin.py that is the wrong question: a row
    labelled 'Singapore' off a market search is every bit as unproven as one
    labelled 'Unknown', and should still be healed by the artist's own Bandcamp
    location. The guard now asks whether the row already carries a TRUSTED
    origin, which is both stricter (a proven country is untouchable) and wider
    (a market label is no longer mistaken for a resolved one).
    """
    src = _src("server.py")
    i = src.index("origin_source = 'bandcamp_page'")
    window = src[i:i + 400]
    assert "origin_source IS NULL" in window and "ANY(%s)" in window, (
        "the play-time band-location backfill may FILL an unproven origin but "
        "never override a resolved one — MB origin outranks a Bandcamp "
        "profile string")
    assert "TRUSTED_ORIGIN_SOURCES" in src, (
        "the guard must be parameterised on lib/origin.TRUSTED_ORIGIN_SOURCES, "
        "not a hand-copied tier list that can drift from it")


def test_playtime_heal_lands_a_provenance_not_just_a_label():
    """Writing only `region` would leave the row indistinguishable from a
    market artefact, and served_region would keep returning Unknown forever —
    the heal would run every play and change nothing anyone could see."""
    src = _src("server.py")
    i = src.index("origin_source = 'bandcamp_page'")
    window = src[max(0, i - 300):i + 200]
    assert "origin_region = %s" in window, (
        "the heal must land the country in origin_region, the one field "
        "served_region reads")


def test_misses_are_retried_eventually_not_never():
    src = _src("scripts/backfill_unknown_regions.py")
    assert "RETRY_S" in src, (
        "a miss must carry a retry horizon — MusicBrainz grows, so 'MB "
        "doesn't know them' is a timestamp, not a verdict")


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
