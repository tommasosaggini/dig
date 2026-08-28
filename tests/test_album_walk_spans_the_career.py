#!/usr/bin/env python3
"""The album walk must not be a recency filter.

ingest_mb_artists is the ONLY Spotify ingestion path that still works — /search
has been permanently 403/429 since Spotify's Development Mode restriction (see
tests/test_ingest_without_search.py). It walks artist_albums -> album_tracks,
and it opened `items[:ALBUM_SCAN]`: the three NEWEST releases, because that is
the order Spotify returns.

Applied to every artist DIG has ever ingested, that is a recency filter on the
entire catalogue. An artist who debuted in 1988 and still releases contributed
a 2024 single and nothing else. Measured 2026-08-18:

  tracks from this path      2,489
    pre-2000                    72   (2.9%)
    2020s                    1,750  (70.3%)

  pending artists whose career began
    1970s  2,011   1980s  3,457   1990s  4,632   2000s  3,383

The back catalogue was never missing. It was being converted to contemporary
tracks at the door, and no amount of picker weighting can serve what ingestion
never wrote.

  python3 tests/test_album_walk_spans_the_career.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from scripts.ingest_mb_artists import (  # noqa: E402
    ALBUM_PAGE, ALBUM_SCAN, spread_albums,
)


def _album(i, date):
    return {"id": f"al{i}", "release_date": date, "name": f"Album {i}"}


def _years(picked):
    return [a["release_date"][:4] for a in picked]


def test_the_earliest_record_is_what_gets_ingested():
    """The case the whole change exists for: an artist active since the 80s.

    resolve_track returns the FIRST acceptable track and stops, and only one
    track per artist is inserted — so position 0 of this list is, in the
    ordinary case, the artist's entire contribution to the pool."""
    career = [_album(i, f"{y}-01-01") for i, y in
              enumerate([1988, 1991, 1995, 1999, 2004, 2009, 2015, 2021, 2024])]
    picked = spread_albums(career, 3)
    assert len(picked) == 3
    assert _years(picked)[0] == "1988", (
        f"the album actually ingested is {_years(picked)[0]}, not the debut — "
        "this is the recency filter, still in place")
    # The spares exist for when the debut is unreachable or all trash, and are
    # spread so the fallback is a different era rather than a reissue.
    years = _years(picked)
    assert years[1] > "1999" and years[2] > years[1], f"spares not spread: {years}"


def test_the_newest_three_are_not_what_comes_back():
    """The defect, stated directly."""
    career = [_album(i, f"{y}-01-01") for i, y in
              enumerate([1985, 1990, 1996, 2002, 2020, 2022, 2024])]
    picked = _years(spread_albums(career, 3))
    assert set(picked) != {"2020", "2022", "2024"}, (
        "still taking the three most recent releases")
    assert picked[0] == "1985", (
        "the earliest record must be the one opened first — anything else and "
        "the single ingested track is a recent one again")


def test_spotifys_own_ordering_does_not_decide_the_answer():
    """artist_albums returns newest-first. The rule has to sort for itself, or
    it is just reading Spotify's order back out."""
    years = [1988, 1994, 2001, 2010, 2024]
    forward = [_album(i, f"{y}-01-01") for i, y in enumerate(years)]
    backward = list(reversed(forward))
    assert _years(spread_albums(forward, 3)) == _years(spread_albums(backward, 3))


def test_a_short_catalogue_is_taken_whole():
    two = [_album(0, "2019-01-01"), _album(1, "2023-01-01")]
    assert len(spread_albums(two, 3)) == 2
    assert spread_albums([], 3) == []


def test_undated_albums_are_kept_but_never_take_the_oldest_slot():
    """Both halves matter. A missing release_date is common on the long tail
    this queue exists for, so dropping them would re-bias the walk toward
    well-tagged — Western, recent — releases. But an undated album cannot fill
    a decade either: its track lands with no year and the picker's era term
    treats it as neutral. So it gets the slots nothing else wants."""
    # Enough dated albums to fill every slot: the undated one waits.
    crowded = [{"id": "a", "release_date": None}] + \
              [{"id": f"d{y}", "release_date": f"{y}-01-01"}
               for y in (1990, 2001, 2012, 2024)]
    assert "a" not in {x["id"] for x in spread_albums(crowded, 3)}, (
        "an undated album displaced a dated one — that slot can never fill a "
        "decade, which is the entire point of the walk")
    # Nothing else competing: it is still music and still gets walked.
    sparse = [{"id": "a", "release_date": None},
              {"id": "b", "release_date": "2019-01-01"}]
    assert "a" in {x["id"] for x in spread_albums(sparse, 3)}, (
        "an undated album was discarded outright")


def test_rows_without_an_id_cannot_be_opened():
    """A pick with no id costs a slot and fetches nothing."""
    items = [{"id": None, "release_date": "1985-01-01"},
             _album(1, "1990-01-01"), _album(2, "2000-01-01"),
             _album(3, "2024-01-01")]
    assert all(a.get("id") for a in spread_albums(items, 3))


def test_album_page_never_exceeds_the_measured_api_cap():
    """ALBUM_PAGE = 50 took the hourly job to ingested=0 on the next run.

    Spotify DOCUMENTS limit<=50 for /artists/{id}/albums. Under the Development
    Mode restriction this app is subject to, anything above 10 returns 400 —
    probed against prod 2026-08-18: 10 OK, 11/12/15/16/18/20/49/50 all 400. The
    documented ceiling is not the real one, and the failure is total rather than
    partial: every artist in the batch errors.
    """
    assert ALBUM_PAGE <= 10, (
        f"ALBUM_PAGE={ALBUM_PAGE} exceeds the MEASURED cap of 10; the docs say "
        "50 and the docs are wrong for this app. Probe before raising.")
    assert ALBUM_SCAN == 3, "the per-artist album_tracks budget changed"


def test_the_debut_is_reachable_past_the_page_cap():
    """With a 10-album page, a prolific artist's whole window can post-date
    2015 — the recency filter again, with more steps. The walk must page to the
    far end using `total` rather than trusting the first page."""
    src = open(os.path.join(ROOT, "scripts", "ingest_mb_artists.py"),
               encoding="utf-8").read()
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    assert "offset=max(0, total - ALBUM_PAGE)" in code.replace(" ", "").replace(
        "offset=max(0,total-ALBUM_PAGE)", "offset=max(0, total - ALBUM_PAGE)") or \
        "total - ALBUM_PAGE" in code, (
        "nothing pages to the oldest releases — the debut is unreachable for "
        "any artist with more than ALBUM_PAGE releases")
    assert "total > ALBUM_PAGE" in code, (
        "the second page must be conditional, or short catalogues pay for a "
        "call that returns what they already have")


def test_no_caller_slices_the_newest_off_the_top():
    """The rule has to be the only way albums get chosen."""
    src = open(os.path.join(ROOT, "scripts", "ingest_mb_artists.py"),
               encoding="utf-8").read()
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    assert "for al in items[:" not in code, (
        "an unspread slice is back — this is the original defect verbatim")
    assert "spread_albums(" in code


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
    print(f"\n{failed} failed" if failed else "\nall passed")
    sys.exit(1 if failed else 0)
