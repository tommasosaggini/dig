"""An ingest source must not be able to declare itself finished.

The Bandcamp job enumerates (genre x sort x page) cells and remembered which
it had swept. The set was PERMANENT: 27 genres x 3 sorts x 6 pages = 486 cells,
and once all 486 were in it the job made zero HTTP calls, inserted nothing,
printed "calls=0 | NEW=0" and exited 0 — eight times a day. No Bandcamp track
entered the pool between 2026-06-13 and 2026-08-01, seven weeks, while every
log line and exit code said the source was healthy.

The error was treating a TIME-VARYING query as an enumeration. Bandcamp's `new`
is new arrivals and `top` is a rolling chart; the same cell returns different
releases next week. A cell is never "done", only "fresh until".

    python3 tests/test_ingest_freshness.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DAY = 86400


def _mod():
    import importlib
    return importlib.import_module("scripts.ingest_bandcamp")


def test_a_swept_cell_goes_stale():
    m = _mod()
    now = 1_000_000_000
    cell = ("ambient", "new", 0)
    assert m.cell_is_fresh(cell, {cell: now - 60}, now), "just-swept cell should be skipped"
    assert not m.cell_is_fresh(cell, {cell: now - 30 * DAY}, now), (
        "a cell swept a month ago is still being skipped — this is exactly the "
        "state that silently killed Bandcamp ingest for seven weeks"
    )


def test_no_sort_is_fresh_forever():
    """Every sort must expire. The bug was one that never did."""
    m = _mod()
    now = 1_000_000_000
    for sort in list(m.SWEEP_TTL) + ["a_sort_added_later"]:
        cell = ("ambient", sort, 0)
        assert not m.cell_is_fresh(cell, {cell: now - 400 * DAY}, now), (
            f"sort {sort!r} never goes stale — an unknown sort must fall back "
            "to a finite TTL, not to 'forever'"
        )


def test_new_arrivals_expire_faster_than_the_slow_charts():
    m = _mod()
    assert m.SWEEP_TTL["new"] < m.SWEEP_TTL["top"] <= m.SWEEP_TTL["rec"], (
        "`new` turns over daily and must be re-swept most often"
    )
    assert m.SWEEP_TTL["new"] <= DAY, "new arrivals go stale within a day"


def test_an_unseen_cell_is_never_fresh():
    m = _mod()
    assert not m.cell_is_fresh(("ambient", "new", 0), {}, 1_000_000_000)


def test_the_old_permanent_shape_is_not_readable_as_state():
    """The migration must treat the seven-week-old list as fully due.

    The old file is {"swept": [[g,s,p], ...]} with no timestamps. Reading it as
    if those cells were swept "now" would extend the outage by another TTL.
    """
    src = open(os.path.join(ROOT, "scripts", "ingest_bandcamp.py"),
               encoding="utf-8").read()
    body = src[src.index("def main("):]
    assert 'state.get("swept_at")' in body, "state must be read from the timestamped key"
    assert 'state.get("swept"' not in body, (
        "still reading the old permanent-set key — those cells would be "
        "skipped forever again"
    )


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
    print("all ingest-freshness checks passed")
