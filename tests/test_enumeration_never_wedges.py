"""Regression lock for "enumeration ran every night for a month and did nothing".

Measured 2026-08-04. The last mb_enum_state row was `country:TW, 2026-07-08` —
26 days earlier — while the ingest queue drained toward empty.

The mechanism, and it is a nasty one:

  1. The rotation after TW is HK. MusicBrainz models Hong Kong as a subdivision,
     not an iso1 country, so lookup_country_area_mbid('HK') returns None.
  2. enumerate_country bailed on that WITHOUT recording state.
  3. pick_rotation_country returns "any country never walked" FIRST — and HK had
     no state row, because of (2).
  4. So the nightly job picked HK again. And again. For 26 nights, exiting in
     0.1s each time.

Nothing alerted, because a run that enumerates 0 artists prints the same shape
as a run that finished. That is the real lesson here and it is shared with two
other failures found the same day (gap analysis crashing to "(analysis failed)",
and /search returning 429 while the pipeline reported clean aborts): **silence
and success looked identical**.

Both halves are locked below — the state write that stops the wedge, and the
in-run skip so a single bad code costs seconds instead of a night.

    python3 tests/test_enumeration_never_wedges.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402

SCRIPT = "scripts/enumerate_mb_artists.py"


def _src() -> str:
    with open(os.path.join(ROOT, SCRIPT), encoding="utf-8") as fh:
        return fh.read()


def _code_only(src: str) -> str:
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    return re.sub(r"(?m)^\s*#.*$", "", src)


def _no_comments(src: str) -> str:
    """Strip `#` comments but KEEP string literals.

    The two existing helpers were each wrong for one of these tests: _code_only
    eats the triple-quoted SQL we assert on, and raw source contains a comment
    that mentions 503 — the very token another test forbids. Both failed
    against correct code.
    """
    return re.sub(r"(?m)^\s*#.*$", "", src)


def _fn(src: str, decl: str) -> str:
    start = src.index(decl)
    nxt = src.find("\ndef ", start + 1)
    return src[start:nxt if nxt != -1 else len(src)]


def test_an_unresolvable_country_records_state():
    """Without this the rotation picks it again forever.

    pick_rotation_country prefers countries with NO state row, so bailing
    without writing one guarantees the same pick tomorrow.
    """
    # RAW source, not _code_only: the SQL is a triple-quoted literal and the
    # docstring-stripper eats it, which failed this test against correct code.
    body = _fn(_no_comments(_src()), "def enumerate_country")
    head = body[:body.index("state = fetchone(")]
    assert "INSERT INTO mb_enum_state" in head, (
        "the unresolvable branch must record state or the rotation wedges")
    # NOT `"done = TRUE" or "done, last_run_at"` — the second alternative
    # matches the INSERT's column LIST, so the assertion passed with the
    # conflict branch's `done = TRUE` deleted. Caught by mutation.
    assert "done = TRUE" in head, (
        "the ON CONFLICT branch must set done — an unwalkable ISO code cannot "
        "ever succeed, and a row that exists but is not done is picked again")
    assert "VALUES (%s, 0, 0, TRUE" in head, (
        "and the INSERT itself must mark it done on first sight")


def test_the_unresolvable_branch_is_distinguishable_from_success():
    """A 0-artist run must not be reported as a completed walk."""
    code = _code_only(_src())
    body = _fn(code, "def enumerate_country")
    assert '"unresolvable": True' in body, (
        "callers need to tell 'nothing to walk' from 'walked and found none' — "
        "this is exactly the distinction whose absence hid the bug for 26 days")


def test_one_run_skips_past_several_dead_codes():
    """The state write alone still burns a whole night on one bad pick, and the
    rotation holds several subdivision-style codes that can sit consecutively."""
    code = _code_only(_src())
    assert "MAX_ROTATION_SKIPS" in code, "the skip budget must be explicit"
    m = re.search(r"MAX_ROTATION_SKIPS\s*=\s*(\d+)", _src())
    assert m and 1 < int(m.group(1)) <= 25, (
        "skip several, but bounded — an unbounded loop would walk the whole "
        "rotation in one run")
    main = _fn(code, "def main")
    assert "unresolvable" in main, (
        "main must react to the flag; ignoring it reinstates the one-per-night "
        "stall")


def test_a_real_outage_is_NOT_marked_done():
    """The mirror rule. MusicBrainz 503s and network errors are transient; a
    country marked done for one would never be enumerated again."""
    body = _fn(_no_comments(_src()), "def enumerate_country")
    head = body[:body.index("state = fetchone(")]
    for transient in ("503", "timeout", "ConnectionError"):
        assert transient not in head, (
            f"{transient} is transient and must not retire a country")


def test_rotation_still_prefers_unwalked_countries():
    """The fix must not have changed WHICH country gets picked, only that a
    dead pick cannot repeat."""
    code = _code_only(_src())
    pick = _fn(code, "def pick_rotation_country")
    assert "not in state" in pick, "never-walked countries still come first"
    assert "done" in pick, "completed countries are still skipped"


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
            except Exception as exc:                       # noqa: BLE001
                failed += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    if failed:
        print(f"\n{failed} failed")
        sys.exit(1)
    print("all enumeration-wedge checks passed")
