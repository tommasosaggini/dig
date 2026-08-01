"""The progress bar must never paint a clock that isn't the current track's.

Reported 2026-07-31, and long-standing: "it kept going from 25/26 secs to 1:47
back and forth", and "whenever I skip, first it resets then it goes back to the
previous song timestamp".

Both come from the 250 ms interpolator in app.html, which read `_lastState`
and painted unconditionally. Two things were wrong with that:

  * It painted a clock belonging to a DIFFERENT track. The mismatch was already
    computed — as `match:` inside the BAR JUMP log payload — and then ignored.
    Measured: clockTrackId=6D75KWhmhK against intentTrackId=76HIAc01XF,
    match:false, painting 100% → 34.6%.

  * It extrapolated that clock with no age ceiling. `clockAgeMs: 123915` — two
    minutes of invented position — drove the bar to the end of the track while
    the real poll kept repainting the true one. The bar flipped between them.

The poll had a stale guard already, but it only fires within 2.5 s of dispatch;
the bad paint above was 218,840 ms after one, so it sailed straight through.

Static assertions: app.html is a single 330 KB inline script with nothing
importable. The guard CONDITIONS are also simulated below as pure functions, so
the logic is checked and not merely the text.

    python3 tests/test_progress_bar_clock.py
"""
import os
import re
import sys

from browser_source import browser_source

SRC = browser_source()


def _interpolator() -> str:
    i = SRC.index("function _startProgressInterpolator()")
    return SRC[i:SRC.index("// Cadence + drift telemetry", i)]


# ── the guards exist, and run BEFORE the paint ────────────────────────────────

def test_a_foreign_clock_is_not_painted():
    body = _interpolator()
    assert "_lastState.trackId !== _connectTrackId" in body, (
        "the mismatch was computed for the log and ignored for the paint"
    )


def test_the_mismatch_guard_returns_rather_than_painting():
    body = _interpolator()
    guard = body[body.index("_lastState.trackId !== _connectTrackId"):]
    guard = guard[:guard.index("const elapsed")]
    assert "return;" in guard, "must bail out, not fall through to the paint"


def test_extrapolation_has_a_ceiling():
    body = _interpolator()
    assert "_CLOCK_MAX_EXTRAPOLATION_MS" in body
    assert re.search(r"elapsed > _CLOCK_MAX_EXTRAPOLATION_MS", body)


def test_the_ceiling_is_sane_against_the_poll_cadence():
    """Polls land ~1.5 s apart; the ceiling must clear ordinary throttling but
    stay far below the 124 s that caused this."""
    m = re.search(r"const _CLOCK_MAX_EXTRAPOLATION_MS = (\d+);", SRC)
    assert m, "the ceiling must be a named constant, not a literal in a branch"
    ms = int(m.group(1))
    assert 5000 <= ms <= 30000, f"{ms} ms is not a plausible ceiling"


def test_both_guards_precede_any_width_assignment():
    body = _interpolator()
    assert "style.width" in body, "the interpolator must still paint something"
    first_paint = body.index("style.width")
    for needle in ("_lastState.trackId !== _connectTrackId",
                   "elapsed > _CLOCK_MAX_EXTRAPOLATION_MS"):
        # `in` before `.index` on purpose: a missing guard is a FAILURE, and
        # .index would raise ValueError, which this file's runner does not
        # catch — it aborted the run and silently skipped every later test.
        assert needle in body, f"{needle} is missing entirely"
        assert body.index(needle) < first_paint, f"{needle} must gate the paint"


def test_skipping_a_paint_clears_the_bounce_baseline():
    """Otherwise the next real paint looks like a jump and spams BAR JUMP."""
    body = _interpolator()
    head = body[:body.index("const interpPos")]
    assert head.count("_interpLastPct = null") >= 2


def test_the_bar_jump_telemetry_survives():
    """It is the evidence that diagnosed this; the guards must not silence it."""
    body = _interpolator()
    assert "BAR JUMP" in body and "clockAgeMs" in body and "match:" in body


# ── the clock must know which track it is for ─────────────────────────────────

def _anchor() -> str:
    i = SRC.index("Player._anchorProgress = function")
    return SRC[i:SRC.index("Player._getStateCachedOrFresh", i)]


def test_anchor_sets_the_track_it_anchors_for():
    """Object.assign carried the PREVIOUS track's id onto a clock anchored at 0
    for a new song. Nothing read that id before, so it went unnoticed — and it
    would have frozen the bar for the whole propagation window."""
    body = _anchor()
    assert "trackId" in body, "an anchored clock that lies about its track is the bug"
    assert "function (position, duration, paused, trackId)" in body


def test_anchor_keeps_the_old_id_only_when_none_is_given():
    body = _anchor()
    assert "trackId !== undefined" in body, (
        "callers that pass nothing must not be silently given null"
    )


def test_every_anchor_call_names_a_track():
    calls = re.findall(r"Player\._anchorProgress\(([^;]*?)\);", SRC)
    assert calls, "no anchor calls found — the regex is wrong"
    for call in calls:
        assert call.count(",") >= 3, (
            f"anchor call without a trackId: Player._anchorProgress({call})"
        )


# ── the conditions themselves, as logic ───────────────────────────────────────

def _should_paint(clock_track, intent_track, elapsed_ms, ceiling=10000):
    """The two guards, transcribed."""
    if intent_track and clock_track and clock_track != intent_track:
        return False
    if elapsed_ms > ceiling:
        return False
    return True


def test_the_reported_oscillation_is_refused():
    # the measured pair, both of which painted
    assert _should_paint("6D75KWhmhK", "76HIAc01XF", 62) is False
    assert _should_paint("76HIAc01XF", "76HIAc01XF", 123915) is False


def test_ordinary_playback_still_paints():
    assert _should_paint("76HIAc01XF", "76HIAc01XF", 250) is True
    assert _should_paint("76HIAc01XF", "76HIAc01XF", 1500) is True


def test_an_unknown_id_on_either_side_does_not_freeze_the_bar():
    """A null id means "we don't know", which must not be read as "wrong"."""
    assert _should_paint(None, "76HIAc01XF", 250) is True
    assert _should_paint("76HIAc01XF", None, 250) is True


def test_a_freshly_anchored_new_track_paints_from_zero():
    """The skip case: anchor(0, dur, false, t.id) then interpolate."""
    assert _should_paint("newtrack", "newtrack", 300) is True


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print("ok   %s" % name)
        except AssertionError as e:
            failed += 1
            print("FAIL %s\n     %s" % (name, e))
        except Exception as e:  # noqa: BLE001
            # Anything else is still a failed check, not a reason to stop:
            # an uncaught ValueError from a .index() lookup aborted this run
            # once and skipped every test after it, which reads as a pass.
            failed += 1
            print("ERROR %s\n     %s: %s" % (name, type(e).__name__, e))
    sys.exit(1 if failed else 0)
