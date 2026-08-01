"""Auto-advance must actually be wired to the Bandcamp <audio> 'ended' event.

"Auto play next song still doesn't work", reported repeatedly and never fixed,
because it failed silently and asymmetrically:

    audio ended … activeSource=bandcamp wired=false vis=visible willAdvance=false

Foreground, page alive, event delivered, guard satisfied — and no handler. The
iOS branch did this, ~1000 lines below the Player IIFE:

    Player.onTrackEnd = function(fn) { Player._onTrackEnd = fn; _onTrackEnd = fn; };

`_onTrackEnd` is declared with `let` INSIDE `const Player = (() => { … })()`.
Outside it the name is undeclared, so in sloppy mode that assignment quietly
created a global nothing reads, and the IIFE's copy stayed null for the life of
the page. Spotify tracks kept auto-advancing because the Connect poll uses
Player._onTrackEnd, which really was set — so the failure looked like "Bandcamp
is flaky" rather than "the handler was never attached".

The scope relationship is the whole bug, so it is asserted here directly: if
someone moves the override inside the IIFE later, the direct assignment becomes
correct and this test should be revisited rather than worked around.

    python3 tests/test_track_end_wiring.py
"""
import os
import re
import sys

from browser_source import browser_source

SRC = browser_source()
LINES = SRC.split("\n")


def _line_of(needle: str) -> int:
    for i, line in enumerate(LINES, 1):
        if needle in line:
            return i
    raise AssertionError(f"not found in app.html: {needle!r}")


def _iife_bounds() -> tuple[int, int]:
    """First and last line of `const Player = (() => { … })()`."""
    start = _line_of("const Player = (() => {")
    depth = 0
    for n in range(start, len(LINES) + 1):
        for ch in LINES[n - 1]:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return start, n
    raise AssertionError("the Player IIFE never closes")


# ── the scope relationship that makes the direct assignment wrong ─────────────

def test_the_private_state_lives_inside_the_iife():
    start, end = _iife_bounds()
    decl = _line_of("let _onTrackEnd = null;")
    assert start < decl < end, (
        "if this ever stops holding, the override's direct assignment would be "
        "correct and this whole test needs rethinking"
    )


def test_the_bandcamp_listener_reads_that_same_private_state():
    start, end = _iife_bounds()
    listener = _line_of("a.addEventListener('ended'")
    assert start < listener < end


def test_the_ios_override_is_outside_the_iife():
    """The override must sit below the IIFE's closing brace.

    Both now live in web/js/player.js — the Connect override IS the player on
    iPhone, so putting it anywhere else was the botched half of an extraction.
    Which module they share is therefore not the question; lexical scope is.
    Inside the IIFE, `_onTrackEnd` is in scope and assigning it by name would
    silently shadow the setter this whole file exists to route through.
    """
    start, end = _iife_bounds()
    override = _line_of("Player.onTrackEnd = function(fn)")
    assert override > end, (
        f"the override is at line {override}, inside the IIFE ({start}..{end}) "
        "— it can assign the private variable by name again"
    )

def test_the_override_does_not_assign_the_private_name_directly():
    override = LINES[_line_of("Player.onTrackEnd = function(fn)") - 1]
    assert not re.search(r"(?<![.\w])_onTrackEnd\s*=", override), (
        "assigning `_onTrackEnd` here creates a global nothing reads"
    )


def test_no_code_outside_the_iife_touches_the_bare_name_at_all():
    """Reads are as broken as writes, and easier to add by accident.

    While fixing the original bug I wrote `if (_onTrackEnd) _onTrackEnd();` in
    the Connect play-failure path — which is outside the IIFE — and it would
    have silently done nothing, an hour after the same mistake was diagnosed.
    Outside the IIFE the only valid handle is `Player._onTrackEnd`.
    """
    start, end = _iife_bounds()
    offenders = []
    for i, line in enumerate(LINES, 1):
        if start <= i <= end:
            continue
        code = re.sub(r"//.*$", "", line)
        if re.search(r"(?<![.\w$])_onTrackEnd\b", code):
            offenders.append(f"line {i}: {line.strip()[:70]}")
    assert not offenders, (
        "bare `_onTrackEnd` outside the Player IIFE resolves to an undeclared "
        "global that nothing sets:\n  " + "\n  ".join(offenders)
    )


def test_the_override_delegates_to_the_captured_setter():
    override = LINES[_line_of("Player.onTrackEnd = function(fn)") - 1]
    assert "_setInnerTrackEnd(fn)" in override


def test_the_setter_is_captured_before_it_is_replaced():
    capture = _line_of("const _setInnerTrackEnd = Player.onTrackEnd.bind(Player);")
    override = _line_of("Player.onTrackEnd = function(fn)")
    assert capture < override, (
        "capturing after the replacement would capture the replacement, "
        "and calling it would recurse"
    )


def test_the_connect_path_is_still_wired_too():
    """Spotify auto-advance uses Player._onTrackEnd and must not regress."""
    override = LINES[_line_of("Player.onTrackEnd = function(fn)") - 1]
    assert "Player._onTrackEnd = fn" in override


def test_the_iife_still_exposes_a_setter_to_delegate_to():
    start, end = _iife_bounds()
    setter = _line_of("onTrackEnd(fn) { _onTrackEnd = fn; }")
    assert start < setter < end, "the captured setter must be the IIFE's own"


# ── the listener reports enough to catch this again ───────────────────────────

def test_the_ended_listener_logs_whether_a_handler_existed():
    i = SRC.index("a.addEventListener('ended'")
    body = SRC[i:i + 900]
    assert "wired:" in body, (
        "`wired` is what turned this from 'autoplay is flaky' into a one-line "
        "diagnosis; without it the failure is invisible"
    )
    assert "willAdvance" in body and "activeSource" in body


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
        except Exception as e:  # noqa: BLE001 — a crash is a failed check too
            failed += 1
            print("ERROR %s\n     %s: %s" % (name, type(e).__name__, e))
    sys.exit(1 if failed else 0)
