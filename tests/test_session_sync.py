"""Regression lock for the cross-device sync overwriting a playing device.

What happened, on a phone on 4G (2026-07-30): the player showed one track's
cover — Praverb the Wyse, "Professional Hobbyist" — above a completely different
title and artist, "Chúng Ta Của Hiện Tại" by Sơn Tùng M-TP, with the status line
"playing on another device" and a 0:00 / 0:00 progress bar. Skipping produced no
audio, twice.

One poll caused all of it. `_startSessionPoll` reads /api/session every 10s and,
if another device's state differs, repaints the card and splices that device's
track into the local queue. Its guard was

    if (_hasPlayedThisSession && _sessionHeartbeat) return;

and the heartbeat stops on every pause — so a skip cleared it for a moment, the
poll slipped through, and the laptop's session landed on the phone. Two things
followed from that one gap:

  * `paintTrackInfo()` was called without `paintArt()`. The session payload
    carried no cover at all, so the title changed and the sleeve did not: one
    card asserting two different songs.
  * `allDiscovery.splice(dIdx, 0, restored)` inserted the remote track into the
    live queue, so the next skip stepped into a foreign track and playback died.

These are static assertions against web/app.html because the file is one 300 KB
inline script with no module boundary to import — the checks are cheap and they
fail loudly if the guard is ever loosened again.

    python3 tests/test_session_sync.py      # bare, no deps
    pytest tests/test_session_sync.py       # if pytest is installed
"""
import os
import re
import sys

from browser_source import browser_source



def _app() -> str:
    return browser_source()


def _function_body(src: str, name: str) -> str:
    """The source of `function name(...) { ... }`, brace-matched."""
    start = src.index(f"function {name}")
    depth, i = 0, src.index("{", start)
    for j in range(i, len(src)):
        if src[j] == "{":
            depth += 1
        elif src[j] == "}":
            depth -= 1
            if depth == 0:
                return src[start:j + 1]
    raise AssertionError(f"unbalanced braces in {name}")


def test_poll_yields_to_a_device_that_has_played():
    """A device that has played owns its own player. `&&` let a paused one through."""
    body = _function_body(_app(), "_startSessionPoll")
    guard = re.search(r"if \((_hasPlayedThisSession[^)]*)\) return;", body)
    assert guard, "the poll lost its early-return guard"
    assert "||" in guard.group(1), (
        "guard must bail on EITHER signal: with && a paused device (heartbeat "
        "cleared on skip) is repainted from another device's session"
    )


def test_the_poll_paints_art_whenever_it_paints_text():
    """Half a card is worse than none — that mismatch is the reported bug."""
    body = _function_body(_app(), "_startSessionPoll")
    assert "paintTrackInfo(" in body
    assert "paintArt(" in body, (
        "the poll sets the title without the cover: the previous track's sleeve "
        "stays on screen above a different song"
    )
    assert body.index("paintTrackInfo(") < body.index("paintArt(") + 400, (
        "art must be painted alongside the text, not somewhere unrelated"
    )


def test_the_page_load_restore_also_paints_art():
    body = _function_body(_app(), "_tryRestoreSession")
    if "paintTrackInfo(" in body:
        assert "paintArt(" in body, "restore path repaints text without the cover"


def test_the_session_payload_carries_the_cover():
    """Without it a receiving device cannot paint a complete card at all."""
    body = _function_body(_app(), "_buildSessionState")
    assert "albumArt" in body, (
        "session state must carry the cover; otherwise any sync is doomed to "
        "show the wrong sleeve or none"
    )


def test_only_the_guarded_poll_may_mutate_the_queue():
    """The splice is what killed playback after a skip."""
    body = _function_body(_app(), "_startSessionPoll")
    if "allDiscovery.splice(" in body:
        guard = re.search(r"if \((_hasPlayedThisSession[^)]*)\) return;", body)
        assert guard and "||" in guard.group(1), (
            "the queue is mutated behind a guard that a paused device passes"
        )


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"ok   {name}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {name}: {e}")
        except Exception as e:  # noqa: BLE001
            # A crash is a failed check, not a reason to stop. An uncaught
            # ValueError aborted a sibling file mid-run and it still printed
            # only "ok" lines — a red suite reading as green.
            failed += 1
            print(f"ERROR {name}: {type(e).__name__}: {e}")
    print("\nall session-sync checks passed" if not failed else f"\n{failed} failed")
    sys.exit(1 if failed else 0)
