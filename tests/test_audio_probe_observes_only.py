"""The audio-health probe observes. It must never command playback.

Reported live 2026-08-05, three times in twenty minutes: "I just completed a
Spotify handshake via dig and the song reset back to 0 3 times", "I just
skipped and the next song also reset to 0 after a few seconds. It was showing
cover art but it's not showing it anymore", "I just went back to dig when a
song was playing and it got reset to 0 (akon - beautiful day)".

One mechanism, logged end to end:

    03:59:17.579  lifecycle    visibilitychange visible
    03:59:17.984  audio-probe  recovery: re-dispatching current track
    03:59:17.986  pbar         instant-snap pct 0        <- back to 0:00
    03:59:17.987  art          cleared to placeholder    <- cover gone

Eight of those in ninety minutes, every one on a track that was playing.

WHY THE PROBE MAY NOT COMMAND, which is the part worth keeping:

  * The hazard is the MECHANISM, and Invariant A already says so. A play goes
    out as TRANSFER-then-play, and the transfer pauses the device — that is
    what cost "Ari Ari" on 2026-08-03. Invariant A blocks re-commanding a track
    Spotify is already playing EXCEPT for playback DIG started itself, and the
    probe's recovery walked through that exception to do precisely the damage
    the invariant exists to prevent.

  * Its input is a proxy that is legitimately wrong on the Connect path. A
    polled /me/player position freezes for three ordinary reasons that are not
    silence: a dispatch still propagating (Spotify keeps reporting the OUTGOING
    track), iOS suspending JS on a hidden tab, and the moment of resume.

  * The recovery was written on 2026-07-30 for the desktop SDK path, where
    getState was a real-time local read, _startPoll restarted on every
    spotify.play (reseeding the baseline), and a re-dispatch went through the
    SDK rather than as transfer-then-play. None of those three survive on
    Connect; e1b770a (2026-08-04) moved the recovery onto that path without
    them, reading 162 logged detections as 162 real stalls. They were not.

  * The probe's own comment has described it as an observer since June:
    "Surface it once so the failure is observable in the server log instead of
    inferred."

If a real stall pattern does show up in the WARNs — and one real stall is on
record (L'islam, frozen at 35061 for 100s after a seek) — the answer is
something that does NOT command: a seek on the no-transfer path, which
Invariant A already recognises as harmless. Not the re-dispatch.

    python3 tests/test_audio_probe_observes_only.py
"""
import re
import sys

from browser_source import browser_source


def _code_only(src: str) -> str:
    """`src` with `//` and `/* … */` comments removed.

    The comments in the probe quote the very construct this file forbids — the
    block above `_startPoll` names `playCurrentTrack` while explaining why it
    must not be called — so a bare substring search would find the explanation
    and report the bug it prevents.
    """
    src = re.sub(r"/\*[\s\S]*?\*/", "", src)
    return re.sub(r"(?m)^\s*//.*$", "", src)


def _probe_body(src: str) -> str:
    """The body of `_startPoll`, comments stripped."""
    start = src.index("function _startPoll()")
    # To the end of the interval callback — `}, 500);` closes setInterval.
    end = src.index("}, 500);", start)
    return _code_only(src[start:end])


def test_probe_never_commands_playback():
    body = _probe_body(browser_source())
    for forbidden in ("playCurrentTrack", "Player.play", "_transfer", "/api/play"):
        assert forbidden not in body, (
            f"the audio probe calls {forbidden}. It is an observer: a play goes out "
            "as transfer-then-play and the transfer pauses the device, so commanding "
            "from here restarts a healthy track at 0:00 and blanks its cover — eight "
            "times in ninety minutes on 2026-08-05. If a real stall needs answering, "
            "use a seek on the no-transfer path, which Invariant A allows."
        )
    assert "_silentRecovered" not in body, (
        "_silentRecovered was the once-per-track guard on the re-dispatch. Its "
        "presence means the recovery is back"
    )


def test_probe_is_keyed_to_the_track_it_measures():
    body = _probe_body(browser_source())
    assert "state.trackId !== _probeTrack" in body, (
        "the probe must reset its baseline when the track changes. Without it the "
        "outgoing track left _probeLastPos at 104807, the incoming one reported "
        "0 -> 3000, and 'position not advancing' was true six polls running — a "
        "false stall on every single skip"
    )


def test_a_stall_that_clears_is_recorded_as_such():
    body = _probe_body(browser_source())
    assert "CLEARED" in body.upper(), (
        "the probe must log when a stall resolves on its own, and how long it "
        "lasted. Counting only detections is what made 162 propagation artefacts "
        "read as 162 real stalls, which is the reading that put a destructive "
        "recovery on this path"
    )
    assert "sinceDispatchMs" in body, (
        "a stall a second after a dispatch is propagation lag, not silence. The "
        "log has to carry the distinction now that nothing acts on it"
    )


def main() -> int:
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"ok   {name}")
        except AssertionError as exc:
            failures += 1
            print(f"FAIL {name}: {exc}")
    print("\nall good" if not failures else f"\n{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
