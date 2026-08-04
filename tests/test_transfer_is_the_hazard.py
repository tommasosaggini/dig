"""Regression lock for "I go back to DIG and the song resets / stops".

Reported from the phone 2026-08-03/04, three symptoms that turned out to be one
mechanism at three severities. `PUT /me/player {device_ids:[…], play:false}`
PAUSES the device; the play that follows is what starts it again. So the pause
lasts exactly as long as the transfer round-trip, and if the play then fails the
device stays paused. Every case measured on this account sits on that one axis:

    transfer  990ms → 204, play 204    works — the audible "stopped for half a
                                       second" IS the transfer, nothing else
    transfer 2250ms → 204, play 502    device left paused, nothing restarts it
                                       (2026-08-03 15:18:56, "Ari Ari")
    transfer 3505ms → 404              device gone before the transfer landed
                                       (2026-08-04 01:50:06) → Bandcamp + banner
    no transfer     → play 204 in      position preserved across the swap
                      298ms            (measured live, twice)

The transfer is there to WAKE A SLEEPING DEVICE — server.py says so: "Spotify
returns 404 if the device hasn't been actively playing recently". In all three
failures the device was PLAYING; that is why DIG was adopting it. The transfer
was never needed and was the only thing that could strand the music.

The other half of the report — "Pop a Top is back to 0" — is a different defect
that fires from the same tap, and is locked below too: `_anchorProgress` wrote
DIG's optimistic "snap the bar to 0" into `_lastState`, the same variable the
poll fills from /me/player and that `lastSpotifyState()` serves to Invariant A
as EVIDENCE. Adoption then followed DIG's own intent and adopted the song at 0.

    python3 tests/test_transfer_is_the_hazard.py
    pytest tests/test_transfer_is_the_hazard.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT, browser_source  # noqa: E402


def _player() -> str:
    with open(os.path.join(ROOT, "web", "js", "player.js"), encoding="utf-8") as fh:
        return fh.read()


def _server() -> str:
    with open(os.path.join(ROOT, "server.py"), encoding="utf-8") as fh:
        return fh.read()


def _code_only(src: str) -> str:
    """`src` minus comments — the comments here quote the very thing under
    test, so a bare substring search finds the explanation and calls it the
    implementation."""
    src = re.sub(r"/\*[\s\S]*?\*/", "", src)
    src = re.sub(r"(?m)^\s*//.*$", "", src)
    return re.sub(r"(?m)^\s*#.*$", "", src)


# ─────────────────────── the transfer ───────────────────────

def test_the_server_can_be_told_to_skip_the_transfer():
    code = _code_only(_server())
    assert 'qs.get("no_transfer"' in code, (
        "the caller that has just read the device's state must be able to say "
        "so; without it every adopt pauses the phone for a full round-trip")
    assert "if device_id and not skip_transfer:" in code, (
        "the transfer must be CONDITIONAL. Left unconditional, a play that "
        "fails after it leaves the device paused — that is 'Ari Ari'.")


def test_skipping_the_transfer_does_not_skip_the_wake_recovery():
    """A wrong assertion must degrade, not break.

    If the device turns out NOT to be playing, the play 404s and the server's
    existing wake-and-reissue recovery still runs — it just does the transfer at
    the moment it is genuinely needed. Deleting that recovery would turn a
    recoverable miss into a dead play.
    """
    code = _code_only(_server())
    assert 'if first.code != 404:' in code and 'raise' in code, (
        "the play-404 wake-and-reissue recovery is what makes no_transfer safe "
        "to get wrong")
    assert '"play": False' in code, "the wake itself still transfers, on demand"


def test_only_a_caller_holding_a_fresh_state_read_may_skip_the_transfer():
    """no_transfer is an assertion about the world, not a speed switch.

    _installLookaheadOnAdopted is handed the state read that says this device is
    playing this track — that is the evidence. A caller without one that flips
    this flag is claiming something it cannot know, and the failure mode is the
    one this whole file exists to prevent.
    """
    code = _code_only(_player())
    assert code.count("noTransfer: true") == 1, (
        "exactly one caller should assert this; a second one means somebody "
        "claimed a device was playing without having looked")
    install = code[code.index("function _installLookaheadOnAdopted"):]
    install = install[:install.index("Player.getState")]
    assert "noTransfer: true" in install, (
        "the adopt path is the one with the evidence and the one that broke")


def test_the_play_url_carries_the_flag_only_when_asserted():
    code = _code_only(_player())
    assert "opts.noTransfer) ? '&no_transfer=1' : ''" in code, (
        "the flag has to reach the server or the fix is inert — and it must "
        "default to today's behaviour for every other caller")


# ─────────────────────── Invariant A ───────────────────────

def test_the_invariant_a_exception_is_conditional_on_being_harmless():
    """The exception must not be readable as `installLookahead` alone.

    That was the bug: the exception was justified by "installing the look-ahead
    is not redundant", but Invariant A exists because the command is
    DESTRUCTIVE, not because it is redundant. Non-redundant and destructive is
    exactly what stopped the music.
    """
    code = _code_only(_player())
    assert "opts.installLookahead && opts.noTransfer" in code, (
        "the exception survives only while it cannot break working playback")
    assert "!(opts && opts.installLookahead))" not in code, (
        "the unconditional bypass is what cost 'Ari Ari' — it must not return")


def test_invariant_a_still_blocks_a_plain_redundant_command():
    """Guarding the exception must not have disarmed the guard."""
    code = _code_only(_player())
    guard = code[code.index("const digDispatchedThis"):]
    guard = guard[:guard.index("Multi-URI") if "Multi-URI" in guard else 2000]
    for needed in ("already", "!already.paused", "already.trackId === trackId",
                   "!digDispatchedThis"):
        assert needed in guard, f"Invariant A lost its `{needed}` term"


def test_the_lookahead_feature_is_not_quietly_disabled():
    """Deleting the exception outright would have been the other wrong fix.

    Without it, adoption can never install DIG's context and Spotify plays the
    rest of its own album on the next advance — the "lil soda boy" report. The
    point is a look-ahead that installs WITHOUT stopping the music, not no
    look-ahead.
    """
    code = _code_only(_player())
    assert "installLookahead: true" in code, "the install must still happen"
    assert "_installLookaheadOnAdopted(state)" in code, (
        "adoptPlaying must still reach for the context")


# ─────────────────── the anchor is not an observation ───────────────────

def test_an_anchor_is_marked_as_intent():
    code = _code_only(_player())
    anchor = code[code.index("Player._anchorProgress = function"):]
    anchor = anchor[:anchor.index("Player._getStateCachedOrFresh")]
    assert "_anchored: true" in anchor, (
        "_anchorProgress writes into the same variable the poll fills from "
        "/me/player; unmarked, DIG's intent is indistinguishable from Spotify's "
        "answer")


def test_last_spotify_state_refuses_to_serve_an_anchor_as_evidence():
    """This is the reset-to-0.

    Invariant A asks lastSpotifyState() "what is Spotify doing", and got back
    DIG's own `position: 0` from the dispatch 1.7s earlier — measured
    2026-08-04 01:50:04, "already playing this — following, not commanding,
    posMs: 0", when a poll had read 67779 moments before. Adoption then took
    position 0 and the card snapped to 0:00.
    """
    code = _code_only(_player())
    fn = code[code.index("Player.lastSpotifyState = function"):]
    fn = fn[:fn.index("Player.adoptPlaying")]
    assert "_anchored" in fn, (
        "the evidence reader must reject an anchor; 'I don't know' beats "
        "handing back our own intent as Spotify's answer")
    assert "return null" in fn


def test_the_progress_bar_still_reads_the_anchor():
    """The anchor exists FOR the bar — that must keep working.

    Snapping to 0 the instant we dispatch is the whole reason it is written
    eagerly; the fix separates who may treat it as truth, it does not stop the
    interpolator using it.
    """
    code = _code_only(_player())
    interp = code[code.index("Player._getStateCachedOrFresh"):]
    interp = interp[:interp.index("Player.lastSpotifyState")]
    assert "_lastState" in interp and "_anchored" not in interp, (
        "the interpolator wants intent — it must NOT have inherited the "
        "evidence check")


def test_an_observed_poll_clears_the_anchor_flag():
    """Otherwise one dispatch would poison every later read.

    Both observed writers replace the object wholesale rather than merging, so
    the flag cannot survive a real state read. Changing either to Object.assign
    over the previous state would reintroduce the bug silently.
    """
    code = _code_only(_player())
    assert "_lastState = state;" in code, "adoptPlaying replaces, not merges"
    assert "_lastState = out;" in code, "the poll replaces, not merges"


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
                # A test that raises is a FAILING test, not a stopped run. An
                # unguarded .index() aborted the whole file and grep-for-FAIL
                # reported it as a clean pass.
                failed += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    if failed:
        print(f"\n{failed} failed")
        sys.exit(1)
    print("all transfer-hazard checks passed")
