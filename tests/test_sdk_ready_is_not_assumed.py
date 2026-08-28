"""Regression lock: a Spotify player that exists is not a player that works.

Reported 2026-08-17 as "playback issues with Name Is The Answer". The track was
innocent — it played, the progress bar advanced to 0.7% of a 7:19 trance track.
What broke was the NEXT boot. From prod logs, 09:49-09:51:

    09:49:12  pbar SDK-paint pct 0.7            <- audio genuinely running
    09:50:06  handlePlay  discoveryLen: 0, currentTrack: "DIG"   <- fresh boot
    09:50:07  init: skipped (player already up)  hasPlayer: true, initing: false
    09:50:11  consume[discovery-loaded] held — player not ready yet
    09:50:30  bail: Player not ready, _playPending=true (waiting for SDK ready)
    09:51:16  transition: none->bandcamp        <- the skip; Bandcamp needs no SDK

`spotify.init()` guards on `spotify._initing || spotify.player` — built to stop
a reconnect storm from stacking N players, each with its own listener set. But
`spotify.player` is assigned BEFORE connect() resolves and long before the SDK
announces a device, so the guard reads "an object exists" as "a player works".
When 'ready' never arrived, nothing could ever rebuild it: the app waited on an
event that was not coming, for 70 seconds, until a Bandcamp track let it dodge
the SDK entirely.

Three ways a player can be dead, and only ONE of them was handled:

  * authentication_error  -> torn down and rebuilt.            (was handled)
  * connect() returns false -> no 'ready' is coming.           (was logged, dropped)
  * connect() true, 'ready' never fires -> silent, no callback. (was unhandled)

The SDK offers no failure signal for the third, so silence is the only symptom
and a timeout is the only detector.

Static rather than behavioural: provoking it needs a stubbed SDK that connects
and then withholds 'ready'. These lock the invariants instead, the same trade
test_ios_playback_divergence.py makes.

    python3 tests/test_sdk_ready_is_not_assumed.py
    pytest tests/test_sdk_ready_is_not_assumed.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from browser_source import ROOT  # noqa: E402

PLAYER = os.path.join(ROOT, "web", "js", "player.js")


def _src() -> str:
    with open(PLAYER, encoding="utf-8") as fh:
        return fh.read()


def _code_only(src: str) -> str:
    """`src` minus comments — the notes explaining each fix name the very
    symbols the tests require, so a bare search would pass on prose alone."""
    src = re.sub(r"/\*[\s\S]*?\*/", "", src)
    return re.sub(r"(?m)^\s*//.*$", "", src)


def _function_body(src: str, name: str) -> str:
    """The source of `function name(...) { ... }`, brace-matched."""
    start = src.index(f"function {name}")
    depth, i = 0, src.index("{", src.index(")", start))
    for j in range(i, len(src)):
        if src[j] == "{":
            depth += 1
        elif src[j] == "}":
            depth -= 1
            if depth == 0:
                return src[start:j + 1]
    raise AssertionError(f"unbalanced braces in {name}")


def _listener_body(src: str, event: str) -> str:
    """The body of `addListener('event', … )`, brace-matched from its ARROW.

    From the arrow, not from the first brace: `addListener('ready', ({ device_id
    }) => {` opens with a DESTRUCTURING brace, so walking from `index("{")`
    matches `{ device_id }` and returns a two-token body that contains none of
    the handler. That silently passes any test asserting something is ABSENT
    and fails every test asserting something is present — which is how this
    helper announced itself.
    """
    start = src.index(f"addListener('{event}'")
    depth, i = 0, src.index("{", src.index("=>", start))
    for j in range(i, len(src)):
        if src[j] == "{":
            depth += 1
        elif src[j] == "}":
            depth -= 1
            if depth == 0:
                return src[i:j + 1]
    raise AssertionError(f"unbalanced braces in the {event} listener")


def test_not_ready_clears_the_ready_flag():
    """It used to only log. `spotify.ready` therefore stayed TRUE after the
    device went offline, and play() issued commands at a device that was gone."""
    body = _code_only(_listener_body(_code_only(_src()), "not_ready"))
    assert re.search(r"spotify\.ready\s*=\s*false", body), \
        "not_ready must clear spotify.ready, or the app believes a dead device is live"


def test_not_ready_does_not_tear_the_player_down_immediately():
    """The other half of that judgement. not_ready is ROUTINE on mobile — it
    fires whenever the tab is backgrounded — and the SDK re-announces 'ready'
    itself. Rebuilding on the spot would churn a healthy player constantly."""
    body = _code_only(_listener_body(_code_only(_src()), "not_ready"))
    assert "_teardownPlayer" not in body, \
        "not_ready is routine (tab backgrounded); tearing down here rebuilds for no reason"


def test_a_device_that_dies_mid_session_still_recovers():
    """THE HOLE IN THE FIRST VERSION OF THIS FIX, found an hour after shipping
    it. The connect-time watchdog is cancelled by the first successful 'ready',
    so a device that died LATER had nothing watching: play() bails without
    reconnecting, init()'s guard still sees a live spotify.player and skips.
    Observed: `spotify NOT ready — bailing, deviceId: null` on every Spotify
    track for 15 minutes while Bandcamp played fine. not_ready must re-arm."""
    body = _code_only(_listener_body(_code_only(_src()), "not_ready"))
    assert "_armReadyWatchdog" in body, \
        ("not_ready must re-arm the watchdog, or a mid-session device death is "
         "permanent — the SDK usually re-announces 'ready', but 'usually' is "
         "not a recovery path")


def test_the_watchdog_is_armed_through_one_helper():
    """Connect-time and mid-session are the same question asked twice. Two
    copies is how one of them keeps a stale timer or forgets the counter."""
    code = _code_only(_src())
    assert code.count("function _armReadyWatchdog") == 1
    assert len(re.findall(r"_readyWatchdog\s*=\s*setTimeout", code)) == 1, \
        "the watchdog must be armed in exactly one place"


def test_connect_failing_is_acted_on_and_not_merely_logged():
    code = _code_only(_src())
    m = re.search(r"connectOk\s*=\s*await\s+spotify\.player\.connect\(\)", code)
    assert m, "connect() result must still be captured"
    after = code[m.end():m.end() + 900]
    assert re.search(r"if\s*\(\s*!\s*connectOk\s*\)", after), \
        "a false connect() means no 'ready' will ever arrive — it must be handled"
    assert "_teardownPlayer" in after, \
        "an inert player must be torn down or init()'s guard skips forever"


def test_a_ready_that_never_arrives_is_detected_by_a_timeout():
    code = _code_only(_src())
    assert "SDK_READY_TIMEOUT_MS" in code, \
        "the SDK gives no failure callback for this; a timeout is the only detector"
    assert re.search(r"_readyWatchdog\s*=\s*setTimeout", code), \
        "the timeout must actually be armed after connect()"


def test_the_watchdog_is_cancelled_when_ready_arrives():
    """Otherwise it fires mid-session and tears down a WORKING player — turning
    a fix for a rare wedge into a routine outage."""
    body = _code_only(_listener_body(_code_only(_src()), "ready"))
    assert "clearTimeout(spotify._readyWatchdog)" in body, \
        "a live player must cancel its own watchdog"


def test_rebuilding_is_bounded():
    """The init guard exists because unbounded rebuilds stacked N players. A
    recovery path that loops forever recreates the storm it was added to fix."""
    code = _code_only(_src())
    assert "SDK_READY_MAX_REBUILDS" in code
    assert re.search(r"_readyRebuilds\s*>\s*SDK_READY_MAX_REBUILDS", code), \
        "the rebuild counter must gate further attempts"
    assert re.search(r"_readyRebuilds\s*=\s*0", code), \
        "a successful ready must reset the allowance, or a long session runs out"


def test_every_death_path_uses_the_one_teardown():
    """Three ways to die, one teardown. Each must leave exactly the same state
    behind: player null (so the guard rebuilds) and ready false (so nothing
    plays at it). Two hand-rolled versions is how one of them drifts into
    leaving `spotify.player` set — which is this whole bug."""
    code = _code_only(_src())
    assert code.count("function _teardownPlayer") == 1, "one teardown, one meaning"
    # No path may hand-roll the nulling any more.
    others = re.findall(r"spotify\.player\s*=\s*null", code)
    assert len(others) == 1, (
        f"spotify.player is nulled in {len(others)} places; all death paths "
        "must go through _teardownPlayer")


# ── the status label ────────────────────────────────────────────────────────
# "I still see 'loading tracks' although I'm using the tool normally" — and it
# was true: 7,991 tracks were loaded and playing. handlePlay() sets that label
# during the one moment the pool is empty, and every path that cleared it ran
# through the Spotify SDK. A Bandcamp-only session never reached any of them.


def test_both_backends_clear_the_status_when_output_starts():
    code = _code_only(_src())
    assert code.count("function _clearTransientStatus") == 1, "one clear, one meaning"
    # Spotify's state_changed and Bandcamp's 'playing' are the two moments
    # audio genuinely starts. Both must call it, or the label outlives reality
    # on whichever backend is left out — which is exactly what happened.
    assert code.count("_clearTransientStatus()") >= 2, \
        "a status owned by one backend cannot describe an app with two"
    playing = code[code.index("addEventListener('playing'"):][:400]
    assert "_clearTransientStatus" in playing, \
        "the Bandcamp path is the one that was missing"


def test_sticky_mode_labels_are_not_wiped():
    """#player-status is shared with 'tailored', 'AI mix' and the journey
    label. A blanket clear would erase the listener's current mode; the old
    Spotify-side clear guarded only 'tailored' and would have wiped the rest."""
    code = _code_only(_src())
    assert "_TRANSIENT_STATUS" in code, "the clear must be a whitelist, not a blanket"
    listed = code[code.index("_TRANSIENT_STATUS"):][:400].lower()
    assert "loading tracks" in listed, "the reported string must be clearable"
    for sticky in ("tailored", "ai mix"):
        assert sticky not in listed, f"'{sticky}' is a mode label and must survive"

    # The list EXISTING proves nothing — the first version of this test passed
    # against a blanket `s.textContent = ''` that ignored it entirely. Assert
    # the whitelist is actually consulted, and that the assignment is guarded.
    fn = _function_body(code, "_clearTransientStatus")
    assert "_TRANSIENT_STATUS" in fn, \
        "the whitelist must be read INSIDE the clear, not merely declared near it"
    for line in fn.splitlines():
        if re.search(r"textContent\s*=\s*''", line):
            assert re.search(r"\bif\b|\?", line), (
                "the clear must be conditional on the whitelist; an "
                f"unguarded assignment wipes mode labels: {line.strip()!r}")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: SDK readiness invariants passed ({len(fns)} tests)")
