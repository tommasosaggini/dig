"""Regression lock for the page owning its own media session.

What happened, on a Mac in Chrome (2026-07-30): double-tapping the earbuds
stopped skipping. The gesture was not lost — the server log has it — but it
arrived as the wrong action:

    13:58 → 14:23   [CLIENT media] nexttrack (AirPods/lock screen)   x22   ok
    14:23:33        [CLIENT media] pause (AirPods/lock screen)
      ... 20 min idle, then the play button in the UI ...
    14:44:12        [CLIENT media] play (AirPods/lock screen)        <- not nexttrack
    14:44:20        [CLIENT media] play (AirPods/lock screen)
    14:44:48        [CLIENT media] play (AirPods/lock screen)

`play` while already playing calls Player.resume() on a playing player, which
is a no-op — hence "nothing happens". The OS sent `play` because it believed
DIG was paused, and it believed that because `navigator.mediaSession
.playbackState` is ADVISORY in Chrome: the OS-facing state is derived from a
real media element in the frame tree. On the Spotify path the audio lives in
the SDK's cross-origin iframe, so this page owned no element and the state was
never ours to set. The skip at 14:44:24 re-ran the whole re-arm path (metadata
+ playbackState='playing' + setActionHandler) and the very next tap at 14:44:48
still came through as `play` — which is what rules out "the page forgot to tell
the OS" and leaves "the page has nothing to tell the OS with".

The fix is ownership, not repair: a silent looping <audio> in the top frame
that plays exactly while DIG is logically playing. These assertions pin the
three properties that make it work at all — long enough for Chrome to grant a
session (>5s), unmuted (Chrome ignores muted elements when picking the session
owner), and reconciled from the single place that already knows the truth.

Static assertions against web/app.html: the file is one 300 KB inline script
with no module boundary to import.

    python3 tests/test_media_anchor.py      # bare, no deps
    pytest tests/test_media_anchor.py       # if pytest is installed
"""
import os
import re
import sys

APP = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "web", "app.html")


def _app() -> str:
    with open(APP, encoding="utf-8") as fh:
        return fh.read()


def _anchor() -> str:
    """The msAnchor object literal."""
    src = _app()
    start = src.index("const msAnchor = {")
    end = src.index("\n  };", start)
    return src[start:end]


def test_the_anchor_clip_outlasts_chromes_media_session_threshold():
    """Chrome grants a media session only to media longer than ~5s."""
    body = _anchor()
    rate = re.search(r"const SR = (\d+), n = SR \* (\d+);", body)
    assert rate, "the anchor no longer builds its own clip"
    seconds = int(rate.group(2))
    assert seconds > 5, (
        f"anchor clip is {seconds}s; Chrome ignores media <=5s and the OS "
        "session falls back to the Spotify iframe"
    )
    assert ".loop = true" in body, "a 6s clip that does not loop stops owning the session"


def test_the_anchor_stays_audible_to_chrome():
    """A muted element is not a session owner, however loudly we set playbackState."""
    body = _anchor()
    assert ".muted = false" in body, "muted anchor cannot own the media session"
    assert ".volume = 1" in body, "volume 0 reads as muted to Chrome"
    assert "fill(128)" in body, (
        "the samples must be silence (128 = unsigned 8-bit zero) — an audible "
        "anchor would play noise under every track"
    )


def test_the_anchor_is_reconciled_from_the_playback_state_we_already_poll():
    """One place owns OS state, and it is the one that reads the real player."""
    src = _app()
    sync = src[src.index("async function syncMediaSessionPlayback()"):]
    sync = sync[:sync.index("\n  // ── Public API")] if "\n  // ── Public API" in sync else sync[:6000]
    assert "msAnchor.apply(!st.paused)" in sync, (
        "the anchor must follow the backend's paused flag, not a separate guess"
    )
    # Every early return that declares 'none' must stop the anchor too, or it
    # keeps claiming the session after playback is gone.
    assert sync.count("playbackState = 'none'") == sync.count("msAnchor.apply(false)"), (
        "a path that reports 'none' to the OS but leaves the anchor playing "
        "leaves the session owned by silence"
    )


def test_the_anchor_never_hijacks_a_session_that_belongs_elsewhere():
    """Bandcamp owns a real <audio>; on iOS the Spotify app owns Now Playing."""
    body = _anchor()
    want = re.search(r"const want = ([^;]+);", body)
    assert want, "the anchor no longer gates when it runs"
    cond = want.group(1)
    assert "!DIG_IS_IOS" in cond, (
        "on iOS playback is Spotify Connect — audio is in the Spotify app and "
        "the anchor would steal its lock-screen controls"
    )
    assert "activeSource === 'spotify'" in cond, (
        "the Bandcamp path already owns a real <audio>; a second playing "
        "element there is a conflict, not a fix"
    )


def test_autoplay_refusal_is_retried_on_a_gesture_not_on_the_poll():
    """The poll runs every 500ms; a rejected play() must not run 120 times a minute."""
    body = _anchor()
    assert "_retryOnGesture" in body, "a refused anchor play() is dropped silently"
    assert "pointerdown" in body and "removeEventListener" in body, (
        "the retry hook must arm on a real interaction and unhook itself"
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
    print("\nall media-anchor checks passed" if not failed else f"\n{failed} failed")
    sys.exit(1 if failed else 0)
