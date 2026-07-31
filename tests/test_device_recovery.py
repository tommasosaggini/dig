"""Regression lock for the NO_ACTIVE_DEVICE recovery in /api/play.

The bug: on iOS, DIG pins no device ("let Spotify decide"), so it issues
`PUT /me/player/play` with no device_id. That call requires an ALREADY-ACTIVE
device. A Spotify app that gets backgrounded stays in /me/player/devices but
flips to is_active=false, so the play 404s "No active device found" — and the
client's recovery was to reissue the identical device-less call, which could
only fail the same way, then deep-link into an app that does nothing on a locked
phone. Net effect: playback died mid-session and DIG reported success.

The fix wakes the listed-but-sleeping device and reissues against it explicitly.
These pin the selection rule that decides WHICH device gets woken.

    python3 tests/test_device_recovery.py
    pytest tests/test_device_recovery.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import server  # noqa: E402

pick = server._pick_playback_device


def test_no_devices_means_no_recovery():
    assert pick([]) is None
    assert pick(None or []) is None


def test_restricted_devices_are_never_chosen():
    # is_restricted devices reject API control outright — waking one is a no-op
    # that would burn the retry and land the user back in the dead state.
    devs = [{"id": "a", "type": "Smartphone", "is_restricted": True, "is_active": True}]
    assert pick(devs) is None


def test_a_device_with_no_id_is_unusable():
    assert pick([{"id": None, "type": "Computer"}]) is None


def test_prefers_the_already_active_device():
    devs = [
        {"id": "phone", "type": "Smartphone", "is_active": False},
        {"id": "desktop", "type": "Computer", "is_active": True},
    ]
    assert pick(devs)["id"] == "desktop", "an active device is the cheapest to wake"


def test_prefers_a_phone_when_nothing_is_active():
    # The exact production case: iPhone app backgrounded, nothing active.
    devs = [
        {"id": "desktop", "type": "Computer", "is_active": False},
        {"id": "phone", "type": "Smartphone", "is_active": False},
    ]
    assert pick(devs)["id"] == "phone", (
        "on iOS the phone's Spotify app IS the user's player — waking the "
        "desktop would start music on a machine they aren't at"
    )


def test_an_idle_unrelated_device_is_never_chosen():
    """This assertion is the reverse of the one it replaces, on purpose.

    There used to be a "whatever is left" fallback, and on 2026-07-31 it played
    a track on a Mac at home while the user was outside with their phone: the
    phone sent no device id, the only listed candidate was an idle DIG web-SDK
    device on the laptop, Spotify answered 204 because it was listed, and the
    phone sat at a progress bar frozen at 0.

    An idle device that is neither the caller's nor a phone is a guess about
    which room the user is standing in, and a wrong guess puts music in an
    empty house. None is the honest answer; the client turns it into the
    "Spotify went to sleep" banner.
    """
    devs = [{"id": "someone-elses-mac", "type": "Computer", "is_active": False}]
    assert pick(devs) is None


def test_the_callers_own_device_wins_even_when_idle():
    """Waking the device the client NAMED is never a guess — it asked for it."""
    devs = [
        {"id": "mac-at-home", "type": "Computer", "is_active": True},
        {"id": "this-browser", "type": "Computer", "is_active": False},
    ]
    assert pick(devs, "this-browser")["id"] == "this-browser", (
        "an active device elsewhere must not outrank the one the caller named"
    )


def test_an_unknown_caller_device_does_not_block_recovery():
    """If the named device is gone entirely, fall through the normal order."""
    devs = [{"id": "phone", "type": "Smartphone", "is_active": False}]
    assert pick(devs, "a-device-that-no-longer-exists")["id"] == "phone"


def test_skips_restricted_even_when_it_would_otherwise_win():
    devs = [
        {"id": "restricted-phone", "type": "Smartphone", "is_active": True, "is_restricted": True},
        {"id": "ok-desktop", "type": "Computer", "is_active": True},
    ]
    assert pick(devs)["id"] == "ok-desktop"


def test_restriction_is_what_excluded_it_not_the_new_rule():
    """Guards the test above from passing for the wrong reason."""
    devs = [{"id": "restricted-phone", "type": "Smartphone", "is_active": True,
             "is_restricted": True}]
    assert pick(devs) is None


def test_devices_helper_never_raises():
    # It's a recovery path; a failure here must not become the thing that
    # fails the play. Bad headers -> [] rather than an exception.
    assert server._spotify_devices({"Authorization": "Bearer not-a-real-token"}) == []


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("ok   %s" % name)
            except AssertionError as e:
                print("FAIL %s\n     %s" % (name, e))
                fails += 1
            except Exception as e:  # noqa: BLE001
                # A crash is a failed check, not a reason to stop. An uncaught
                # ValueError aborted a sibling file mid-run and it still printed
                # only "ok" lines — a red suite reading as green.
                print("ERROR %s\n     %s: %s" % (name, type(e).__name__, e))
                fails += 1
    sys.exit(1 if fails else 0)
