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


def test_falls_back_to_any_usable_device():
    devs = [{"id": "speaker", "type": "Speaker", "is_active": False}]
    assert pick(devs)["id"] == "speaker"


def test_skips_restricted_even_when_it_would_otherwise_win():
    devs = [
        {"id": "restricted-phone", "type": "Smartphone", "is_active": True, "is_restricted": True},
        {"id": "ok-speaker", "type": "Speaker", "is_active": False},
    ]
    assert pick(devs)["id"] == "ok-speaker"


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
    sys.exit(1 if fails else 0)
