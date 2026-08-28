"""Where saves go: two destinations, and the user is told which one.

Two things this locks, both of which failed silently before:

  1. There are exactly TWO destinations. A third ("any playlist you own")
     existed, was chosen by nobody — 3,830 accounts, all on the default — and
     cost a Spotify call on every settings open to populate its picker. If it
     comes back, it should come back deliberately, with this test updated.
  2. Saving the setting produces a message naming the destination. It used to
     just close the dialog, which is indistinguishable from the dialog closing.
     This setting has no visible effect until the NEXT save lands somewhere the
     listener didn't expect, so silence is the one unacceptable response.

The label lives in server.save_destination_label and is sent to the client, so
the panel's wording and the mirror's actual target can't drift apart.

    python3 tests/test_save_destination.py     # bare, no deps
    pytest tests/test_save_destination.py       # if pytest is installed
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import server  # noqa: E402

WEB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web")


def _read(*parts):
    with open(os.path.join(WEB, *parts), encoding="utf-8") as fh:
        return fh.read()


def test_exactly_two_destinations():
    assert server.SAVE_DESTINATIONS == ("dig_playlist", "liked_songs")
    assert not hasattr(server, "SAVE_DEST_PLAYLIST"), \
        "the retired third destination is back — update this test on purpose"


def test_every_destination_has_a_label():
    for dest in server.SAVE_DESTINATIONS:
        label = server.save_destination_label(dest)
        assert label and label.startswith("your "), dest
    # Unknown input must not produce a blank sentence fragment ("go to .").
    assert server.save_destination_label("nonsense")


def test_settings_panel_offers_the_same_two():
    html = _read("app.html")
    offered = re.findall(r'name="saveDest"\s+value="([^"]+)"', html)
    assert offered == list(server.SAVE_DESTINATIONS), offered
    assert "saveDestPlaylist" not in html, "the retired playlist picker is still in the markup"


def test_saving_the_setting_says_something_back():
    js = _read("js", "app.js")
    block = js.split("SETTINGS: where DIG saves land on Spotify", 1)[1]
    # A success path that only closes the dialog is the bug this guards.
    assert "out.where" in block, "the confirmation no longer names the destination"
    assert re.search(r"showNote\(`Saved\.", block), "no success confirmation on save"
    assert "tone: 'ok'" in block, "success and failure must not look alike"
    # And it must still handle the two ways saving can go wrong.
    assert "needs_relink" in block
    assert "Could not save that." in block


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    print("all save-destination tests passed" if not failures
          else f"{failures} failed")
    sys.exit(1 if failures else 0)
