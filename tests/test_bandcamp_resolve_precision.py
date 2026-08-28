"""resolve_track must reject re-uploads, not just non-matches.

lib/bandcamp.py's own header says PRECISION OVER RECALL: "a wrong match is
worse than no match — it puts someone else's music in the pool under a
curator's name, and nothing downstream can tell." It then used the LOOSE
`_agree` on the artist side, and that is exactly the hole the header warns
about, because a remixer's account name CONTAINS the artist's name:

    asked "David Bowie"  ->  account "David Bowie - Heroes (Mindsodt-D ReWork)"

Token containment either way accepts that. `is_same_artist` — the strict form,
already in the file, already wired into tracks_by_artist for the same reason —
rejects it, because the account name may be a SHORTER form of who we asked for
and never a longer one.

Measured on doubleudiego's 71 candidates (2026-08-17): 23 resolved, 8 of them
re-uploads — a Bowie "ReWork", a Smiths remix, two MJ edits, an MJ mashup, a
Cure remix, a Beatles cover. Every fixture below is one of those.

    python3 tests/test_bandcamp_resolve_precision.py
    pytest tests/test_bandcamp_resolve_precision.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.bandcamp import (_gained_a_bootleg_mark,  # noqa: E402
                          _title_restates_the_artist, is_same_artist)


def test_a_remixer_account_is_not_the_artist():
    # THE regression. Every one of these passed the old loose check.
    assert not is_same_artist(
        "David Bowie", "David Bowie - Heroes (Mindsodt-D ReWork)")
    assert not is_same_artist(
        "Michael Jackson", "LmbrJck_T; Michael Jackson, J. Dilla, Pete Rock")


def test_the_real_artist_still_resolves():
    # The strict rule must not cost the matches that made this worth doing.
    assert is_same_artist("Les Rallizes Dénudés", "Les Rallizes Dénudés")
    assert is_same_artist("SLOW CRUSH", "Slow Crush")
    # A shorter form is fine — an accentless spelling of the same name.
    assert is_same_artist("Cesária Évora", "Cesaria Evora")


def test_a_title_that_gained_a_remix_marker_is_a_reupload():
    for asked, got in (("Bigmouth Strikes Again",
                        "Bigmouth Strikes Again (Choice Cuts Remix)"),
                       ("Pictures of You",
                        "The Cure - Pictures Of You (remixed by Dept Mine)"),
                       ("Baby Be Mine", "Baby Be Mine (Mike Tempo Edit)"),
                       ("A Day In The Life", "A Day In The Life (cover)")):
        assert _gained_a_bootleg_mark(asked, got), (asked, got)


def test_a_marker_the_curator_asked_for_is_not_a_reupload():
    """Directional, or a genuine remix single could never resolve — and
    "GLOW - Alternative Master" is the release title Slow Crush shipped."""
    assert not _gained_a_bootleg_mark("Windowlicker (Remix)",
                                      "Windowlicker (Remix)")
    assert not _gained_a_bootleg_mark("GLOW - Alternative Master",
                                      "GLOW - Alternative Master")
    assert not _gained_a_bootleg_mark("Trees & Flowers", "Trees & Flowers")


def test_a_title_restating_the_artist_is_an_upload_not_a_release():
    # A release does not put its own artist in the song title; an uploader does.
    assert _title_restates_the_artist(
        "Michael Jackson", "Michael Jackson - Baby Be Mine (Mike Tempo Edit)")
    assert _title_restates_the_artist("The Cure", "The Cure – Pictures Of You")
    assert _title_restates_the_artist("Perfume", "Perfume: edge")


def test_the_artist_name_inside_a_title_is_not_a_credit():
    """The separator carries the whole claim. Without requiring it, a song
    that merely mentions the act — or an act whose name opens its own title —
    would be thrown away."""
    assert not _title_restates_the_artist("Bowie", "Bowie Knife Blues")
    assert not _title_restates_the_artist("Broadcast", "Broadcasting Live")
    assert not _title_restates_the_artist("Sadness", "on a green")
    assert not _title_restates_the_artist("", "anything")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: bandcamp resolve precision passed ({len(fns)} tests)")
