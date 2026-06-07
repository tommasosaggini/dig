"""Regression lock for lib.track_filter.is_trash.

is_trash() drives destructive deletes (scripts/purge_junk.py) and gates every
ingested track, so a silent change in its behaviour is high-impact. These
cases pin CURRENT behaviour (verified 2026-06-07) — they are a change-detector,
not an aspiration. If you intentionally change the filter and a case flips,
update the expectation in the same commit.

track_filter imports cleanly without a DB (the artist blacklist fails open to
an empty set), so this runs anywhere.

    python3 tests/test_track_filter.py     # bare, no deps
    pytest tests/test_track_filter.py       # if pytest is installed
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import track_filter  # noqa: E402

# (track_name, artist_name, album_name)
TRASH = [
    ("Sleep Music for Babies", "Relaxing Spa Ambient", ""),       # wellness factory
    ("Deep Sleep Rain Sounds", "Sleep Sounds Academy", ""),        # sleep factory
    ("Synthpop Tokyo Girl XIX", "Neon Beat Station", ""),          # numbered-variant + stock artist
    ("微笑旋律音樂", "療癒輕音樂", ""),  # CJK wellness compound
    ("Bien Dormir Musique", "Relaxation", ""),                     # multilingual sleep idiom
    ("Meditation Music for Yoga and Spa", "Meditation Relax Club", ""),
    ("Rain Sounds for Sleeping", "Nature Sounds", ""),             # nature field-recording
]

GOOD = [
    ("Soro", "Salif Keita", ""),
    ("Zombie", "Fela Kuti", ""),
    ("Yeke Yeke", "Mory Kanté", ""),
    ("Chan Chan", "Buena Vista Social Club", ""),
    ("Paranoid Android", "Radiohead", ""),
    ("Strings of Life", "Derrick May", ""),
    ("Idioteque", "Radiohead", ""),
]


def test_known_trash_is_rejected():
    wrong = [f"{a} — {n}" for (n, a, al) in TRASH if not track_filter.is_trash(n, a, al)]
    assert not wrong, "Expected TRASH but is_trash returned False:\n  " + "\n  ".join(wrong)


def test_real_music_is_kept():
    wrong = [f"{a} — {n}" for (n, a, al) in GOOD if track_filter.is_trash(n, a, al)]
    assert not wrong, "Expected GOOD but is_trash returned True:\n  " + "\n  ".join(wrong)


def test_empty_input_is_not_trash():
    assert track_filter.is_trash("", "", "") is False


if __name__ == "__main__":
    test_known_trash_is_rejected()
    test_real_music_is_kept()
    test_empty_input_is_not_trash()
    print(f"OK: track_filter regression lock passed ({len(TRASH)} trash + {len(GOOD)} good)")
