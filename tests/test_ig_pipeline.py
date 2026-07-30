"""Unit tests for the Instagram pipeline's pure logic (no DB / no network).

Covers the two things most likely to break silently:
  - next_slot() scheduling math (cadence, never-in-the-past fast-forward)
  - caption templating (shape + funnel line + hashtags)

lib.ig_queue imports lib.db at module load but opens NO connection, so this
runs anywhere.

    python3 tests/test_ig_pipeline.py
    pytest tests/test_ig_pipeline.py
"""
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import ig_queue, ig_caption  # noqa: E402

UTC = datetime.timezone.utc


def test_next_slot_from_empty_picks_post_hour():
    now = datetime.datetime(2026, 6, 28, 9, 0, tzinfo=UTC)  # before POST_HOUR_UTC (18)
    slot = ig_queue.next_slot(None, now=now)
    assert slot.hour == ig_queue.POST_HOUR_UTC
    assert slot.date() == now.date()  # same day, later today


def test_next_slot_from_empty_rolls_to_tomorrow_after_hour():
    now = datetime.datetime(2026, 6, 28, 20, 0, tzinfo=UTC)  # after POST_HOUR_UTC
    slot = ig_queue.next_slot(None, now=now)
    assert slot.hour == ig_queue.POST_HOUR_UTC
    assert slot.date() == (now + datetime.timedelta(days=1)).date()


def test_next_slot_advances_by_cadence():
    last = datetime.datetime(2026, 6, 28, 18, 0, tzinfo=UTC)
    now = datetime.datetime(2026, 6, 28, 18, 5, tzinfo=UTC)
    slot = ig_queue.next_slot(last, now=now)
    assert slot == last + datetime.timedelta(hours=ig_queue.CADENCE_HOURS)


def test_next_slot_never_in_the_past():
    # A stale last-slot far in the past must fast-forward past `now`, in whole
    # cadence steps — never dump a backlog by returning a past time.
    last = datetime.datetime(2026, 6, 1, 18, 0, tzinfo=UTC)
    now = datetime.datetime(2026, 6, 28, 9, 0, tzinfo=UTC)
    slot = ig_queue.next_slot(last, now=now)
    assert slot > now
    delta_h = (slot - last).total_seconds() / 3600
    assert delta_h % ig_queue.CADENCE_HOURS == 0  # landed on a cadence boundary


def test_template_caption_shape():
    cap = ig_caption.template_caption("Idioteque", "Radiohead", ["electronic", "idm"])
    assert cap.startswith("Idioteque — Radiohead")
    assert ig_caption.BIO_LINE in cap
    assert "#dig" in cap
    assert "#electronic" in cap  # genre folded into hashtags


def test_template_caption_no_genres():
    cap = ig_caption.template_caption("Soro", "Salif Keita")
    assert "Soro — Salif Keita" in cap
    assert ig_caption.BIO_LINE in cap


def test_stable_hash_is_deterministic():
    assert ig_queue._stable_hash("bc:1:2") == ig_queue._stable_hash("bc:1:2")
    assert ig_queue._stable_hash("a") != ig_queue._stable_hash("b")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: ig pipeline logic passed ({len(fns)} tests)")
