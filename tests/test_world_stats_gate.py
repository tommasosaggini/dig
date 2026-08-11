"""The world-coverage view is admin-only while it is being shaped.

The bubbles map shipped straight to everyone, crashed on zoom, confused more
than it explained, and had to be pulled. Its replacement launches the other
way round: /api/world-stats answers only ADMIN_UID until the view has been
tested by an actual human, and the page (world.html) shows a quiet
"not public yet" card on the 403.

    python3 tests/test_world_stats_gate.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402


def _src(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def _handler() -> str:
    py = _src("server.py")
    i = py.index('if parsed.path == "/api/world-stats":')
    return py[i:i + 2500]


def test_the_gate_comes_before_any_query():
    block = _handler()
    gate = block.find("ADMIN_UID")
    first_query = block.find("fetchall")
    assert 0 < gate < first_query, (
        "/api/world-stats must refuse non-admin callers BEFORE running any "
        "aggregate — the view is unlaunched")


def test_the_gate_refuses_anonymous_too():
    block = _handler()
    assert re.search(r"if not user_id or user_id != ADMIN_UID", block), (
        "an anonymous request must be refused by the same check")


def test_the_page_handles_the_gate_quietly():
    html = _src("web/world.html")
    assert "isn't public yet" in html, (
        "a non-admin opening world.html should see a calm card, not a "
        "broken fetch error")


def test_all_three_dimensions_are_served():
    block = _handler()
    for dim in ("countries", "genres", "decades"):
        assert f'"{dim}"' in block, f"world-stats must include {dim}"


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok  {name}")
            except AssertionError as e:
                failed += 1
                print(f"FAIL  {name}: {e}")
    sys.exit(1 if failed else 0)
