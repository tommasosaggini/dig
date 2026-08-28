#!/usr/bin/env python3
"""Cell selection must lean toward the starved decades without banning the rest.

THE DEFECT IT GUARDS
--------------------
pick_unexplored_cells was decade-blind, so it scanned cells roughly in
proportion to how many happened to exist per decade — 14 days to 2026-08-18:
2020s 156 cells, 2010s 146, 2000s 125, 1990s 86, 1980s 65. Those counts are
themselves a product of the same recency skew (a genre first seen on a 2024
track only ever gets a 2020s cell — see scripts/backfill_era_cells.py), so the
pipeline compounded its own bias and the served era mix followed it exactly.

THE TRAP IN THE OBVIOUS FIX
---------------------------
Adding `(decade = ANY(starved)) DESC` to the ORDER BY reads like a tier and
behaves like a filter. There are ~6,500 never-scanned cells in the starved
decades, so they fill the whole candidate list and nothing outside 1980-2009
gets scanned for about two hundred days. Measured while writing the change: the
first 240 candidates came back 100% starved.

The picker loop is GREEDY — it walks the candidate list and stops at n — so a
budget cannot be expressed by putting one list after another either. Whatever
comes first takes everything. Interleaving is the only shape that survives a
greedy reader, which is what these pin.

  python3 tests/test_cell_selection_favours_starved_eras.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.era import starved_decades  # noqa: E402

SRC = open(os.path.join(ROOT, "pipeline", "discover.py"), encoding="utf-8").read()


def _load_interleave():
    """Pull the pure helper out without importing discover.py, which builds a
    live Spotify client at module scope."""
    import ast
    tree = ast.parse(SRC)
    ns = {}
    for node in tree.body:
        keep = (isinstance(node, ast.FunctionDef)
                and node.name == "interleave_starved")
        keep |= (isinstance(node, ast.Assign)
                 and getattr(node.targets[0], "id", "") == "STARVED_PER_GENERAL")
        if keep:
            exec(compile(ast.Module([node], []), "<t>", "exec"), ns)
    return ns["interleave_starved"], ns["STARVED_PER_GENERAL"]


def test_the_lean_is_a_lean_and_not_a_ban():
    """The whole point. Two thirds starved, a third left open."""
    interleave, ratio = _load_interleave()
    starved = [("S", i) for i in range(100)]
    general = [("G", i) for i in range(100)]
    head = interleave(starved, general)[:60]
    s = sum(1 for x in head if x[0] == "S")
    assert 0.55 < s / 60 < 0.80, (
        f"{s}/60 starved — a lean should be roughly {ratio}:1, and neither a "
        "coin flip nor a monopoly")
    assert any(x[0] == "G" for x in head), (
        "no decade-blind cell in the first 60 candidates: the greedy picker "
        "would never scan outside 1980-2009 again")


def test_nothing_is_dropped():
    """Both lists must be fully represented — the tail matters when one side
    runs out, which is exactly what happens as the starved backlog drains."""
    interleave, _ = _load_interleave()
    out = interleave([("S", i) for i in range(7)], [("G", i) for i in range(3)])
    assert len(out) == 10, f"interleave lost rows: {len(out)}"
    assert sum(1 for x in out if x[0] == "S") == 7
    assert sum(1 for x in out if x[0] == "G") == 3


def test_an_empty_side_does_not_stall_or_loop():
    """When the starved backlog is finally exhausted this must degrade to the
    old behaviour, not spin."""
    interleave, _ = _load_interleave()
    assert interleave([], [("G", i) for i in range(4)]) == [("G", i) for i in range(4)]
    assert len(interleave([("S", i) for i in range(4)], [])) == 4


def test_the_query_still_ranks_never_scanned_before_barren():
    """The era tier must not have eaten the ordering it sits beside. These are
    the two rules pick_unexplored_cells existed for."""
    code = re.sub(r"(?m)^\s*#.*$", "", SRC)
    body = code[code.index("def pick_unexplored_cells"):]
    body = body[:body.index("def do_random_searches")]
    assert body.count("last_scanned IS NOT NULL") >= 2, (
        "never-searched-first was dropped from one of the two candidate queries")
    assert body.count("(returned = 0) IS TRUE") >= 2, (
        "proven-barren-last was dropped — cells get demoted for having been "
        "visited rather than for being empty")


def test_the_starved_query_is_actually_scoped_to_starved_decades():
    code = re.sub(r"(?m)^\s*#.*$", "", SRC)
    assert "WHERE decade = ANY(%s)" in code, (
        "the starved candidate list is not filtered to the starved decades")
    assert "interleave_starved(" in code, "the two lists are not interleaved"
    assert starved_decades() == ["1980s", "1990s", "2000s"]


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
    print(f"\n{failed} failed" if failed else "\nall passed")
    sys.exit(1 if failed else 0)
