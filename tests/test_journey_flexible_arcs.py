"""Journey blocks are a judgment call, not a quota.

The original journey prompt hard-coded every block's shape — CLOSE x 3-4,
EXPAND x 2-3, STRETCH x 1-2, always anchored on the original seed — so block 5
orbited the seed at the same distance as block 1 (a wheel, not a journey), the
mix could not adapt to the seed or to engagement, and on thin-supply seeds the
mandatory "close" quota was filled by mislabeling whatever the resolver could
find (measured 2026-08-11: an Ethiopian ethio-jazz seed got Chicago blues
tagged "close" — Ethiopia had 11 pool tracks, mostly already heard).

Now: the arc MIX is the model's choice per block, later blocks walk from the
FRONTIER (the most recently engaged tracks) with the seed as heritage, and the
client reports each served track's region so the model can see resolver drift
and widen instead of re-asking narrower.

    python3 tests/test_journey_flexible_arcs.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402


def _src(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def _journey_system() -> str:
    py = _src("lib/ai_recommend.py")
    i = py.index("_JOURNEY_SYSTEM")
    return py[i:py.index('"""', py.index('"""', i) + 3)]


def test_no_fixed_arc_quota():
    sysprompt = _journey_system()
    assert not re.search(r"CLOSE\s*×\s*\d", sysprompt), (
        "per-arc quotas are back — the fixed 3-4/2-3/1-2 recipe is the "
        "rigidity this change removed")
    assert "NO quota" in sysprompt, (
        "the prompt must state explicitly that the arc mix is the model's "
        "choice — silence defaults the model back to an even formula")


def test_later_blocks_walk_from_the_frontier():
    sysprompt = _journey_system()
    assert "FRONTIER" in sysprompt and "heritage" in sysprompt, (
        "every block anchored on the original seed = orbiting at fixed "
        "distance; later blocks must walk from recently engaged tracks")


def test_thin_supply_is_named_and_handled():
    sysprompt = _journey_system()
    assert "THIN" in sysprompt, (
        "the prompt must tell the model what served-far-from-asked means — "
        "otherwise it re-asks the same narrow query and the resolver "
        "mislabels another far-off track as close")


def test_client_reports_served_regions():
    js = _src("web/js/app.js")
    m = re.search(r"journeyHistory\.push\(\{[\s\S]{0,400}?\}\)", js)
    assert m and "region" in m.group(0), (
        "journeyHistory entries must carry the served track's region — "
        "without it the model cannot see resolver drift at all")


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
