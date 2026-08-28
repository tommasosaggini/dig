#!/usr/bin/env python3
"""The era target must mean the same thing in Python and in the browser.

This repo already knows what happens otherwise. STATUS_RANK is written in
web/js/app.js, lib/spotify_sync.py and a SQL function, and the comment above
each copy says "three copies is two too many" — because the copy that drifts is
the one deciding what the listener hears.

The era target has the same shape: pipeline/discover.py steers INGESTION by it
and web/js/app.js weights every PICK by it. If they disagree, DIG spends its
Spotify budget filling decades the picker is not reaching for.

The JS keeps a literal rather than fetching it from the server, because the
picker has to work on the very first pick — before any network call returns.
So the literal is checked here instead.

  python3 tests/test_era_target_is_one_definition.py
"""
import ast
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.era import (  # noqa: E402
    ERA_SUPPLY_HEADROOM, ERA_TARGET, STARVED_FROM, STARVED_TO,
    decade_of, is_starved, starved_decades,
)

APP_JS = os.path.join(ROOT, "web", "js", "app.js")


def _js_era_target():
    """The ERA_TARGET object literal out of app.js, as a dict."""
    src = open(APP_JS, encoding="utf-8").read()
    m = re.search(r"const ERA_TARGET = \{(.*?)\n\};", src, re.S)
    assert m, "ERA_TARGET literal not found in web/js/app.js"
    body = m.group(1)
    body = re.sub(r"//[^\n]*", "", body)          # strip trailing comments
    out = {}
    for k, v in re.findall(r"'([0-9]{4}s)'\s*:\s*([0-9.]+)", body):
        out[k] = float(v)
    assert out, "ERA_TARGET literal parsed to nothing"
    return out


def test_the_browser_and_python_agree_on_the_target():
    js = _js_era_target()
    py = {k: float(v) for k, v in ERA_TARGET.items()}
    assert js == py, (
        "web/js/app.js and lib/era.py disagree about the era target.\n"
        f"  only in js: { 
            {k: v for k, v in js.items() if py.get(k) != v} }\n"
        f"  only in py: { {k: v for k, v in py.items() if js.get(k) != v} }")


def test_the_headroom_agrees_too():
    src = open(APP_JS, encoding="utf-8").read()
    m = re.search(r"const ERA_SUPPLY_HEADROOM = ([0-9.]+);", src)
    assert m, "ERA_SUPPLY_HEADROOM not found in app.js"
    assert float(m.group(1)) == float(ERA_SUPPLY_HEADROOM), (
        "the supply clamp differs between the picker and the pipeline — one of "
        "them is chasing a decade the other has written off")


def test_the_target_is_deliberately_not_uniform():
    """The whole reason this axis is special. If someone flattens it, the
    listener gets as much 1940s as 2020s, which is what they said they did not
    want."""
    assert ERA_TARGET["2020s"] > ERA_TARGET["1940s"] * 10
    assert ERA_TARGET["1990s"] > ERA_TARGET["1960s"]
    assert len(set(ERA_TARGET.values())) > 3, "the target has been flattened"


def test_starved_window_is_where_the_supply_actually_is():
    """Measured yield per catalog cell, 21 days to 2026-08-18:
         1990s 3.5  2000s 3.0  2010s 3.0  2020s 3.0
         1980s 2.4  1970s 1.9  1960s 1.0  1950s 0.7
    Steering below 1970 trades 3.0 tracks per Spotify search for 0.7."""
    assert starved_decades() == ["1980s", "1990s", "2000s"]
    assert is_starved("1980s") and not is_starved("2020s")
    assert not is_starved("1960s"), (
        "the 1960s has ~1.0 tracks per cell — steering ingestion there burns "
        "the search budget for a decade the target only asks 4% of")
    assert STARVED_FROM == 1980 and STARVED_TO == 2009


def test_decade_of_refuses_to_guess():
    assert decade_of("1987") == "1980s"
    assert decade_of(1987) == "1980s"
    for bad in (None, "", "198", "unknown", "19870", "abcd"):
        assert decade_of(bad) is None, f"guessed a decade from {bad!r}"


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
