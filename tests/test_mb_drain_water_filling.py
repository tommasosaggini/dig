"""Regression lock for "the pool's country mix is an accident of crawl order".

Diagnosed 2026-08-11. The mb_artists drain was ORDER BY enumerated_at — plain
FIFO — so the only countries ever ingested were the ones enumerated on day one
(2026-04-26): GH 213, NG 204, UA 197, LB 150 … while KR (1,027 queued),
IN (1,141), PH (1,067), TH (381) all sat at ZERO behind a ~15-month backlog.
The listener experienced this as "suddenly everything is African, where is
Asia?" — the feed picker was fine; the pool's inflow was ordered by an
enumeration timestamp that encodes nothing about representation.

The fix is water-filling: rank each queued artist by the ingested count its
country would reach if picked next (already-ingested + position in that
country's queue) and drain ascending. The least-fed country is always served
first, one artist per round; a country with nothing queued drops out on its
own. No quotas, no hardcoded target ratios — long-run imbalance can only
reflect genuine supply.

Verified on prod data the same day: `--dry-run --limit 40` returned 40 rows
from 40 distinct countries (round 1 of the fill), with GH/NG/UA absent
exactly because they are 200+ artists ahead.

    python3 tests/test_mb_drain_water_filling.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402


def _src(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def _code_only(src: str) -> str:
    """Strip #-comments only. The drain's SQL lives in a triple-quoted
    f-string, so docstring-stripping would delete the very code under test."""
    return re.sub(r"(?m)^\s*#.*$", "", src)


def test_drain_is_not_fifo():
    """The defect: a bare ORDER BY enumerated_at let crawl order decide the
    pool's country mix. enumerated_at may only appear inside the per-country
    window (PARTITION BY country) where it orders a single country's queue."""
    code = _code_only(_src("scripts/ingest_mb_artists.py"))
    for m in re.finditer(r"ORDER BY\s+enumerated_at", code, re.I):
        window = code[max(0, m.start() - 80):m.start()]
        assert "PARTITION BY country" in window, (
            "enumerated_at ordering outside the per-country window brings "
            "back FIFO — countries enumerated first monopolize the drain")


def test_drain_ranks_by_least_fed_country():
    """The mechanism: position within the country's queue plus that country's
    already-ingested count, drained ascending."""
    code = _code_only(_src("scripts/ingest_mb_artists.py"))
    assert re.search(r"row_number\(\)\s*OVER\s*\(\s*PARTITION BY country", code), (
        "per-country queue position is half of the water-filling rank")
    assert re.search(r"WHERE ingested_at IS NOT NULL GROUP BY country", code), (
        "already-ingested-per-country is the other half")
    assert re.search(r"ORDER BY COALESCE\(fed\.n,\s*0\)\s*\+\s*q\.pos", code), (
        "the drain must sort by the count the country would REACH, "
        "so the least-fed country is always served next")


def test_null_country_join_is_null_safe():
    """254 queued rows have country NULL. A plain equality join scores them
    perpetually unfed (NULL = NULL is not true); the join must be null-safe
    so they form one ordinary 'country' that fills like the rest."""
    code = _code_only(_src("scripts/ingest_mb_artists.py"))
    assert "IS NOT DISTINCT FROM" in code, (
        "fed-count join must be null-safe or NULL-country rows are "
        "treated as a bottomless zero-count country")


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
