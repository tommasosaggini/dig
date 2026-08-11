"""The /discovery sample is the listener's horizon — pin how it fills.

Stage 1 of sample-not-sync (2026-08-11). The client picker only ever sees the
working set this function returns, so its composition decides what can be
discovered at all. Two rules, both hyperparameter-free:

  * across regions, weighted water-filling — each slot goes to the region with
    the lowest (taken + 1) x (1 + lifetime plays), the same least-fed rule as
    the mb_artists ingestion drain;
  * within a region, genres interleave (shuffled), so a region's few slots
    carry its breadth instead of the top of its ingestion order.

    python3 tests/test_bootstrap_sample_coverage.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import server  # noqa: E402

sample = server._bootstrap_sample


def _tracks(region, n, genre="g"):
    return [{"id": f"{region}-{i}", "genres": [genre], "region": region}
            for i in range(n)]


def _count(out):
    return {r: len(ts) for r, ts in out.items()}


def test_anon_is_fair_round_robin():
    """No coverage → every region weighs 1 → equal shares (old behaviour)."""
    disc = {r: _tracks(r, 50) for r in ("A", "B", "C", "D")}
    got = _count(sample(disc, 40))
    assert got == {"A": 10, "B": 10, "C": 10, "D": 10}, got


def test_heavily_played_region_yields_to_unheard_ones():
    """A listener 500 plays deep into region A gets a sample that leans hard
    toward the regions they have never heard."""
    disc = {r: _tracks(r, 200) for r in ("A", "B", "C")}
    cov = {"countries": {"A": 500}}
    got = _count(sample(disc, 90, coverage=cov))
    assert got.get("A", 0) <= 2, f"played-out region over-served: {got}"
    assert got["B"] + got["C"] >= 88, got
    assert abs(got["B"] - got["C"]) <= 1, f"unheard regions must split evenly: {got}"


def test_exhausted_region_drops_out_and_limit_is_still_met():
    """Water-filling is supply-limited: a small region contributes all it has,
    then the rest of the fill comes from elsewhere — the sample never comes up
    short because one region ran dry."""
    disc = {"tiny": _tracks("tiny", 3), "big": _tracks("big", 500)}
    got = _count(sample(disc, 100))
    assert got["tiny"] == 3
    assert got["big"] == 97
    assert sum(got.values()) == 100


def test_within_region_genres_interleave():
    """400 rock tracks and 10 each of jazz/folk: 30 slots must walk the genres
    round-robin (~10 each), not hand rock the slots by sheer mass — the same
    stereotype-collapse guard the client picker applies, enforced at the
    source."""
    disc = {"A": (_tracks("A", 400, "rock")
                  + _tracks("A", 10, "jazz")
                  + _tracks("A", 10, "folk"))}
    out = sample(disc, 30)["A"]
    by_genre = {}
    for t in out:
        by_genre[t["genres"][0]] = by_genre.get(t["genres"][0], 0) + 1
    assert by_genre.get("jazz", 0) >= 9, by_genre
    assert by_genre.get("folk", 0) >= 9, by_genre
    assert by_genre.get("rock", 0) <= 12, by_genre


def test_draws_are_shuffled_not_ingestion_order():
    """The old sampler took rows in stored (ingestion) order, so every
    bootstrap led with the same tracks. Distinct requests must differ."""
    disc = {"A": _tracks("A", 500)}
    ids1 = [t["id"] for t in sample(disc, 20)["A"]]
    ids2 = [t["id"] for t in sample(disc, 20)["A"]]
    assert ids1 != ids2, "two draws of 20 from 500 should essentially never match"


def test_limit_zero_returns_pool_untouched():
    disc = {"A": _tracks("A", 5)}
    assert sample(disc, 0) is disc


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
