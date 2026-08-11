"""One spelling per place — the region axis stays canonical.

Audited 2026-08-11: 442 distinct region values for ~200 real places, because
three writers each invented their own spelling — the MB drain wrote bare ISO
codes ('UA') and MB areas that are really cities ('Tbilisi'), Bandcamp's
location fallthrough returned raw last-comma tokens ('Ontario',
'Antarctica'), and legacy Spotify-era buckets said 'USA' while MusicBrainz
said 'United States'. Every consumer that groups by region — the picker's
coverage axis, the working-set sampler, world coverage — fragmented across
the variants. lib/region_norm.canonical_region is the single answer, wired
into the _upsert_track chokepoint and each writer; the backfill collapsed
the axis to 200 values.

    python3 tests/test_region_norm.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.region_norm import canonical_region  # noqa: E402


def test_iso_codes_become_country_names():
    assert canonical_region("UA") == "Ukraine"
    assert canonical_region("us") == "United States"
    assert canonical_region("GB") == "United Kingdom"
    assert canonical_region("CD") == "DR Congo"     # not to be merged with CG
    assert canonical_region("CG") == "Congo"


def test_synonyms_collapse():
    assert canonical_region("USA") == "United States"
    assert canonical_region("UK") == "United Kingdom"
    assert canonical_region("The Netherlands") == "Netherlands"
    assert canonical_region("Türkiye") == "Turkey"
    assert canonical_region("Czech Republic") == "Czechia"
    assert canonical_region("Russian Federation") == "Russia"
    assert canonical_region("Trinidad") == "Trinidad and Tobago"


def test_cities_and_subdivisions_land_on_their_country():
    assert canonical_region("Ontario") == "Canada"
    assert canonical_region("Québec") == "Canada"        # diacritic-folded
    assert canonical_region("Tbilisi") == "Georgia"
    assert canonical_region("Kyïv") == "Ukraine"
    assert canonical_region("Kharkivs'ka Oblast'") == "Ukraine"
    assert canonical_region("Lagos") == "Nigeria"
    assert canonical_region("Bakı") == "Azerbaijan"
    assert canonical_region("São Paulo") == "Brazil"
    assert canonical_region("D.C.") == "United States"


def test_macro_regions_pass_through_untouched():
    """The taxonomy uses macro-regions ON PURPOSE — normalizing them away
    would collapse the deliberate design, not fix drift."""
    for macro in ("West Africa", "Nordic", "Caribbean", "Eastern Europe",
                  "Sahel", "Horn of Africa", "South Asia", "Baltic States"):
        assert canonical_region(macro) == macro


def test_unknown_values_pass_through_not_guessed():
    """The map never guesses: a value it doesn't know survives verbatim so
    new drift stays VISIBLE instead of being mangled."""
    assert canonical_region("Some New Scene") == "Some New Scene"
    assert canonical_region("Santa Cruz") == "Santa Cruz"  # US city OR Bolivia — ambiguous, untouched


def test_joke_locations_are_unknown_not_countries():
    assert canonical_region("Antarctica") == "Unknown"
    assert canonical_region("[Worldwide]") == "Unknown"


def test_empty_is_empty():
    assert canonical_region(None) == ""
    assert canonical_region("") == ""
    assert canonical_region("  ") == ""


def test_the_upsert_chokepoint_is_wired():
    src = open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "lib/discovery_lock.py")).read()
    i = src.index("def _upsert_track")
    assert "canonical_region(region)" in src[i:i + 800], (
        "every writer funnels through _upsert_track — without the chokepoint "
        "call, one un-wired writer re-fragments the axis")


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
