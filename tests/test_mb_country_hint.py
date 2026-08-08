"""The country hint in mb_resolve.resolve_artist (no network — _get is stubbed).

The case being guarded is a silent one. MusicBrainz lists 131 artists called
Zodiac; the Latvian space-disco band a curator was naming is not among them at
all (MB files it as "Zodiaks"). Without the hint the lookup returned the German
stoner-rock Zodiac and staged it as a confident match, which is worse than
returning nothing: a wrong artist in the pool looks exactly like a right one.

    python3 tests/test_mb_country_hint.py
    pytest tests/test_mb_country_hint.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import mb_resolve  # noqa: E402


def _artist(name, country=None, score=100, mbid=None):
    return {"id": mbid or f"mbid-{name}-{country}", "name": name,
            "score": score, "country": country}


def _stub(search_results, monkey):
    """Point mb_resolve at a canned search response and swallow the writes."""
    calls = {}

    def fake_get(url, params):
        if "/artist/" in url:                  # the follow-up lookup
            return {"relations": [], "tags": []}
        # Record only the SEARCH call — the lookup that follows carries no
        # limit and would blank this out.
        calls["limit"] = params.get("limit")
        return {"artists": search_results}

    monkey.append((mb_resolve, "_get", mb_resolve._get))
    monkey.append((mb_resolve, "_remember", mb_resolve._remember))
    monkey.append((mb_resolve, "time", mb_resolve.time))
    mb_resolve._get = fake_get
    mb_resolve._remember = lambda *a, **k: None

    class _NoSleep:
        @staticmethod
        def sleep(_):
            return None
    mb_resolve.time = _NoSleep
    return calls


def _restore(monkey):
    for obj, attr, orig in monkey:
        setattr(obj, attr, orig)


def _resolve(results, **kw):
    monkey = []
    calls = _stub(results, monkey)
    try:
        return mb_resolve.resolve_artist("Zodiac", use_cache=False, **kw), calls
    finally:
        _restore(monkey)


def test_one_shared_word_is_not_a_name_match():
    # Every wrong resolution in the 63-artist curator batch had this shape: a
    # single shared word carrying the whole match. The artist named on the left
    # is not the act on the right in any of these.
    from lib.mb_resolve import _name_agrees
    for query, candidate in (("Christina Edmund", "Edmund"),
                             ("Mamy Andy Lala", "MAMY"),
                             ("Nappy Mayers", "Nappy"),
                             ("Bilo Albán", "Bilo"),
                             ("Graf", "Elfi Graf"),
                             ("Zodiac", "Zodiac Mindwarp")):
        assert not _name_agrees(query, candidate), (query, candidate)


def test_two_shared_words_still_match():
    # And every CORRECT non-identical match in that batch had this shape, so
    # the bar cannot be raised to exact equality without losing them.
    from lib.mb_resolve import _name_agrees
    assert _name_agrees("Marino Marini & his Quartet forever", "Marino Marini")
    assert _name_agrees("Luz De America", "Trío Luz de América")
    assert _name_agrees("Terekke", "Terekke")


def test_the_country_vouches_for_a_thin_name_match():
    # A single shared word is allowed back in when something other than the
    # name agrees — which is what the caption's flag is for.
    from lib.mb_resolve import _name_agrees
    assert not _name_agrees("Vina", "Vina Panduwinata")
    assert _name_agrees("Vina", "Vina Panduwinata", corroborated=True)


def test_a_prose_place_is_read_against_musicbrainzs_own_area():
    # Label headlines name the country in words — "ANDROMEDA, LEBANON 1982" —
    # and building a name→ISO table to read that would duplicate knowledge
    # every MB search result already carries in its `area`.
    from lib.mb_resolve import _place_agrees
    lebanese = {"country": "LB", "area": {"name": "Lebanon"}}
    polish = {"country": "PL", "area": {"name": "Poland"}}
    assert _place_agrees(lebanese, "ANDROMEDA, LEBANON 1982")
    assert not _place_agrees(polish, "ANDROMEDA, LEBANON 1982")
    # A two-letter hint is still read as a country code, not as prose.
    assert _place_agrees(polish, "PL")
    assert not _place_agrees(polish, "LB")
    assert not _place_agrees(polish, None)


def test_the_hint_picks_the_namesake_from_that_country():
    hit, _ = _resolve([_artist("Zodiac", "DE"), _artist("Zodiac", "LV")],
                      country="LV")
    assert hit["country"] == "LV"


def test_several_namesakes_and_none_from_that_country_is_not_found():
    # The real Zodiac case: many matches, none Latvian, so the honest answer
    # is None rather than the best-scoring stranger.
    hit, _ = _resolve([_artist("Zodiac", "DE"), _artist("Zodiac", "TT"),
                       _artist("Zodiac", "GR")], country="LV")
    assert hit is None


def test_a_lone_match_survives_a_hint_it_cannot_confirm():
    # MusicBrainz simply does not record a country for many obscure artists —
    # "Sum Alvarinho" is one — and there is nothing to confuse it with, so a
    # hint it cannot corroborate must not throw the answer away.
    hit, _ = _resolve([_artist("Zodiac", None)], country="LV")
    assert hit is not None and hit["name"] == "Zodiac"


def test_no_hint_keeps_the_old_behaviour():
    hit, _ = _resolve([_artist("Zodiac", "DE"), _artist("Zodiac", "LV")])
    assert hit["country"] == "DE"          # best-scoring first, as before


def test_a_hint_widens_the_search_since_the_right_one_ranks_low():
    _, calls = _resolve([_artist("Zodiac", "LV")], country="LV")
    assert calls["limit"] == 25
    _, calls = _resolve([_artist("Zodiac", "LV")])
    assert calls["limit"] == 5


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: mb_resolve country hint passed ({len(fns)} tests)")
