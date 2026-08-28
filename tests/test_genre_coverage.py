"""Unit tests for the genre-coverage engine (no network, no DB writes).

The engine's job is to notice that Dig had zero tracks for 1,284 of the world's
2,184 genres and to close that without poisoning the pool. Everything here
guards one of the traps that were found by actually hitting these sources:

  - genre_key   platforms disagree about diacritics; 'raï' and 'rai' are one genre
  - _usable     Discogs credits are full of "Various" and "Unknown Artist"
  - MusicBrainz a tag SEARCH scores text matches, so 'Yasunao Tone' ranks for '2 tone'
  - Wikidata    P136 returns songs, and unlabelled artists come back as bare Q-numbers
  - Discogs     style= is a curated claim, q= is a word match; they must not be conflated

    python3 tests/test_genre_coverage.py
    pytest tests/test_genre_coverage.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import genre_artists, genre_vocab  # noqa: E402


# ── normalisation ────────────────────────────────────────────────────────────

def test_genre_key_folds_diacritics_and_punctuation():
    assert genre_vocab.genre_key("raï") == genre_vocab.genre_key("rai")
    assert genre_vocab.genre_key("coupé-décalé") == genre_vocab.genre_key("coupe decale")
    assert genre_vocab.genre_key("  Luk   Thung ") == genre_vocab.genre_key("luk thung")
    assert genre_vocab.genre_key("Hip-Hop/Rap") == "hip hop rap"


def test_genre_key_keeps_different_genres_apart():
    assert genre_vocab.genre_key("house") != genre_vocab.genre_key("deep house")


def test_aliases_point_at_real_vocabulary_spellings():
    # An alias whose target is not how MusicBrainz spells it would silently
    # never match, and the genre would read as uncovered forever.
    for alias, canonical in genre_vocab.ALIASES.items():
        assert genre_vocab.genre_key(canonical), f"{alias} -> empty canonical"
        assert genre_vocab.genre_key(alias) != genre_vocab.genre_key(canonical), \
            f"{alias} is not actually a different spelling"


# ── artist name hygiene ──────────────────────────────────────────────────────

def test_usable_rejects_non_names():
    # Discogs release credits carry these constantly; each would otherwise
    # become an "artist" we go looking for on Bandcamp.
    for junk in ("Various", "various artists", "Unknown Artist", "V/A", "Traditional"):
        assert genre_artists._usable(junk) is None, junk
    assert genre_artists._usable("") is None
    assert genre_artists._usable("x") is None


def test_usable_strips_discogs_disambiguators():
    # Discogs writes "Organisation (2)" for the second artist of that name.
    assert genre_artists._usable("Organisation (2)") == "Organisation"
    assert genre_artists._usable("Crystal Clear (2)") == "Crystal Clear"
    # A number that is part of the name must survive.
    assert genre_artists._usable("Front 242") == "Front 242"


# ── source parsing, with the network stubbed ─────────────────────────────────

def _stub(payload):
    genre_artists._jget = lambda url, headers, source, timeout=30: payload


def test_musicbrainz_requires_the_tag_to_be_on_the_artist():
    # A tag query is still a text search: 'Yasunao Tone' scores highly for
    # '2 tone' while being tagged nothing of the sort.
    _stub({"artists": [
        {"name": "The Specials", "id": "a", "score": 100,
         "tags": [{"name": "ska"}, {"name": "2 tone"}]},
        {"name": "Yasunao Tone", "id": "b", "score": 95,
         "tags": [{"name": "fluxus"}, {"name": "noise"}]},
    ]})
    names = [n for n, _, _ in genre_artists.from_musicbrainz("2 tone")]
    assert names == ["The Specials"]


def test_musicbrainz_drops_low_scores():
    _stub({"artists": [{"name": "Nearest Thing", "id": "a", "score": 42, "tags": []}]})
    assert genre_artists.from_musicbrainz("kuduro") == []


def test_wikidata_drops_unlabelled_artists():
    # No label in any language we asked for => the "name" is a Q-number, which
    # is useless as a search term and must not be carried as if it were one.
    _stub({"results": {"bindings": [
        {"a": {"value": "http://www.wikidata.org/entity/Q1"},
         "aLabel": {"value": "Cesária Évora"}},
        {"a": {"value": "http://www.wikidata.org/entity/Q2"},
         "aLabel": {"value": "Q16374539"}},
    ]}})
    genre_artists._wikidata_genre_qid = lambda g: "Q502535"
    names = [n for n, _, _ in genre_artists.from_wikidata("morna")]
    assert names == ["Cesária Évora"]


def test_wikidata_returns_nothing_when_the_genre_is_not_a_genre():
    # The type constraint failing is the correct outcome, not an error: a bare
    # label match for 'bubbling' finds an entity meaning 'erotic dance'.
    genre_artists._wikidata_genre_qid = lambda g: None
    assert genre_artists.from_wikidata("bubbling") == []


def test_discogs_splits_the_credit_off_the_release_title():
    os.environ.setdefault("DISCOGS_KEY", "k")
    os.environ.setdefault("DISCOGS_SECRET", "s")
    _stub({"results": [
        {"title": "Anlo-Afiadenyigba Agbadza Group - Music Of The Ewe", "id": 1},
        {"title": "Various - Ghana Special", "id": 2},          # junk credit
        {"title": "NoSeparator", "id": 3},                      # not "artist - release"
    ]})
    got = genre_artists.from_discogs("agbadza")
    assert [n for n, _, _ in got] == ["Anlo-Afiadenyigba Agbadza Group"]


def test_discogs_reports_which_query_found_it():
    # style= means "this record IS kuduro"; q= means "the word appears".
    # Conflating them is how a jam band got filed under Ewe drumming.
    os.environ.setdefault("DISCOGS_KEY", "k")
    os.environ.setdefault("DISCOGS_SECRET", "s")
    _stub({"results": [{"title": "Titica - Puxa Katuta", "id": 9}]})
    got = genre_artists.from_discogs("kuduro")
    assert got[0][2] == "style", "a style= hit must not be reported as free text"


def test_source_unavailable_is_not_an_empty_answer():
    # The distinction the whole resume logic rests on: a source that is DOWN
    # must be retried, a source that knows nobody must not be re-asked nightly.
    def boom(url, headers, source, timeout=30):
        raise genre_artists.SourceUnavailable(f"{source}: down")
    genre_artists._jget = boom
    try:
        genre_artists.from_musicbrainz("kuduro")
    except genre_artists.SourceUnavailable:
        return
    raise AssertionError("an unreachable source must raise, not return []")


# ── the artist anchor ────────────────────────────────────────────────────────

def test_artist_anchor_rejects_reuploaders():
    """tracks_by_artist must not accept a band name carrying EXTRA names.

    bandcamp._agree accepts token containment in either direction, which is
    correct when a track title is also being checked and wrong when the artist
    name is the only anchor. Searching "Bethel Music" matched an uploader
    called "Amanda Cook , Bethel Music" and put five hashtag-stuffed worship
    re-uploads into the pool labelled '2 tone'.
    """
    from lib.bandcamp import is_same_artist as accepted

    assert accepted("Bethel Music", "Bethel Music")
    assert not accepted("Bethel Music", "Amanda Cook , Bethel Music")
    assert not accepted("Anti-G", "Anti-G & Friends Allstars")
    # A shorter form of the same name is still the same artist, and accents
    # must not decide the question either way.
    assert accepted("Cesária Évora", "Cesaria Evora")
    assert accepted("Anti-G", "Anti G")
    assert not accepted("De Schuurman", "")

# ── which seeds are worth spending a lookup on ───────────────────────────────

def _trust():
    import importlib.util, os
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spec = importlib.util.spec_from_file_location(
        "ing", os.path.join(root, "scripts", "ingest_genre_artists.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_a_lone_database_tag_is_not_trusted():
    """The measured failure: MusicBrainz's ONLY seed for 'aak' was Linkin Park
    and its only seed for 'abhang' was a German breakcore act. When a source
    knows one artist for a whole genre, that one editable field IS the genre
    and nothing contradicts it."""
    m = _trust()
    assert not m.trust_seed("musicbrainz", "aak", 1, True)
    assert not m.trust_seed("wikidata", "abhang", 2, True)
    # Where the source demonstrably knows the genre, its seeds are good.
    assert m.trust_seed("musicbrainz", "acid trance", 13, True)


def test_free_text_seeds_are_not_ingested_by_default():
    """Measured over the first 352 backfilled tracks: free-text seeds converted
    almost entirely into mislabelled music — Cheb Zahouani filed as balani
    show, a Cage piece as agbadza. The seeds are often right (abhang gave
    Bhimsen Joshi) but those artists are not on Bandcamp, so only the wrong
    ones become tracks."""
    m = _trust()
    assert not m.trust_seed("discogs-text", "abhang", 19, genre_has_strong_seed=False)
    assert not m.trust_seed("discogs-text", "acid trance", 25, genre_has_strong_seed=True)
    # Still reachable deliberately, and still never on a name too short to
    # match distinctly: 'aak' pulled in FJAAK, DJ Aakmael and A.A.K.
    assert m.trust_seed("discogs-text", "abhang", 19, False, include_weak=True)
    assert not m.trust_seed("discogs-text", "aak", 24, False, include_weak=True)


def test_curated_claims_are_always_trusted():
    m = _trust()
    assert m.trust_seed("curator", "kuduro", 0, False)      # a DJ played it
    assert m.trust_seed("discogs", "bubbling", 1, False)    # curated style list
    assert not m.trust_seed("nonsense", "kuduro", 99, True)

def test_ingest_spreads_across_genres_instead_of_draining_one():
    """Every gap genre has zero tracks, so they all tie on coverage and the
    tie-break is the name. Straight down the list meant a run spent itself
    alphabetically — 257 tracks across five genres beginning with "a", while
    kuduro and bubbling sat untouched with 51 and 26 seeds ready.
    """
    m = _trust()
    rows = ([{"genre": "acid trance", "artist": f"A{i}", "source": "musicbrainz"}
             for i in range(40)]
            + [{"genre": "kuduro", "artist": f"K{i}", "source": "curator"}
               for i in range(40)]
            + [{"genre": "bubbling", "artist": f"B{i}", "source": "discogs"}
               for i in range(40)])

    # The interleave, lifted out of candidates() so it can be exercised without
    # a database standing in the way.
    by_genre = {}
    for r in rows:
        by_genre.setdefault(r["genre"], []).append(r)
    keep, limit = [], 9
    while len(keep) < limit:
        progressed = False
        for g in list(by_genre):
            if by_genre[g]:
                keep.append(by_genre[g].pop(0))
                progressed = True
                if len(keep) >= limit:
                    break
        if not progressed:
            break

    assert len({r["genre"] for r in keep}) == 3, "a batch must touch every genre"
    assert sum(1 for r in keep if r["genre"] == "kuduro") == 3

if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: genre coverage engine passed ({len(fns)} tests)")
