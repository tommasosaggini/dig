"""Unit tests for the IG curator caption parser (no network).

Every fixture below is a real caption head from a real account. The case that
matters most is orientation: lyon__beatsonandon writes TRACK first and every
other curator here writes ARTIST first, so reading his 108 posts with the house
rule produced 81 perfectly-formed rows with the two fields swapped — a silent
corruption, because a swapped row looks exactly like a good one.

    python3 tests/test_ig_curator_scrape.py
    pytest tests/test_ig_curator_scrape.py
"""
import importlib.util
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_SPEC = importlib.util.spec_from_file_location(
    "scrape_ig_curator", os.path.join(os.path.dirname(_HERE), "scripts",
                                      "scrape_ig_curator.py"))
ig = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(ig)


def _one(caption):
    rows = ig.parse_caption(caption)
    return rows[0] if rows else None


def test_by_form_states_which_side_is_the_artist():
    r = _one("Жалам хар (A Black Horse) by The Bayan Mongol Variety Group (1980) 🇲🇳✨")
    assert r["orient"] == "track-first"     # the caption said so
    # Written order is preserved here; main() applies the swap in one place.
    assert r["artist"] == "Жалам хар (A Black Horse)"
    assert r["track"] == "The Bayan Mongol Variety Group"
    assert r["year"] == "1980"
    assert r["country"] == "MN"             # decoded from the flag emoji


def test_dash_form_is_parsed_but_left_undecided():
    r = _one("Восточный Сувенир (Oriental Souvenir) – Gunesh Ensemble (1980) 🌙✨")
    assert r["orient"] is None              # a dash claims nothing
    assert r["artist"] == "Восточный Сувенир (Oriental Souvenir)"
    assert r["track"] == "Gunesh Ensemble"
    assert r["year"] == "1980"
    assert r["country"] is None             # 🌙 is not a flag


def test_a_trailing_emoji_anchors_a_caption_with_no_year():
    # 39 of the 108 captions carry no year at all. Without this anchor they
    # fell through to the generic dash rule, which left the emoji inside the
    # name: artist "Noel Perera 🇱🇰✨".
    r = _one("Colourful Environment – Gboyega Adelaja 🇳🇬")
    assert r["artist"] == "Colourful Environment"
    assert r["track"] == "Gboyega Adelaja"
    assert r["country"] == "NG"
    assert r["year"] is None


def test_decoration_is_stripped_from_whichever_side_carries_it():
    r = _one("Dur-Dur Band 🇸🇴 – Yabaal")
    assert "🇸🇴" not in r["artist"] + r["track"]
    assert {r["artist"], r["track"]} == {"Dur-Dur Band", "Yabaal"}
    # The hyphen inside "Dur-Dur" must not be read as the separator.
    assert "Dur-Dur Band" in (r["artist"], r["track"])


def test_quotation_marks_name_the_track_and_settle_the_order():
    # As firm a statement as the word "by". Without it this row inherited the
    # handle's majority reading and filed the orchestra as the song.
    r = _one('T.P. Orchestre Poly-Rythmo – “Aihe Ni Kpe We” (1978) 🇧🇯✨')
    assert r["orient"] == "artist-first"
    assert r["artist"] == "T.P. Orchestre Poly-Rythmo"
    assert r["track"] == "Aihe Ni Kpe We"
    # And the same signal on the other side.
    r = _one('“Black Pepper” – Yoruba Singers (1984) 🌶️✨')
    assert r["orient"] == "track-first"
    assert r["artist"] == "Black Pepper" and r["track"] == "Yoruba Singers"


def test_prose_with_a_dash_is_not_a_citation():
    # Both anchors absent — this is a sentence, not a record.
    assert not ig.parse_caption("The Levantine groove has never sounded this... cinematic.")
    assert not ig.parse_caption("What if 1980s disco travelled between continents? 🌍✨")


def test_hashtags_become_style_minus_the_curatorial_ones():
    r = _one("A2 – Argo (1980) 🇱🇹✨\n\nsome prose\n\n"
             "#lithuania #spacedisco #synthdisco #raregrooves #worldmusic")
    assert "spacedisco" in r["style"]
    assert "raregrooves" not in r["style"]   # curatorial, not a genre
    assert "worldmusic" not in r["style"]


def test_the_other_curators_grammars_still_read_artist_first():
    # stirred.blessings: quoted title, artist after "by". Curly quotes, which
    # is what the account actually writes — a straight-quoted fixture also
    # collides with the apostrophe in "dell'Indugio".
    r = _one("swimming in “Strumenti dell'Indugio” by g.l.o.m.a.r.i")
    assert r["artist"] == "g.l.o.m.a.r.i" and r["track"] == "Strumenti dell'Indugio"
    # A plain structured head stays artist-first: no year bracket, no emoji, so
    # the sleeve rule must not claim it.
    r = _one("Kassel Jaeger – Swamps/Things  Label : Editions Mego  Année : 2018")
    assert r["artist"] == "Kassel Jaeger" and r["track"] == "Swamps/Things"
    assert r["label"] == "Editions Mego"


def test_a_tracklist_block_still_yields_many_rows():
    rows = ig.parse_caption("essay text\n\nTracks mentioned:\n"
                            "Boards of Canada - Everything You Do Is A Balloon\n"
                            "Boards of Canada - Hi Scores\n")
    assert len(rows) == 2
    assert all(r["artist"] == "Boards of Canada" for r in rows)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: ig curator caption parsing passed ({len(fns)} tests)")
