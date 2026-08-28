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


def test_a_label_announcement_yields_the_artist_and_no_track():
    # habibifunk is a reissue label: 225 posts, 6 of which cite a track. The
    # rest announce releases, and the artist is in the headline.
    ig._LABEL_HINT = "habibifunk"
    try:
        r = _one("NECHAZZ, AMMAN, JORDAN, 1988 (Habibi Funk 036): after nechazz "
                 "recorded their only album…")
        assert r["orient"] == "artist-only"
        assert r["artist"] == "NECHAZZ" and r["track"] is None
        assert r["year"] == "1988"

        r = _one("OUT TODAY => AHMED MALEK 🇩🇿 (Habibi Funk 027): thrilled to share…")
        assert r["artist"] == "AHMED MALEK" and r["country"] == "DZ"

        # The catalogue can come first, and rejecting the head must not end the
        # attempt — the band is on the other side of the dash.
        r = _one("HABIBI FUNK 019 - FERKAT AL ARD: \"Oghneya\" is the title track…")
        assert r["artist"] == "FERKAT AL ARD"
        r = _one("2022 - FERKAT AL ARD: We’re happy to see…")
        assert r["artist"] == "FERKAT AL ARD"
    finally:
        ig._LABEL_HINT = None


def test_label_admin_posts_are_not_artists():
    ig._LABEL_HINT = "habibifunk"
    try:
        for head in ("FREE STICKERS: we got another selection of free stickers…",
                     "DJ GIGS: we have a few dj gigs coming up this summer…",
                     "PLAYLISTS (link in bio): we spend quite some time…",
                     "HABIBI FUNK 019: out now",          # the catalogue itself
                     "UPDATE 26.9.2023: some news"):
            assert not ig.parse_caption(head), head
    finally:
        ig._LABEL_HINT = None


def test_the_label_grammar_never_pre_empts_a_track_citation():
    # It sits ahead of the generic dash split but behind every grammar that
    # reads a real citation, so a caption naming a track must still name one.
    ig._LABEL_HINT = "habibifunk"
    try:
        for cap, who in (("Cacau by Sum Alvarinho (1982) 🇸🇹✨", "sleeve"),
                         ("Colourful Environment – Gboyega Adelaja 🇳🇬", "sleeve"),
                         ("essay\n\nTracks mentioned:\nBoards of Canada - Hi Scores\n",
                          "tracklist")):
            rows = ig.parse_caption(cap)
            assert rows and all(r["track"] for r in rows), who
    finally:
        ig._LABEL_HINT = None


def test_a_tracklist_block_still_yields_many_rows():
    rows = ig.parse_caption("essay text\n\nTracks mentioned:\n"
                            "Boards of Canada - Everything You Do Is A Balloon\n"
                            "Boards of Canada - Hi Scores\n")
    assert len(rows) == 2
    assert all(r["artist"] == "Boards of Canada" for r in rows)


# ── the flag-headed block (doubleudiego) ────────────────────────────────────
# 274 captions yielded 55 candidates before this grammar, and the 55 were
# damaged: 37 artist names carried "🇬🇧 (United Kingdom) " and every single row
# came back with country=None and year=None while the caption stated both.


def test_a_country_header_files_the_lines_under_it():
    rows = ig.parse_caption(
        "Best Songs of All Time: Part 94\n"
        "\n"
        "🇬🇧 (United Kingdom)\n"
        "The Smiths - Still Ill (1984)\n"
        "The Smiths - Barbarism Begins At Home (1985)\n"
        "\n"
        "#MusicDiscovery #MusicReels")
    assert len(rows) == 2, rows
    assert all(r["artist"] == "The Smiths" for r in rows), rows
    assert [r["track"] for r in rows] == ["Still Ill", "Barbarism Begins At Home"]
    assert [r["year"] for r in rows] == ["1984", "1985"]
    assert all(r["country"] == "United Kingdom" for r in rows)


def test_the_country_can_change_mid_caption():
    """An album rundown files each entry under the flag directly above it —
    a caption-wide country would put my bloody valentine in Scotland."""
    rows = ig.parse_caption(
        "🏴󠁧󠁢󠁳󠁣󠁴󠁿 (Scotland, UK 🇬🇧)\n"
        "Album 1: Cocteau Twins - Heaven or Las Vegas (1990)\n"
        "\n"
        "🇮🇪 (Ireland)\n"
        "Album 2: my bloody valentine - Loveless (1991)\n")
    assert [(r["artist"], r["country"]) for r in rows] == [
        ("Cocteau Twins", "Scotland"), ("my bloody valentine", "Ireland")], rows


def test_a_flag_in_front_of_the_pair_is_not_part_of_the_name():
    rows = ig.parse_caption("🇯🇵 (Japan) Toshiki Kadomatsu - Airport Lady (1984)")
    assert rows[0]["artist"] == "Toshiki Kadomatsu", rows
    assert rows[0]["country"] == "Japan"
    assert rows[0]["year"] == "1984"


def test_a_recorded_bracket_is_the_year_not_the_title():
    rows = ig.parse_caption(
        "🇯🇵 (Japan)\n"
        "Les Rallizes Dénudés - The Last One_1980 (2025) [Recorded 1980]\n")
    assert rows[0]["track"] == "The Last One_1980", rows
    assert rows[0]["year"] == "1980", "the recording date beats the reissue date"


def test_a_songs_header_hangs_bare_lines_off_the_artist_above():
    rows = ig.parse_caption(
        "🇯🇵 (Japan) Les Rallizes Dénudés\n"
        "\n"
        "Songs:\n"
        "\n"
        "1. The Last One_1980 (2025) [Recorded 1980]\n"
        "2. 夜、暗殺者の夜 (2025) [Recorded 1980]\n")
    assert len(rows) == 2, rows
    assert all(r["artist"] == "Les Rallizes Dénudés" for r in rows), rows
    assert rows[0]["track"] == "The Last One_1980"


def test_list_furniture_never_reaches_the_artist_name():
    for cap, want in (
            ("🇺🇸 (United States)\n1. Sadness - Tomorrow (2021)", "Sadness"),
            ("album recommendation: The Underdark - Wormwitch (2019)",
             "The Underdark"),
            ("Album Review: Gaelle - Transient (2020)", "Gaelle"),
            ("🇬🇧 (United Kingdom)\n1. Broadcast - Black Cat (2005)\n"
             "2. Broadcast - I Found the F (2005)", "Broadcast")):
        rows = ig.parse_caption(cap)
        assert rows and rows[0]["artist"] == want, (cap, rows)


def test_a_number_in_a_band_name_survives_an_unnumbered_caption():
    """The bare-number strip is the dangerous one — it is why it needs the
    caption to prove it numbers its lines before it fires at all."""
    rows = ig.parse_caption("🇺🇸 (United States)\n50 Cent - In Da Club (2003)")
    assert rows[0]["artist"] == "50 Cent", rows
    rows = ig.parse_caption("🇬🇧 (United Kingdom)\n808 State - Oops (1991)")
    assert rows[0]["artist"] == "808 State", rows


def test_a_trailing_flag_stays_with_the_sleeve_grammar():
    """Filed under a country and decorated with one are different claims.
    Reading the second here returned it artist-first with the flag still in
    the title — a silent swap, which is the failure this file exists for."""
    rows = ig.parse_caption("Colourful Environment – Gboyega Adelaja 🇳🇬")
    assert rows[0]["orient"] is None and rows[0]["country"] == "NG", rows
    assert rows[0]["track"] == "Gboyega Adelaja"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: ig curator caption parsing passed ({len(fns)} tests)")
