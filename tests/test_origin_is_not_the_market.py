"""A track's country must be a claim about the ARTIST, never about the search.

pipeline/discover.py:838 wrote `region_name = market`, so `tracks.region` on
every Spotify lane holds the STOREFRONT a search ran in. The read path then
served it as the track's country. Concretely, from one 200-track session on
2026-08-27:

    Jaromir Nohavica  (Czech folk)      served as  Hong Kong
    Mo' Horizons      (German nu-jazz)  served as  Sahel
    Great Collapse    (US hardcore)     served as  Egypt
    Legras Guillaume  (Reunion maloya)  served as  Singapore
    Sanggar Kirana    (MB: Indonesia)   served as  Singapore

This was not cosmetic. diversityShuffle()'s region lens, AI Mix's cell
bucketing and db_get_user_coverage()'s water-filling all read that field as
origin, so the pipeline recorded coverage of countries it had never reached:
199 tracks labelled North Korea with one actually North Korean, and Singapore
showing 285 tracks against a true supply of 35. A country that looks fed is a
country the water-filling stops digging for — which is exactly why the listener
noticed the absence before the dashboard did.

These tests pin the rule that fixes it: an origin is servable only when
`origin_source` names evidence about the artist (lib/origin.py), and it is read
from `origin_region` alone so a trusted tier with a missing answer can never
fall back to the market label.

    python3 tests/test_origin_is_not_the_market.py
    pytest tests/test_origin_is_not_the_market.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.origin import (  # noqa: E402
    DECLARED_ORIGIN_SOURCES, ORIGIN_SQL, ORIGIN_SQL_VERIFIED,
    TRUSTED_ORIGIN_SOURCES, UNTRUSTED, VERIFIED_ORIGIN_SOURCES,
    classify, served_region,
)

FAILED = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name} {detail}")
        FAILED.append(name)


def test_market_never_names_a_country():
    """The whole bug in one assertion."""
    row = {"source": "spotify", "region": "Singapore", "origin_region": None,
           "query": "catalog:maloya year:2000-2009"}
    src, country = classify(row)
    check("market lane classifies as `market`", src == "market", f"got {src}")
    check("market lane yields no country", country is None, f"got {country}")
    check("market row serves no region",
          served_region(dict(row, origin_source=src)) == "",
          "a storefront was served as an origin")


def test_a_trusted_tier_cannot_fall_back_to_region():
    """The subtle version of the same bug.

    If served_region ever consulted `region` as a second choice, a trusted row
    that arrived without an origin_region would silently serve the market
    label — reintroducing the bug through the door marked 'trusted'.
    """
    row = {"source": "spotify", "region": "Egypt", "origin_region": None,
           "origin_source": "wikidata_spotify_id",
           "query": "catalog:mawawil"}
    check("trusted tier + empty origin serves nothing",
          served_region(row) == "", f"served {served_region(row)!r}")


def test_the_uploader_is_not_the_artist():
    """'Sahel Sounds' is a US label that reissues Nigerien music."""
    row = {"source": "youtube", "region": "West Africa", "origin_region": None,
           "query": "youtube:Sahel Sounds"}
    src, country = classify(row)
    check("youtube lane classifies as `uploader`", src == "uploader", f"got {src}")
    check("uploader is not trusted", "uploader" not in TRUSTED_ORIGIN_SOURCES)
    check("uploader serves no region",
          served_region(dict(row, origin_source=src)) == "")


def test_real_evidence_still_works():
    """The gate must not throw away the claims that ARE about the artist."""
    bc = {"source": "bandcamp", "region": "Singapore", "origin_region": None,
          "query": "bandcamp-tag:singapore"}
    src, country = classify(bc)
    check("bandcamp band_location is trusted", src == "bandcamp_location")
    check("bandcamp location keeps its country", country == "Singapore")
    check("bandcamp row serves its country",
          served_region({"origin_source": src, "origin_region": country}) == "Singapore")

    mb = {"source": "spotify", "region": "Singapore",
          "origin_region": "Indonesia", "query": "catalog:gamelan indonesian"}
    src, country = classify(mb)
    check("MusicBrainz overrides the market", src == "musicbrainz")
    check("MusicBrainz country wins", country == "Indonesia")
    check("gamelan group serves as Indonesia, not Singapore",
          served_region({"origin_source": src, "origin_region": country}) == "Indonesia")


def test_classify_never_demotes_proven_evidence():
    """A re-ingest re-derives `market`; the resolver's answer must survive it.

    _upsert_track calls classify() on every ingest pass. Without this guard a
    nightly re-crawl would walk back every country resolve_origin.py found.
    """
    row = {"source": "spotify", "region": "Hong Kong",
           "origin_region": "Czechia",
           "origin_source": "mb_artists_spotify_id",
           "query": "catalog:country folk year:1990-1999"}
    src, country = classify(row)
    check("proven tier is preserved", src == "mb_artists_spotify_id", f"got {src}")
    check("proven country is preserved", country == "Czechia", f"got {country}")


def test_a_half_written_row_is_not_treated_as_proven():
    """A trusted TIER with no country is not evidence, it is an unfinished write.

    The don't-demote guard originally short-circuited on the tier name alone.
    That froze 72,876 `bandcamp_location` rows in a half-written state: the
    first backfill pass stamped the tier while the country was still sitting in
    `region`, and the second pass — the one whose whole job was to move it
    across — hit the early return and preserved the emptiness. The pool read
    86.6% trusted and 26.7% servable at the same time, which is the shape this
    test exists to catch.
    """
    row = {"source": "bandcamp", "region": "Singapore", "origin_region": None,
           "origin_source": "bandcamp_location",
           "query": "bandcamp-tag:singapore"}
    src, country = classify(row)
    check("half-written row is re-derived, not frozen", country == "Singapore",
          f"got {country!r}")
    check("its tier survives the re-derivation", src == "bandcamp_location")
    check("and it is then servable",
          served_region({"origin_source": src, "origin_region": country}) == "Singapore")


def test_declared_and_verified_are_told_apart():
    """Self-declared is servable, but it is not proof a scene exists.

    Bandcamp locations are typed by the artist. Usually honest, always a claim
    about the ARTIST (which is why they outrank a market label) — but unpoliced:
    of 199 tracks tagged North Korea, 195 carried a self-declared DPRK location
    and the acts behind them were Incel Crew, ACID SHIT and Assisted Suicide.
    One verified North Korean artist exists in the pool.

    So coverage arithmetic gets its own gate. The failure this prevents is the
    water-filling reading 196 North Korean tracks, calling the country fed, and
    never digging for it again — the same failure as the market bug, one tier up.
    """
    check("VERIFIED and DECLARED partition TRUSTED",
          VERIFIED_ORIGIN_SOURCES | DECLARED_ORIGIN_SOURCES == TRUSTED_ORIGIN_SOURCES)
    check("they do not overlap",
          not (VERIFIED_ORIGIN_SOURCES & DECLARED_ORIGIN_SOURCES))
    check("bandcamp is declared, not verified",
          "bandcamp_location" in DECLARED_ORIGIN_SOURCES
          and "bandcamp_location" not in VERIFIED_ORIGIN_SOURCES)
    check("the verified gate excludes bandcamp",
          "'bandcamp_location'" not in ORIGIN_SQL_VERIFIED)
    check("the serving gate still includes it",
          "'bandcamp_location'" in ORIGIN_SQL,
          "a self-declared Leeds is a fine thing to serve")


def test_sql_gate_matches_python_gate():
    """ORIGIN_SQL and served_region must not drift apart.

    They are the same rule in two languages; the readers split between them
    (explore.py and server.py use the SQL, discovery_lock uses the Python), so
    a divergence would show up as the picker and the coverage map disagreeing
    about which countries you have heard.
    """
    for tier in TRUSTED_ORIGIN_SOURCES:
        check(f"SQL gate admits {tier}", f"'{tier}'" in ORIGIN_SQL)
    for tier in UNTRUSTED:
        check(f"SQL gate excludes {tier}", f"'{tier}'" not in ORIGIN_SQL)
    check("SQL gate reads origin_region, not region",
          "origin_region" in ORIGIN_SQL and "NULLIF(t.region" not in ORIGIN_SQL)
    # The drift that actually happened: served_region() rejected MusicBrainz's
    # XW/XE/XG ('worldwide'/'Europe'/'unknown') while the SQL gate let them
    # through, so one track was served as Unknown and counted under 'XW' by the
    # coverage queries at the same time. Both gates must reject the same set.
    from lib.origin import NOT_A_PLACE
    for junk in sorted(x for x in NOT_A_PLACE if x):
        check(f"SQL gate rejects the non-place {junk!r}", f"'{junk}'" in ORIGIN_SQL,
              "served_region rejects it, so the SQL gate must too")
        check(f"python gate rejects the non-place {junk!r}",
              served_region({"origin_source": "musicbrainz",
                             "origin_region": junk}) == "")


if __name__ == "__main__":
    print("origin provenance — a country must describe the artist\n")
    for fn in [test_market_never_names_a_country,
               test_a_trusted_tier_cannot_fall_back_to_region,
               test_the_uploader_is_not_the_artist,
               test_real_evidence_still_works,
               test_classify_never_demotes_proven_evidence,
               test_a_half_written_row_is_not_treated_as_proven,
               test_declared_and_verified_are_told_apart,
               test_sql_gate_matches_python_gate]:
        print(f"\n{fn.__name__}:")
        fn()
    print()
    if FAILED:
        print(f"FAILED {len(FAILED)}: {', '.join(FAILED)}")
        sys.exit(1)
    print("all origin-provenance checks passed")
