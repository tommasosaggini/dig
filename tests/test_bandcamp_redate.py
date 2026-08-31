"""Bandcamp reissues get their ORIGINAL year back — the guards.

Bandcamp's `release_date` is the date the page was published, and Bandcamp has
no field for the date the music came out. So a reissue enters the pool wearing
a modern year, and because lib/era.py weights every candidate against
ERA_TARGET divided by the decade's share of unheard supply, a 1975 record
filed as 2017 is not merely mislabelled: it is counted as evidence the 2010s
are over-supplied, and can never be served as the seventies.

Verified on the live pool 2026-08-20 — Bobby Darin filed 2020 (really 1969),
David Bowie 2013 (1967), Donovan 2013 (1965), Manu Dibango 2015 (1976).

    python3 tests/test_bandcamp_redate.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.mb_resolve import _lucene, _NOT_AN_ARTIST  # noqa: E402


def _src(rel: str) -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, rel), encoding="utf-8") as fh:
        return fh.read()


def test_redate_only_ever_moves_a_year_backwards():
    src = _src("scripts/redate_bandcamp_reissues.py")
    i = src.index("REDATE_SQL")
    body = src[i:i + 400]
    assert "AND year > %s" in body, (
        "without the comparison this is not a re-dating script, it is a "
        "script that overwrites Bandcamp's year with whatever MusicBrainz "
        "said — including a LATER one, which would push real 2020s releases "
        "into the past and is unrecoverable. It is also what makes a re-run "
        "idempotent")


def test_the_pool_write_updates_the_decade_with_the_year():
    src = _src("scripts/redate_bandcamp_reissues.py")
    i = src.index("REDATE_SQL")
    assert "SET year = %s, decade = %s" in src[i:i + 400], (
        "the era axis reads `decade`, never `year` — a corrected year with a "
        "stale decade fixes nothing that anybody can hear")


def test_the_title_regex_signal_stays_out():
    """Measured 2026-08-20: reissue words in the album title yielded 0 older
    dates in 40 sampled pairs, against 4 in 40 picked at random. "collection",
    "archive", "rare" and "vault" are what modern netlabel compilations are
    called, so the regex selects FOR music MusicBrainz has never heard of. It
    reads like an obvious win and is worse than nothing; this is the note that
    stops it being re-added."""
    src = _src("scripts/redate_bandcamp_reissues.py")
    order = src[src.index("ORDER BY sig_mb"):src.index("LIMIT %s")]
    assert "sig_marker" not in order and "REISSUE_MARKERS" not in src, (
        "the album-title regex was measured as WORSE than random selection")
    assert order.index("sig_mb") < order.index("sig_spotify"), (
        "mb-cache-old artists yielded 27% vs the Spotify lane's 20% — the "
        "higher-yield signal goes first because a drip this long is only "
        "ever partly finished")


def test_person_and_group_lifespans_get_different_thresholds():
    src = _src("scripts/redate_bandcamp_reissues.py")
    i = src.index("old_mb AS (")
    body = src[i:i + 700]
    assert "type =  'Group'" in body and "type <> 'Group'" in body, (
        "a Group's lifespan_begin is its formation date but a Person's is "
        "their BIRTH date — one threshold for both either misses seventies "
        "bands or drags every producer born in the eighties into the "
        "priority lane")


def test_release_groups_not_recordings():
    src = _src("lib/mb_resolve.py")
    i = src.index("def original_release_year")
    body = src[i:i + 3000]
    assert "MB_RG_SEARCH_URL" in body and "release-groups" in body, (
        "a recording is one performance and MusicBrainz holds many per song. "
        "Asked for Gyedu-Blay Ambolley's 'Simigwa-Do' the recording search "
        "answers 2020 — the reissue's own recording entity, i.e. exactly the "
        "date we are trying to see past. The release group answers 1975")


def test_the_earliest_agreeing_group_wins():
    src = _src("lib/mb_resolve.py")
    i = src.index("def original_release_year")
    body = src[i:i + 3000]
    assert "year < best" in body, (
        "a title can be both a 1966 single and a 1998 album under one name "
        "(Donovan, 'Sunshine Superman') and the single is the original")


def test_both_names_must_agree():
    src = _src("lib/mb_resolve.py")
    i = src.index("def original_release_year")
    body = src[i:i + 3000]
    assert body.count("_titles_agree(") >= 2, (
        "MusicBrainz search scores are generous — a query for something it "
        "has never seen still returns the closest thing at 100 — so the "
        "score decides nothing. Artist AND album both have to agree or this "
        "writes a stranger's release date over a real one")


def test_compilation_credits_never_reach_musicbrainz():
    assert "various artists" in _NOT_AN_ARTIST, (
        "'Various Artists' plus a generic album title agrees with half of "
        "MusicBrainz, and the name checks cannot catch it because the names "
        "genuinely do match")
    src = _src("lib/mb_resolve.py")
    i = src.index("def original_release_year")
    body = src[i:i + 3000]
    assert 'artist.count(",") >= 3' in body, (
        "split-artist credits like 'DISPOSABLE MEDIA ARCHIVE, EZEKIEL, "
        "FCKTARDS & MORE' are real pool rows and are not a name MB can "
        "match — skipping them is the one cost the drip avoids for free")


def test_quotes_in_a_title_cannot_break_the_query():
    """A bare double quote ends a Lucene phrase; a backslash escapes the next
    character. Real pool rows carry both, and an unbalanced quote is a 400
    rather than a miss — a silent hole in the sweep."""
    assert _lucene('N.O.L.A. "Rhythum"') == r'N.O.L.A. \"Rhythum\"'
    assert _lucene(r"back\slash") == r"back\\slash"


def test_a_503_is_retried_not_recorded():
    """MusicBrainz refused ~11% of requests at 1.6s spacing with nothing else
    running (measured 2026-08-20). Treating one 503 as a terminal answer would
    be the worst outcome available: the pair gets marked checked and the state
    table never asks again, so the record is lost permanently to a transient
    error."""
    src = _src("scripts/redate_bandcamp_reissues.py")
    i = src.index("def ask_mb")
    body = src[i:i + 1500]
    assert "MBRateLimited" in body and "time.sleep" in body, (
        "ask_mb must absorb and retry a 503, not surface it as 'no match'")
    main = src[src.index("def main"):]
    assert "transient += 1" in main and "consecutive_503" in main, (
        "a throttled pair must NOT be written to bc_redate_checked — an "
        "unwritten pair is retried next run, a written one never is")
    assert "if not args.dry_run" in main, (
        "--dry-run has to reach MusicBrainz and print, and write nothing; "
        "a dry run that records outcomes burns the backlog silently")


def test_the_drip_refuses_to_stack_on_a_sibling():
    src = _src("scripts/redate_bandcamp_reissues.py")
    assert "source_genre_artists" in src and "mb_sibling_running" in src, (
        "MusicBrainz pacing is per-process. Two of this project's scripts "
        "running at once doubles our rate against MB and gets the whole "
        "night throttled — the hazard backfill_night.sh already documents")


def test_cron_wrapper_checks_the_tunnel():
    src = _src("redate_cron.sh")
    assert "lib/pg_tunnel.sh" in src and "pg_tunnel_ensure" in src, (
        "the DB is reached over an SSH tunnel from this Mac. Without it the "
        "run is not a crash, it is a silent no-op — and worse than a no-op, "
        "because a half-connected run could mark pairs checked")
    # This assertion used to require `nc -z 127.0.0.1`, and the half-connected
    # run it warns about above is exactly what that check permitted: on
    # 2026-08-31 a bound port with a dead channel behind it passed `nc -z` for
    # fifteen minutes while every query failed. pg_tunnel_ensure asks the
    # database instead of the socket. See lib/pg_tunnel.sh.
    code = "".join(l for l in src.splitlines(True) if not l.lstrip().startswith("#"))
    assert "nc -z" not in code, (
        "a port check cannot tell a working tunnel from a dead one — the "
        "guard has to be a query")
    assert 'pgrep -f "redate_bandcamp_reissues"' in src, (
        "an hourly cron on a batch that can take longer than an hour will "
        "stack on itself and double the MB request rate")


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
