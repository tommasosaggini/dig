#!/usr/bin/env python3
"""Recover ORIGINAL release years for Bandcamp reissues, from MusicBrainz.

THE PROBLEM.

Bandcamp's `release_date` is the date that Bandcamp page was published, not
the date the music came out, and Bandcamp's data model has no field for the
latter. So a 1975 Ghanaian record reissued by a crate-digging label in 2017
enters the pool as a 2017 record. backfill_bc_years.py does not help — it
reads the same field from the tralbum payload, so it fills the backlog in
with the same wrong year, faithfully.

Measured against the pool on 2026-08-20, cross-checking only those Bandcamp
artists who ALSO appear in the Spotify lane with pre-1995 material:

    Soft Machine          Spotify 1969  ->  filed 2020
    Donovan               Spotify 1966  ->  filed 2013
    Deep Purple           Spotify 1980  ->  filed 2026
    Gyedu-Blay Ambolley   Spotify 1982  ->  filed 2017

148 rows fail that test, and it is a floor rather than an estimate: it can
only see artists who happen to be in both lanes, and most reissue labels are
not. The consequence is not cosmetic. Dig's era axis weights every candidate
against ERA_TARGET divided by the decade's share of unheard supply
(lib/era.py), so a seventies record wearing a 2020s year is not merely
mislabelled — it is counted as evidence the 2020s are over-supplied and can
never be served as the seventies. The pool holds 1980s music the era axis
cannot see.

WHY THIS IS WORTH A DRIP RATHER THAN A GATE.

The obvious cheap gate is the cached mb_artists table: 222k artists with
lifespan data, free to query, no requests. It reaches only ~630 of the 53,803
candidate pairs, because that cache was built by country enumeration and
overlaps a tenth of Bandcamp's long tail. Gating on it would leave most of the
recoverable music behind — so every unchecked pair is eligible, and the two
signals that survived measurement decide the ORDER rather than membership
(see the table above BACKLOG_SQL). The deep recoveries land in the first
evening; the 10%-yield tail is swept over about three weeks at MusicBrainz's
1 req/sec, which costs nothing but time — MB is free and unmetered, and this
spends no Spotify quota at all.

Expect roughly 5,000 re-dated records from a full sweep, of which about a
quarter move back past 2010. That is the honest ratio: most of the tail's
corrections are 2014 -> 2012 and change no decade. The quarter that does move
is what the era axis is currently blind to.

The unit of work is the (artist, album) pair, not the track: 55,557 rows
collapse to 53,803 pairs, and one request re-dates every track on the record.

Usage:
    python3 scripts/redate_bandcamp_reissues.py --limit 300
    python3 scripts/redate_bandcamp_reissues.py --limit 20 --dry-run
"""
import argparse
import os
import re
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.db import execute, execute_returning, fetchall, fetchone  # noqa: E402
from lib.mb_resolve import (MBRateLimited, MB_RATE_LIMIT_S,        # noqa: E402
                            original_release_year)

# Yield measured on 2026-08-20, sampling each cohort against MusicBrainz and
# counting how often it returns a year OLDER than Bandcamp's:
#
#     cohort                    pairs   sampled   matched   older
#     artist old per MB cache     631        30        14      8   (27%)
#     artist old per Spotify      135        30        15      6   (20%)
#     no signal (random)       53,803        40         7      4   (10%)
#     reissue words in title      ~800        40         2      0   ( 0%)
#
# The title regex was the first thing tried and it is the one thing that does
# not work: "collection", "archive", "rare" and "vault" are what modern
# netlabel compilations and beat tapes are called, so it selects FOR music
# MusicBrainz has never heard of. It scored worse than picking at random and
# is deliberately absent below rather than kept as a weak tiebreak.
#
# The two signals that do work are both free — our own Spotify lane and the
# already-cached mb_artists enumeration — and they are also where the deep
# recoveries are: The Spencer Davis Group 1966, Arsenio Rodríguez 1966, Piper
# 1983, Arthur Russell 1986, Manu Dibango 1976. The random tail mostly yields
# small corrections (2014 -> 2012) that change no decade. They are still worth
# sweeping, at about 10%, but they go last.
BACKLOG_SQL = """
WITH pairs AS (
    SELECT lower(artist) AS artist_key,
           lower(album)  AS album_key,
           min(artist)   AS artist,
           min(album)    AS album,
           min(year)     AS bc_year,
           count(*)      AS n_tracks
    FROM tracks
    WHERE source = 'bandcamp'
      AND decade IN ('2010s', '2020s')
      AND coalesce(artist, '') <> ''
      AND coalesce(album,  '') <> ''
    GROUP BY 1, 2
),
old_spotify AS (
    -- Artists the OTHER lane already proved are old. Free, and the most
    -- reliable of the three signals: it is our own data, not an inference.
    SELECT DISTINCT lower(artist) AS artist_key FROM tracks
    WHERE source = 'spotify' AND year ~ '^[0-9]{4}$' AND year < '1995'
),
old_mb AS (
    -- Artists the cached MusicBrainz enumeration already dated. A Group's
    -- lifespan_begin is its formation, a Person's is their BIRTH — so the
    -- two need different thresholds, and treating a 1971-born producer as a
    -- seventies act would put the whole tail of modern Bandcamp in the
    -- priority lane.
    SELECT DISTINCT lower(name) AS artist_key FROM mb_artists
    WHERE lifespan_begin ~ '^[0-9]{4}'
      AND ((type =  'Group' AND left(lifespan_begin, 4) < '1995')
        OR (type <> 'Group' AND left(lifespan_begin, 4) < '1975'))
)
SELECT p.artist_key, p.album_key, p.artist, p.album, p.bc_year, p.n_tracks,
       (om.artist_key IS NOT NULL)           AS sig_mb,
       (os.artist_key IS NOT NULL)           AS sig_spotify
FROM pairs p
LEFT JOIN old_spotify os ON os.artist_key = p.artist_key
LEFT JOIN old_mb      om ON om.artist_key = p.artist_key
LEFT JOIN bc_redate_checked c
       ON c.artist_key = p.artist_key AND c.album_key = p.album_key
WHERE c.artist_key IS NULL
ORDER BY sig_mb DESC, sig_spotify DESC, p.n_tracks DESC,
         p.artist_key, p.album_key
LIMIT %s
"""

# Moves rows BACKWARDS only. The year comparison is the idempotence guard as
# well as the safety one: re-running cannot walk a record forward, and a pair
# whose rows were already corrected updates nothing the second time.
REDATE_SQL = """
UPDATE tracks SET year = %s, decade = %s
WHERE source = 'bandcamp'
  AND lower(artist) = %s AND lower(album) = %s
  AND year > %s
"""


def ask_mb(artist, album, pace, attempts=3):
    """original_release_year with the 503s absorbed.

    A MusicBrainz 503 is "slow down", not "no". Measured on 2026-08-20 it
    lands on ~28% of requests at 1.05s spacing and ~11% at 1.6s, so treating
    one as terminal would both lose recoverable rows and mark them checked —
    the worst outcome available, since the state table would never ask again.

    Returns (year_or_None, was_throttled). Raises MBRateLimited only when
    every attempt was refused, which the caller counts toward giving up.
    """
    throttled = False
    for attempt in range(attempts):
        try:
            year = original_release_year(artist, album)
            # original_release_year already sleeps MB_RATE_LIMIT_S; this is
            # the extra spacing on top, and 1.05s alone is measurably not
            # enough (28% refused).
            time.sleep(max(0.0, pace - MB_RATE_LIMIT_S))
            return year, throttled
        except MBRateLimited:
            throttled = True
            if attempt == attempts - 1:
                raise
            # 2s, 6s. Long enough to leave MB's window, short enough that a
            # 300-pair batch still finishes inside a cron slot.
            time.sleep(2 + attempt * 4)
    return None, throttled


# Siblings that also spend MusicBrainz requests. Their pacing is per-process,
# so two of them at once silently doubles this project's rate against MB and
# gets the whole night throttled — the hazard scripts/backfill_night.sh
# already documents and guards for its own two stages. MB refuses ~11% of
# requests at 1.6s spacing with nothing else running; stacked, it refuses most
# of them, and every refusal here is a pair that does not get checked.
#
# The list has to name what is actually ON THE SCHEDULE, not what sounds
# related. /etc/cron.d/dig runs crawl_genre_seeds hourly at :45 for up to 50
# minutes, backfill_unknown_regions hourly at :36, and resolve_origin on three
# separate slots — all three spend MusicBrainz requests, and none of them was
# named here. A guard that misses the busiest consumer on the box is worse than
# no guard, because it reads as coverage.
MB_SIBLINGS = (
    "source_genre_artists|ingest_mb_artists|enumerate_mb_artists|"
    "backfill_regions|backfill_unknown_regions|crawl_genre_seeds|"
    "resolve_origin|resolve_curator_artists"
)


def _cmdlines():
    """Every running process's command line, pgrep or no pgrep.

    The container has no procps — `pgrep` is simply absent — so on the box
    where all the other MusicBrainz jobs actually run, this guard was
    answering "nothing else is up" every single time. /proc is always there on
    Linux, costs nothing, and needs no package. pgrep stays as the path for
    macOS, where /proc does not exist.
    """
    out = []
    try:
        for pid in os.listdir("/proc"):
            if not pid.isdigit() or pid == str(os.getpid()):
                continue
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    out.append(f.read().replace(b"\0", b" ").decode(
                        "utf-8", "replace"))
            except OSError:
                continue          # the process exited while we were reading
    except OSError:
        return None               # not Linux — caller falls back to pgrep
    return out


def mb_sibling_running() -> bool:
    lines = _cmdlines()
    if lines is not None:
        return any(re.search(MB_SIBLINGS, ln) for ln in lines)
    try:
        return subprocess.run(["pgrep", "-f", MB_SIBLINGS],
                              capture_output=True).returncode == 0
    except Exception:
        # Neither /proc nor pgrep is not a reason to refuse to run; it is a
        # reason to lose the guard, and the backoff below still handles the
        # throttling.
        return False


def ensure_table():
    execute(open(os.path.join(ROOT, "scripts",
                              "migrate_bc_redate.sql")).read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=300,
                    help="(artist, album) pairs to check this run")
    ap.add_argument("--pace", type=float, default=1.6,
                    help="extra seconds between MB requests, on top of the "
                         "1.05s lib/mb_resolve already sleeps")
    ap.add_argument("--force", action="store_true",
                    help="run even if another MusicBrainz consumer is up")
    ap.add_argument("--dry-run", action="store_true",
                    help="ask MusicBrainz, print, write nothing")
    args = ap.parse_args()

    if mb_sibling_running() and not args.force:
        print("another MusicBrainz consumer is running — skipping this slot "
              "(--force to override).")
        return

    ensure_table()
    rows = fetchall(BACKLOG_SQL, (args.limit,))
    total = fetchone(
        "SELECT count(*) AS n FROM (SELECT DISTINCT lower(artist), lower(album) "
        "FROM tracks WHERE source = 'bandcamp' AND decade IN ('2010s','2020s') "
        "AND coalesce(artist,'') <> '' AND coalesce(album,'') <> '') x")["n"]
    done = fetchone("SELECT count(*) AS n FROM bc_redate_checked")["n"]
    print(f"batch: {len(rows)} pairs  |  checked so far: {done}/{total}"
          f"{'  [DRY RUN]' if args.dry_run else ''}")

    redated = confirmed = no_match = transient = 0
    tracks_moved = 0
    consecutive_503 = 0
    for r in rows:
        try:
            mb_year, throttled = ask_mb(r["artist"], r["album"], args.pace)
        except MBRateLimited:
            # Measured 2026-08-20: MusicBrainz 503s about 11% of requests even
            # at 1.6s spacing, so a single 503 cannot mean "stop" or the drip
            # would never finish a batch. ask_mb absorbs those. Reaching here
            # means the retries ALSO failed, repeatedly — MB is down or this
            # IP is blocked, and continuing would only deepen it.
            consecutive_503 += 1
            transient += 1
            if consecutive_503 >= 3:
                print("  MusicBrainz is refusing sustained traffic — "
                      "stopping this run early; the backlog is resumable.")
                break
            continue
        consecutive_503 = 0
        if throttled:
            transient += 1

        bc_year = r["bc_year"] or ""
        if not mb_year:
            outcome, hit = "no_match", 0
            no_match += 1
        elif not bc_year or mb_year >= bc_year:
            # MB agrees, or knows nothing older. Bandcamp's own date stands.
            outcome, hit = "confirmed", 0
            confirmed += 1
        else:
            outcome = "redated"
            redated += 1
            if args.dry_run:
                hit = r["n_tracks"]
            else:
                hit = len(execute_returning(
                    REDATE_SQL + " RETURNING id",
                    (mb_year, mb_year[:3] + "0s",
                     r["artist_key"], r["album_key"], mb_year)))
            tracks_moved += hit
            flags = "".join(c for c, on in
                            (("B", r["sig_mb"]), ("S", r["sig_spotify"]))
                            if on) or "-"
            print(f"  [{flags:<3}] {r['artist'][:30]:<30} {r['album'][:28]:<28} "
                  f"{bc_year} -> {mb_year}  ({hit} rows)")

        if not args.dry_run:
            execute(
                "INSERT INTO bc_redate_checked "
                "  (artist_key, album_key, mb_year, outcome, tracks_hit) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (artist_key, album_key) DO UPDATE SET "
                "  mb_year = EXCLUDED.mb_year, outcome = EXCLUDED.outcome, "
                "  tracks_hit = EXCLUDED.tracks_hit, checked_at = NOW()",
                (r["artist_key"], r["album_key"], mb_year, outcome, hit))

    print(f"\nredated={redated} confirmed={confirmed} no_match={no_match} "
          f"throttled={transient} | pool rows moved={tracks_moved}")
    if not args.dry_run and redated:
        for row in fetchall(
                "SELECT decade, count(*) AS n FROM tracks WHERE source='bandcamp' "
                "AND decade < '2010s' AND decade <> '' GROUP BY 1 ORDER BY 1"):
            print(f"   bandcamp pool now {row['decade']}: {row['n']}")


if __name__ == "__main__":
    main()
