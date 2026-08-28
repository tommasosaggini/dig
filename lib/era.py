"""DIG — one definition of what era balance means.

WHY THIS FILE EXISTS
--------------------
The era target was written in JavaScript (the picker weights every candidate
against it) and the "which decades are starved" window was written separately
in scripts/ingest_mb_artists.py. Adding a third copy to pipeline/discover.py
would have reproduced, exactly, the problem this repo already has with
STATUS_RANK living in three languages — where the copies drift and the one that
drifts is the one deciding what the listener hears.

So Python reads it from here, and web/js/app.js's literal is checked against
this file by tests/test_era_target_is_one_definition.py. The JS keeps a literal
rather than fetching it because the picker must work on the very first pick,
before any network call has returned.

WHY THE TARGET IS NOT UNIFORM
-----------------------------
Every other coverage axis in DIG water-fills toward "even" — there is no reason
Mali should owe a listener less attention than France. Decades are the
exception, decided with the listener on 2026-08-18: they wanted markedly more
80s/90s/2000s while being explicit that they were NOT asking to hear as much
1940s as 2020s. See ARCHITECTURE.md, Principles 1 and 1b.

The measurements that produced it, over the last 300 served picks that day:
1.0% 1980s, 1.7% 1990s, 5.0% 2000s, 67.0% 2020s — which matched the pool's own
era mix to within 1.06x on every decade. The picker was not amplifying the
skew; it was blind to era and reproducing supply.
"""

# Relative share of picks each decade should get. "Lean modern, real back
# catalogue" — chosen against two measured alternatives.
#
# These are RELATIVE weights, not percentages: they total 102, and both readers
# normalise after the supply clamp has already changed them anyway (a decade the
# pool cannot supply is zeroed, so any sum written here would be wrong by the
# time it is used). Read 12 vs 25 as "the 1980s should get about half the picks
# the 2020s does", which is the only thing the numbers actually assert.
ERA_TARGET = {
    "1900s": 0.4, "1910s": 0.4, "1920s": 0.4, "1930s": 0.4, "1940s": 0.4,
    "1950s": 2,
    "1960s": 4,
    "1970s": 7,
    "1980s": 12,
    "1990s": 15,
    "2000s": 15,
    "2010s": 20,
    "2020s": 25,
}

# How far over its share of the unheard pool a decade may be served. Mirrors
# ERA_SUPPLY_HEADROOM in web/js/app.js. The target is an ambition; supply
# decides how fast it is met.
ERA_SUPPLY_HEADROOM = 3

# The decades the pool is actually short of, and the only ones ingestion is
# steered toward. Deliberately NOT "everything before 2010":
#
#   yield per catalog cell scanned, measured over 21 days to 2026-08-18
#     1990s 3.5   2000s 3.0   2010s 3.0   2020s 3.0
#     1980s 2.4   1970s 1.9   1960s 1.0   1950s 0.7
#
# Scanning a 1990s cell returns MORE than a 2020s one and the 2000s ties it, so
# steering into 1980-2009 is close to free. Below 1970 the supply genuinely is
# not there — chasing it would trade 3.0 tracks per search for 0.7, to fill
# decades the target only asks a few percent of anyway.
STARVED_FROM = 1980
STARVED_TO = 2009

# Career-start window for the MusicBrainz artist walk. One decade wider at the
# bottom than STARVED_FROM on purpose: an artist whose career began in 1972 is
# a strong source of EIGHTIES records, which is what the walk is reaching for.
CAREER_FIRST_FROM = 1970
CAREER_FIRST_TO = 2009


def decade_of(year) -> str | None:
    """1987 -> '1980s'. None for anything that is not a four-digit year — an
    unknown year must not be quietly filed under a decade it might not be in."""
    s = str(year or "").strip()
    if len(s) != 4 or not s.isdigit():
        return None
    return s[:3] + "0s"


def starved_decades() -> list[str]:
    """The decades ingestion should be steered toward, oldest first."""
    return [f"{y}s" for y in range(STARVED_FROM // 10 * 10,
                                   STARVED_TO // 10 * 10 + 10, 10)]


def is_starved(decade: str) -> bool:
    return decade in set(starved_decades())
