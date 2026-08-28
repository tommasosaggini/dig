"""Where a track's country claim came from — one definition, used everywhere.

`tracks.region` was never a claim about the artist. On the Spotify lanes
pipeline/discover.py wrote `region_name = market`, so the column holds the
SEARCH STOREFRONT: a Reunion maloya record found via the SG market was filed
under Singapore, a Czech folk singer under Hong Kong, a US hardcore band under
Egypt. Measured where MusicBrainz could adjudicate, 18.3% of country-level
Spotify rows carried the wrong country; for the rest it was unverifiable.

That leaked into behaviour, not just display: diversityShuffle()'s region lens,
AI Mix's cell bucketing and the water-filling ingest all read `region` as
origin, so the pipeline believed it had covered countries it had never reached
(199 tracks labelled North Korea, none of them North Korean; Singapore showing
285 tracks against a true supply of 22).

`origin_source` fixes that by naming the evidence. The split that matters is
between tiers that are statements ABOUT THE ARTIST and tiers that describe how
we happened to find the track — only the former may name a country.

Two sources were tested against dig's own data and rejected; do not re-add them:

  * A YouTube channel's `country` is the UPLOADER's. 'Sahel Sounds' reports US
    and reissues Nigerien music; 'Spinnin' Records' reports NL and releases
    everyone. Adopting it rebuilds exactly this bug in a new column.
  * Discogs artists carry no structured country (only free-text `profile`), and
    a release's `country` is the PRESSING country — Discogs files INFERNAL
    EXECRATOR, a Singaporean band, under US, Norway and Brazil.
"""

# Tiers whose country is a claim about the artist, strongest first.
TRUSTED = (
    "musicbrainz",            # MB country on the artist
    "mb_artists_spotify_id",  # exact Spotify-ID join into our mb_artists table
    "wikidata_spotify_id",    # Wikidata P1902 -> P495/P740/P27
    "bandcamp_location",      # the artist's own declared band_location
    "bandcamp_page",          # band_location recovered from the artist page
    "mb_artists_name",        # unambiguous name join (one country in MB)
    "wikidata_name",          # unambiguous music-entity name, namesake-guarded
)
TRUSTED_ORIGIN_SOURCES = frozenset(TRUSTED)

# Within TRUSTED there are two grades of evidence, and coverage arithmetic
# should know which it is holding.
#
# VERIFIED — a third party (MusicBrainz, Wikidata) independently states the
# artist's country. Nobody with an interest in the answer supplied it.
#
# DECLARED — the artist typed it into their own Bandcamp profile. Usually
# honest ("Leeds, UK", "Sonoma, California") and always a claim about the
# ARTIST, which is why it outranks a market label and is servable. But it is
# unpoliced, and for a few countries it is plainly a joke: of 199 tracks
# tagged North Korea on 2026-08-27, 195 carried a self-declared DPRK location
# and the artists behind them were Incel Crew, ACID SHIT and Assisted Suicide.
#
# Do NOT infer fakery from "no independent corroboration" alone. Greenland,
# Jersey, the Faroes and Guam also score zero, and there the likelier reading
# is that MusicBrainz has never catalogued a scene that small — treating
# obscurity as dishonesty would repeat, in a new field, exactly the unjustified
# inference this module exists to stop.
VERIFIED = ("musicbrainz", "mb_artists_spotify_id", "wikidata_spotify_id",
            "mb_artists_name", "wikidata_name")
DECLARED = ("bandcamp_location", "bandcamp_page")
VERIFIED_ORIGIN_SOURCES = frozenset(VERIFIED)
DECLARED_ORIGIN_SOURCES = frozenset(DECLARED)

# Recorded, never trusted: these describe the FIND, not the artist.
UNTRUSTED = ("market", "inherited", "uploader", "unknown")

NOT_A_PLACE = frozenset({"", "Unknown", "XW", "XE", "XG", "global", "Various"})

MACRO = frozenset({
    "South Asia", "Southern Africa", "West Africa", "East Africa",
    "Eastern Europe", "Western Europe", "Nordic", "Caribbean", "Middle East",
    "Central Asia", "Horn of Africa", "Sahel", "North Africa",
    "Pacific Islands", "Balkans", "Caucasus", "Southeast Asia",
    "Central America", "South America", "Central Africa", "Latin America",
    "Oceania", "Central Europe", "Southern Europe", "East Asia",
    "Baltic States",
})


def _countries():
    from lib.region_norm import ISO2
    return set(ISO2.values())


def classify(row):
    """Evidence tier for one track row/dict: (origin_source, country_or_None).

    Pure — reads only fields already on the row, makes no network calls, and
    never invents a country. Callers that want to UPGRADE a row (find the real
    origin for a `market` row) go through scripts/resolve_origin.py.

    An already-stamped trusted tier is preserved: this function establishes the
    floor from raw evidence, it must not demote what the resolver has proven.
    """
    existing = (row.get("origin_source") or "").strip()
    existing_country = (row.get("origin_region") or "").strip()
    if existing in TRUSTED_ORIGIN_SOURCES and existing_country:
        return existing, existing_country
    # A trusted tier with NO country is not a proven row, it is a half-written
    # one, and short-circuiting on the tier alone freezes it that way forever.
    # That is exactly what happened to the 72,876 `bandcamp_location` rows on
    # the first backfill pass: the tier was stamped, the country was left in
    # `region`, and the second pass — the one meant to move it across — took
    # this early return and preserved the emptiness instead. Fall through and
    # re-derive, but keep the tier name if the evidence still supports it.

    origin = (row.get("origin_region") or "").strip()
    region = (row.get("region") or "").strip()
    query = row.get("query") or ""
    source = row.get("source") or ""

    # MusicBrainz already adjudicated the artist. Both writers of origin_region
    # (scripts/ingest_mb_artists.py, scripts/backfill_unknown_regions.py) are MB.
    if origin and origin not in NOT_A_PLACE:
        return "musicbrainz", origin

    # Bandcamp's band_location is self-reported by the artist. Not infallible,
    # but a claim about the artist rather than about our search — which is the
    # distinction that matters. Only the feed lanes carry it; the curator and
    # resolve lanes set region='' and inherit their bucket from elsewhere.
    if source == "bandcamp" and query.startswith(("bandcamp:", "bandcamp-tag:")):
        if region in _countries() or region in MACRO:
            return "bandcamp_location", region
        return "unknown", None

    # The market lanes: `region` is the Spotify storefront mapped back through
    # REGIONS. Not an origin claim at any granularity.
    if query.startswith(("catalog:", "random:")):
        return "market", None

    if query.startswith(("crawl", "artist:", "collab", "genre-backfill")):
        return "inherited", None

    if source == "youtube" or query.startswith(("youtube", "channel")):
        return "uploader", None

    if query.startswith(("curator", "import")):
        return "inherited", None

    return "unknown", None


def served_region(row):
    """The country a track may be SERVED under — '' when nothing backs one.

    Reads ONE field. `origin_region` is where every trusted tier lands its
    answer (scripts/backfill_origin_source.py backfilled it for the Bandcamp
    rows whose location previously lived only in `region`), so there is no
    second-guess path back to `region`. That matters: a trusted tier that
    somehow arrived with an empty origin would otherwise fall through to the
    market label and quietly re-create the bug this module exists to kill.

    '' buckets the track under Unknown, which is what it has always actually
    been. Unlike a fictional country, Unknown is a bucket the water-filling
    ingest will try to drain.
    """
    if (row.get("origin_source") or "") not in TRUSTED_ORIGIN_SOURCES:
        return ""
    origin = (row.get("origin_region") or "").strip()
    return origin if origin not in NOT_A_PLACE else ""


# The same gate as served_region(), expressed in SQL so every reader agrees.
# Prefix the table alias where you need one: ORIGIN_SQL.format(t="t.").
#
# Yields NULL — not a fictional country — for any row whose evidence does not
# name the artist. Callers that filter on it therefore drop untrusted rows from
# region-keyed work (coverage cells, region-weighted pool queries) instead of
# crediting them to a country nobody verified.
def _gate(tiers):
    """CASE that yields the country only for `tiers`, and only when it names a
    real place. NULLIF alone was not enough: MusicBrainz uses XW/XE/XG for
    'worldwide'/'Europe'/'unknown', and those flowed straight through the SQL
    gate while served_region() correctly rejected them — so a track served as
    Unknown was simultaneously counted under 'XW' by the coverage queries.
    The two gates must reject the same set or they will disagree forever."""
    return ("(CASE WHEN {t}origin_source = ANY(ARRAY["
            + ",".join("'%s'" % x for x in tiers)
            + "]) AND NULLIF({t}origin_region, '') IS NOT NULL"
            + " AND {t}origin_region <> ALL(ARRAY["
            + ",".join("'%s'" % x for x in sorted(NOT_A_PLACE) if x)
            + "]) THEN {t}origin_region END)")


ORIGIN_SQL = _gate(TRUSTED)


# Third-party-verified origins only. Use where a country's count is being taken
# as evidence that the country is COVERED — a scene propped up entirely by
# self-declaration has not been proven to exist, and telling the water-filling
# it is fed is how a country stops being dug for.
ORIGIN_SQL_VERIFIED = _gate(VERIFIED)
