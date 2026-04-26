-- mb_artists: staging area for MusicBrainz-enumerated artists.
-- Populated by scripts/enumerate_mb_artists.py (browses MB by country/area).
-- Drained by scripts/ingest_mb_artists.py (resolves to Spotify, adds 1 track
-- to the main `tracks` pool per artist). Tracks ingest state so the work is
-- restartable and idempotent.
CREATE TABLE IF NOT EXISTS mb_artists (
    mbid             TEXT PRIMARY KEY,             -- MusicBrainz UUID
    name             TEXT NOT NULL,
    sort_name        TEXT,
    country          TEXT,                          -- ISO-3166-1 (e.g. "BR")
    area             TEXT,                          -- area name (e.g. "Brazil")
    begin_area       TEXT,                          -- formation/birth area
    type             TEXT,                          -- "Group" | "Person" | …
    gender           TEXT,                          -- only for type=Person
    lifespan_begin   TEXT,
    lifespan_end     TEXT,
    mb_tags          TEXT[],                        -- folksonomy tags from MB
    spotify_url      TEXT,                          -- if MB has a streaming-url rel
    spotify_id       TEXT,                          -- extracted from spotify_url
    -- Ingest state
    ingested_at      TIMESTAMPTZ,                   -- NULL until we ingest
    ingested_track_id TEXT,                         -- the track we picked
    ingest_error     TEXT,                          -- last error, if any
    enumerated_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mb_artists_country ON mb_artists (country);
CREATE INDEX IF NOT EXISTS idx_mb_artists_resolution
  ON mb_artists (ingested_at, spotify_id) WHERE ingested_at IS NULL;

-- Track enumeration progress per (country, search-key) so the enumerate
-- script can resume after crashes / time-budget cutoffs.
CREATE TABLE IF NOT EXISTS mb_enum_state (
    scope         TEXT PRIMARY KEY,    -- e.g. "country:BR"
    last_offset   INTEGER NOT NULL DEFAULT 0,
    total_count   INTEGER,             -- MB-reported total (NULL until first page)
    done          BOOLEAN NOT NULL DEFAULT FALSE,
    last_run_at   TIMESTAMPTZ
);
