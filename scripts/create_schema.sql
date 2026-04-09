-- DIG — PostgreSQL schema
-- Run once: psql $DATABASE_URL -f scripts/create_schema.sql

-- ── Discovery pool ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tracks (
    id              TEXT PRIMARY KEY,       -- Spotify ID or 'yt:VIDEO_ID'
    name            TEXT NOT NULL,
    artist          TEXT,
    artist_ids      TEXT[],                 -- Spotify artist IDs
    album           TEXT,
    popularity      INTEGER DEFAULT 0,
    source          TEXT,                   -- 'spotify' | 'youtube'
    region          TEXT,
    decade          TEXT,
    year            TEXT,
    query           TEXT,                   -- search query that found this track
    label_energy    TEXT,
    label_mood      TEXT,
    label_texture   TEXT,
    label_feel      TEXT,
    label_use_case  TEXT,
    added_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracks_region ON tracks(region);
CREATE INDEX IF NOT EXISTS idx_tracks_source ON tracks(source);
CREATE INDEX IF NOT EXISTS idx_tracks_artist ON tracks(artist);

-- ── Artist registry ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS artists (
    slug        TEXT PRIMARY KEY,           -- normalized lowercase name
    name        TEXT NOT NULL,
    regions     TEXT[],
    genres      TEXT[],
    decades     TEXT[],
    sources     TEXT[],
    track_count INTEGER DEFAULT 0,
    track_refs  JSONB DEFAULT '[]',         -- [{id, name}] lightweight back-refs
    first_seen  TIMESTAMPTZ,
    last_seen   TIMESTAMPTZ
);

-- ── Music landscape catalog ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS genres (
    genre       TEXT PRIMARY KEY,
    source      TEXT,                       -- 'seed' | 'discovered' | 'wikipedia'
    added_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS catalog_cells (
    cell_id         TEXT PRIMARY KEY,       -- 'Region|genre|decade'
    region          TEXT NOT NULL,
    genre           TEXT NOT NULL,
    decade          TEXT NOT NULL,
    pool_size       INTEGER,
    explored        INTEGER DEFAULT 0,
    fetched         INTEGER DEFAULT 0,
    last_scanned    TIMESTAMPTZ,
    last_fetched    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_cells_region ON catalog_cells(region);
CREATE INDEX IF NOT EXISTS idx_cells_genre  ON catalog_cells(genre);

CREATE TABLE IF NOT EXISTS catalog_scan_queue (
    position    SERIAL PRIMARY KEY,
    cell_id     TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS catalog_meta (
    key     TEXT PRIMARY KEY,
    value   JSONB NOT NULL
);

-- ── Users ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users (
    id              TEXT PRIMARY KEY,       -- Spotify user ID
    display_name    TEXT,
    email           TEXT,
    image_url       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_tokens (
    user_id     TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    token_data  JSONB NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_history (
    id          SERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    track_id    TEXT,
    track_name  TEXT,
    artist      TEXT,
    region      TEXT,
    status      TEXT,
    listened_at BIGINT                      -- JS millisecond timestamp
);

CREATE INDEX IF NOT EXISTS idx_user_history_user ON user_history(user_id);

CREATE TABLE IF NOT EXISTS user_ledger (
    id          SERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    track_key   TEXT NOT NULL,             -- 'artist - track' (lowercase)
    status      TEXT NOT NULL,             -- 'known' | 'liked' | 'disliked'
    vibe        TEXT[],
    reason      TEXT,
    added_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, track_key)
);

CREATE INDEX IF NOT EXISTS idx_user_ledger_user   ON user_ledger(user_id);
CREATE INDEX IF NOT EXISTS idx_user_ledger_status ON user_ledger(user_id, status);

-- ── Discovery dedup ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS search_queries (
    query_key       TEXT PRIMARY KEY,       -- 'query_string|MARKET'
    count           INTEGER DEFAULT 0,
    runs            INTEGER DEFAULT 0,
    last_searched   TIMESTAMPTZ
);
