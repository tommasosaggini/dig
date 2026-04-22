-- DIG — Deep Crawler migrations.
-- Extends the artists table and creates the artist graph edge table.
-- Idempotent.

-- ── Extend artists table ─────────────────────────────────────────────────────
ALTER TABLE artists ADD COLUMN IF NOT EXISTS spotify_id TEXT;
ALTER TABLE artists ADD COLUMN IF NOT EXISTS popularity INT DEFAULT 0;
ALTER TABLE artists ADD COLUMN IF NOT EXISTS explore_depth INT DEFAULT 0;
ALTER TABLE artists ADD COLUMN IF NOT EXISTS related_fetched BOOLEAN DEFAULT FALSE;
ALTER TABLE artists ADD COLUMN IF NOT EXISTS albums_fetched BOOLEAN DEFAULT FALSE;
ALTER TABLE artists ADD COLUMN IF NOT EXISTS tracks_ingested INT DEFAULT 0;
ALTER TABLE artists ADD COLUMN IF NOT EXISTS discovered_via TEXT;
ALTER TABLE artists ADD COLUMN IF NOT EXISTS last_crawled_at TIMESTAMPTZ;

-- Index for crawler priority queue: unexplored artists first, thin regions first
CREATE INDEX IF NOT EXISTS idx_artists_crawl_queue
    ON artists (related_fetched, explore_depth, tracks_ingested);

-- ── Artist graph edges ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS artist_edges (
    id          BIGSERIAL PRIMARY KEY,
    from_id     TEXT NOT NULL,
    to_id       TEXT NOT NULL,
    edge_type   TEXT NOT NULL,  -- 'related', 'collab', 'compilation', 'genre_search', 'claude_suggested'
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(from_id, to_id, edge_type)
);

CREATE INDEX IF NOT EXISTS idx_artist_edges_from ON artist_edges (from_id);
CREATE INDEX IF NOT EXISTS idx_artist_edges_to ON artist_edges (to_id);

-- ── Genre expansion log (tracks which search terms Claude has suggested) ─────
CREATE TABLE IF NOT EXISTS genre_expansions (
    id          BIGSERIAL PRIMARY KEY,
    genre       TEXT NOT NULL,
    sub_terms   TEXT[] NOT NULL,       -- search queries Claude suggested
    explored    TEXT[] DEFAULT '{}',   -- which sub_terms have been searched
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_genre_expansions_genre ON genre_expansions (genre);

-- Grants
GRANT ALL PRIVILEGES ON TABLE artist_edges TO dig;
GRANT USAGE, SELECT ON SEQUENCE artist_edges_id_seq TO dig;
GRANT ALL PRIVILEGES ON TABLE genre_expansions TO dig;
GRANT USAGE, SELECT ON SEQUENCE genre_expansions_id_seq TO dig;
