-- Per-artist blacklist for one-off stock acts that don't fit any regex
-- pattern in lib/track_filter.py. Track filter consults this on every
-- ingest; the blacklist_artist.py CLI also retroactively deletes existing
-- tracks for newly-blacklisted artists so cleanup is one-shot.
CREATE TABLE IF NOT EXISTS artist_blacklist (
    artist_key  TEXT PRIMARY KEY,        -- lowercased primary artist (first comma-segment)
    artist_display TEXT,                  -- original casing for human reference
    reason      TEXT,
    added_at    TIMESTAMPTZ DEFAULT NOW(),
    added_by    TEXT DEFAULT 'manual'
);
CREATE INDEX IF NOT EXISTS idx_artist_blacklist_added_at
    ON artist_blacklist (added_at DESC);
