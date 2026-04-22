-- DIG — migration: track quality score.
-- Accumulated from user engagement (played_pct, saves, dislikes).
-- Idempotent.

ALTER TABLE tracks ADD COLUMN IF NOT EXISTS quality_score REAL DEFAULT 0;
ALTER TABLE tracks ADD COLUMN IF NOT EXISTS quality_plays INT DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_tracks_quality ON tracks (quality_score DESC);

GRANT ALL PRIVILEGES ON TABLE tracks TO dig;
