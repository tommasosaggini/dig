-- DIG — migration: add played_pct column to user_history.
-- Idempotent: safe to run multiple times.

ALTER TABLE user_history
    ADD COLUMN IF NOT EXISTS played_pct REAL;

-- Index for quick "tracks user listened to >70%" queries used by AI Mix.
CREATE INDEX IF NOT EXISTS idx_user_history_played_pct
    ON user_history (user_id, played_pct)
    WHERE played_pct IS NOT NULL;
