-- Track which mode each play was in (tailored | aimix | journey | discovery).
-- Older rows will remain NULL; we intentionally don't guess retroactively.
ALTER TABLE user_history ADD COLUMN IF NOT EXISTS mode TEXT;
CREATE INDEX IF NOT EXISTS idx_user_history_mode ON user_history (user_id, mode)
  WHERE mode IS NOT NULL;
