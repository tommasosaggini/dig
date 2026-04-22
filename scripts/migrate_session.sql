-- DIG — migration: per-user live session state for cross-device sync.
-- One row per user, overwritten on every heartbeat.
-- Idempotent.

CREATE TABLE IF NOT EXISTS user_session (
    user_id    TEXT PRIMARY KEY,
    state      JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

GRANT ALL PRIVILEGES ON TABLE user_session TO dig;
