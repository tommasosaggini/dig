-- DIG — migration: per-user AI query memory.
-- Stores the last N queries Claude proposed so we can feed them back
-- as "don't repeat these shapes" on subsequent calls.
-- Idempotent.

CREATE TABLE IF NOT EXISTS user_ai_queries (
    id           BIGSERIAL PRIMARY KEY,
    user_id      TEXT NOT NULL,
    ts           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    query_json   JSONB NOT NULL,
    reason       TEXT,
    lens         TEXT
);

CREATE INDEX IF NOT EXISTS idx_user_ai_queries_user_ts
    ON user_ai_queries (user_id, ts DESC);
