-- DIG → Instagram curation queue.
-- Idempotent; apply with:  psql "$DATABASE_URL" -f scripts/migrate_ig_post_queue.sql
-- (Also created automatically at server startup by ensure_ig_schema().)
--
-- One row = one track on its way to becoming an Instagram post. Keyed by the
-- heterogeneous pool id string (spotify id | bc:band:track | yt:videoid |
-- manual:<uuid>) so anything in `tracks` — or hand-added — can be queued.

CREATE TABLE IF NOT EXISTS ig_post_queue (
    id                BIGSERIAL PRIMARY KEY,
    track_id          TEXT,                              -- tracks.id; NULL allowed for pure manual adds
    track_name        TEXT NOT NULL,
    artist            TEXT NOT NULL,
    artwork_url       TEXT,                              -- resolved from audio source (Bandcamp art / yt thumbnail) or uploaded
    audio_source      TEXT,                              -- 'bandcamp' | 'youtube' | 'upload'
    audio_path        TEXT,                              -- local path to the FULL audio once acquired
    audio_duration_ms INTEGER,
    clip_start_ms     INTEGER,                           -- chosen 30s window start; NULL until the admin picks it
    clip_duration_ms  INTEGER NOT NULL DEFAULT 30000,
    caption           TEXT,                              -- auto-generated, editable
    post_feed         BOOLEAN NOT NULL DEFAULT TRUE,     -- publish as a Reel
    post_story        BOOLEAN NOT NULL DEFAULT TRUE,     -- publish to Stories
    queue_order       INTEGER NOT NULL DEFAULT 0,        -- manual ordering (lower = sooner)
    scheduled_at      TIMESTAMPTZ,                       -- NULL = unscheduled; auto-filled on approve
    status            TEXT NOT NULL DEFAULT 'suggested',
        -- lifecycle:
        --   suggested   → proposer put it here; awaiting the admin's yes/no
        --   needs_audio → approved, no playable audio yet (resolver will try)
        --   needs_clip  → audio acquired; awaiting the 30s window pick
        --   ready       → window + caption set, approved for publish
        --   scheduled   → ready + a due time assigned
        --   publishing  → handed to the IG Graph API
        --   published   → live; ig_media_id set
        --   failed      → see error
        --   skipped     → dismissed by the admin
    error             TEXT,
    ig_media_id       TEXT,                              -- IG media id after publish (feed)
    ig_story_media_id TEXT,                              -- IG media id after publish (story)
    rendered_at       TIMESTAMPTZ,                       -- when feed.mp4 / story.mp4 were last built
    published_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Touch updated_at on every UPDATE.
CREATE OR REPLACE FUNCTION ig_post_queue_touch() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ig_post_queue_touch_trg ON ig_post_queue;
CREATE TRIGGER ig_post_queue_touch_trg
    BEFORE UPDATE ON ig_post_queue
    FOR EACH ROW EXECUTE FUNCTION ig_post_queue_touch();

CREATE INDEX IF NOT EXISTS ig_post_queue_status_idx   ON ig_post_queue (status);
CREATE INDEX IF NOT EXISTS ig_post_queue_order_idx    ON ig_post_queue (queue_order);
CREATE INDEX IF NOT EXISTS ig_post_queue_sched_idx    ON ig_post_queue (scheduled_at);
-- Don't suggest the same track twice while it's live in the queue.
CREATE UNIQUE INDEX IF NOT EXISTS ig_post_queue_active_track_idx
    ON ig_post_queue (track_id)
    WHERE track_id IS NOT NULL AND status NOT IN ('skipped', 'failed');

-- The app role used by the server/cron. Mirrors migrate_session.sql.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dig') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON ig_post_queue TO dig;
        GRANT USAGE, SELECT ON SEQUENCE ig_post_queue_id_seq TO dig;
    END IF;
END $$;
