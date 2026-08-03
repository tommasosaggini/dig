-- DIG — migration: make user_history one-row-per-track and mergeable, so the
-- Spotify pull (lib/spotify_sync.py) has somewhere to write that the client
-- won't wipe.
--
-- WHY THIS EXISTS
-- ---------------
-- DIG's history was "what DIG dispatched". Everything Spotify chose on its own
-- — the rest of an album a deep link opened, an AirPods skip to a track outside
-- the pool, anything played from the Spotify app while DIG was closed — was
-- never recorded. Measured 2026-08-03 against /me/player/recently-played:
-- 26 of the last 50 plays were absent from user_history, including the whole
-- run the listener asked about by name.
--
-- The pull can't just insert rows, because POST /history used to
-- `DELETE WHERE user_id = %s` and re-insert the browser's localStorage
-- wholesale. Any row the server learned on its own lived until the next sync.
-- So this migration establishes the key that lets the write become a merge.

-- Cursors for the incremental Liked Songs pull and the sync rate-gate.
ALTER TABLE users ADD COLUMN IF NOT EXISTS spotify_liked_cursor BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS spotify_sync_at      BIGINT;

-- Status priority, mirrored from the client's STATUS_RANK in app.js. Explicit
-- user actions (saved/disliked) are sticky and are never silently downgraded
-- by an automatic event (listened/skipped). The client has enforced this on
-- its own copy since the relisten-clobbers-a-save bug; the server needs it too
-- now that two writers (the browser and the Spotify pull) touch the same row.
CREATE OR REPLACE FUNCTION dig_status_rank(s TEXT) RETURNS INT
  LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE s
             WHEN 'saved'    THEN 5
             WHEN 'disliked' THEN 5
             WHEN 'listened' THEN 2
             WHEN 'skipped'  THEN 1
             ELSE 0
           END
  $$;

-- The client's history has always been one entry per track (addToHistory does
-- `history.find(h => h.id === track.id)` and updates in place; a relisten bumps
-- `time`). The table never enforced that, and full-replace writes hid the
-- difference. Collapse existing duplicates BEFORE adding the constraint —
-- keeping the strongest status and the most recent timestamp, so a save that
-- happens to live on the row we're about to drop is not lost.
UPDATE user_history h
   SET status      = best.status,
       listened_at = best.listened_at
  FROM (
    SELECT user_id, track_id,
           (ARRAY_AGG(status ORDER BY dig_status_rank(status) DESC,
                                      COALESCE(listened_at, 0) DESC))[1] AS status,
           MAX(listened_at) AS listened_at
      FROM user_history
     WHERE track_id IS NOT NULL
     GROUP BY user_id, track_id
    HAVING COUNT(*) > 1
  ) best
 WHERE h.user_id = best.user_id AND h.track_id = best.track_id;

DELETE FROM user_history a
 USING user_history b
 WHERE a.user_id  = b.user_id
   AND a.track_id = b.track_id
   AND a.track_id IS NOT NULL
   AND a.id < b.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_user_history_user_track
    ON user_history (user_id, track_id);

-- Backfilled rows are dated by Spotify's played_at, which can be older than
-- anything the client holds, so the pull reads history by timestamp window.
CREATE INDEX IF NOT EXISTS idx_user_history_user_time
    ON user_history (user_id, listened_at DESC);
