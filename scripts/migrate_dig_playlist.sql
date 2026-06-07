-- 2026-04-29 — switch DIG saves from Spotify Liked Songs to a per-user
-- "DIG" private playlist. We stash the playlist id once we've created it
-- on the user's first save (lazy provisioning).
ALTER TABLE users ADD COLUMN IF NOT EXISTS dig_playlist_id TEXT;
