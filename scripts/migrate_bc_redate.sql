-- Bandcamp reissue re-dating — one row per (artist, album) pair ever asked
-- about, so the drip never spends a MusicBrainz request twice.
--
-- A state FILE was the other option (backfill_bc_years.py keeps one), and it
-- is the wrong shape at this size: that script reads its whole dead-set into
-- memory and pads its LIMIT by len(dead), which walks further into the table
-- every run. There are 54k pairs here, so the padding would outgrow the batch
-- within days. A table lets the backlog query be an anti-join and stay flat.

CREATE TABLE IF NOT EXISTS bc_redate_checked (
    artist_key  TEXT NOT NULL,          -- lower(artist), as the tracks query groups it
    album_key   TEXT NOT NULL,          -- lower(album)
    mb_year     TEXT,                   -- what MusicBrainz answered; NULL on no match
    outcome     TEXT NOT NULL,          -- 'redated' | 'confirmed' | 'no_match'
    tracks_hit  INTEGER DEFAULT 0,      -- rows actually updated by this pair
    checked_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (artist_key, album_key)
);

-- The drip's hot path: "pairs not yet checked". Outcome is in the index so a
-- re-run's anti-join is index-only.
CREATE INDEX IF NOT EXISTS idx_bc_redate_outcome ON bc_redate_checked(outcome);
