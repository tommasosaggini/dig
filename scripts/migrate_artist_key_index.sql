-- DIG — index behind the name-keyed artist cap (lib/artist_cap.py).
--
-- The cap runs one COUNT per candidate track at every ingest path, so
-- without this it is a seq scan over 100k+ rows per track and deep_crawl
-- (which probes a full discography at a time) pays it hundreds of times
-- a run. The existing idx_tracks_artist is on the RAW column and cannot
-- serve lower(btrim(artist)) — Postgres needs the expression itself.
--
-- The expression here must stay byte-identical to the WHERE clause in
-- lib.artist_cap.is_over_cap or the planner silently ignores the index
-- and the only symptom is a slow cron.
--
-- Also serves the over-cap audit/trim queries, which group by the same key.

CREATE INDEX IF NOT EXISTS idx_tracks_artist_key
    ON tracks (lower(btrim(artist)));
