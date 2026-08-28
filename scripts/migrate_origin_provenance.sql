-- Region provenance — 2026-08-27
--
-- `tracks.region` was never a claim about the artist. On the Spotify lanes it
-- is the SEARCH MARKET (pipeline/discover.py wrote `region_name = market`), so
-- a Réunion maloya record found in the SG storefront was filed under Singapore
-- and a Czech folk singer under Hong Kong. Measured against the rows where
-- MusicBrainz can adjudicate, 18.3% of country-level Spotify rows carried the
-- wrong country; for the rest the label was simply unverifiable.
--
-- That mattered beyond cosmetics: diversityShuffle()'s region lens, AI Mix's
-- cell bucketing and the water-filling ingest all treat `region` as origin, so
-- the pipeline believed it had covered countries it had never actually reached
-- (199 tracks labelled North Korea, 0 of them North Korean; Singapore showed
-- 285 tracks against a true supply of 22).
--
-- The fix is not to delete the column — it is a genuinely useful record of
-- WHICH CELL a search belonged to. The fix is to stop letting it impersonate
-- an origin. `origin_source` names where the country claim came from, and the
-- read path honours only the tiers that are actually about the artist.
--
-- Tiers, strongest first:
--   musicbrainz            MB country on the artist (already in origin_region)
--   mb_artists_spotify_id  exact Spotify-ID join into our own mb_artists table
--   wikidata_spotify_id    Wikidata P1902 -> P495/P740/P27
--   bandcamp_location      artist's own declared band_location
--   bandcamp_page          band_location recovered from the artist page later
--   mb_artists_name        unambiguous name join (one country in mb_artists)
--   wikidata_name          unambiguous music-entity name match
--   ---- everything below is NOT a claim about the artist ----
--   market                 the Spotify storefront the search ran in
--   inherited              copied from a crawl seed / genre backfill
--   uploader               a YouTube channel's country (the UPLOADER, not the
--                          artist: 'Sahel Sounds' is US and reissues Nigerien
--                          music). Recorded, never trusted.
--   unknown                no evidence at all

ALTER TABLE tracks ADD COLUMN IF NOT EXISTS origin_source TEXT;
ALTER TABLE tracks ADD COLUMN IF NOT EXISTS origin_checked_at TIMESTAMPTZ;

-- Partial index: the resolver's work queue is "rows with no trusted origin".
CREATE INDEX IF NOT EXISTS idx_tracks_origin_unresolved
    ON tracks (origin_checked_at NULLS FIRST)
    WHERE origin_source IS NULL
       OR origin_source IN ('market', 'inherited', 'uploader', 'unknown');

CREATE INDEX IF NOT EXISTS idx_tracks_origin_source ON tracks (origin_source);
