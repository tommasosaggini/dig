-- Per-stage bookkeeping for the origin ladder — 2026-08-28
--
-- THE BUG. resolve_origin.py walks a six-rung ladder, cheapest and most exact
-- source first, and recorded its progress in ONE column: origin_checked_at.
-- One flag cannot describe six independent questions, and the three network
-- stages each read it differently:
--
--   stage_wikidata      WHERE ... AND origin_checked_at IS NULL   -- honoured it
--   stage_bandcamp      WHERE ... AND source='bandcamp'           -- ignored it
--   stage_musicbrainz   WHERE ... AND artist_ids IS NOT NULL      -- ignored it
--
-- So the two that ignore it have no cursor at all. `LIMIT 150` with no ORDER
-- BY and no "not yet asked" filter re-reads the same rows every run, forever.
-- Measured 2026-08-28, stage_bandcamp's whole population:
--
--     total 2,127   already asked 2,122
--
-- It runs four times a day at 150 rows and advances by however many happen to
-- resolve — the other ~109 come back next run and say no again. That is a
-- repair retried every pass that can never finish, and it is why the trusted
-- share sat at 87.0% while the resolver looked busy in the log.
--
-- Making them all honour origin_checked_at would have swapped one defect for a
-- worse one: whichever stage touched a row FIRST would mark it done for every
-- other stage, so the cheap low-yield rungs would strip the pool before the
-- expensive high-yield ones ever saw it. Measured yields say that is exactly
-- backwards — Wikidata-by-name lands 23%, the Bandcamp artist page 58%.
--
-- So each stage records ITS OWN attempt. A row leaves a stage's backlog when
-- that stage has asked it, and no sooner.
--
-- Rows already marked checked keep an empty array on purpose: we cannot know
-- which stage asked them, and guessing would silently retire rows nothing ever
-- tried. Each stage therefore gets one honest pass over its own population —
-- about 2,100 requests for Bandcamp, four days at the current cadence — and is
-- then genuinely finished rather than looping.

ALTER TABLE tracks
  ADD COLUMN IF NOT EXISTS origin_stages_tried text[] NOT NULL DEFAULT '{}';

-- The stage queries all ask "NOT (<stage> = ANY(origin_stages_tried))" on top
-- of the unresolved predicate. GIN is the index for array containment.
CREATE INDEX IF NOT EXISTS tracks_origin_stages_tried_idx
  ON tracks USING gin (origin_stages_tried);
