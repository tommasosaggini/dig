-- DIG — migration: separate "we played this at you" from "you listened to it".
--
-- WHY THIS EXISTS
-- ---------------
-- user_history.status = 'listened' meant "DIG dispatched this track". The write
-- happened the instant Spotify accepted a play command, when played_pct was by
-- construction zero, and only DIG's own Next button could ever take it back. So
-- the ledger drew ▶ over songs that had been skipped at four seconds, over
-- tracks the listener walked away from, and over 472 rows a playlist script had
-- stamped in bulk without playing anything at all.
--
-- Measured on 2026-08-18, across 3,755 rows carrying 'listened':
--
--     played_pct >= 80          494   13%
--     played_pct 50-80          162    4%
--     played_pct 25-50          252    7%
--     played_pct < 25         1,160   31%
--     played_pct IS NULL      1,687   45%
--
-- Roughly one row in eight was telling the truth.
--
-- THE NEW VOCABULARY
-- ------------------
--   served    playback started; nothing was measured. Says only "this was put
--             in front of you". The floor: any other status outranks it.
--   skipped   the listener moved on before the stream threshold.
--   listened  real forward playback crossed 30s (Spotify's own threshold for
--             counting a stream), or 80% of a track shorter than that.
--   saved     the listener said so.
--   disliked  the listener said so.
--
-- played_pct is untouched by this migration. It was always the honest column;
-- the status is what was lying, and every reclassification below is derived
-- from the number that was already sitting in the row.

BEGIN;

-- ── 1. Rank ────────────────────────────────────────────────────────────────
-- 'served' below 'skipped', since it is the absence of evidence rather than a
-- weak piece of it. skipped moves 1 → 2 and listened 2 → 3; only the ORDER is
-- load-bearing (the function exists to be compared against itself), so the
-- renumbering is free. Mirrored by STATUS_RANK in web/js/app.js and
-- lib/spotify_sync.py — change all three or none.
CREATE OR REPLACE FUNCTION dig_status_rank(s TEXT) RETURNS INT
  LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE s
             WHEN 'saved'    THEN 5
             WHEN 'disliked' THEN 5
             WHEN 'listened' THEN 3
             WHEN 'skipped'  THEN 2
             WHEN 'served'   THEN 1
             ELSE 0
           END
  $$;

-- ── 2. Backfill ────────────────────────────────────────────────────────────
-- Only rows currently claiming 'listened' are touched. 'skipped', 'saved' and
-- 'disliked' rows are left exactly as they are: each of those was written from
-- something that actually happened, and re-deriving them from played_pct would
-- be inventing a second opinion where there is already a first-hand one.
--
-- A row KEEPS 'listened' on either of two grounds, and loses it otherwise:
--
--   (a) played_pct >= 80. Measured, and past the bar.
--
--   (b) mode = 'external' with no played_pct. These are the rows
--       lib/spotify_sync.recent_rows wrote from /me/player/recently-played,
--       which only lists a track once it passed ~30s — Spotify's own stream
--       threshold, and the same bar the client now holds itself to. The
--       percentage is unknown but the threshold is not, so the evidence is
--       real even though the measurement is missing. Note that the client's
--       own external-adopt path does NOT produce mode='external' (addToHistory
--       stamps currentMode(), which never returns it), so this clause cannot
--       catch a browser-side dispatch by accident.
--
-- Everything else becomes 'served'. That includes the 1,160 rows measured
-- under 25%: it would be tempting to call those 'skipped', but nobody recorded
-- a skip for them — the listener may have closed the tab, taken a call, or let
-- a Connect handoff carry playback elsewhere. 'skipped' is a verdict and this
-- migration has no standing to reach one. 'served' says what is actually
-- known, and played_pct keeps the detail for anything that wants finer grain.
-- `mode IS NOT DISTINCT FROM` and not `=`. mode is NULL on every row written
-- before the column existed — 417 of them here — and `NULL = 'external'` is
-- NULL, not false. `NOT NULL` is also NULL, so those rows failed the WHERE and
-- were silently left claiming a listen: the first run of this migration updated
-- 2,650 rows where the preview said 3,026, and the 376 it skipped were exactly
-- the unmeasured legacy rows this exists to catch. Null-safe equality is the
-- fix; the lesson is that a NOT over a nullable comparison is not a negation.
UPDATE user_history
   SET status = 'served'
 WHERE status = 'listened'
   AND NOT (played_pct IS NOT NULL AND played_pct >= 80)
   AND NOT (played_pct IS NULL AND mode IS NOT DISTINCT FROM 'external');

COMMIT;

-- Expected on the 2026-08-18 snapshot: 3,755 'listened' → 730 keep it
-- (494 measured >= 80%, 236 external), 3,026 become 'served'.
--
-- One transient: a browser holding a cached copy of the old app.js keeps
-- POSTing 'listened' at dispatch, and rank 3 will overwrite a 'served' the
-- moment it does. /js/*.js is served under a content hash (see the ?v= rewrite
-- in server.py), so this resolves on each client's next page load rather than
-- needing a flag day.
