# Dig — Guest Mode (Bandcamp-first, Spotify optional)

**Goal:** turn Dig from "25 Spotify-Premium friends" into "anyone with a browser,"
by making Bandcamp the default playable layer (no login / no Premium / no
allowlist) and Spotify an optional upgrade for those who have it.

**Why now:** Spotify playback requires BOTH a slot on the permanent 25-user
dev-mode allowlist AND Spotify Premium — prohibitive. Bandcamp `<audio>`
playback needs none of that. (See memory: project_dig_access, project_dig_bandcamp.)

---

## Hard constraint: do NOT get rate-limited / banned by Bandcamp

Bandcamp has no official API; we use its mobile + discover endpoints. Bans come
from request *rate* and bursts, not from total track count. Key facts:

- `discover/3/get_web` is **bulk**: 1 request → 48 releases with the featured
  track's stream data embedded. ~5,000 tracks ≈ ~105 requests total.
- The per-play `resolve` call is organic (one per track a real user plays).
- All calls share the prod server's single IP, so discover (ingest) + resolve
  (every user's playback) add up on one address.

**Safety rules (Phase 0 enforces these):**
1. Heavy, *jittered* pacing on ingest (4–8 s between discover calls). Never burst.
2. Tight per-run + per-day caps; trickle over days via a low-frequency cron.
3. Circuit-breaker: any 429 / 403 / 503 / "Client Challenge" → record a cooldown,
   stop ingestion immediately, back off for hours (mirrors the Spotify gate).
4. Realistic, stable headers; dedup so we never re-fetch.
5. Light pace + short result cache on the resolve path so popularity doesn't
   spike our request rate.

---

## Phases

### Phase 0 — Bandcamp request safety (FIRST, before any more ingest)
- `lib/bandcamp.py`: shared throttle (jitter) + cooldown circuit-breaker;
  `BandcampBlocked` raised on any pushback signal.
- `ingest_bandcamp.py`: conservative defaults, respects cooldown, aborts on block.
- Gentle cron: a handful of discover calls every few hours, cooldown-aware.

### Phase 1 — Grow the Bandcamp pool carefully
- Trickle genre × page × sort sweeps over days, monitoring for any pushback.
- Target a few thousand well-spread tracks (genre/region diversity).

### Phase 2 — Guest sessions
- Let anonymous visitors into the app without Spotify login.
- Guest identity via a signed cookie; history/taste tracked per guest id.
- Keep the same picker/diversity machinery.

### Phase 3 — Source-aware playback gating
- Determine playability: logged-in + Premium + allowlisted → can play Spotify.
- Guests / free / non-allowlisted → pool filtered to Bandcamp-only.
- Premium users get the full pool (Spotify + Bandcamp interleaved).

### Phase 4 — Reframe onboarding
- Welcome page: "Start listening" (guest) + "Connect Spotify for more" (upsell),
  instead of a hard invite gate. Keep the waitlist only for the Spotify layer.

### Phase 5 — Playback robustness + verify
- On-device Bandcamp playback test (still pending).
- Resolve caching/backoff; graceful skip on expired/blocked resolves.

---

## Status
- 2026-06-06: plan drafted. Starting Phase 0 (request safety) immediately.
