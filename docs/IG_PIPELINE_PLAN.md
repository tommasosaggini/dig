# Dig → Instagram curation pipeline + admin dashboard

**Status:** Phases 0–3 BUILT + verified (2026-06-28); Phase 4 (IG publish) needs
account setup + Meta review; Phase 5 (SoundCloud) not started · **Owner:** Tommaso
(sole admin) · **Setup/ops:** see `docs/IG_SETUP.md`

## 8. What's built (2026-06-28)

Phases 0–3 + the candidate proposer are implemented and verified (render path
tested end-to-end → real 1080×1080 feed.mp4 + 1080×1920 story.mp4 with a 30s clip).

- **DB:** `scripts/migrate_ig_post_queue.sql` + `lib/ig_queue.py::ensure_ig_schema`
  (self-creates at server startup).
- **Logic:** `lib/ig_queue.py` (queue CRUD, candidate selection, cadence
  scheduling, state machine), `lib/ig_audio.py` (Bandcamp→yt-dlp→duration probe),
  `lib/ig_caption.py` (template + optional Haiku polish).
- **Server:** `/admin/ig/*` GET+POST endpoints in `server.py` (ADMIN_UID-gated,
  range-capable media streaming for the waveform/preview).
- **Dashboard:** `web/admin.html` — queue, drag-reorder, **waveform 30s
  picker** (Web Audio decode → canvas peaks → draggable window + audition),
  caption editor, feed/story toggles, schedule, render-preview.
- **Pipeline:** `pipeline/ig_propose.py`, `ig_audio_resolver.py`, `ig_render.py`
  (Pillow card + ffmpeg), `ig_publish.py` (dry-run-safe). Driver: `ig_cron.sh`.
- **Deps:** `Pillow` + `yt-dlp` added to `requirements.txt`; **ffmpeg** is a
  system binary to install on the host/container. `media/` gitignored.
- **Tests:** `tests/test_ig_pipeline.py` (scheduling + caption logic, no DB).

Remaining (need Tommaso): install ffmpeg on prod + turn on cron; IG Business
account + Meta app review + public media route for Phase 4; SoundCloud key for
Phase 5. All in `docs/IG_SETUP.md`.

---

**Original plan / not started** · **Date:** 2026-06-28

## 1. What this is

A semi-automatic pipeline that turns tracks Tommaso loves (seeded from his Spotify
**Liked Songs**) into Instagram posts: a 30-second clip over a clean visual card, published
to **feed (as a Reel)** and to **Stories**, one a day at 18:00 UTC. The bio links back
to Dig (the tool).

**The only human step is curation.** Everything else — audio acquisition, card rendering,
clip encoding, scheduling, publishing — runs unattended. Tommaso works entirely inside one
admin-only dashboard where he:

- sees the **queue** of upcoming posts,
- **scrubs a waveform to pick the exact 30 seconds** of each track,
- **reorders** the queue,
- **adds** tracks to it,
- **edits** the auto-generated **caption**,
- sets / edits **publishing times**,
- **approves** (the single gate) — or skips.

### Why this exists (strategy, not just content)

The loop: known-beautiful likes → a post a day on IG → bio waitlist → fill the 25 Spotify
dev-mode slots with *engaged* users → that engagement is the evidence for a Spotify
**extended-quota** application → uncap. The IG account is honest traction, not a growth hack.
See `[[project_music_discovery]]`, `[[project_dig_access]]`, `[[reference_dig_spotify_quota]]`.

**Critical decoupling:** the IG clip pipeline does **not** touch Spotify. In-app playback uses
the capped Spotify Web Playback SDK; IG clips come from downloadable audio (Bandcamp / manual
upload). So the IG funnel keeps working regardless of the 25-user wall, and growing the
account is what eventually removes the wall.

## 2. Hard constraints (verified 2026-06)

- **Audio source.** Spotify stores no audio in Dig (`scripts/import_likes.py:103`) and its
  `preview_url` is a fixed 30s + increasingly null — unusable for a "pick your own window"
  feature. Full audio comes from **yt-dlp** (universal extractor: YouTube + ~1800 sites,
  `--extract-audio`) as the primary source — YouTube has essentially everything — with
  **Bandcamp** (`lib/bandcamp.py:168`, full MP3-128) preferred when a track exists there
  because it's the cleanest. Manual upload remains a last-resort fallback. **SoundCloud API
  ToS forbids downloading/storing audio** — SoundCloud is a *discovery/streaming* source for
  the tool only, never a clip source.
  - *Note:* yt-dlp pulling from YouTube is against YouTube ToS, and re-uploading copyrighted
    audio carries the small-account risk in the copyright bullet below. Both are accepted at
    the per-track approval step — this is personal curation, decided case by case.
- **Instagram publishing.** Requires an IG **Business** account (Creator is *not* supported
  for API publishing) linked to a Facebook Page. Permissions `instagram_business_basic` +
  `instagram_business_content_publish` need Meta **app review** (~2–4 weeks, screencast
  required). Reels eligibility = **5–90s, 9:16** (our 30s is fine). Container→poll→publish
  model. 100 API posts / 24h (we use ~1). → **Build everything dry-run first; apply for the
  permission in parallel.**
- **Copyright.** Re-uploading 30s of major-label tracks on a small account risks muting /
  takedown more than punishment. Leaning toward Bandcamp/indie gems is cleaner *and* on-ethos.
  The per-track approval step is where Tommaso accepts/declines that risk.

## 3. Architecture (fits existing Dig patterns)

Single-file stdlib `http.server` + Postgres (psycopg2) + vanilla-JS pages. No framework.
Reference: `ARCHITECTURE.md`. New parts mirror the existing **waitlist-admin** precedent
exactly.

### 3.1 Admin gating

Follow `waitlist-admin.html` (`server.py:1687`, `1843`): the **page ships openly** as a static
file; **every data endpoint** checks `if user_id != ADMIN_UID: 403`. No new auth system.
New page: `web/admin.html`. New endpoints under `/admin/ig/*`.

### 3.2 New table — `scripts/migrate_ig_post_queue.sql`

Idempotent (`CREATE TABLE IF NOT EXISTS`) + `GRANT ... TO dig;`, applied via
`psql $DATABASE_URL -f` like `migrate_session.sql`. Keyed by the heterogeneous pool id string.

```sql
CREATE TABLE IF NOT EXISTS ig_post_queue (
    id              BIGSERIAL PRIMARY KEY,
    track_id        TEXT,                 -- tracks.id: spotify id | bc:band:track | yt:videoid | manual:<uuid>
    track_name      TEXT NOT NULL,
    artist          TEXT NOT NULL,
    artwork_url     TEXT,                 -- resolved (Bandcamp art) or uploaded
    audio_source    TEXT,                 -- 'bandcamp' | 'upload'
    audio_path      TEXT,                 -- local path to FULL audio once acquired
    audio_duration_ms INT,
    clip_start_ms   INT,                  -- chosen window start; NULL until picked
    clip_duration_ms INT NOT NULL DEFAULT 30000,
    caption         TEXT,                 -- auto-generated, editable
    post_feed       BOOLEAN NOT NULL DEFAULT TRUE,   -- publish as Reel
    post_story      BOOLEAN NOT NULL DEFAULT TRUE,   -- publish to Stories
    queue_order     INT NOT NULL DEFAULT 0,
    scheduled_at    TIMESTAMPTZ,          -- NULL = unscheduled; cron auto-fills next slot on approve
    status          TEXT NOT NULL DEFAULT 'needs_audio',
    ig_media_id     TEXT,                 -- set after publish
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
GRANT SELECT, INSERT, UPDATE, DELETE ON ig_post_queue TO dig;
GRANT USAGE, SELECT ON SEQUENCE ig_post_queue_id_seq TO dig;
```

**Status lifecycle:**
`needs_audio` → `needs_clip` (audio acquired) → `ready` (clip window + caption set, approved)
→ `scheduled` (time assigned) → `publishing` → `published` | `failed` | `skipped`.

Rendered media (cards + mp4s) live on disk under `media/ig/<id>/` (gitignored), not in DB.

### 3.3 Endpoints (all `do_GET`/`do_POST` blocks, all `ADMIN_UID`-gated)

| Method | Path | Purpose |
|---|---|---|
| GET  | `/admin/ig/queue` | full ordered queue (+ status, preview urls) |
| GET  | `/admin/ig/candidates` | likes not yet queued/posted, for the "add" picker |
| POST | `/admin/ig/add` | `{track_id}` → create draft (status `needs_audio`), kick audio resolve |
| POST | `/admin/ig/item/update` | `{id, caption?, clip_start_ms?, scheduled_at?, post_feed?, post_story?}` |
| POST | `/admin/ig/reorder` | `{ordered_ids:[...]}` → rewrite `queue_order` |
| POST | `/admin/ig/item/audio` | multipart upload of full audio for tracks not on Bandcamp |
| POST | `/admin/ig/item/approve` | `{id}` → `ready`, auto-assign next `scheduled_at` if unset |
| POST | `/admin/ig/item/skip` | `{id}` → `skipped` |
| GET  | `/admin/ig/audio/{id}` | stream the full audio file to the browser for the waveform scrubber |
| GET  | `/admin/ig/preview/{id}/{fmt}` | serve rendered card/mp4 (`fmt` = `feed`/`story`) for in-dashboard preview |

### 3.4 The dashboard — `web/admin.html`

Vanilla JS like `waitlist-admin.html`; 403 → "log in as admin". Sections:

1. **Queue list** — drag-to-reorder cards, each showing artwork, track/artist, status badge,
   scheduled time, feed/story toggles, and an **approve / skip** button. Reorder POSTs
   `/admin/ig/reorder`.
2. **Clip picker (the centerpiece).** Open an item → load full audio from `/admin/ig/audio/{id}`,
   decode with Web Audio (`decodeAudioData`), draw a **canvas waveform** (downsample channel
   data to ~1500 peaks). A **draggable 30s-wide window** overlays it; dragging sets
   `clip_start_ms`. A play button auditions *only that window*. Save → `/admin/ig/item/update`.
3. **Caption editor** — textarea pre-filled with the auto-generated caption; edit + save.
4. **Schedule** — per-item datetime; or "auto" (cron drops it in the next free every-2-days slot).
5. **Add** — search `/admin/ig/candidates`, click to enqueue. Also a manual "audio upload"
   affordance for items stuck in `needs_audio`.

## 4. Background jobs

New shell entry (or extend `dig_cron.sh`). Mutations that touch the shared pool use
`lib/discovery_lock.locked_update`; IG-queue work takes its **own advisory lock key**
(e.g. `pg_advisory_xact_lock(87654322)`) so it never contends with discovery.

0. **`candidate_proposer`** (daily). Picks from likes not yet queued/posted, weighted for
   genre spread + recency + not-recently-posted-artist, and inserts them into a **`suggested`**
   lane (status `suggested`) for Tommaso to approve or dismiss in the dashboard. Keeps the feed
   varied without him hunting; he still hand-approves every track. Manual-add coexists.
1. **`audio_resolver`** (frequent). For approved-but-`needs_audio` items: try **Bandcamp**
   first (`artist + name` match → full MP3-128, cleanest); else **yt-dlp** (search
   `ytsearch1:"<artist> <name>"`, `--extract-audio --audio-format mp3`). Save to
   `media/ig/<id>/source.mp3`, store `audio_path` + `audio_duration_ms` + `artwork_url`,
   advance to `needs_clip`. Total failure → leave in `needs_audio` (dashboard prompts manual upload).
2. **`renderer`** (on approve, before publish). With ffmpeg + Pillow:
   - cut `[clip_start_ms, +30s]` from `source.mp3`,
   - Pillow renders the **card**: artwork + gradient + track/artist + Dig mark, at **1080×1080**
     (feed) and **1080×1920** (story/reel),
   - ffmpeg muxes still image + clip → `feed.mp4` (square/Reel) and `story.mp4` (9:16).
   Outputs to `media/ig/<id>/`. Dashboard previews them. **This is the dry-run milestone** — real
   posts visible end-to-end *before* any IG wiring.
3. **`publisher`** (frequent). For `ready`+`scheduled` items whose `scheduled_at` is due: IG Graph
   container→poll→publish for `feed.mp4` (Reel) and, if `post_story`, a STORIES container. Store
   `ig_media_id`; on error set `failed` + `error`.

### Captions

Template first (`"<track> — <artist>"` + a short line + standing hashtags + "tool in bio"),
optionally enriched by a one-shot LLM call using the existing `ANTHROPIC_API_KEY` /
`OPENAI_API_KEY` in `.env`. Always editable in the dashboard before publish.
(See `[[project_dig_labeling_model]]` for model choices already in use.)

## 5. New dependencies & secrets

- **System:** `ffmpeg` (clip cut + image→video mux). Add to the Docker image
  (`[[reference_dig_deploy]]`). `yt-dlp` ships as a Python package so no separate system binary,
  but it *requires* ffmpeg (already covered).
- **Python:** `Pillow` (card rendering) + `yt-dlp` (universal audio extractor) → add to
  `requirements.txt`. HTTP reuses `requests`.
- **`.env` additions:** `IG_GRAPH_TOKEN`, `IG_BUSINESS_ACCOUNT_ID`, `IG_APP_SECRET`,
  (later, for the discovery-source workstream) `SOUNDCLOUD_CLIENT_ID`.

## 6. Phasing

- **Phase 0 — skeleton.** Migration + `/admin/ig/queue|add|reorder|update` + dashboard shell
  (list, add, reorder, skip). No audio/render yet. Proves the admin loop.
- **Phase 1 — audio + clip picker.** Multi-source resolver (Bandcamp → yt-dlp → manual upload);
  the waveform 30s scrubber. End state: every queued item has a chosen 30s window.
- **Phase 2 — render (DRY RUN).** Pillow card + ffmpeg mp4, previewed in-dashboard, written to
  `media/ig/`. **No publishing.** ← *the "see real posts from real likes within a day" checkpoint;
  decide here if the cards are beautiful enough to put your name on.*
- **Phase 3 — captions.** Template + optional LLM + editor.
- **Phase 4 — publish.** IG Graph feed Reel + Story + scheduling cron. Gated on Meta app-review
  approval (apply at start of Phase 2 so it lands in time). Set up the IG Business + FB Page now.
- **Phase 5 — SoundCloud as a Dig discovery source (parallel, independent).** Official API
  re-apply (open again as of 2026); resolve + stream **inside the tool only**, never for IG clips.
  Breaks Spotify dependence for non-Spotify users. Likely its own short plan doc.

## 7. Open decisions (need Tommaso)

1. ~~Auto-seed vs manual-add.~~ **DECIDED:** build the `candidate_proposer` cron (suggests from
   likes) *and* keep manual-add. Tommaso approves every track.
2. ~~Bandcamp-miss policy.~~ **DECIDED:** multi-source — Bandcamp → yt-dlp (YouTube etc.) →
   manual upload. Whole likes pool is postable; copyright call made per-track at approval.
3. **IG account setup.** Confirm willingness to run an IG **Business** account + linked FB Page
   (required; Creator won't publish via API). This unblocks Phase 4's review timeline.
4. **Card aesthetic.** Decide the visual template (artwork-forward? typographic? the Dig mark)
   before Phase 2 — it's the thing people actually see.
