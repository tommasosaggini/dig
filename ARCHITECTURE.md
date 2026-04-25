# DIG — Architecture

## What is DIG?

DIG is a music discovery tool. Not a recommendation engine, not a radio, not a playlist generator — a tool for genuine discovery.

Most listening apps narrow over time. They learn your taste and feed it back to you, slightly remixed. DIG does the opposite: by default, it pushes outward. It surfaces local icons and regional legends, artists that a handful of obsessives have been playing on repeat for two years and nobody else has heard of, long-out-of-print recordings from scenes that never made it to global streaming. The goal is to lead people on real journeys — across eras, continents, and scenes they didn't know existed.

Music resists tidy categories. A track carries a region, a genre, a decade — but also a vibe, a tempo, a texture, a feeling, an energy. These qualities connect music in ways that geography or genre alone can't. That's why every track DIG surfaces gets AI-labelled: mood, energy, feel, instrumentality, use-case. These labels are the foundation for something richer — the ability to notice that a 1990s New York rap record and a 1970s Hong Kong cantopop ballad share something real in feel and tempo, and to use that shared quality to take someone somewhere unexpected.

DIG has four player modes:

- **Discovery mode** (default): pure breadth and range. No knowledge of you, no feedback loop — just an honest attempt to fairly represent the full range of music that exists. Every region, genre, and decade gets a shot. Miley Cyrus and Chaweewan Damnern sit in the same pool.
- **Tailored mode**: aware of your listening history. It gently weights toward patterns in what you've already loved, but still prioritises genuine discovery. The intent is not to trap you in a bubble but to use what it knows as a compass — pointing you deeper into territory you might actually care about.
- **AI Mix mode**: a server-side recommender that picks from coverage gaps in your personal exposure — regions/genres/cells you haven't been served from yet — biased toward fresh artists. No taste anchoring; pure systematic exploration of unseen territory.
- **Journey mode**: seed-anchored sequential play. You pick a track and the system unfolds an infinite arc from it: close → expand → stretch → loop. Each next track is chosen to share something meaningful with what came before — region, era, vibe, scene lineage — without simply repeating it.

Across all modes, **a track you've already encountered is never proposed again** (clean-pool guarantee, see below). Saves and skips don't influence Discovery mode at all — only Tailored and AI Mix's scoring layers consult them.

Beyond the player, DIG is also a personal **music ledger** — a permanent, portable record of everything you've encountered, liked, or passed on. Something closer to a listening diary than a playlist.

---

## Repository layout

```
dig/
├── server.py               Web server + Spotify OAuth + API endpoints (entry point)
├── dig_cron.sh             Cron orchestrator — runs every 3 hours
├── docker-compose.yml      Production deployment (Traefik + dig container)
├── Dockerfile              Python 3.12 image; bind-mounts /srv/dig/app at runtime
│
├── web/                    Static frontend
│   ├── app.html            Main SPA (self-contained — no build step)
│   ├── map.html            Genre/artist map visualisation
│   ├── bubbles.html        Experimental pool visualisation
│   ├── noise.html          Experimental noise visualisation
│   ├── privacy.html        Privacy policy page
│   ├── manifest.json       PWA manifest
│   ├── apple-touch-icon.png, favicon.svg, icon-{192,512}.png
│   └── (no build tooling — pure HTML/JS/CSS)
│
├── lib/                    Shared Python modules (imported by pipeline + scripts)
│   ├── db.py               PostgreSQL connection helper (get_conn, fetchall, execute)
│   ├── discovery_lock.py   load_discovery(user_id) returns user-scoped clean pool
│   ├── artist_db.py        Artist registry — register_tracks(), get_stats()
│   ├── search_history.py   Search dedup — load(), record(), freshness()
│   ├── track_filter.py     Quality filter — wellness/factory/SEO/CJK rejection
│   ├── api_budget.py       Spotify API call budget tracker (JSON file, ephemeral)
│   ├── pool_search.py      query_to_track() — resolve weighted dim queries → real tracks
│   ├── ai_recommend.py     ai_recommend_v2 + journey_recommend (Claude → pool queries)
│   ├── explore.py          coverage_explore() — server-side AI Mix picker
│   ├── cell_accounting.py  Per-cell budgets and ingest accounting
│   └── genres.py           Genre vocabulary helpers
│
├── pipeline/               Cron scripts
│   ├── discover.py         Spotify catalog-guided discovery (every 3 h)
│   ├── discover_artists.py Artist-graph crawler (related artists, 2-hop)
│   ├── discover_youtube.py YouTube channel mining (curated label whitelist)
│   ├── label_discovery.py  AI labelling via Claude Haiku (mood/energy/feel/etc.)
│   ├── analyze_pool.py     Gap analyser — generates next-run priorities via Claude
│   └── deep_crawl.py       Deeper artist/compilation/related crawler (8×/day)
│
├── scripts/                One-time and utility scripts
│   ├── create_schema.sql   PostgreSQL DDL (run once)
│   ├── migrate_*.sql       Schema migrations (session, played_pct, mode, …)
│   ├── audit_pool.py, audit_genres.py, audit_regions.py    Pool audits
│   ├── backfill_*.py       Region/decade/genre backfills
│   ├── compute_quality.py  Compute tracks.quality_score from engagement signals
│   ├── purge_junk.py       Bulk cleanup using current track_filter rules
│   ├── import_likes.py     Spotify Liked-Songs / playlist import → user_ledger
│   ├── catalog.py, bootstrap_genres.py, fix_genres*.py
│   ├── cleanup_polluted_genres.py, relabel_regions.py
│   ├── genre_embeddings.py / track_embeddings.py / build_map.py / export_data.py
│   ├── radar.py            Legacy CLI discovery tool (pre-web)
│   └── spotify_import.py   Import Spotify saved tracks/playlists
│
├── logs/                   Cron + deep-crawl logs (production only)
└── backups/                Timestamped JSON backups (gitignored)
```

---

## Data model (PostgreSQL)

Database `dig` lives in the same Postgres container that serves trustbuild-platform; dig connects via `db:5432` over the shared Docker network. All tables below.

### Discovery pool

**`tracks`** — the main pool of discovered music. Every track that any pipeline script finds ends up here.

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | Spotify ID or `yt:VIDEO_ID` |
| `name`, `artist`, `album` | TEXT | |
| `artist_ids` | TEXT[] | Spotify artist IDs |
| `popularity` | INTEGER | **Dead for Spotify rows (always 0)**; used as YouTube view-count for YouTube rows. See "Popularity is unavailable" below. |
| `source` | TEXT | `spotify` or `youtube` |
| `region` | TEXT | Pipeline-tagged macro region (e.g. "Japan", "West Africa") |
| `origin_region` | TEXT | MusicBrainz-derived country (e.g. "Nigeria", "Madagascar"); populated on ~45% of tracks. Country-level granularity that overrides the macro `region` where present. |
| `decade`, `year` | TEXT | Release period |
| `query` | TEXT | The search query / phase that found this track |
| `genres` | TEXT[] | Genre tags (multi-genre per track) |
| `label_energy/mood/texture/feel/use_case` | TEXT | AI-generated semantic labels |
| `quality_score` | REAL | Derived from collective save/skip ratios across users (where available) |
| `quality_plays` | INTEGER | Count of plays informing `quality_score` |
| `added_at` | TIMESTAMPTZ | |

**`artists`** — every artist seen across all sources.

| Column | Type | Notes |
|---|---|---|
| `slug` | TEXT PK | Normalised lowercase artist name |
| `name` | TEXT | Display name |
| `regions`, `genres`, `decades`, `sources` | TEXT[] | Aggregated from all their tracks |
| `track_count` | INTEGER | |
| `track_refs` | JSONB | `[{id, name}]` — up to 50 lightweight back-refs |
| `first_seen`, `last_seen` | TIMESTAMPTZ | |

### Catalog / exploration grid

**`genres`** — the genre vocabulary DIG knows about. Currently ~2,000 entries, seeded from Wikipedia + musicgenreslist.com, expanded by Claude's `analyze_pool` and `discover.py` runs, plus periodic curated additions for under-covered scenes (kwela, marabi, salegy, marrabenta, plena, garífuna punta, …).

**`catalog_cells`** — every active `(region × genre × decade)` combination. **Emergent**, not pre-created: a cell exists only when at least one ingested track has matched its coordinates, or when an explicit search has tried it. ~6,600 cells today (vs the ~700,000 the old Cartesian seeding produced — 99.8% of which were dead). Each cell records `explored` (search attempts), `fetched` (cumulative tracks landed), and `last_scanned`.

**`catalog_scan_queue`** — the prioritised order in which cells should be explored next. Rebuilt by `analyze_pool.py` after each cron run.

**`catalog_meta`** — key/value store for pipeline state: `discovery_priorities` (Claude's gap-analysis output for the next run), `artist_crawl_state`, and catalog-level bookkeeping.

### Users

**`users`** — one row per Spotify user who has authenticated.

**`user_tokens`** — Spotify OAuth token (JSONB), refreshed automatically by the server.

**`user_history`** — every track a user has encountered in the player.

| Column | Notes |
|---|---|
| `track_id`, `track_name`, `artist`, `region` | Denormalised so history survives track deletion |
| `status` | `listened` / `saved` / `skipped` / `disliked` |
| `listened_at` | Epoch ms (Date.now()) |
| `played_pct` | 0–100, fraction of the track actually played at the moment status was set |
| `mode` | `discovery` / `tailored` / `aimix` / `journey` — which player mode the track was served in |

**`user_ledger`** — the user's permanent record: `known`, `liked`, `disliked` tracks. The core feature for the listening diary.

**`user_session`** — per-user cross-device session state (current track, mode, dIdx, journey seed).

**`user_ai_queries`** — log of Claude prompts/strategies generated for this user (for debugging and lens-history).

### Infrastructure

**`search_queries`** — records every Spotify search query across cron runs (`query_key = "query|MARKET"`). Prevents re-searching exhausted cells.

---

## Popularity is unavailable

In November 2024 Spotify removed `popularity`, `audio-features`, batch `/tracks?ids=`, `/recommendations`, and related-artists from the **Development-Mode** API surface. DIG runs in Development Mode (no commercial-quota application), so:

- Every Spotify track lands with `popularity = 0`. The column is preserved in the schema but is not a usable signal.
- DIG never ranks or filters by popularity for Spotify rows. There's no choice in this — Spotify won't tell us.
- For YouTube rows, `popularity` is repurposed as raw view-count (a different scale entirely). YouTube discovery uses a curated channel whitelist instead.

This restriction lines up with DIG's design intent: blind picking from a clean pool, no preferential treatment for famous tracks. The constraint is enforced by Spotify and confirmed by principle.

---

## Discovery pipeline

The 3-hour cron runs `dig_cron.sh` which executes:

```
dig_cron.sh
│
├── discover_youtube.py          YouTube mining (whitelisted curated labels only)
├── discover_youtube.py --merge  Merge YouTube results into tracks table
├── discover.py                  Spotify discovery (Phase 0 → 2 below)
├── discover_artists.py          Spotify: artist-graph + Claude similar-artist suggestions
├── label_discovery.py           Claude Haiku: labels unlabelled tracks
└── analyze_pool.py              Claude: writes next-run priorities to catalog_meta
```

A separate cron entry runs `pipeline/deep_crawl.py` 8 times per day (offset 1.5 h from the main run) for collaboration parsing, compilation mining, taxonomy expansion, and related-artist hopping. Both crons live in `/etc/cron.d/dig` on the host and execute via `docker exec dig …`.

### How `discover.py` works

1. **Phase 0 — AI strategies**: reads `discovery_priorities` from `catalog_meta` (written by the previous run's `analyze_pool.py`). Each strategy is a `(genre, market[s], reason, family)` tuple aimed at a specific gap. Phase 0 enforces geographic balance — at most 2 Anglo-only strategies per batch, ≥4 non-Western, ≥1 zero-coverage country, ≥2 thin (<25-track) countries — and prefers native-language genre names (salegy, marrabenta, mor lam, muwashshah, rebetiko…).
2. **Phase 1 — Catalog-guided exploration**: picks unexplored / under-explored cells from `catalog_cells`. Per-region cap of 3 per run keeps any one region from monopolising a single batch.
3. **Phase 1.5 — Thin region boost**: regions with < 50 tracks get extra random-genre searches.
4. **Phase 2 — Serendipity**: a small slice of pure random region/genre searches.
5. **Genre expansion**: asks Claude for genres missing from the current pool, saves them to the `genres` table. Cells are NOT pre-created — they emerge when tracks actually land.

Every ingested track passes `is_trash()` from [`lib/track_filter.py`](lib/track_filter.py) which receives `(name, artist, album)` and rejects: cover/compilation/medley junk; wellness/sleep/yoga/spa/meditation factories; SEO-stuffed "Whispers of Ambient Indie Nights"-style album titles; CJK wellness compounds (微笑旋律音樂, 疗愈音律, 輕音樂…); multilingual sleep idioms (bien dormir, schlaf gut, babyschlaf…); stand-up comedy albums; nature field-recording titles; numbered-variant factory ladders ("Synthpop Tokyo Girl III/XI/XIX"); and stock artist naming patterns (Genre + Beat/Station/International, comma-glued descriptor piles).

### How `discover_artists.py` works

1. **Phase 1 — Seed resolution**: resolves a curated list of hand-picked seed artists (regional icons and legends) to Spotify IDs.
2. **Phase 2 — Seed harvest**: fetches tracks for each seed artist via the search endpoint.
3. **Phase 3 — Collaborators**: for each seed, finds artists who appear on the same tracks and harvests their discography.
4. **Phase 4 — AI suggestions**: samples up to 8 seed artists and asks Claude Haiku for 2-3 genuinely similar acts per seed — obscure artists from the same scene that the Spotify graph would never surface. Capped to keep costs minimal.

### How `deep_crawl.py` works

A wider set of expansion strategies, run 8×/day:

1. **Collaboration parsing** — scans existing tracks for featured-artist patterns and harvests the lesser-known collaborators.
2. **Compilation mining** — searches Spotify for region/genre compilations and harvests their artist lists.
3. **Genre-tag walking** — follows `tracks.genres` array values to discover adjacent acts.
4. **Related-artist hopping** — 2-hop walks through Spotify's artist graph (where still allowed).
5. **Claude taxonomy expansion** — asks Claude for genre families adjacent to current coverage, surfacing new search terms.
6. **Track ingestion / playlist mining / new release scanning** — three more Spotify search strategies, budget permitting.

All writes go through `locked_update()` in `lib/discovery_lock.py`, which acquires a PostgreSQL advisory lock to prevent concurrent cron runs from corrupting the pool.

### The self-improvement loop

```
discover.py + deep_crawl.py
  │  search Spotify → write tracks → update catalog_cells.{explored,fetched,last_scanned}
  │
  ↓
label_discovery.py → adds mood/energy/feel labels to unlabelled tracks (Claude Haiku)
  │
  ↓
analyze_pool.py
  │  reads tracks (origin_region distribution, family balance, decade spread)
  │  reads catalog_cells (what's tried? what's still untouched?)
  │  asks Claude for 10 strategies that close BOTH genre-family AND geographic gaps
  │  Claude must allocate at most 2 Anglo-only, ≥4 non-Western, ≥1 zero-coverage,
  │  ≥2 thin (<25 track) countries — uses native genre names where applicable.
  ↓
catalog_meta['discovery_priorities'] → read by NEXT discover.py run (Phase 0)
```

Every run leaves both the pool and the map better than it found them. The emergent cells model means dead `(region × genre × decade)` triples are never stored — only combinations that actually produced tracks (or were explicitly tried-and-empty) take up rows.

---

## Player picker logic (frontend / server, by mode)

All four modes operate on a **clean pool**: `/discovery` filters out any track in the user's `user_history` AND any track whose `(artist, name)` key matches a `user_ledger` entry, before sending the catalog to the client. The client never sees tracks the user has been served before. There's currently no toggle to re-enable replay.

### Discovery mode (default — taste-agnostic)

`web/app.html` shuffles the clean pool with `diversityShuffle()` — a rotating-lens algorithm cycling through `region → genre → vibe → era → artist → equal → wander` every 8 tracks. Anti-clustering only; no scoring against history.

When picking the next track, Discovery scans up to 40 forward candidates and selects the one with the lowest **artist cooldown penalty** (linear decay: 3 at gap 0, 0 at gap 80). This stops the same artist from clustering in a session, while still letting deep discographies surface across a long listen.

### Tailored mode

`pickNextTrack()` in `web/app.html` scores every clean-pool candidate against the user's taste profile (`/api/taste-profile`):

- Genre match (×4 weight) + lateral genre neighbours via `GENRE_MAP`
- Mood match (×2) + region match (×0.5)
- Energy diversity bonus (under-represented in last 10 picks → bonus, over-represented → penalty)
- Skip-aware negative weighting: instant-skip plays (played_pct < 10) feed into the profile as -0.3 weight
- Artist cooldown penalty (linear decay, see above)
- Genre-alignment gate, recent region/genre dock, missing-label dock

The picker scores up to 400 candidates and picks the highest. No anchor rotation.

### AI Mix mode

`/api/ai-recommend` calls `coverage_explore()` in `lib/explore.py`. The picker:

1. Excludes all heard track IDs and heard artists (server-side hard filter, plus frontend `recent_ids` to close the DB-sync race).
2. Buckets remaining tracks by `(region, genre, decade)` cell.
3. Prefers cells the user has zero exposure to — pure systematic coverage of unseen territory.
4. Within a cell, prefers artists never heard at all, and uses inverse-frequency weighting so artists with deep pool concentration don't dominate by volume.
5. Returns N recommendations spread across cells.

No taste anchoring, no Claude call (cheap and fast). The trade-off: AI Mix surfaces obscure thin-cell content, which sometimes has lower listen-quality than Tailored — the design intent is exploration, not engagement maximisation.

### Journey mode

Seeded by a track. Each block calls `journey_recommend()` in `lib/ai_recommend.py`. Claude is asked to output **dimensional pool queries** (region/genre/decade/vibe + weights + arc), not artist+track names. Each query is then resolved against the live `tracks` table by `query_to_track()` in `lib/pool_search.py`, with `exclude_ids = heard ∪ previous_journey ∪ seed`.

This means Claude never names tracks from its training data — it only describes the *shape* of what should play next, and our pool finds a real track that fits. Eliminates Claude hallucinations and guarantees the next pick is in the clean pool.

Block structure: ~8 tracks per block, with arcs `close × 3-4` → `expand × 2-3` → `stretch × 0-2`.

---

## Frontend

`web/app.html` is a self-contained SPA (no build step). It fetches:

- `/discovery` → clean pool for the current user (heard tracks + ledger filtered)
- `/genre_map.json`, `/track_map.json` → pre-computed 2D embedding coordinates
- `/data.json` → legacy export for the map/stats views
- `/ledger`, `/history`, `/me`, `/token`, `/save`, `/listened`, `/unsave` → user state
- `/api/taste-profile`, `/api/ai-recommend`, `/api/journey`, `/api/session` → mode-specific helpers
- `/api/play`, `/api/health`, `/api/client-log` → playback + diagnostics

Mode is recorded per play (in `user_history.mode`) so any future analysis can tell whether a save came in Discovery, Tailored, AI Mix, or Journey.

---

## Production

| | |
|---|---|
| **Server** | Hetzner VPS (`91.99.188.232`, hostname `trustbuild-ai`), Ubuntu |
| **URL** | ohdig.com |
| **Process** | Docker container `dig` (image `dig:local`) on `docker-compose` |
| **DB** | PostgreSQL 16 (pgvector image) — same container that serves trustbuild-platform; dig connects via `db:5432/dig` over the shared Docker network |
| **Cron** | Host-level `/etc/cron.d/dig` runs `docker exec dig …`. Main pipeline `0 */3 * * *`; deep_crawl `30 1,4,7,10,13,16,19,22 * * *` |
| **Python** | 3.12 (in container), deps via `requirements.txt` baked at image build |
| **Reverse proxy** | Traefik v3.1 (`coolify-proxy`) — TLS via Let's Encrypt, gzip middleware, HTTPS redirect |
| **App source** | Bind-mounted from `/srv/dig/app/` on host → `/app/` in container (live-edit, no rebuild for Python changes — restart container to pick up) |
| **Logs** | `/srv/dig/logs/cron.log`, `/srv/dig/logs/deep_crawl.log`; container stdout via `docker logs dig` |

---

## Current scale (snapshot)

- **~30,000 tracks** across **91 distinct macro regions**, **~38,000 artists**
- Country-level depth via `origin_region`: **132 countries** populated (44.6% of tracks)
- **~2,000 genres** in vocabulary
- **~6,600 active catalog cells** (emergent — fraction of the old 170k Cartesian product)
- AI labelling on >95% of tracks (mood, energy, feel, texture, use-case)
- Daily ingest: ~800–1,200 new tracks across all phases

---

## Known gaps and next steps

**Modern-decade skew.** ~63% of the pool is 2010s–2020s; pre-1990 is only ~11%. Spotify's catalog is heavily modern and the pipeline has been correcting slowly. `analyze_pool.py` was recently rewritten to push older decades for non-Anglo scenes (rebetiko 1930s, mor lam 1970s, salegy 1960s, etc.) — early signs of improvement.

**Anglo-share creep.** Recent ingest has been ~55% USA/UK/Anglo, partly because catalog cells in popular Anglo genres (trap, bluegrass, bebop, country) consistently produce tracks while many non-Western native-language searches return less. The new geographic-balance rules in `analyze_pool.py` (≤2 Anglo-only strategies, ≥4 non-Western, ≥1 zero-coverage, ≥2 thin) are designed to counter-weight Phase 1's natural Anglo bias.

**Genuinely thin/zero-coverage countries.** Mozambique, Madagascar, Cape Verde, Syria, Bhutan, Tibet, Paraguay, Honduras, El Salvador had **zero** `origin_region` matches at last audit. The recent vocab expansion added native-language anchors (marrabenta, salegy, morna, muwashshah, guarania, polka paraguaya, …) so the next analyse_pool runs can target them directly.

**Artist genre coverage (~38% of artists have no genre).** Genre is propagated from search query → track → artist for `discover.py` Phases 1 and 1.5, but still missing for the YouTube pipeline and parts of the artist-graph crawler. A backfill via AI inference on artist name + region + source would help.

**Ledger is a flat list.** The user ledger is currently a simple known/liked/disliked list. Next iteration: filterable and groupable by genre, region, decade, vibe. Schema already supports it.

**Genre depth vs. cultural tourism.** "Folk music from X" on YouTube yields very shallow results — re-recorded traditional music, tourist-facing content, low-quality uploads. Current defences:
- `track_filter.py`: rejects tourist-folk bait, wellness factories, SEO compilation albums, factory artist names (Genre + Beat / International / Station, comma-glued descriptor piles, CJK wellness compounds, multilingual sleep idioms)
- YouTube channel whitelist (curated labels: Sahel Sounds, Nyege Nyege Tapes, Analog Africa, etc.)
- The Spotify-first approach (professional releases are filtered by Spotify's own curation)

The goal is always depth over breadth within each genre — a great obscure 1975 highlife recording beats ten YouTube uploads of "Traditional African Music".

**`data.json` and embedding maps are static exports.** Generated by `scripts/export_data.py`, `scripts/genre_embeddings.py`, `scripts/track_embeddings.py`. Need manual regeneration after major discovery runs. Should be scheduled.

**No external ingestion beyond Spotify Liked.** `scripts/import_likes.py` covers Spotify Liked Songs and playlists. Last.fm scrobble import, CSV imports from other services, and YouTube playlist import would each be small additions on top of the current schema.

**No re-listen toggle yet.** Heard tracks are silently filtered out forever. A user toggle to re-enable replays for a subset (e.g. only Saved tracks) is on the roadmap but not built.

---

## Principles

1. **Breadth first, then depth.** Every region and genre deserves at least a foothold before any single one gets deep coverage.
2. **No popularity bias — by design AND by force.** DIG would never rank by popularity anyway; Spotify Development Mode no longer exposes the field, so the policy is enforced by API. `popularity = 0` is the only value Spotify gives us.
3. **Self-correcting.** Every run leaves an audit trail of what it found and what it missed, so the next run can do better. `analyze_pool.py` closes the loop every 3 hours.
4. **Depth over tourism.** An obscure pressing matters more than a YouTube compilation. Curated sources (record labels, specialist channels) are trusted over generic searches.
5. **A heard track never returns.** The clean-pool guarantee is strict: anything in `user_history` or `user_ledger` is excluded from `/discovery`, from every picker, and from every Claude rec. Discovery is for things you've never met.
6. **AI describes shape, not tracks.** Claude is never asked to name an artist or song from its training data. It writes dimensional queries (region/genre/decade/vibe + weights), and DIG's pool resolver finds the actual track. This kills hallucinations and famous-name bias.
7. **The ledger is permanent.** A user's listening history should outlast any particular music service. The data is theirs.
