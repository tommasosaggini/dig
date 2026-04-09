# DIG — Architecture

## What is DIG?

DIG is a music discovery tool. Not a recommendation engine, not a radio, not a playlist generator — a tool for genuine discovery.

Most listening apps narrow over time. They learn your taste and feed it back to you, slightly remixed. DIG does the opposite: by default, it pushes outward. It surfaces local icons and regional legends, artists that a handful of obsessives have been playing on repeat for two years and nobody else has heard of, long-out-of-print recordings from scenes that never made it to global streaming. The goal is to lead people on real journeys — across eras, continents, and scenes they didn't know existed.

Music resists tidy categories. A track carries a region, a genre, a decade — but also a vibe, a tempo, a texture, a feeling, an energy. These qualities connect music in ways that geography or genre alone can't. That's why every track DIG surfaces gets AI-labelled: mood, energy, feel, instrumentality, use-case. These labels are the foundation for something richer — the ability to notice that a 1990s New York rap record and a 1970s Hong Kong cantopop ballad share something real in feel and tempo, and to use that shared quality to take someone somewhere unexpected.

DIG has two modes:

- **Discovery mode** (default): pure breadth and range. No knowledge of you, no feedback loop — just an honest attempt to fairly represent the full range of music that exists. Every region, genre, and decade gets a shot, regardless of popularity. Miley Cyrus and Chaweewan Damnern sit in the same pool.
- **Tailored mode**: aware of your listening history. It gently weights toward patterns in what you've already loved, but still prioritises genuine discovery. The intent is not to trap you in a bubble but to use what it knows as a compass — pointing you deeper into territory you might actually care about.

Beyond the player, DIG is also a personal **music ledger** — a permanent, portable record of everything you've encountered, liked, or passed on. Something closer to a listening diary than a playlist.

---

## Repository layout

```
dig/
├── server.py               Web server + Spotify OAuth (entry point)
├── dig_cron.sh             Cron orchestrator — runs every 3 hours
│
├── web/                    Static frontend
│   ├── app.html            Main SPA (self-contained)
│   ├── map.html            Genre/artist map visualisation
│   ├── bubbles.html        Experimental pool visualisation
│   ├── noise.html          Experimental noise visualisation
│   ├── favicon.svg
│   ├── icon-192.png
│   ├── icon-512.png
│   └── manifest.json       PWA manifest
│
├── lib/                    Shared Python modules (imported by pipeline + scripts)
│   ├── db.py               PostgreSQL connection helper (get_conn, fetchall, execute)
│   ├── discovery_lock.py   Discovery pool R/W — load_discovery(), locked_update()
│   ├── artist_db.py        Artist registry — register_tracks(), get_stats()
│   ├── search_history.py   Search dedup — load(), record(), freshness()
│   ├── track_filter.py     Track quality filter — rejects covers, compilations, junk
│   └── api_budget.py       Spotify API call budget tracker (JSON file, ephemeral)
│
├── pipeline/               Cron scripts — run in sequence by dig_cron.sh
│   ├── discover.py         Spotify genre/region/decade discovery (main crawler)
│   ├── discover_artists.py Artist-graph crawler (related artists, 2-hop)
│   ├── discover_youtube.py YouTube channel mining (fills Spotify blind spots)
│   ├── label_discovery.py  AI labelling via Claude Haiku (mood, energy, feel, etc.)
│   └── analyze_pool.py     Gap analyser — generates next-run priorities via Claude
│
├── scripts/                One-time and utility scripts (run manually)
│   ├── create_schema.sql   PostgreSQL DDL (run once to create tables)
│   ├── migrate_to_pg.py    One-time migration from JSON files to PostgreSQL
│   ├── catalog.py          Catalog builder — maps (region × genre × decade) cells
│   ├── bootstrap_genres.py Seeds genres table from Wikipedia + musicgenreslist.com
│   ├── genre_embeddings.py Computes genre similarity map (OpenAI embeddings + t-SNE)
│   ├── track_embeddings.py Computes per-track 2D map (genre + labels + region + year)
│   ├── build_map.py        Generates map.html from user ledger + MusicBrainz
│   ├── export_data.py      Exports data.json for frontend consumption
│   ├── backfill_decades.py One-time: fills missing decade/year via Claude
│   ├── spotify_import.py   Import Spotify saved tracks/playlists into ledger
│   └── radar.py            Legacy CLI discovery tool (pre-web)
│
└── backups/                Timestamped JSON backups (gitignored)
```

---

## Data model (PostgreSQL)

Hosted at `localhost/dig` on the production server. All tables below.

### Discovery pool

**`tracks`** — the main pool of discovered music. Every track that any pipeline script finds ends up here.

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | Spotify ID or `yt:VIDEO_ID` |
| `name`, `artist`, `album` | TEXT | |
| `artist_ids` | TEXT[] | Spotify artist IDs |
| `popularity` | INTEGER | 0–100, Spotify metric |
| `source` | TEXT | `spotify` or `youtube` |
| `region` | TEXT | Geographic region (e.g. "Japan", "West Africa") |
| `decade`, `year` | TEXT | Release period |
| `query` | TEXT | The search query that found this track |
| `label_energy/mood/texture/feel/use_case` | TEXT | AI-generated semantic labels |
| `added_at` | TIMESTAMPTZ | |

**`artists`** — every artist seen across all sources. Used to measure breadth and avoid re-crawling.

| Column | Type | Notes |
|---|---|---|
| `slug` | TEXT PK | Normalised lowercase artist name |
| `name` | TEXT | Display name |
| `regions`, `genres`, `decades`, `sources` | TEXT[] | Aggregated from all their tracks |
| `track_count` | INTEGER | |
| `track_refs` | JSONB | `[{id, name}]` — up to 50 lightweight back-refs |
| `first_seen`, `last_seen` | TIMESTAMPTZ | |

### Catalog / exploration grid

**`genres`** — the full genre vocabulary DIG knows about (~417 genres, seeded from Wikipedia + musicgenreslist.com, expanded by Claude every discovery run). When new genres are added here, corresponding `catalog_cells` are immediately created for them across all known regions and decades — so the exploration map grows automatically, not just the vocabulary.

**`catalog_cells`** — every `(region × genre × decade)` combination (~170K rows). Each cell records `explored` (how many times it's been searched), `fetched` (cumulative tracks found), and `last_scanned`. This is DIG's exploration map: cells that have never been searched come first, then those with the fewest attempts, then the oldest. Every discovery run writes back to this table — empty results are marked too, so the system knows "tried this, found nothing" vs "never tried".

**`catalog_scan_queue`** — the prioritised order in which cells should be explored next. Rebuilt by `analyze_pool.py` after each cron run.

**`catalog_meta`** — key/value store for pipeline state: `discovery_priorities` (Claude's gap analysis output for the next run), `artist_crawl_state` (crawler progress), and catalog-level bookkeeping.

### Users

**`users`** — one row per Spotify user who has authenticated.

**`user_tokens`** — Spotify OAuth token (JSONB), refreshed automatically by the server.

**`user_history`** — every track a user has encountered in the player, with timestamp and status.

**`user_ledger`** — the user's permanent record: `known`, `liked`, `disliked` tracks. The core feature for the listening diary.

### Infrastructure

**`search_queries`** — records every Spotify search query across cron runs (`query_key = "query|MARKET"`). Prevents the same cell being searched repeatedly when it's already been exhausted.

---

## Discovery pipeline

The cron job runs every 3 hours and executes these scripts in order:

```
dig_cron.sh
│
├── discover_youtube.py          YouTube mining (no Spotify quota cost)
├── discover_youtube.py --merge  Merge YouTube results into tracks table
├── discover.py                  Spotify: catalog-guided + AI-gap-filling + random
│                                  └─ also asks Claude for new genres, creates catalog_cells
├── discover_artists.py          Spotify: artist-graph + Claude similar-artist suggestions
├── label_discovery.py           Claude Haiku: labels unlabelled tracks
└── analyze_pool.py              Claude: generates next-run priorities
```

### How `discover.py` works

1. **Phase 0 — AI strategies**: reads `discovery_priorities` from `catalog_meta` (written by the previous run's `analyze_pool.py`) and executes targeted searches for the gaps Claude identified.
2. **Phase 1 — Catalog-guided (85% of effort)**: picks `catalog_cells` with the best unexplored-to-pool-size ratio, searches Spotify for each `(genre year:decade)` combination.
3. **Phase 1.5 — Thin region boost**: regions with < 50 tracks get extra random genre searches.
4. **Phase 2 — Serendipity (10%)**: 2 random regions × 2 random genre searches.
5. **Genre + map expansion**: asks Claude for ~30 genres missing from the current pool, saves them to the `genres` table, and immediately creates `catalog_cells` for each new genre × every known region × every known decade (~539 new cells per genre). Both the vocabulary and the exploration map grow every run.

### How `discover_artists.py` works

1. **Phase 1 — Seed resolution**: resolves a curated list of hand-picked seed artists (regional icons and legends) to Spotify IDs.
2. **Phase 2 — Seed harvest**: fetches tracks for each seed artist via the search endpoint.
3. **Phase 3 — Collaborators**: for each seed, finds artists who appear on the same tracks and harvests their discography too.
4. **Phase 4 — AI suggestions**: samples up to 8 seed artists and asks Claude Haiku for 2-3 genuinely similar artists per seed — obscure acts from the same scene that the Spotify graph would never surface. Those artists are then harvested the same way. Capped to keep costs minimal.

All writes go through `locked_update()` in `lib/discovery_lock.py`, which acquires a PostgreSQL advisory lock to prevent concurrent cron runs from corrupting the pool.

### The self-improvement loop

```
discover.py
  │  searches catalog_cells (never-searched first, then fewest explores)
  │  writes tracks → tracks table
  │  writes back → catalog_cells.explored / fetched / last_scanned
  │  asks Claude for new genres → genres table + new catalog_cells created
  ↓
label_discovery.py → adds mood/energy/feel labels to unlabelled tracks
  ↓
analyze_pool.py
  │  reads tracks table (what do we have?)
  │  reads catalog_cells (what have we tried? what's still virgin territory?)
  │  asks Claude: "What's thin? What should we prioritise next run?"
  ↓
catalog_meta['discovery_priorities'] → read by NEXT discover.py run (Phase 0)
```

Every run leaves both the pool and the map better than it found them. The `catalog_cells` table means the system can distinguish "explored but sparse" from "never touched" — which is the difference between giving up on a cell and never having tried it.

---

## Frontend

`web/app.html` is a self-contained SPA (no build step). It fetches:
- `/discovery` → API endpoint, serves the full track pool live from the `tracks` table
- `/genre_map.json`, `/track_map.json` → pre-computed 2D embedding coordinates
- `/data.json` → legacy export for the map/stats views
- `/ledger`, `/history`, `/me`, `/token` → API endpoints served by `server.py`

---

## Production

| | |
|---|---|
| **Server** | Hetzner VPS, Ubuntu 24.04 |
| **URL** | ohdig.co |
| **Process** | `dig.service` (systemd), auto-restarts |
| **DB** | PostgreSQL 16, local socket, database `dig`, user `dig` |
| **Cron** | `0 */3 * * *` via root crontab |
| **Python** | 3.12, venv at `/opt/dig/venv` |
| **Reverse proxy** | nginx |

---

## Current status and known gaps

### What works well
- Discovery across 56 regions, 417 genres, building toward 170K cells
- AI labelling (mood, energy, feel, use_case) via Claude Haiku on 99.6% of tracks
- Self-improving gap analysis loop via Claude
- YouTube mining to fill regions where Spotify is thin (Central Africa, Laos, etc.)
- Per-user history + ledger with PostgreSQL-backed persistence
- Spotify OAuth, token refresh, multi-user sessions

### Known gaps and next steps

**Artist genre coverage (~38% of artists have no genre)**
The genre is not propagated from the search query to the track or artist at ingest time for every code path. Fixed for `discover.py` Phase 1 and 1.5; still missing for the YouTube pipeline and artist-graph crawler. A backfill via AI inference on artist name + region + source would help.

**Ledger is a flat list**
The user ledger (`user_ledger` table) is a simple list of known/liked/disliked tracks. The intended next iteration: filterable and groupable by genre, region, decade, vibe. The data is already structured to support this (each track in the pool has full metadata).

**No external ingestion**
Users can only accumulate tracks by using the app. The intended next iteration: import from Spotify liked songs, CSV export from other services (Last.fm, Bandcamp, SoundCloud), or YouTube playlist URL. The schema already has the right shape for this — it's a pipeline/import problem, not a schema problem.

**Genre depth vs. cultural tourism**
The biggest philosophical risk: "folk music from X" on YouTube yields very shallow results — re-recorded traditional music, tourist-facing content, low-quality uploads. The current defences are:
- `track_filter.py`: rejects tracks with titles that look like tourist-folk bait
- YouTube channel whitelist (curated labels: Sahel Sounds, Nyege Nyege Tapes, Analog Africa, etc.)
- The Spotify-first approach (professional releases are filtered by Spotify's own curation)

This needs continued attention. The goal is always depth over breadth within each genre — a great obscure 1975 highlife recording beats ten YouTube uploads of "Traditional African Music".

**Catalog cells → Spotify pool sizes are mostly null**
The `catalog_cells.pool_size` column tracks how many tracks Spotify says exist for a given `(region × genre × decade)` query. Currently most are null (not yet scanned). Running `catalog.py` more aggressively would fill these in and allow much smarter exploration prioritisation.

**`data.json` and `genre_map.json` / `track_map.json` are static exports**
These are generated by `scripts/export_data.py`, `scripts/genre_embeddings.py`, and `scripts/track_embeddings.py`. They need to be regenerated manually after major discovery runs. Should be scheduled or triggered automatically.

---

## Principles

1. **Breadth first, then depth.** Every region and genre deserves at least a foothold before any single one gets deep coverage.
2. **No popularity bias.** `popularity = 0` tracks are as welcome as `popularity = 80`. The filter is quality, not fame.
3. **Self-correcting.** Every run should leave an audit trail of what it found and what it missed, so the next run can do better.
4. **Depth over tourism.** An obscure pressing matters more than a YouTube compilation. Curated sources (record labels, specialist channels) are trusted over generic searches.
5. **The ledger is permanent.** A user's listening history should outlast any particular music service. The data is theirs.
