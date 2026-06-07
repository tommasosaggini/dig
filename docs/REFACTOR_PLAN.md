# DIG — Refactor & Hardening Plan

_Audit + plan: 2026-06-07. Derived from a five-area code-quality audit (frontend,
server, lib, pipeline, scripts/hygiene)._

## TL;DR

The codebase is healthier than the raw line counts suggest. The long files
(`app.html` 6.9k, `server.py` 2.0k) are long mostly because of **duplication and
flat boilerplate**, not irreducible complexity — they shrink ~25–35% from
de-duplication alone, before any file-splitting. SQL is fully parameterized
(no injection), there are no IDOR holes, admin endpoints are gated, and there
are zero hardcoded secrets. The real risks are operational: **git/prod drift**,
a **single-threaded server**, **duplicated-twice playback logic**, and
**fragmented Spotify-quota handling**.

## Guiding constraints (do not break these)

- **Live app.** `ohdig.com` / `diiiiiiiig.xyz` serves real users. Every change
  must be deployable without downtime.
- **File-based deploy.** `web/app.html` is static: `scp` to
  `/srv/dig/app/web/app.html`, hard-refresh — no restart. `server.py` and
  `lib/`/`pipeline/` changes need `docker compose restart dig`. Verify with
  md5 (local vs remote) after every scp.
- **No-build frontend.** `app.html` is hand-served, no bundler. Any split must
  use plain `<script src>` / `<link>` against ordered global scripts — no
  toolchain.
- **Spotify Development-Mode quota is permanent and tiny.** Bursting trips a
  ~24h app-wide lockout that pauses ALL crons. Quota changes are the highest-risk
  area; never increase call volume without the gate.
- **Test before deploy.** `node --check` the extracted inline JS; run
  `python3 tests/*.py`; deploy; confirm on device (esp. iPhone Connect path).

---

## Phase 0 — Stop the bleeding ✅ (this commit)

Zero runtime risk (git + new files only; nothing deployed).

- [x] Commit the working tree (3.3k uncommitted lines: guest mode, multi-tenant
      server, playback fixes) so git == production.
- [x] Track the 6 in-use but untracked files (`lib/{spotify_gate,spotify_health,
      artist_cap,bandcamp}.py`, `web/{welcome,waitlist-admin}.html`) and the two
      untracked `migrate_*.sql`. Fixes the broken-clean-checkout bug (committed
      code imported modules absent from git).
- [x] Push to `origin/main` (was 24 commits + all uncommitted work behind the
      only off-machine backup).
- [x] `requirements.txt`, pinned to the production container versions.
- [x] `tests/` — `test_repo_integrity.py` (py_compile all + lib-import
      resolution: a regression guard for the untracked-module bug) and
      `test_track_filter.py` (locks `is_trash`, which drives destructive deletes).

Open follow-ups from Phase 0:
- [ ] Decide on `web/bctest.html` (Bandcamp test scratchpad) — commit or delete;
      left untracked for now.
- [ ] Add a CI step (GitHub Actions) running `python3 tests/*.py` + the inline-JS
      `node --check` on push. Would have caught the untracked-module bug.
- [ ] Resolve the **nested-repo mess**: `~/` (home) is itself a git repo that
      also tracks `Sites/dig/*`. The authoritative repo is `~/Sites/dig/.git`.
      The home repo also tracks `.ssh/`, `.aws/` etc. — a separate security
      concern, out of scope here but worth addressing.

---

## Phase 1 — Backend reliability (Critical / High)

The server is the reliability ceiling. Sequence matters: threading is unsafe
until the shared globals are locked and a connection pool exists.

1. **Connection pool** (`lib/db.py`). Today every `fetchone`/`fetchall` opens a
   fresh `psycopg2.connect`, re-reads `.env`, runs 2 `SET`s, commits. AI-Mix does
   ~6 per request. Introduce `psycopg2.pool.ThreadedConnectionPool`; move the
   `SET`s to pool init; cache the parsed `.env`. Connections are already closed
   correctly everywhere (no leaks) — this is pure overhead/exhaustion removal.
2. **Make shared globals thread-safe.** `_HEALTH` dict + its list trims
   (`server.py:51, 1911`) are mutated without locks. Use `collections.deque(maxlen=N)`
   (atomic append) + a `threading.Lock` for counters.
3. **`ThreadingHTTPServer`** (`server.py:2058`). One-line switch — but only AFTER
   1 & 2. Blocking Spotify/SMTP/DB I/O currently serializes all users.
4. **OAuth `state` validation** (`server.py:943, 980`). Generate a random `state`,
   store in a short-lived signed cookie, verify on `/callback`. Closes a
   login-CSRF / session-fixation vector.
5. **CORS.** Drop `Access-Control-Allow-Origin: *` (`server.py:925, 1623`) — the
   SPA is same-origin. At minimum stop sending it on `/token` (returns the live
   Spotify access token).
6. **Hardening:** cap `Content-Length` on POST bodies (memory/thread DoS);
   stop leaking `str(e)` to clients in 500s (`1372,1393,1440,1719,1778`); fix
   `/api/session` returning `{"ok":true}` after a swallowed write failure (1856);
   per-user lock on token refresh (104–161) once threaded.

Deploy note: restart-required. Test `/discovery`, `/token`, OAuth round-trip,
and a concurrent-request burst before and after.

---

## Phase 2 — Pipeline quota consolidation (Critical / High)

The #1 production incident (dev-mode lockout) traces here.

1. **`lib/spotify_client.py`** — one `gated_call(fn, *args)` wrapping the gated
   client with a single 429 policy + budget accounting; one `extract_track()`.
   Replace the **3 divergent `safe_call`s** (`discover.py:94`,
   `discover_artists.py:62`, `deep_crawl.py:80` — different give-up thresholds,
   30s vs 60s) and the **2 `extract_track`s**. Remove per-caller `time.sleep`
   (the gate owns pacing — today there are 3 uncoordinated pacing layers).
2. **`deep_crawl.py` must use the shared budget.** It keeps a private counter
   (`MAX_API_CALLS_PER_RUN=80`) and runs on a separate cron → no global spend
   authority across `dig_cron + deep_crawl + youtube + MB ingest`. Fold counting
   into the gate (fcntl-serialized → atomic).
3. **`lib/regions.py`** — single source for the market map. Kills the stale
   duplicate in `discover_artists.py:146` whose `Tibet → ["CN"]` is an invalid
   Spotify market (canonical is `["NP","IN"]`) → silent zero-result searches.
4. **`deep_crawl.py` writes through `lib/discovery_lock.locked_update`** instead
   of hand-rolled per-track connections + manual cap checks (`128–272`).
5. **`lib/genre_pool.py`** — make `GENRE_POOL` importable data; removes the hack
   where `label_discovery.py:349` regex-scrapes `discover.py`'s source text.
6. `lib.load_env()` — delete the `.env` loader copy-pasted in ~22 scripts +
   6 pipeline files.

---

## Phase 3 — Frontend de-duplication (High)

`web/app.html`. The structural root of the playback bugs we keep chasing.

1. **Paint helpers.** Extract `paintArt(url)` (album-art `innerHTML` template is
   copy-pasted 6×), `paintProgress(posMs,durMs)` (3×), `paintTrackInfo(...)` (3×),
   `fmtTime(ms)` (3 different mm:ss formatters). Highest leverage, lowest risk.
2. **Delete dead code.** The whole YouTube backend (`DIG_USE_YOUTUBE=false`,
   never on, ~150 lines) + `_prequeueNextTracks` (disabled) + the **142 dead
   `typeof clientLog === 'function'` guards** (always defined).
3. **Unify SDK ↔ Connect behind one `PlaybackBackend` interface** chosen at init
   by `DIG_IS_IOS`, instead of the 787-line monkey-patch override block
   (`2420–3207`). This is the change that stops every playback fix having to be
   made twice (and the threshold drift: 2000ms SDK vs 5000ms Connect).
4. **Extract `dig.css`** (lines 15–322) and **`explore.js`** (the ~1,000-line
   self-contained canvas view, 4519–5520) to linked static files — preserves the
   no-build/scp deploy.

Deploy note: static (scp + hard-refresh). Test on iPhone Connect path each step.

---

## Phase 4 — Module / structure refactor (Medium)

1. **Split `lib/ai_recommend.py`** (1,005 lines, 3 recommenders ~95% copy-paste):
   `lib/ai/context.py`, `lib/ai/llm.py` (one `_call_claude` owning import/
   key-check/create/parse), one module per mode.
2. **One `track_key.normalize(artist, name)`** — dedupes the heard-key normalizer
   copied 4× (`ai_recommend.py:364,632,905` + `pool_search.py:25`); current
   byte-identical copies silently desync dedup if one diverges.
3. **Route table + `@requires_auth` in `server.py`** — replace the 758-line
   `do_GET` if/elif ladder and the ~12 hand-repeated `if not user_id: 401` checks.
   Still pure-stdlib.

---

## Phase 5 — Scripts hygiene (Low / Medium)

1. `blacklist_artist.py` — add `--dry-run` default (only destructive script
   without one; deletes by artist-name prefix immediately).
2. Archive one-time/superseded: `radar.py` (legacy pre-web), `catalog.py`,
   `migrate_to_pg.py`.
3. Consolidate: 3 genre fixers → `fix_genres.py --mode {...}`; 3 audits →
   `audit.py --dimension {...}`; review the region backfillers (keep
   `backfill_regions.py`, used by cron).
4. Reuse `_extract_json_array` (the good defensive LLM-JSON parser in
   `ai_recommend.py`) in `label_discovery.py` and `genre_embeddings.py`.

---

## Reconciliation notes (audit caveats verified)

- **Model IDs are current** — `claude-sonnet-4-6`, `claude-haiku-4-5-20251001`,
  `gpt-5-mini` are all valid. One agent flagged the date-suffixed Haiku ID as
  "outdated"; that was wrong (it's a valid pinned snapshot). No model migration
  needed.
- **Git findings corrected** — one agent reported "branch master, 2 commits";
  that was the accidental home-directory repo. The real Dig repo is `main`,
  32 commits, GitHub remote. The substance (untracked in-use modules, large
  uncommitted drift, unpushed commits) was confirmed against the correct repo
  and is fixed in Phase 0.

## What's already good (don't regress)

Parameterized SQL throughout · no IDOR · gated admin · env-only secrets, zero
hardcoded credentials · uniform commit/rollback/close transaction hygiene ·
the `spotify_gate` cross-process drip-lane · `_extract_json_array` defensive
parsing · `ARCHITECTURE.md` (accurate except the Dockerfile/compose/requirements
references — see Phase 0).
