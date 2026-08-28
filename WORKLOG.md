# DIG — Worklog

What is open, what is running, and what was measured rather than assumed.

**Read this first, then `ARCHITECTURE.md` § "Known gaps and next steps"** for the
long-form reasoning behind the pool-shape problems. This file is the short list:
things with an owner and a next action. The architecture doc is the *why*.

Convention: every open point states the **measurement** that says it is a
problem. A point with no number under it is a hunch and belongs in a comment,
not here. When a point closes, move it to **Closed** with the date and the
number that proves it — a job that "looks fine now" is not a closed point.

_Last swept: 2026-08-28._

---

## Open

### 1. The era famine is now supply-bound, and the supply is rationed
**Owner:** design · **Measured 2026-08-28**

The three code-level causes are fixed (one era definition, cells that exist,
Bandcamp reissues re-dated) and the two lanes that feed the starved decades are
both throttled by something outside the repo:

- **Spotify.** `ingest_mb_artists --era-first` is the only Spotify ingestion
  path left. Historically 334 successful runs against 3,831 rate-limited
  aborts — about 8% of hourly slots. The escalation bug behind that is now
  fixed (see Closed), but the ceiling underneath it is Spotify's Development
  Mode quota, and no amount of politeness removes that.
- **MusicBrainz.** Free and unmetered, but ~1 req/sec **per IP**, and the
  server now runs four MB consumers behind one shared lock. That is correct —
  it stopped the 503s — but it means the box has one MusicBrainz budget and
  four claimants on it.

**Next:** watch whether the 429 fix restores the pre-incident regime (19
consecutive hourly successes with no cooldown). If it does, the era lane is
healthy and this point closes on the numbers. If it does not, the honest
conclusion is that Spotify cannot serve this and the era work has to come from
Bandcamp re-dating alone — which now runs 24/7 and will finish.

### 2. Four MusicBrainz jobs, one budget, no allocation
**Owner:** design · **Measured 2026-08-28**

`crawl_genre_seeds` is scheduled `--max-minutes 50` at `:45`, so once it has
seeds again it can hold `dig-mb.lock` for most of every hour, and the other
three MB jobs — `backfill_unknown_regions`, `enumerate_mb_artists` and the
Bandcamp re-dating — would each skip their slot. Today it is exhausted
(`processed=2`) so nothing notices.

**Next:** decide the split before the genre lane is refilled, not after. The
options are shorter `--max-minutes`, staggered hours, or a real queue. Doing
nothing means the re-dating job silently loses the hour it was just moved here
to get.

### 3. `audio_analyze` will take a year at this rate
**Owner:** low · **Measured 2026-08-28**

Not the failure rate — that turned out to be fine (see Closed). The backlog:

| analysed | attempted, no audio | pending |
|---|---|---|
| 17,044 | 92 | **107,188** |

At the observed ~100/hr that is roughly 1,070 hours of runtime, on a laptop
that sleeps, while the pool grows ~2,400/day. Nothing depends on it urgently,
which is exactly why it will still be pending in a year unless it moves to the
server or gets more per-run throughput.

**Next:** decide whether this feature is worth the server capacity. If yes,
move it prod-side like the re-dating job. If no, say so here and stop counting.

### 4. The curator lane needs a curator, not a fix
**Owner:** Tommaso · **Measured 2026-08-28**

`ingest_curator` had processed 2,310 of 2,310 candidates and was doing nothing
24 times a day; it is now daily at 05:40 so its log means something again. But
it stays empty until someone picks the next curator to scrape —
`scripts/scrape_nts_show.py` and `scripts/scrape_ig_curator.py` are both ready
and neither can choose for you.

**Next:** name a curator. That is the whole task.

### 5. Instagram Phase 4 and 5
**Owner:** Tommaso · from `docs/IG_PIPELINE_PLAN.md`

Phases 0–3 are built and verified. Phase 4 (publishing) needs the IG Business
account and Meta app review; Phase 5 (SoundCloud) needs the key. Both are
account work, not code. Steps in `docs/IG_SETUP.md`.

---

## Closed

### 2026-08-28 — 16 days of work were on one laptop
3,666 uncommitted lines across 45 files plus 49 untracked files, including whole
modules (`lib/era.py`, `lib/origin.py`, `lib/genre_vocab.py`,
`lib/genre_artists.py`, `lib/artist_ig.py`) and 20 tests. `main` was level with
`origin/main`, so GitHub had none of it. Prod had it, which is what made it easy
not to notice: the code was *running*, just nowhere backed up. Second occurrence
— `docs/REFACTOR_PLAN.md` Phase 0 was written for the first. Committed in
thematic commits and pushed. **CI now exists** so a fresh clone is tested on
every push, which is the control that was missing both times.

### 2026-08-28 — the re-date job was losing ground, and ran in the wrong place
Progress on the Bandcamp reissue backlog:

| date | checked | backlog |
|---|---|---|
| 2026-08-20 | 40 | 53,798 |
| 2026-08-28 | 17,251 | 75,582 |

17,211 pairs checked in 8 days against a backlog that grew by 21,784 — losing
by ~550/day, and 15 of its 65 runs died outright when the laptop's SSH tunnel
dropped. Moved into `/etc/cron.d/dig`, hourly at `:12`, no tunnel and no sleep:
300 pairs a run × 24 clears ~7,200/day against ~2,700 of new intake.

### 2026-08-28 — four MusicBrainz jobs were competing with themselves
Moving the re-date job to the server surfaced this. Same user-agent, same
spacing, same minute:

| where | refused (503) |
|---|---|
| server, another MB job running | 2 of 6 |
| server, nothing else running | 0 of 8 |
| laptop | 0 of 6 |

Never IP reputation, never MB being down. MusicBrainz limits per IP at ~1
req/sec and four jobs each paced *themselves* at that rate behind four separate
locks, so any overlap asked for 2–4× the allowance. They now share an outer
`dig-mb.lock`, exactly as the three Spotify jobs share `dig-spotify.lock`.

### 2026-08-28 — `ig_likes_sync` had not run in two weeks
`0 7 * * *` in cron on a laptop that is asleep at 07:00; cron does not catch up
a slot it slept through and says nothing. Two runs in three weeks. Moved to a
launchd agent (`~/Library/LaunchAgents/xyz.diiiiiiiig.likes-sync.plist`), whose
`StartCalendarInterval` *does* fire on wake. It has to stay on this machine —
its YouTube half reads the session out of the local Chrome profile.

Its YouTube half was also failing (`HTTP Error 404`, then `The playlist does not
exist`). The cookie jar had only the third-party Google cookies; `SID`, `HSID`,
`SSID`, `APISID`, `SAPISID`, `LOGIN_INFO` and `__Secure-1PSID` were all absent,
so the request was not authenticated as anyone. Re-exported: 106 cookies, all
present, and the likes fetch returned 10 reads → 5 new tracks.

### 2026-08-28 — one run of hammering cost the era lane an order of magnitude
`_abort_if_locked_out` handled a long cooldown correctly and a short one not at
all: Spotify sends its soft warning with no `Retry-After`, so `wait` was 0 and
`time.sleep(wait)` was a no-op. A run that met the warning replied at network
speed, uncounted.

    === DONE ===  ingested=0, errors={'spotify_429_brief': 181}, elapsed=112.0s
    [spotify-health] ingest_mb_artists: app rate-limited, Retry-After=79200s

181 brief 429s in 112 seconds, nothing ingested, and the first app-wide 22-hour
ban this project has taken. Either side of that log line: 19 consecutive hourly
runs succeeded with no cooldown before it; one success per 11–12 slots after,
and it never recovered on its own. A brief 429 now backs off 5→10→20→40→60s and
a run stands down after 8 of them. `tests/test_brief_429_is_not_free.py`.

### 2026-08-28 — 87.0% was not a ceiling, it was two stages with no cursor
The trusted-origin share sat at 87.0% while the resolver reported real work four
times a day. Two of its three network stages had no cursor: `LIMIT 150`, no
`ORDER BY`, no "not yet asked" filter, so each run re-read the same rows.
`stage_bandcamp`'s whole population was 2,127 rows of which 2,122 had already
been asked. Making every stage honour the single `origin_checked_at` flag would
have been worse — the cheap 23%-yield rung runs more often and would have
retired the pool before the 58% rung saw it. Each stage now records its own
attempt in `origin_stages_tried`. Verified on prod: two consecutive 12-row runs
asked 24 *different* rows. `tests/test_origin_ladder_has_a_cursor.py`.

### 2026-08-28 — the genre engine was built and never wired up
`genre_vocabulary` holds 2,185 world genres, 906 with zero tracks, while
`genre_artists` already held 17,145 names sourced for those genres — **14,531 of
them never looked up**. The sourcing half had been run by hand; the ingesting
half was never put on a schedule, so the names sat in a table and the coverage
number did not move. Now hourly at `:05`, Bandcamp-based so it costs no Spotify
quota. Hit rate is modest and honest: 1 artist in 6 has anything on Bandcamp.

### 2026-08-28 — prod and the laptop had quietly drifted
A five-file spot check said they matched. A full 213-file comparison found ten
runtime files where the laptop was ahead, including `lib/mb_resolve.py` — which
was missing `original_release_year`, so the re-date script could not even import
on the server. Deployed with timestamped backups after checking the direction of
every diff. **A spot check is not a comparison.**

### 2026-08-28 — `audio_analyze`'s 18% failure rate is not a bug
3,120 distinct names across 3,293 failure lines, so it is not re-attempting the
same tracks. The code already separates the three cases properly: an environment
fault stops the run rather than burning the queue, an extractor fault (yt-dlp
behind YouTube) stops after two with nothing downloaded and names the remedy,
and only a genuinely unfetchable track is marked attempted. 18% is the honest
rate at which pool tracks have no obtainable audio. The real number worth
watching is the backlog, now open point 3.

### 2026-08-28 — the last three Phase 0 follow-ups
`web/bctest.html` kept (it is the cited reference implementation for iOS
background audio in `web/js/player.js:1100`, and it had been tracked for a while
— the item was stale). CI added. Nested repo resolved: `~/.git` tracked 195
files, all of them dig, with no remote — the security half of that item was
wrong, but `~` had **no `.gitignore` at all**, so one `git add -A` would have
staged every credential in the home directory. It now denies everything by
default. Details in `docs/REFACTOR_PLAN.md`.

---

## Known-healthy (so a change here is a signal)

Snapshot 2026-08-28, so that "it broke" has something to be measured against.

- **Pool growth:** 121,620 → 124,324 tracks in ~22h (~2,400/day). `dig_cron.sh`
  runs 3-hourly at 118–120 Spotify calls a run.
- **`bc_years`:** `remaining_backlog=0`. Caught up.
- **`compute_quality`:** scored 16,228 tracks, clean.
- **Prod == laptop** across all 213 tracked source files (verified by hash, not
  by sample). Deploy is `scp`, not git — see `ARCHITECTURE.md` § Production.
- **Container `dig`:** up. 18 prod cron lanes, all writing fresh logs.
- **Test suite:** 63 suites, green — and green in a clean clone with no `.env`,
  no database and no tokens, which is what CI checks on every push.
