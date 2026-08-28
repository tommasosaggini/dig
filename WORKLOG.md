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

### 1. The re-date job is losing ground, and runs in the wrong place
**Owner:** ops · **Measured 2026-08-28**

`scripts/redate_bandcamp_reissues.py` recovers original release years for
Bandcamp reissues (see the era commit and `ARCHITECTURE.md`). Progress:

| date | checked | backlog |
|---|---|---|
| 2026-08-20 | 40 | 53,798 |
| 2026-08-28 | 17,251 | 75,582 |

In 8 days it checked 17,211 pairs while the backlog grew by 21,784. **It is
falling behind at roughly 550 pairs a day and will never finish**, which makes
it exactly the self-perpetuating repair the pipeline is supposed to avoid.

Two causes, both structural:

- It is the **only** job that runs on the laptop instead of the server, reaching
  prod's Postgres through the SSH tunnel that `redate_cron.sh` opens. The laptop
  sleeps, so the hourly schedule is not hourly.
- The tunnel drops. **15 of its 65 runs died** on
  `psycopg2.OperationalError … port 5433` — "server closed the connection
  unexpectedly" or "Connection refused".

**Next:** move it into `/etc/cron.d/dig` on the server beside every other lane,
where there is no tunnel and no sleep. Then re-measure the two columns above; if
it still loses ground with a 24/7 schedule, the batch size is the next lever.

### 2. `ig_likes_sync` has not run for two weeks
**Owner:** ops · **Measured 2026-08-28**

`ig_likes_sync.log` holds exactly two runs — 7 Aug and 14 Aug — and nothing
since. The crontab entry is `0 7 * * *`, and the laptop is asleep at 07:00, so
the job simply never fires. cron does not catch up a missed run; launchd's
`StartCalendarInterval` does.

The 14 Aug run also failed its YouTube half:

```
ERROR: [youtube:tab] LL: Unable to download API page: HTTP Error 404: Not Found
  no likes visible — is the cookie session still valid?
```

That is after `5a8127a` (12 Aug) fixed the Chrome-profile bug in
`scripts/export_yt_cookies.py`, so the cookies need re-exporting, or the fix
does not cover this path.

**Next:** move the schedule to launchd so a missed run fires on wake, then
re-export the YouTube cookies and confirm the likes fetch returns rows.

### 3. The era lane is quota-starved to ~1.7% of intake
**Owner:** design · **Measured 2026-08-28**

`scripts/ingest_mb_artists.py --era-first` is the lane built to cure the era
famine, and the only Spotify ingestion path that still works. Current
behaviour, from `mb_ingest.log`:

- one successful run, 50 artists → 40 tracks
- the **very next** run: `RATE LIMITED on artist_albums for 82737s` (23 hours)
- 9 hourly runs since, each aborting cleanly, counting 75,600s → 18,000s

So a single 50-artist run buys a ~23h app-wide lockout. The lane is capped at
about **40 tracks/day against ~2,400/day of total intake**. The era gap cannot
close from here however well the picker steers.

Compounding it: `/etc/cron.d/dig` guards this slot with
`flock -n /var/run/dig-spotify.lock`, shared with `dig_cron.sh` and
`deep_crawl.py`. A locked-out run writes **no line at all**, so an absent log
entry means "skipped", not "did not fire".

**Next:** find the per-run call budget that does *not* trip the lockout — the
useful experiment is a smaller `--limit` measured against time-to-next-429, not
a bigger one. If no budget clears it, the lane is structurally done and the era
work has to come from Bandcamp re-dating (point 1) instead.

### 4. Two hourly lanes run empty
**Owner:** design · **Measured 2026-08-28**

- `curator_ingest`: `2310 candidates | 2310 already processed`,
  `processed=0 calls=0`. Every hour, nothing.
- `genre_crawl`: `resume: 6645 MBIDs already crawled`, `processed=2,
  new_seeds=0, skipped_pre_crawled=5153`.

Neither is broken; both are exhausted. They cost nothing but they also prove
nothing, and a green log that reports zero work looks the same as a healthy one.

**Next:** refill or retire. `scripts/curator_to_genre_seeds.py` and
`scripts/source_genre_artists.py` exist to refill the second one — wire them to
the schedule, or take the empty lane off it.

### 5. Origin resolution is flat at 87.0%
**Owner:** design · **Measured 2026-08-28**

Three consecutive `origin.log` writes all report `pool trusted origin: 87.0%`
with 16,149 rows still needing resolution. The resolver works — it clears 15 to
41 rows a run — but the denominator grows about as fast.

**Next:** measure the two rates against each other the way point 1 does, then
decide whether to raise the per-run limit or accept 87% as the ceiling this
ladder reaches and record that as the answer.

### 6. `audio_analyze` fails ~18% of tracks
**Owner:** low · **Measured 2026-08-28**

`183/722 ok=150 failed=33` — "no audio / analysis failed". Not investigated;
unknown whether these are dead sources (fine, should be marked) or a fixable
fetch bug (not fine, it is re-attempted forever).

**Next:** sample ten failures and find out which of the two it is. That answer
decides whether this is a bug or a bookkeeping gap.

### 7. Repo hygiene — the last three Phase 0 follow-ups
**Owner:** low · from `docs/REFACTOR_PLAN.md`

- **No CI.** Nothing runs `tests/` on push. The suite is 52 Python files and 11
  browser suites and takes well under a minute; it would have caught the
  untracked-module bug that Phase 0 was written for.
- **`web/bctest.html`** — Bandcamp scratchpad, still undecided: commit or delete.
- **The nested-repo mess.** `~/` is itself a git repo that also tracks
  `Sites/dig/*`, so every file here has two histories and the home one also
  tracks `.ssh/` and `.aws/`. The authoritative repo is `~/Sites/dig/.git`.

### 8. Instagram Phase 4 and 5
**Owner:** Tommaso · from `docs/IG_PIPELINE_PLAN.md`

Phases 0–3 are built and verified. Phase 4 (publishing) needs the IG Business
account and Meta app review; Phase 5 (SoundCloud) needs the key. Both are
account work, not code. Steps in `docs/IG_SETUP.md`.

---

## Closed

**2026-08-28 — 16 days of work were on one laptop.** 3,666 uncommitted lines
across 45 files plus 49 untracked files, including whole modules (`lib/era.py`,
`lib/origin.py`, `lib/genre_vocab.py`, `lib/genre_artists.py`,
`lib/artist_ig.py`) and 20 tests. `main` was level with `origin/main`, so
GitHub had none of it. Prod had it — every file hash matched — which is exactly
what made it easy not to notice: the code was *running*, just nowhere backed up.
This is the second occurrence; `docs/REFACTOR_PLAN.md` Phase 0 was written for
the first. Committed in nine thematic commits and pushed.

---

## Known-healthy (so a change here is a signal)

Snapshot 2026-08-28, so that "it broke" has something to be measured against.

- **Pool growth:** 121,620 → 124,041 tracks in 21h (~2,400/day). `dig_cron.sh`
  runs 3-hourly at 118–120 Spotify calls a run.
- **`bc_years`:** `remaining_backlog=0`. Caught up.
- **`compute_quality`:** scored 16,228 tracks, clean.
- **Prod == laptop** on `server.py`, `web/app.html`, `web/js/app.js`,
  `lib/explore.py`, `lib/era.py`. Deploy is `scp`, not git — see
  `ARCHITECTURE.md` § Production.
- **Container `dig`:** up. All 17 prod cron lanes writing fresh logs.
- **Test suite:** 52 Python files, 11 browser suites, all green.
