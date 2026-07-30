# Instagram pipeline — setup & operations

Everything in Phases 0–3 is **built and verified** (see IG_PIPELINE_PLAN.md §8).
This file is the checklist of things only you can do, plus how to run it.

## What's already done (no action needed)

- DB table `ig_post_queue` self-creates on next deploy (`ensure_ig_schema()` at
  server startup) — or apply manually now: `psql "$DATABASE_URL" -f scripts/migrate_ig_post_queue.sql`.
- Admin dashboard lives at **`/ig-admin.html`** — open it while logged in as the
  admin Spotify account (gated by `ADMIN_UID`, same model as `waitlist-admin.html`).
- Pipeline scripts: `pipeline/ig_propose.py`, `ig_audio_resolver.py`,
  `ig_render.py`, `ig_publish.py`, driven by `ig_cron.sh`.
- Render path verified end-to-end locally: produces Reel-eligible
  `feed.mp4` (1080×1080) + `story.mp4` (1080×1920) with a 30s clip.

## 1. Install the new dependencies (deploy)

`requirements.txt` now pins `Pillow` + `yt-dlp`. **`ffmpeg` is a system binary**,
not pip — it must be on the server/container PATH. In the Docker image add:

```dockerfile
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
```

Then `pip install -r requirements.txt`. (Local mac dev already has all three.)

## 2. Turn on the cron

```cron
17 * * * * /Users/tommasosaggini/Sites/dig/ig_cron.sh >> /Users/tommasosaggini/Sites/dig/ig_cron.log 2>&1
```

Hourly is plenty for an every-2-days cadence — it just tops up suggestions,
resolves audio for approved items, renders scheduled ones, and (once live)
publishes due ones. Costs **zero Spotify quota**, so it runs even during a
Spotify cooldown.

Tunable env vars (`.env`): `IG_CADENCE_HOURS` (default 48), `IG_POST_HOUR_UTC`
(default 18), `IG_TARGET_SUGGESTED` (default 5).

## 3. Instagram publishing (Phase 4 — needs you + Meta review)

Publishing is the only stage that needs external setup. `ig_publish.py` is a
**safe no-op (dry-run) until creds exist**, so the cron is harmless before then.

1. **Convert the IG account to a Business account** (Creator does NOT work for
   API publishing) and **link it to a Facebook Page**.
2. Create a Meta app → add the Instagram Graph API product.
3. Submit for **app review** for `instagram_business_basic` +
   `instagram_business_content_publish` (screencast required, ~2–4 weeks). Start
   this early — it's the long pole.
4. Generate a long-lived token; put these in `.env`:
   ```
   IG_GRAPH_TOKEN=...
   IG_BUSINESS_ACCOUNT_ID=...
   IG_PUBLIC_MEDIA_BASE=https://diiiiiiiig.xyz/ig-media
   ```
5. **Serve the rendered media publicly.** Instagram fetches `feed.mp4`/`story.mp4`
   server-side, so they must be reachable at `IG_PUBLIC_MEDIA_BASE/<id>/feed.mp4`
   WITHOUT auth. The admin preview path (`/admin/ig/preview`) is admin-gated and
   won't work for IG — add a public read-only static route for `media/ig/` (or
   upload the mp4 to object storage and point the base there). **TODO before
   Phase 4 flips on.**

Reels rules to respect (already satisfied by the renderer): 5–90s, 9:16/1:1,
≤100 API posts per 24h.

## 4. SoundCloud as a Dig discovery source (Phase 5 — separate, optional)

Not wired here on purpose — SoundCloud API ToS forbids downloading/storing
audio, so it can't feed IG clips. It's valuable only as an in-app
discovery/streaming source (loosens Spotify dependence). To enable later:
register at https://developers.soundcloud.com/docs/api/register-app, add
`SOUNDCLOUD_CLIENT_ID` to `.env`, and build a resolver in `lib/` (own plan doc).

## Daily operating loop (you)

1. Open `/ig-admin.html`.
2. **Suggested** cards (from your likes): *Queue it* or *Skip*.
3. Queued → *Find audio* (auto) or *Upload file* if it's not on YouTube/Bandcamp.
4. *Edit / pick 30s* → drag the red band to the moment you want, audition it,
   tweak the caption, choose feed/story, optionally set a time.
5. *Render preview* → watch the exact post. Happy? *Approve & schedule*.
6. The cron publishes it at its slot (once Phase 4 is live).
