# SoundCloud as a Dig discovery + streaming source

**Status:** backend BUILT 2026-06-28 (untested — needs API credentials); frontend
HLS playback = a ready-to-apply patch below, apply + on-device test once creds exist.

SoundCloud is a **discovery/streaming source for the app only**. Its API Terms
forbid downloading/storing audio, so it is deliberately NOT used by the Instagram
clip pipeline (that uses yt-dlp/Bandcamp). It loosens Dig's Spotify dependence and
reaches niche electro/techno gems found nowhere else.

## What's built

- `lib/soundcloud.py` — OAuth client_credentials (token cached to
  `.soundcloud_token.json`, rate-limit aware), `resolve_url`, `search`,
  `get_stream` (returns HLS `.m3u8` URLs), `discover` (genre search), id helpers
  (`sc:<track_id>`).
- `GET /api/soundcloud/resolve?id=sc:<id>` in `server.py` — returns
  `{ok, url, url_lo, preview, kind:"hls"}`; mirrors `/api/bandcamp/resolve`.
- `scripts/ingest_soundcloud.py` — genre-enumerating pool ingest (mirror of
  `ingest_bandcamp.py`); stores only `sc:<id>` + metadata.

## 1. Get credentials (you — required first)

Register an app at https://developers.soundcloud.com/docs/api/register-app
(availability fluctuates; if registration is closed, retry later). Add to `.env`:

```
SOUNDCLOUD_CLIENT_ID=...
SOUNDCLOUD_CLIENT_SECRET=...
```

Smoke-test the backend (no frontend needed):

```bash
python3 -c "from lib import soundcloud as s; \
  t=s.search('dub techno', limit=3); print(t[0]); \
  print(s.get_stream(t[0]['id']))"
```

Then trickle some into the pool: `python3 scripts/ingest_soundcloud.py --limit 100`.

## 2. The streaming reality: HLS, not MP3

Since SoundCloud's Dec 31 2025 migration, streams are **HLS AAC** (`.m3u8`
playlists), not progressive MP3. That matters for playback:

- **Safari / iOS** play `.m3u8` natively in an `<audio>` element (same as Bandcamp).
- **Chrome / Firefox / Android** need **hls.js** (Media Source Extensions) to play it.

The token endpoint is hard rate-limited (50/12h per app, 30/h per IP), so
`lib/soundcloud.py` caches the access token and reuses it — never mint per play.
Stream URLs are signed + expiring, so we resolve fresh per play and never store
them (identical discipline to Bandcamp).

## 3. Frontend patch — wiring playback (apply + on-device test with creds)

SoundCloud rides the **existing `<audio>` backend** in `web/app.html` (the
`bandcamp` module — stall watchdog, iOS unlock, ended→next are all source-agnostic).
The only new thing is attaching an HLS URL via hls.js. Minimal, additive changes:

### a. Load hls.js (once, in `<head>`)

```html
<script src="https://cdn.jsdelivr.net/npm/hls.js@1/dist/hls.min.js"></script>
```

### b. A URL-loader that handles HLS (add near the `bandcamp` module)

```js
// Attach a stream URL to the shared <audio> element. HLS (.m3u8) needs hls.js
// except on Safari/iOS, which plays it natively. MP3 (Bandcamp) sets .src directly.
function loadStreamUrl(audioEl, url, kind) {
  if (audioEl._hls) { try { audioEl._hls.destroy(); } catch (e) {} audioEl._hls = null; }
  const isHls = kind === 'hls' || /\.m3u8(\?|$)/.test(url);
  if (isHls && window.Hls && Hls.isSupported() &&
      !audioEl.canPlayType('application/vnd.apple.mpegurl')) {
    const hls = new Hls({ enableWorker: true });
    hls.loadSource(url);
    hls.attachMedia(audioEl);
    audioEl._hls = hls;
  } else {
    audioEl.src = url;   // Safari native HLS, or a plain MP3
  }
}
```

### c. In `bandcamp.play(track)` — branch the resolve endpoint by id prefix

Where it currently does `fetch('/api/bandcamp/resolve?id=' + ...)` and then
`bandcamp.audio.src = url`, replace with:

```js
const isSC = String(track.id || '').startsWith('sc:');
const endpoint = isSC ? '/api/soundcloud/resolve' : '/api/bandcamp/resolve';
const r = await fetch(endpoint + '?id=' + encodeURIComponent(track.id));
const data = await r.json();
if (!data.ok) { /* existing failure path → skip */ }
const url = data.url;
loadStreamUrl(bandcamp.audio, url, data.kind);   // <-- was: bandcamp.audio.src = url
art = data.art || track.art || null;             // SC art comes from the pool row, not resolve
```

### d. Route `sc:` tracks to the `<audio>` backend, never Spotify Connect

Every place that checks `String(t.id).startsWith('bc:')` or
`t.source === 'bandcamp'` must ALSO accept `sc:` / `'soundcloud'`. Grep them:

```bash
grep -n "startsWith('bc:')\|=== 'bandcamp'" web/app.html
```

For each, widen the test, e.g.:

```js
const isAudioBackend = (t) => {
  const id = String(t && t.id || '');
  return t && (t.source === 'bandcamp' || t.source === 'soundcloud'
               || id.startsWith('bc:') || id.startsWith('sc:'));
};
```

The critical ones are the Connect-exclusion loops (≈ lines 2521, 2621) — a `sc:`
id sent to the Spotify SDK builds `spotify:track:sc:...` and rejects the whole
play call, exactly as documented there for `bc:`.

### e. Test on-device (the playback layer is finicky — see project_dig_playback)

- Desktop Chrome: SoundCloud track plays via hls.js, advances on end, seek works.
- iPhone Safari: plays via native HLS incl. lock-screen + auto-advance.
- Confirm Bandcamp playback is byte-for-byte unchanged (regression check).

## Notes / gotchas

- `get_stream` returns `url_lo` (AAC 96) as a fallback — wire it on hls.js error
  if the 160 stream stalls.
- Track URN vs numeric id: the API accepts the numeric id on `/tracks/{id}/streams`;
  `lib/soundcloud.parse_id` yields the numeric part of `sc:<id>`.
- If many tracks come back `access != playable`, keep `only_playable=True`
  (default) so the pool never holds un-streamable rows.
