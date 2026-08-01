/**
 * DIG's own playback must not be the thing that gets DIG rate-limited.
 *
 * Spotify's quota is Development Mode, app-wide, and Extended Quota is not
 * available to this app. Tripping it locks EVERYTHING out for ~18 hours — the
 * crawlers included. Measured 2026-08-01: 175 of the last 200 hourly ingest
 * runs aborted `app rate-limited`, 26,363 artists sat resolved and unqueryable,
 * and the pool grew 317 tracks in a fortnight while the listener played 362.
 *
 * lib/spotify_gate holds every crawler to one call per 1.5s, cross-process,
 * behind a shared flock. The iPhone Connect poll ran at a flat 1.5s and went
 * straight to api.spotify.com from the browser — outside the gate, outside any
 * accounting. One listening phone matched the entire crawler fleet's budget.
 *
 * These pin the fix: the poll rate follows uncertainty. Fast when a dispatch is
 * unconfirmed or a track is about to end, slow when nothing is about to change.
 *
 *   node tests/test_spotify_quota.mjs
 */
import { loadApp, test, run, assert } from './harness.mjs';

const SP = (i) => 'sp' + String(i).padStart(20, '0');

/** Count of direct Spotify player-state reads the app has made. */
const stateCalls = (app) =>
  app.fetches.filter((f) => f.url === 'https://api.spotify.com/v1/me/player').length;

async function playing() {
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 200 }, (_, i) => ({
    id: SP(i), name: `Track ${i}`, artist: `Artist ${i}`, source: 'spotify',
    genres: ['g'], region: 'R', duration_ms: 300000,
  }));
  w.allDiscovery = tracks;
  w.allTracksPool = tracks.slice();
  w.dIdx = 0;
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices: [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }] }));
  app.route('/api/play', () => ({ ok: true, device: 'dev1' }));
  // Steady mid-track playback of a 5-minute track: nothing is about to change.
  app.route((u) => u === 'https://api.spotify.com/v1/me/player', () => ({
    is_playing: true, progress_ms: 60000,
    item: { id: SP(0), duration_ms: 300000, name: 'n', artists: [], album: { images: [] } },
  }));
  w.playCurrentTrack();
  return app;
}

test('steady playback does not poll Spotify flat out', async () => {
  const app = await playing();
  await app.tick(20000, 3000);          // let the dispatch settle
  const settled = stateCalls(app);
  await app.tick(60000, 3000);          // one minute of ordinary listening
  const perMinute = stateCalls(app) - settled;

  assert(perMinute > 0, 'the poll stopped entirely — drift would never be corrected');
  assert(perMinute <= 10,
    `${perMinute} Spotify calls per minute of steady playback. At 1.5s flat `
    + 'that is 2,400/hour from one phone, matching the entire crawler fleet\'s '
    + 'gated budget and locking the whole app out for ~18h when it trips');
});

test('a fresh dispatch is still confirmed promptly', async () => {
  // The slow rate must not delay the one window where correctness depends on
  // seeing Spotify quickly: the context-jump guard and _connectTrackConfirmed
  // both hang off the poll noticing the track DIG just sent.
  const app = await playing();
  await app.tick(20000, 3000);
  const before = stateCalls(app);
  app.win.playCurrentTrack();          // user skips
  await app.tick(6000, 3000);
  const during = stateCalls(app) - before;
  assert(during >= 3,
    `only ${during} polls in the 6s after a dispatch — too slow to confirm the `
    + 'track landed, which is what the context-jump guard depends on');
});

test('the end of a track is watched closely', async () => {
  // With the screen locked Spotify advances through the look-ahead context on
  // its own and no JS of ours runs. The poll is the only thing that will ever
  // notice, so it must be fast approaching the boundary even though nothing
  // has changed yet.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 200 }, (_, i) => ({
    id: SP(i), name: `Track ${i}`, artist: `Artist ${i}`, source: 'spotify',
    genres: ['g'], region: 'R', duration_ms: 300000,
  }));
  w.allDiscovery = tracks; w.allTracksPool = tracks.slice(); w.dIdx = 0;
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices: [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }] }));
  app.route('/api/play', () => ({ ok: true, device: 'dev1' }));
  // Five seconds from the end.
  app.route((u) => u === 'https://api.spotify.com/v1/me/player', () => ({
    is_playing: true, progress_ms: 295000,
    item: { id: SP(0), duration_ms: 300000, name: 'n', artists: [], album: { images: [] } },
  }));
  w.playCurrentTrack();
  await app.tick(20000, 3000);
  const before = stateCalls(app);
  await app.tick(9000, 3000);
  const near = stateCalls(app) - before;
  assert(near >= 4,
    `only ${near} polls in 9s while 5s from the end of the track — a native `
    + 'auto-advance would go unnoticed, which on a locked phone is the only '
    + 'way DIG ever learns the track changed');
});

run('spotify quota');
