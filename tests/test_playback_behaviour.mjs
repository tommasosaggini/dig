/**
 * Behavioural tests for the iPhone playback path — the first tests in this repo
 * that RUN the app rather than grep it.
 *
 * Each one reproduces something a person reported from the phone, driving the
 * real shipped script through tests/harness.mjs with the network stubbed:
 *
 *   "when I skip I see some title but the song playing definitely is not that
 *    title, and then the title also sets itself to the correct song"
 *   "I skip to the next song and Spotify reopens every time"
 *   "the first Spotify handshake attempt failed and I saw some song titles come
 *    and go"
 *   "double tapping my earbuds with the phone locked does not work to skip"
 *
 * The static suite (test_ios_playback_divergence.py) still has a job — it locks
 * invariants that are cheaper to assert than to provoke, and it records WHY
 * each one exists. What it cannot do is notice that the code, as written, walks
 * the queue forever. These can: the harness bounds the app's own timer loop and
 * fails when it does not settle, which is how the UNPLAYABLE run limit below
 * was found rather than reasoned about.
 *
 *   node tests/test_playback_behaviour.mjs
 */
import { loadApp, test, run, assert, equal } from './harness.mjs';

const SP = (i) => 'sp' + String(i).padStart(20, '0');

/**
 * An iPhone session with a queue of Spotify tracks and a working Connect
 * device, unless a test overrides the routes.
 *
 * `allTracksPool` matters as much as `allDiscovery`: the look-ahead context
 * DIG hands Spotify is built from the pool, and with an empty pool every play
 * sends a single URI — which would quietly make the context-jump tests
 * vacuous, since there is no position 13 to resume into.
 */
async function iphone(opts = {}) {
  const { tracks = 40, source = 'spotify' } = opts;
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const mk = (i) => ({
    id: source === 'bandcamp' ? `bc:${i}:${i}` : SP(i),
    name: `Track ${i}`, artist: `Artist ${i}`, source,
    genres: ['test genre'], region: 'Testland', duration_ms: 180000,
  });
  w.allDiscovery = Array.from({ length: tracks }, (_, i) => mk(i));
  w.allTracksPool = w.allDiscovery.slice();
  w.dIdx = 0;

  app.route('/api/devices', () => ({
    devices: [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }],
  }));
  app.route('/api/pause', () => ({ ok: true }));
  // Player.getState calls Spotify directly and needs a bearer token first.
  // Without this every poll returns null, four of those shut the poll down,
  // and the app is then blind to anything a test does — which is exactly how
  // the first two context-jump tests came to fail against working code.
  app.route('/token', () => ({ access_token: 'test-token', expires_in: 3600 }));

  // A WORKING Spotify is the default: it plays what it was told to play, and
  // /me/player says so. Returning 204 instead — "no state" — is not a neutral
  // stub, it is a broken phone: the poll counts four of those and shuts itself
  // down, after which a test can move Spotify anywhere and nothing observes it.
  // Tests simulate divergence by PINNING a different track, never by silence.
  let dispatched = null;
  let pinned = null;
  app.route('/api/play', (u) => {
    const first = (u.match(/tracks=([^&]*)/) || [, ''])[1].split(',')[0];
    if (first) dispatched = decodeURIComponent(first);
    return { ok: true, device: 'dev1' };
  });
  app.route((u) => u.includes('api.spotify.com/v1/me/player'), () => {
    const id = pinned || dispatched;
    if (!id) return { __status: 204 };
    return {
      is_playing: true,
      progress_ms: 5000,
      item: {
        id, name: 'whatever', duration_ms: 180000,
        artists: [{ name: 'x' }], album: { images: [] },
      },
    };
  });

  /** Spotify is on `id` regardless of what DIG asked for. */
  app.playing = (id) => { pinned = id; };
  /** Spotify obeys again, from the next dispatch on. */
  app.obeys = () => { pinned = null; };
  app.playUrls = () => app.fetches
    .filter((f) => f.url.startsWith('/api/play'))
    .map((f) => f.url);
  app.playedIds = () => app.playUrls()
    .map((u) => (u.match(/tracks=([^&]*)/) || [, ''])[1].split(',')[0]);
  return app;
}

// ── The failed first handshake ─────────────────────────────────────────────

test('DIG never switches apps on its own', async () => {
  const app = await iphone();
  app.route('/api/play', () => ({ error: 'spotify_404', no_device: true }));
  app.route('/api/devices', () => ({ devices: [] }));

  app.win.playCurrentTrack();
  await app.tick(30000, 2000);

  equal(app.deepLinks.filter((l) => l.startsWith('spotify:')).length, 0,
    'the automatic deep link is gone. It fired on the first no-device play of '
    + 'a session — which is the normal state of picking up your phone — so the '
    + 'first thing DIG did on being opened was throw the listener into another '
    + 'app they had not asked for');
  assert(app.el('spotify-asleep-banner')._classes.has('visible'),
    'something has to say why Spotify is not playing, or the fallback is just '
    + 'DIG quietly refusing to do what was asked');
});

test('the banner says which situation this is', async () => {
  const app = await iphone();
  app.route('/api/play', () => ({ error: 'spotify_404', no_device: true }));
  app.route('/api/devices', () => ({ devices: [] }));

  app.win.playCurrentTrack();
  await app.tick(30000, 2000);

  // Never saw a device this session: Spotify was never awake to fall asleep.
  const copy = app.el('spotify-asleep-copy').textContent;
  assert(/isn't running/.test(copy),
    `first-run copy should say Spotify is not running, got: ${copy}`);
  assert(!/went to sleep/.test(copy),
    'telling a listener whose Spotify was never open that it "went to sleep" '
    + 'reads as nonsense rather than as instruction');
});

test('the handshake is a round trip, not a one-way exit', async () => {
  const app = await iphone();
  const w = app.win;
  let devices = [];
  app.route('/api/devices', () => ({ devices }));
  app.route('/api/play', () => (devices.length
    ? { ok: true, device: 'dev1' }
    : { error: 'spotify_404', no_device: true }));

  w.playCurrentTrack();
  await app.tick(30000, 2000);
  assert(app.el('spotify-asleep-banner')._classes.has('visible'), 'no banner to tap');

  // The listener taps it. DIG opens Spotify — that trip out is unavoidable,
  // since opening the app is the only way a Connect device can exist.
  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });
  equal(app.deepLinks.filter((l) => l === 'spotify:').length, 1,
    'the tap must open Spotify');

  // Spotify is now running, so a device exists. The listener comes back.
  devices = [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }];
  const before = app.playUrls().length;
  app.emit('visibilitychange');
  await app.tick(8000, 2000);

  // THE PART THAT WAS MISSING. Nothing watched for the return, so the listener
  // came back to DIG still on Bandcamp with the banner still up — the handshake
  // "failing" was DIG never looking again, not Spotify refusing.
  assert(app.logged('handshake result').length >= 1,
    'coming back from Spotify must be noticed and reported');
  assert(app.playUrls().length > before,
    'a device appeared and DIG did not use it — finding it and not playing '
    + 'leaves the listener exactly where they started, having done what we asked');
  assert(!app.el('spotify-asleep-banner')._classes.has('visible'),
    'the banner must clear once Spotify is actually playing');
});


test('a hopeless failure advances once per track, then stops', async () => {
  const app = await iphone();
  const w = app.win;
  app.route('/api/play', () => ({ error: 'spotify_404', no_device: true }));
  app.route('/api/devices', () => ({ devices: [] }));

  w.playCurrentTrack();
  // The bound is the point: without it this never settles, and the harness
  // throws rather than letting the loop allocate until node dies.
  await app.tick(30000, 2000);

  const attempts = app.playUrls().length;
  assert(attempts <= 6, `walked ${attempts} tracks; the run limit is 6`);
  assert(app.logged('UNPLAYABLE RUN').length === 1,
    'stopping must be announced, or a silent queue reads as a crash');
  // One attempt per track — never two, which is what produced three titles
  // flashing past in 3.7s on 2026-08-01.
  const ids = app.playedIds();
  equal(new Set(ids).size, ids.length, 'a track was attempted twice');
});

test('once Spotify answers, the fallback releases and it plays again', async () => {
  const app = await iphone();
  const w = app.win;
  w.SpotifyDevice.giveUp('test setup');

  w.playCurrentTrack();
  await app.tick(5000, 2000);

  equal(w.SpotifyDevice.isUnavailable(), false,
    'a play that lands is proof the device is back; staying on the fallback '
    + 'after that is what withheld 18,213 Spotify tracks');
  equal(app.deepLinks.length, 0, 'nothing may throw the user into Spotify on a working play');
});

// ── The wrong title on skip ────────────────────────────────────────────────

test('Spotify resuming deep inside the look-ahead is overruled', async () => {
  const app = await iphone();
  const w = app.win;

  w.playCurrentTrack();
  // Under the poll interval, so the look-ahead is readable before Spotify has
  // been observed anywhere — which is the state the guard exists to judge.
  await app.tick(50, 2000);

  const ctx = w.Player._connectContextIds || [];
  assert(ctx.length > 2, `look-ahead was ${ctx.length} URIs — nothing to resume into`);
  const intent = ctx[0];
  const deep = ctx[ctx.length - 1];          // the "position 13" case
  const before = app.playUrls().length;

  // The suspended app wakes up and comes back on the wrong slot.
  app.playing(deep);
  await app.tick(6000, 2000);

  const reasserts = app.playUrls().slice(before);
  assert(reasserts.length >= 1, 'DIG followed Spotify to a track it never asked for');
  assert(reasserts[reasserts.length - 1].includes(intent),
    'the re-assert must ask for DIG\'s own track, not Spotify\'s');
  assert(app.logged('context-jump').length >= 1, 'the decision must be in the log');
});

test('a confirmed track is never second-guessed', async () => {
  const app = await iphone();
  const w = app.win;

  w.playCurrentTrack();
  await app.tick(50, 2000);
  const ctx = w.Player._connectContextIds || [];
  assert(ctx.length > 2, 'no look-ahead to work with');

  // Spotify arrives where it was sent, and a poll sees it.
  await app.tick(6000, 2000);
  const before = app.playUrls().length;

  // Now the user double-taps their earbuds. The tap goes to the Spotify app —
  // it never reaches DIG on the Connect path — and Spotify steps to the next
  // track of DIG's own look-ahead.
  app.playing(ctx[1]);
  await app.tick(6000, 2000);

  equal(app.playUrls().length, before,
    'DIG re-issued over the user\'s own skip, which from the earbuds is a dead button');
  assert(app.logged('context-jump').length === 0, 'a confirmed track was second-guessed');
});

test('the guard still catches a jump the user cannot have made', async () => {
  const app = await iphone();
  const w = app.win;

  w.playCurrentTrack();
  await app.tick(6000, 2000);                // Spotify obeys, poll confirms it
  const before = app.playUrls().length;

  // …then a NEW dispatch, which must clear confirmation. A leap deep into the
  // fresh look-ahead before Spotify has been seen on it is the resume bug.
  w.nextTrack(true);
  await app.tick(50, 2000);
  const ctx2 = w.Player._connectContextIds || [];
  assert(ctx2.length > 2, 'no fresh look-ahead');
  app.playing(ctx2[ctx2.length - 1]);
  await app.tick(6000, 2000);

  assert(app.playUrls().length > before + 1,
    'confirmation must reset on dispatch, or the guard is off for the session');
});

// ── Bandcamp does not disturb Spotify it cannot see ────────────────────────

test('a Bandcamp track does not pause a Spotify that is not playing', async () => {
  const app = await iphone({ source: 'bandcamp' });
  app.win.playCurrentTrack();
  await app.tick(3000, 2000);

  const pauses = app.fetches.filter((f) => f.url.startsWith('/api/pause'));
  equal(pauses.length, 0,
    'pausing a Spotify that is not playing is what made its device reclaimable '
    + 'for nothing — every such call answered nothing_to_pause');
});

test('the unlock primer erroring does not move the queue', async () => {
  const app = await iphone({ source: 'bandcamp' });
  const w = app.win;
  // Resolve slowly, so the element sits on the silent unlock primer exactly as
  // it did on 2026-08-01: 1,348 ms between play() and the real src landing.
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/stream.mp3', duration: 248 }));

  w.playCurrentTrack();
  await app.tick(50, 2000);
  const cursorBefore = w.dIdx;

  // The primer raises errCode 4 while the resolve is still in flight. This is
  // not the track failing — the element is not even on it yet.
  assert(app.audios.length > 0, 'the Bandcamp backend never built an <audio>');
  const target = app.audios[0];
  target.src = 'data:audio/wav;base64,UklGRrQ=';
  target.error = { code: 4, message: '' };
  target.dispatchEvent({ type: 'error' });
  await app.tick(200, 2000);

  equal(w.dIdx, cursorBefore,
    'an error on the silent unlock primer advanced the queue out from under a '
    + 'track that then played perfectly — audio on one track, cursor on '
    + 'another, and every progress paint suppressed as a mismatch');
});

test('a lasting cursor disagreement stops suppressing the bar', async () => {
  const app = await iphone();
  const w = app.win;
  // The guard exists for a dispatch BEAT. Past that, the audio is the fact.
  const src = (await import('./harness.mjs')).appScript();
  const guard = src.slice(src.indexOf('const intended = queue.currentTrack();'));
  const body = guard.slice(0, guard.indexOf('pbarLog(\'SDK-paint\''));
  assert(/_PBAR_MISMATCH_GRACE_MS/.test(body),
    'the mismatch guard had no time bound, so one desync left the bar at zero '
    + 'for the whole track while the clock ran behind it');
  assert(/queue cursor disagrees with the audio/.test(body),
    'a desync that heals silently is a desync nobody fixes — it must be logged');
});

test('a dead Spotify burns one title, not three', async () => {
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  // A realistic MIXED pool: roughly three-fifths Spotify, as the real one is.
  // With a single-source pool this bug cannot appear at all.
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 5 < 3 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 5 < 3 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 180000,
  }));
  w.allDiscovery = tracks;
  w.allTracksPool = tracks.slice();
  w.dIdx = 0;

  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices: [] }));
  app.route('/api/play', () => ({ error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));

  w.playCurrentTrack();
  await app.tick(30000, 3000);

  const attempts = app.fetches.filter((f) => f.url.startsWith('/api/play')).length;
  assert(attempts <= 1, `${attempts} Spotify tracks were attempted against a `
    + 'device already proven gone. Not narrowing to Spotify is not the same as '
    + 'narrowing to Bandcamp: the pool is mostly Spotify, so every extra pick '
    + 'is a guaranteed 404 that burns a title on the way past');
});

await run('playback behaviour');
