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

test('the handshake is a round trip that lands on Spotify', async () => {
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  // MIXED pool, and the cursor starts on a Spotify track. This matters: with an
  // all-Spotify pool the cursor is on Spotify no matter what, and the test
  // cannot tell "resumed onto Spotify" from "replayed the current track" — the
  // exact regression it exists to catch.
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 5 < 3 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 5 < 3 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 180000,
  }));
  w.allDiscovery = tracks;
  w.allTracksPool = tracks.slice();
  w.dIdx = 0;

  let devices = [];
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices }));
  app.route('/api/play', () => (devices.length
    ? { ok: true, device: 'dev1' }
    : { error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));

  w.playCurrentTrack();
  await app.tick(30000, 3000);
  assert(app.el('spotify-asleep-banner')._classes.has('visible'), 'no banner to tap');
  // The fallback has walked the cursor onto Bandcamp — that is the point of it.
  assert(/^bc:/.test((w.allDiscovery[w.dIdx] || {}).id || ''),
    'the cursor should be on Bandcamp once Spotify is proven gone');

  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });
  const links = app.deepLinks.filter((l) => l.startsWith('spotify:'));
  equal(links.length, 1, 'the tap must open Spotify');
  assert(/^spotify:track:/.test(links[0]),
    'the link must name a TRACK. `spotify:` alone opens the app without '
    + 'playing, and a Spotify that has never played registers a device the API '
    + `lists but cannot control. Got: ${links[0]}`);
  assert(!/spotify:track:bc:/.test(links[0]),
    'the link must come from the next SPOTIFY pick, not the Bandcamp cursor');

  // Spotify is running now, so a device exists. The listener comes back.
  devices = [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }];
  const before = app.playUrls().length;
  app.emit('visibilitychange');
  await app.tick(8000, 3000);

  assert(app.logged('handshake result').length >= 1,
    'coming back from Spotify must be noticed and reported');
  assert(app.playUrls().length > before,
    'a device appeared and DIG did not use it — finding it and not playing '
    + 'leaves the listener exactly where they started, having done what we asked');
  // AND IT MUST BE A SPOTIFY TRACK. Replaying the Bandcamp cursor sends nothing
  // to the device, and a Spotify that never plays drops its registration within
  // minutes — measured 04:06:23 device present, 04:09:12 devices_seen=0. The
  // handshake succeeds and is thrown away.
  const resumed = app.playUrls().slice(before).join(' ');
  assert(/tracks=sp/.test(resumed),
    `resuming must dispatch a Spotify track, got: ${resumed.slice(0, 140)}`);
  assert(!app.el('spotify-asleep-banner')._classes.has('visible'),
    'the banner must clear once Spotify is actually playing');
});

test('the handshake lets go of the audio session on the way out', async () => {
  // iOS grants the audio session to ONE app. DIG kept it for the whole trip:
  // measured 2026-08-01 04:21:17, the <audio> went on producing output while
  // the app was hidden and the listener was inside Spotify (pos 24436 ->
  // 26630 across `vis: hidden`). So Spotify usually could not start at all —
  // the album opened and nothing played — and on the one run where it did
  // win the session, returning to DIG handed it straight back: the Spotify
  // track stopped, the Bandcamp track carried on. Either way Spotify goes
  // quiet, a silent Spotify drops its Connect registration, and the play that
  // follows gets 404 against a device the probe saw ALIVE seconds earlier.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 5 < 3 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 5 < 3 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 180000,
  }));
  w.allDiscovery = tracks;
  w.allTracksPool = tracks.slice();
  w.dIdx = 0;

  let devices = [];
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices }));
  app.route('/api/play', () => (devices.length
    ? { ok: true, device: 'dev1' }
    : { error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));

  w.playCurrentTrack();
  await app.tick(30000, 3000);

  // The real stream, not the silent anchor or the unlock primer — those are
  // `data:` and are supposed to sit there paused.
  const stream = () => app.audios.find(
    (a) => /^https?:/.test(String(a.currentSrc || a.src || '')));
  assert(stream(), 'no Bandcamp audio is playing — nothing for this test to catch');
  assert(!stream().paused, 'precondition: Bandcamp should be making sound');

  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });

  assert(app.deepLinks.some((l) => l.startsWith('spotify:')), 'the tap must open Spotify');
  assert(stream().paused,
    'DIG is still making sound while handing over to Spotify. iOS gives the '
    + 'audio session to one app, so Spotify either never starts or loses it '
    + 'the moment the listener comes back — and the Connect device dies with it');
});

test('coming back FOLLOWS the song Spotify started; it does not command it', async () => {
  // This test used to assert the opposite, and the assertion was the bug.
  //
  // The handshake link makes Spotify play a track. DIG then dispatched a play
  // for THAT SAME TRACK at that same position, purely to install its
  // look-ahead context — a command issued to an app that had been backgrounded
  // two seconds earlier. Measured 2026-08-01 06:55:54:
  //
  //   probe   count:1 usable:1 names:["iPhone"] active:true
  //   PUT /me/player       -> 500
  //   PUT /me/player/play  -> 502 "Bad gateway."   (device did not answer)
  //   ...unpin, retry device-less -> 502 -> 404 NO_ACTIVE_DEVICE
  //   -> "Spotify unreachable" -> Bandcamp
  //
  // Le beaujolais was playing the whole time. The listener watched it freeze
  // at 7 seconds while a Bandcamp track started over it. Every step after the
  // 502 was DIG's own doing, and the 502 itself was provoked by a command that
  // never needed to be sent: the desired state already held.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 5 < 3 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 5 < 3 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 180000,
  }));
  w.allDiscovery = tracks;
  w.allTracksPool = tracks.slice();
  w.dIdx = 0;

  let devices = [];
  let nowPlaying = null;
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices }));
  // Exactly what prod returned: a device that is listed, active, playing —
  // and answers 502 to every command.
  app.route('/api/play', () => (devices.length
    ? { error: 'spotify_502', detail: 'Bad gateway.' }
    : { error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));
  app.route((u) => u === 'https://api.spotify.com/v1/me/player', () => nowPlaying);

  w.playCurrentTrack();
  await app.tick(30000, 3000);
  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });
  const opened = (app.deepLinks.find((l) => l.startsWith('spotify:track:')) || '')
    .replace('spotify:track:', '');
  assert(opened, 'the tap must open a track');

  devices = [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }];
  nowPlaying = {
    is_playing: true, progress_ms: 7242,
    device: { id: 'dev1', name: 'iPhone', is_active: true },
    item: { id: opened, duration_ms: 180000, name: 'Le beaujolais', artists: [{ name: 'x' }], album: { images: [] } },
  };
  const before = app.playUrls().length;
  // The setup deliberately failed a play to raise the banner, so count the
  // fallbacks from HERE rather than asserting none ever happened.
  const fellBackBefore = app.logged('falling back to Bandcamp').length;
  app.emit('visibilitychange');
  await app.tick(15000, 3000);

  equal(app.playUrls().length, before,
    'DIG commanded Spotify to play a track Spotify was ALREADY playing. That '
    + 'command can only fail, and when it does DIG reads the failure as '
    + '"Spotify is gone" and starts Bandcamp over the top of a playing song');
  assert(app.logged('adopted the track Spotify is already playing').length >= 1,
    'the handshake return must adopt what is playing, not re-issue it');
  // And the queue must agree, or the next skip resumes from the wrong slot.
  equal((w.allDiscovery[w.dIdx] || {}).id, opened,
    'the cursor must land on the track Spotify is playing');
  equal(app.logged('falling back to Bandcamp').length, fellBackBefore,
    'a successful handshake ended on Bandcamp — the listener tapped the '
    + 'banner, Spotify started playing, and DIG put a different song over it');
});

test('a 502 never means Spotify is gone', async () => {
  // 502 is Spotify saying "I found the device and it did not answer in time".
  // It was being read as a missing device twice over: the pinned device was
  // unpinned and the play retried with none (which can only return
  // NO_ACTIVE_DEVICE), and then giveUp() latched provenUnreachable — a
  // PERMANENT verdict, which switches the source to Bandcamp and narrows the
  // picker away from Spotify for the rest of the session. One busy moment and
  // Spotify was written off.
  const app = await iphone();
  // PIN A DEVICE FIRST. Without a pinned device the unpin branch is
  // unreachable and this test proves nothing — the failure in prod was
  // specifically DIG discarding a device it knew about.
  app.win.playCurrentTrack();
  await app.tick(15000, 3000);
  equal(app.win.Player._connectDeviceId, 'dev1', 'precondition: a device is pinned');

  app.route('/api/play', () => ({ error: 'spotify_502', detail: 'Bad gateway.', device: 'dev1' }));
  app.win.playCurrentTrack();
  await app.tick(30000, 3000);

  assert(!app.win.SpotifyDevice.isUnavailable(),
    'a transient gateway error marked Spotify permanently unreachable, which '
    + 'switches the source and narrows the picker for the whole session');
  // A first play with no device known is legitimately unaddressed; what must
  // never happen is DISCARDING a known device because of a 502.
  equal(app.logged('play failed on pinned device, retrying unpinned').length, 0,
    'a 502 unpinned the device and retried with none, which can only return '
    + 'NO_ACTIVE_DEVICE — manufacturing "Spotify is gone" out of "Spotify was '
    + 'busy"');
});

test('adoption lasts one track — then DIG picks again', async () => {
  // Adoption is deliberately one track long, and this is the other half of it.
  // DIG sends no look-ahead for the adopted track, so whatever Spotify plays
  // next is Spotify's own: the rest of the album the deep link opened.
  // Reported as "song finished naturally and another Indonesian traditional
  // song followed, so recommendations are not being enforced" — 07:32:00,
  // "Lgm. Satria Sejati" -> "Lgm. Sumpah Pemuda" at ctxPos -1.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 5 < 3 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 5 < 3 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 180000,
  }));
  w.allDiscovery = tracks; w.allTracksPool = tracks.slice(); w.dIdx = 0;

  let devices = [];
  let nowPlaying = null;
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices }));
  app.route('/api/play', () => (devices.length
    ? { ok: true, device: 'dev1' }
    : { error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));
  app.route((u) => u === 'https://api.spotify.com/v1/me/player', () => nowPlaying);

  w.playCurrentTrack();
  await app.tick(30000, 3000);
  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });
  const opened = (app.deepLinks.find((l) => l.startsWith('spotify:track:')) || '')
    .replace('spotify:track:', '');

  devices = [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }];
  const state = (id, pos) => ({
    is_playing: true, progress_ms: pos,
    device: { id: 'dev1', name: 'iPhone', is_active: true },
    item: { id, duration_ms: 180000, name: id, artists: [{ name: 'x' }], album: { images: [] } },
  });
  nowPlaying = state(opened, 46075);
  app.emit('visibilitychange');
  await app.tick(15000, 3000);
  const afterAdopt = app.playUrls().length;
  assert(app.logged('adopted the track Spotify is already playing').length >= 1,
    'precondition: the handshake must adopt');

  // The track ends and Spotify rolls on to the next cut of ITS OWN album —
  // a track DIG never chose and never sent.
  nowPlaying = state('spotifys-own-album-track', 1200);
  await app.tick(20000, 3000);

  assert(app.playUrls().length > afterAdopt,
    'Spotify moved on to its own album track and DIG followed it. Adoption is '
    + 'one track long by design; when it ends DIG must dispatch its own pick, '
    + 'or the listener is on Spotify recommendations, not DIG ones');
  const took = app.playUrls().slice(afterAdopt).join(' ');
  assert(!took.includes('spotifys-own-album-track'),
    'DIG re-dispatched the track Spotify had chosen instead of its own pick');
  assert(app.logged('adopted track ended — DIG taking back control').length >= 1,
    'the handover back to DIG must be recorded — it is the moment the '
    + 'listener either does or does not get a recommendation');
});

test('the like button never carries over from the previous track', async () => {
  // Reported 2026-08-01: liked "500 Miles", tapped the Spotify banner, a track
  // by someone else started, and the heart stayed filled. The heart is a claim
  // about the song you are listening to; a stale one invites a tap that saves
  // the wrong track.
  //
  // The adoption path hand-rolled a subset of the now-playing paint — title
  // and art but not the save/dislike state — which is the exact failure mode
  // ui.js was created to end: "every past bug here was one of them being
  // updated without the other".
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 5 < 3 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 5 < 3 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 180000,
  }));
  w.allDiscovery = tracks; w.allTracksPool = tracks.slice(); w.dIdx = 0;

  let devices = [];
  let nowPlaying = null;
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices }));
  app.route('/api/play', () => (devices.length
    ? { ok: true, device: 'dev1' }
    : { error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));
  app.route((u) => u === 'https://api.spotify.com/v1/me/player', () => nowPlaying);

  w.playCurrentTrack();
  await app.tick(30000, 3000);
  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });
  const opened = (app.deepLinks.find((l) => l.startsWith('spotify:track:')) || '')
    .replace('spotify:track:', '');

  // The listener liked the track they were on, before the handshake returns.
  const liked = w.allDiscovery[w.dIdx];
  w.history.unshift({ id: liked.id, status: 'saved', artist: liked.artist, track: liked.name });
  app.el('btn-save').textContent = '♥';
  app.el('btn-save')._classes.add('saved');
  app.el('mc-save')._classes.add('saved');

  devices = [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }];
  nowPlaying = {
    is_playing: true, progress_ms: 9000,
    device: { id: 'dev1', name: 'iPhone', is_active: true },
    item: { id: opened, duration_ms: 180000, name: 'Azo track', artists: [{ name: 'Azo' }], album: { images: [] } },
  };
  app.emit('visibilitychange');
  await app.tick(15000, 3000);

  assert(w.allDiscovery[w.dIdx].id === opened, 'precondition: adopted the new track');
  assert(!app.el('btn-save')._classes.has('saved'),
    'the heart stayed filled over a track the listener never saved — tapping '
    + 'it now would save the wrong song');
  assert(app.el('btn-save').textContent !== '♥',
    'the heart glyph did not reset with the track');
  assert(!app.el('mc-save')._classes.has('saved'),
    'the mobile save button kept the previous track state — the two surfaces '
    + 'must never disagree');
});

test('tailored mode gives Spotify somewhere to go', async () => {
  // The look-ahead is what Spotify auto-advances through, and tailored mode
  // returned nothing — so every play was a ONE-TRACK context. Measured
  // 2026-08-01 11:09:44: /api/play?tracks=0F51HZ9YjVPfVozvZoD30i, ctxLen 1.
  // "Hold On" started, the listener double-tapped their earbuds, Spotify had
  // nowhere to go, and playback stopped at 0:08 of 3:47 with the bar frozen
  // there. The same dead end waits at the natural end of every track.
  const app = await iphone({ tracks: 200 });
  const w = app.win;
  w.tailoredMode = true;

  const before = app.playUrls().length;
  w.playCurrentTrack();
  await app.tick(20000, 3000);

  const url = app.playUrls().slice(before)[0] || '';
  const ids = decodeURIComponent((url.match(/tracks=([^&]*)/) || [, ''])[1]).split(',');
  assert(ids.length > 1,
    `tailored dispatched a ${ids.length}-track context. Spotify has nowhere `
    + 'to advance to: a skip or the end of the track stops playback dead');
  assert(new Set(ids).size === ids.length,
    'the look-ahead repeats a track — the peek is returning the same best '
    + 'pick instead of advancing');
  assert(!ids.some((id) => id.startsWith('bc:')),
    'a Bandcamp id in a Spotify context 400s the whole play');
});

test('peeking the tailored queue does not consume it', async () => {
  // The peek must leave no trace. Marking speculative picks as played would
  // retire tracks the listener never heard — silently shrinking the pool
  // every time a track is dispatched.
  const app = await iphone({ tracks: 200 });
  const w = app.win;
  w.tailoredMode = true;

  const before = w.playedIds.size;
  const peek = w._peekTailoredContext(10);
  assert(peek.length > 1, 'precondition: the peek returns tracks');
  equal(w.playedIds.size, before,
    'peeking marked tracks as played; those picks are now retired and the '
    + 'listener will never be offered them');
  // Nor may it write history — that would mark unheard tracks as listened.
  // (Exact picks are NOT comparable across peeks: the scorer adds deliberate
  // jitter, `score += Math.random() * 0.5`. What must hold is that nothing
  // was consumed, not that the same tracks come back.)
  const histBefore = w.history.length;
  const again = w._peekTailoredContext(10);
  assert(again.length > 1, 'a second peek still finds candidates');
  equal(w.history.length, histBefore, 'peeking wrote history');
  equal(w.playedIds.size, before, 'the second peek consumed the queue');

  // Every entry must be distinct, and the ONLY thing making that true is the
  // exclusion set: nothing marks a speculative pick as used, so a scorer left
  // to itself hands back its same best track every call — a 24-slot context
  // holding one song. That cannot be shown against the real scorer here, whose
  // jitter (`score += Math.random() * 0.5`) shuffles a homogeneous fixture
  // pool into distinct picks by luck. So drive the contract directly: a
  // ranking with a clear favourite, which is what a real taste profile makes.
  const pool = w.allTracksPool;
  w.pickNextTrack = ({ exclude = null } = {}) =>
    pool.find((t) => !(exclude && exclude.has(t.id))) || null;
  const full = w._peekTailoredContext(6);
  equal(full.length, 6, 'the peek stopped short of the depth it was asked for');
  equal(new Set(full.map((t) => t.id)).size, full.length,
    'the look-ahead repeats tracks: each pick must be fed back as an '
    + 'exclusion, or Spotify auto-advances onto the same song again');
});

test('the handshake keeps the cover Spotify hands us', async () => {
  // Folding the two paint paths together dropped the one art source the
  // adoption had. /me/player carries album.images for the track it is playing,
  // and for a track reached through the handshake both of DIG's own sources —
  // the pool row and the Spotify art cache — are cold. Measured 2026-08-01
  // 12:11:04: poolArt:false cached:false painted:"(placeholder)". The cover
  // the listener was looking at was cleared to a ♫ while Spotify played on.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 5 < 3 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 5 < 3 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 180000,
    // NO `art` on the Spotify rows — exactly the cold case.
  }));
  w.allDiscovery = tracks; w.allTracksPool = tracks.slice(); w.dIdx = 0;

  let devices = [];
  let nowPlaying = null;
  const COVER = 'https://i.scdn.co/image/the-real-cover';
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices }));
  app.route('/api/play', () => (devices.length
    ? { ok: true, device: 'dev1' }
    : { error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));
  app.route((u) => u === 'https://api.spotify.com/v1/me/player', () => nowPlaying);

  w.playCurrentTrack();
  await app.tick(30000, 3000);
  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });
  const opened = (app.deepLinks.find((l) => l.startsWith('spotify:track:')) || '')
    .replace('spotify:track:', '');

  devices = [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }];
  nowPlaying = {
    is_playing: true, progress_ms: 3703,
    device: { id: 'dev1', name: 'iPhone', is_active: true },
    item: {
      id: opened, duration_ms: 147453, name: 'Trite puti',
      artists: [{ name: 'Bulgarian Folk Ensemble' }],
      album: { images: [{ url: COVER }] },
    },
  };
  app.emit('visibilitychange');
  await app.tick(15000, 3000);

  // Chronological, not two filtered lists concatenated: what matters is the
  // LAST thing painted, and concatenating puts every "painted" before every
  // "cleared" regardless of when each happened.
  const art = app.clientLogs.filter((l) => l.tag === 'art' && /painted|cleared/.test(l.msg));
  const last = art[art.length - 1];
  assert(last && last.msg === 'painted' && last.data.url === COVER,
    'the adoption painted a placeholder over a playing track. Spotify handed '
    + 'DIG the artwork in the same response it read the position from — '
    + `last art event was: ${last ? last.msg + ' ' + last.data.url : 'none'}`);
});

test('a transient null from /me/player does not cost the adoption', async () => {
  // /me/player returns null TRANSIENTLY. Measured 2026-08-01 12:13:09.212 —
  // trackId 6kvwyMeandp…, paused:false, position 2512, deviceActive:true,
  // contextUri spotify:album:32JM3S… — and the handshake's own read 1.5s later
  // came back null. Treating that one null as "Spotify is not playing" took
  // the dispatch path instead of adopting, and that dispatch 500'd on the
  // transfer, 502'd on the play, 404'd on the retry, and dropped the listener
  // onto Bandcamp while Spotify was audibly playing.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 5 < 3 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 5 < 3 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 180000,
  }));
  w.allDiscovery = tracks; w.allTracksPool = tracks.slice(); w.dIdx = 0;

  let devices = [];
  let nowPlaying = null;
  let nullTheNextRead = false;
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices }));
  app.route('/api/play', () => (devices.length
    ? { ok: true, device: 'dev1' }
    : { error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));
  app.route((u) => u === 'https://api.spotify.com/v1/me/player', () => {
    if (nullTheNextRead) { nullTheNextRead = false; return { __status: 204 }; }
    return nowPlaying;
  });

  w.playCurrentTrack();
  await app.tick(30000, 3000);
  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });
  const opened = (app.deepLinks.find((l) => l.startsWith('spotify:track:')) || '')
    .replace('spotify:track:', '');

  devices = [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }];
  nowPlaying = {
    is_playing: true, progress_ms: 2512,
    device: { id: 'dev1', name: 'iPhone', is_active: true },
    item: { id: opened, duration_ms: 443350, name: 'x', artists: [{ name: 'y' }], album: { images: [] } },
  };
  // The visibility read lands (good state), then the handshake's own read
  // comes back empty — the exact prod sequence.
  app.emit('visibilitychange');
  await app.tick(1000, 500);
  nullTheNextRead = true;
  await app.tick(20000, 3000);

  assert(app.logged('adopted the track Spotify is already playing').length >= 1,
    'one transient null lost the adoption. DIG dispatched instead, and that '
    + 'dispatch is the one that fails — Spotify was playing the whole time');
  assert(!app.logged('falling back to Bandcamp').slice(1).length,
    'the handshake ended on Bandcamp while Spotify was playing');
});

test('nothing starts playing locally while the listener is in Spotify', async () => {
  // beginHandshake pauses the audio to hand the session over, but a play
  // already IN FLIGHT resolves afterwards and starts it again. Measured
  // 2026-08-01: a skip at 12:12:57 was still resolving when the banner was
  // tapped at 12:12:59.256; releaseAudio paused at 12:12:59.3, the in-flight
  // Bandcamp play landed at 12:12:59.972, and DIG came back from a WORKING
  // handshake with audioPaused:false — Bandcamp playing over the track
  // Spotify had just started.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 5 < 3 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 5 < 3 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 180000,
  }));
  w.allDiscovery = tracks; w.allTracksPool = tracks.slice(); w.dIdx = 0;

  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices: [] }));
  app.route('/api/play', () => ({ error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));

  w.playCurrentTrack();
  await app.tick(30000, 3000);
  const stream = () => app.audios.find(
    (a) => /^https?:/.test(String(a.currentSrc || a.src || '')));
  assert(stream() && !stream().paused, 'precondition: Bandcamp is playing');

  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });
  assert(stream().paused, 'precondition: the handover went quiet');

  // A dispatch that was already on its way now lands.
  w.playCurrentTrack();
  await app.tick(8000, 3000);

  assert(stream().paused,
    'a play landed after the handover and started the audio again. The '
    + 'listener is in Spotify; DIG making sound here takes the session '
    + 'straight back and the handshake fails for it');
  assert(app.logged('suppressed — waiting on the Spotify handshake').length >= 1,
    'the suppression must be recorded, or a dropped play looks like a bug');
});

test('a failed handshake does not leave the listener in silence', async () => {
  // The other half of releasing the session: we stopped the music for a
  // Spotify that never arrived. Staying silent would be strictly worse than
  // not having tried.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
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
  app.route('/api/devices', () => ({ devices: [] }));          // never shows up
  app.route('/api/play', () => ({ error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));

  w.playCurrentTrack();
  await app.tick(30000, 3000);
  const stream = () => app.audios.find(
    (a) => /^https?:/.test(String(a.currentSrc || a.src || '')));
  assert(stream() && !stream().paused, 'precondition: Bandcamp should be playing');

  app.el('spotify-wake-btn').dispatchEvent({ type: 'click' });
  assert(stream().paused, 'precondition: the handover should have gone quiet');

  app.emit('visibilitychange');
  await app.tick(8000, 3000);

  assert(app.logged('handshake result').length >= 1, 'the return must be noticed');
  assert(!stream().paused,
    'the handshake failed and DIG stayed silent — it stopped the music to make '
    + 'room for a Spotify that never showed up, and never started it again');
});

test('a playing Spotify is not declared dead by an empty device list', async () => {
  // /me/player and /me/player/devices answer different questions. The devices
  // list contains what is ADVERTISING itself; a backgrounded iPhone stops
  // advertising while playing perfectly well. Measured 2026-08-01, four
  // seconds apart:
  //
  //   06:43:34  connect-poll  gotState:true paused:false  59689/272554ms
  //   06:43:38  probe         count:0 usable:0 names:[]
  //
  // Spotify was a minute into the track. DIG read that state, discarded the
  // device that came with it, asked the other endpoint, was told "no devices",
  // and fell back to Bandcamp mid-song. The failed play went out as `device=-`
  // because the only place DIG ever learned an id was the empty list.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 2 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 2 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 300000,
  }));
  w.allDiscovery = tracks;
  w.allTracksPool = tracks.slice();
  w.dIdx = 1;                                   // a Spotify track

  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  // The phone is backgrounded: it advertises nothing...
  app.route('/api/devices', () => ({ devices: [] }));
  // ...but it is unmistakably playing, and names itself while doing so.
  app.route((u) => u === 'https://api.spotify.com/v1/me/player', () => ({
    is_playing: true, progress_ms: 59689,
    device: { id: 'dev-backgrounded', name: 'iPhone', is_active: true },
    item: { id: SP(1), duration_ms: 272554, name: 'n', artists: [], album: { images: [] } },
  }));
  // Prod's exact shape: the FIRST play landed (Spotify was still active, 204),
  // then the phone backgrounded and the device-less skip 404'd. Aiming at the
  // id must keep working across that transition.
  // Faithful to server.py: the success body echoes back the device the CLIENT
  // sent, not the one Spotify actually played on. So a device-less play that
  // works teaches the client nothing — which is why the id has to come from
  // /me/player instead.
  let served = 0;
  app.route('/api/play', (url) => {
    served++;
    const asked = (url.match(/device=([^&]*)/) || [, ''])[1];
    if (asked) return { ok: true, device: asked };
    if (served === 1) return { ok: true, device: null };   // 06:42:50, status 204
    return { error: 'spotify_404', no_device: true };      // 06:43:33, the skip
  });
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));

  w.playCurrentTrack();
  await app.tick(20000, 3000);                  // let the poll read the state
  const before = app.playUrls().length;
  w.playCurrentTrack();                         // the skip that failed in prod
  await app.tick(20000, 3000);

  const sent = app.playUrls().slice(before).join(' ');
  assert(/device=dev-backgrounded/.test(sent),
    'the play went out without a device id. The id was in a /me/player '
    + 'response DIG had already parsed and thrown away, and a device-less play '
    + `can only reach a Spotify that is already active. Got: ${sent.slice(0, 160)}`);
  assert(!app.logged('falling back to Bandcamp').length,
    'DIG gave up on a Spotify that was audibly playing a minute into a track');
});

test('a registered but idle device is played to by name', async () => {
  // The other half, and the failure that started all of this. A Spotify that
  // is running but not playing REGISTERS a device and reports no state at all
  // (/me/player answers 204). So the poll has nothing to learn from, and the
  // only thing that knows the id is the probe — which returned a bare boolean.
  //
  // Measured 2026-08-01 04:19: probe count:1 usable:1 active:false, DIG played
  // device-less, and Spotify answered 404 "Device not found" against a device
  // it had just listed. Naming it lets the server transfer-then-play and wake it.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  const tracks = Array.from({ length: 400 }, (_, i) => ({
    id: i % 2 ? SP(i) : `bc:${i}:${i}`,
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 2 ? 'spotify' : 'bandcamp',
    genres: ['g'], region: 'R', duration_ms: 300000,
  }));
  w.allDiscovery = tracks; w.allTracksPool = tracks.slice(); w.dIdx = 1;

  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  // Running, registered, idle — and NOT active.
  app.route('/api/devices', () => ({
    devices: [{ id: 'dev-idle', name: 'iPhone', type: 'Smartphone', is_active: false }],
  }));
  // Nothing is playing, so Spotify reports no state whatsoever.
  app.route((u) => u === 'https://api.spotify.com/v1/me/player', () => ({ __status: 204 }));
  app.route('/api/play', (url) => (/device=dev-idle/.test(url)
    ? { ok: true, device: 'dev-idle' }
    : { error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));

  await w.SpotifyDevice.probeNow('test-setup');   // the probe DIG makes anyway
  w.playCurrentTrack();
  await app.tick(20000, 3000);

  const sent = app.playUrls().join(' ');
  assert(/device=dev-idle/.test(sent),
    'the probe found the device, reported "yes" and threw the id away, so the '
    + 'play went out unaddressed and 404\'d against a device Spotify had just '
    + `listed. Got: ${sent.slice(0, 160)}`);
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

// ── Reported 2026-08-02: "I opened the app, tapped play. Some song title came
//    up, was skipped and another song started playing. It took many seconds."

test('a cold open with no Spotify does not spend a dispatch finding out', async () => {
  // Half the queue is Bandcamp, so there is something to play instead. The
  // reported session had 32,455 tracks and still burned a Spotify title first.
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  w.allDiscovery = Array.from({ length: 40 }, (_, i) => ({
    id: i % 2 ? `bc:${i}:${i}` : SP(i),
    name: `Track ${i}`, artist: `Artist ${i}`,
    source: i % 2 ? 'bandcamp' : 'spotify',
    genres: ['test genre'], region: 'Testland', duration_ms: 180000,
  }));
  w.allTracksPool = w.allDiscovery.slice();
  w.dIdx = 0;
  app.route('/token', () => ({ access_token: 't', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices: [] }));      // Spotify not running
  app.route('/api/play', () => ({ error: 'spotify_404', no_device: true }));
  app.route('/api/bandcamp/resolve', () => ({ ok: true, url: 'https://bc/s.mp3', duration: 200 }));

  // What the startup probe does: ask before the first pick needs the answer.
  await w.SpotifyDevice.probeNow('startup');
  w.playCurrentTrack();
  await app.tick(30000, 2000);

  equal(app.playUrls().length, 0,
    'the probe had already been told there is no device. Dispatching anyway '
    + 'buys the same fact with a 404 and a title the listener watches appear '
    + 'and vanish — 4s on 2026-08-02 06:14:51');
});

test('a probe that found nothing is not a forecast — evidence overrides it', async () => {
  // The guard on the whole idea. The 45s-lease version of this withheld 18,213
  // Spotify tracks because "not seen lately" was read as "gone"; this must
  // collapse the moment anything proves otherwise.
  const app = await iphone();
  const w = app.win;
  let devices = [];
  app.route('/api/devices', () => ({ devices }));

  await w.SpotifyDevice.probeNow('startup');
  assert(w.SpotifyDevice.isAbsent(), 'a probe that looked and found nothing IS a fact');

  devices = [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }];
  await w.SpotifyDevice.probeNow('pick');
  assert(!w.SpotifyDevice.isAbsent(),
    'one probe finding a device has to clear it, or the cold-open answer '
    + 'outlives every proof to the contrary and becomes the forecast this is not');
});

// ── Reported 2026-08-02: "And Spotify handshake just failed"

test('the handshake will not declare a listed-but-idle device live', async () => {
  const app = await iphone();
  const w = app.win;
  // Exactly what the probe returned at 06:16:45.861: the iPhone is LISTED and
  // is not playing. usableOf() passes it (it must — server.py agrees), and the
  // transfer that follows 404s.
  app.route('/api/devices', () => ({
    devices: [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: false }],
  }));

  w.beginHandshake('user-tap');
  const before = app.playUrls().length;
  app.emit('visibilitychange');          // back from Spotify, page visible
  await app.tick(30000, 2000);

  const result = app.logged('handshake result').pop();
  assert(result && result.data && result.data.live === false,
    'listed is not playing. Declaring live here is what sent the play that '
    + 'answered transfer 404 / play 500 at 06:16:46');
  equal(app.playUrls().length - before, 0,
    'and nothing may be dispatched to a device we just decided we cannot reach');
});

test('the handshake still succeeds on a device that is actually playing', async () => {
  const app = await iphone();   // fixture device: is_active true
  const w = app.win;
  w.beginHandshake('user-tap');
  app.emit('visibilitychange');
  await app.tick(30000, 2000);

  const result = app.logged('handshake result').pop();
  assert(result && result.data && result.data.live === true,
    'the working handshake must keep working — this is the mutation check on '
    + 'the test above, which would also pass if live were hard-wired false');
});

// ── The silence afterwards: nothing was logged for three minutes

test('a 5xx retry is not scheduled into a page iOS has frozen', async () => {
  const app = await iphone();
  const w = app.win;
  app.route('/api/play', () => ({ error: 'spotify_500', device: 'dev1' }));

  w.playCurrentTrack();
  await app.flush();
  w.document.visibilityState = 'hidden';   // phone locked inside the settle gap
  w.document.hidden = true;
  await app.tick(30000, 2000);

  assert(app.logged('hidden before the 5xx retry').length > 0,
    'the retry waits 1500ms and iOS freezes that timer. Measured 2026-08-02: '
    + 'announced at 06:16:50.496 and never fired — three minutes of silence '
    + 'with Bandcamp already paused for the handover');

  const before = app.playUrls().length;
  w.document.visibilityState = 'visible';
  w.document.hidden = false;
  app.emit('visibilitychange');
  await app.tick(30000, 2000);
  assert(app.playUrls().length > before,
    'and abandoning it is only acceptable because the return re-arms it');
});

await run('playback behaviour');
