/**
 * A dispatch is not a listen.
 *
 * DIG wrote status='listened' the instant Spotify accepted a play command —
 * when played_pct was, by construction, zero — and only DIG's own Next button
 * could ever take it back. So the ledger drew ▶ over every track the app had
 * ever put on. Measured 2026-08-18, across the 3,755 rows carrying 'listened':
 * 1,687 had no played_pct at all, 1,160 measured under 25%, and 494 got past
 * 80%. One row in eight was telling the truth. The listener's report was "in my
 * ledger it looks like I fully listened to most songs but that's not the case".
 *
 * These drive the REAL Connect poll against a fake clock, because the mechanism
 * being tested is a measurement over time and nothing else can show that it
 * measures. The poll reads /me/player, accumulates forward progress, and is the
 * only thing in the app allowed to promote a row to 'listened'.
 *
 *   node tests/test_listened_means_listened.mjs
 */
import { loadApp, test, run, assert, equal } from './harness.mjs';

const SP = (i) => `sp${String(i).padStart(19, '0')}`;

/**
 * A session on a working Connect device, with Spotify's reported playback
 * position under the test's control.
 *
 * `position()` is called on every poll, so a test expresses "the listener heard
 * 35 seconds" as a clock-driven position rather than as a number poked into the
 * app — which is the difference between testing the accumulator and testing a
 * variable assignment.
 */
async function session({ durationMs = 180000 } = {}) {
  const app = await loadApp({ isIOS: true });
  const w = app.win;
  w.allDiscovery = Array.from({ length: 5 }, (_, i) => ({
    id: SP(i), name: `Track ${i}`, artist: `Artist ${i}`, source: 'spotify',
    genres: ['test genre'], region: 'Testland', duration_ms: durationMs,
  }));
  w.allTracksPool = w.allDiscovery.slice();
  w.dIdx = 0;

  app.route('/api/devices', () => ({
    devices: [{ id: 'dev1', name: 'iPhone', type: 'Smartphone', is_active: true }],
  }));
  app.route('/api/pause', () => ({ ok: true }));
  app.route('/token', () => ({ access_token: 'test-token', expires_in: 3600 }));

  let dispatched = null;
  let startedAt = null;
  let pinned = null;
  app.route('/api/play', (u) => {
    // Decode BEFORE splitting: DIG sends the look-ahead context as one
    // %2C-joined list, so splitting first hands back every id at once and the
    // stub then reports a track Spotify could never be playing.
    const ids = decodeURIComponent((u.match(/tracks=([^&]*)/) || [, ''])[1]);
    const first = ids.split(',')[0];
    if (first) { dispatched = first; startedAt = app.now(); }
    return { ok: true, device: 'dev1' };
  });
  app.route((u) => u.includes('api.spotify.com/v1/me/player'), () => {
    const id = pinned || dispatched;
    if (!id) return { __status: 204 };
    // Playback advances with the clock, the way playback does.
    const progress = Math.min(durationMs, app.now() - (startedAt || app.now()));
    return {
      is_playing: true,
      progress_ms: progress,
      item: {
        id, name: 'whatever', duration_ms: durationMs,
        artists: [{ name: 'x' }], album: { images: [] },
      },
    };
  });

  /** Spotify moved to `id` on its own — Connect auto-advance, an AirPods tap. */
  app.movedTo = (id) => { pinned = id; startedAt = app.now(); };
  /** The history row for a track, as the ledger reads it. */
  app.row = (id) => w.history.find((h) => h.id === id);
  return app;
}

// ── The dispatch itself ────────────────────────────────────────────────────

test('a track that has only just started is served, not listened', async () => {
  const app = await session();
  app.win.playCurrentTrack();
  await app.tick(2000, 3000);

  const row = app.row(SP(0));
  assert(row, 'the play was not recorded at all');
  equal(row.status, 'served',
    'a dispatch claimed a listen — this is the bug, exactly');
});

test('four seconds of playback never becomes a listen', async () => {
  const app = await session();
  app.win.playCurrentTrack();
  await app.tick(4000, 5000);

  equal(app.row(SP(0)).status, 'served');
});

test('past the stream threshold, and only then, it is a listen', async () => {
  const app = await session();
  app.win.playCurrentTrack();

  // 29 seconds of wall clock: the meter can only ever have counted LESS than
  // that (it trails the poll), so it cannot have reached the threshold.
  await app.tick(29000, 20000);
  equal(app.row(SP(0)).status, 'served',
    'promoted before the threshold was reached');

  // Well past it. The lag is real and deliberate — mid-track the Connect poll
  // sits at 9s, so DIG cannot know playback crossed 30s until a sample lands
  // after it did. It under-credits rather than guessing, which is the whole
  // posture of this change.
  await app.tick(25000, 20000);
  equal(app.row(SP(0)).status, 'listened',
    'real playback past the threshold did NOT earn a listen');
  assert(app.row(SP(0)).played_pct > 0, 'a promotion recorded no measurement');
});

test('a short track earns it on percentage instead', async () => {
  // 20 seconds long: 30s of playback is not available to be measured, so the
  // percentage is the only bar it can clear.
  const app = await session({ durationMs: 20000 });
  app.win.playCurrentTrack();
  await app.tick(3000, 20000);
  equal(app.row(SP(0)).status, 'served');

  await app.tick(20000, 20000);
  equal(app.row(SP(0)).status, 'listened',
    'four fifths of a short track is a listen by any reading');
  assert(app.row(SP(0)).played_pct >= 80,
    'promoted on percentage without recording one');
});

test('scrubbing to the end does not earn a listen', async () => {
  // The meter counts forward playback, not position. Dragging the bar to 2:50
  // moves the position by 170 seconds in the space of one poll; playback cannot
  // do that, so the jump is not credited. Without this the easiest way to mark
  // a whole queue 'listened' would be to scrub through it.
  const app = await session();
  const w = app.win;
  w.playCurrentTrack();
  await app.tick(3000, 5000);
  equal(app.row(SP(0)).status, 'served');

  const before = w.getListenedMs();
  // One sample 170 seconds further in, 1 second after the last one.
  w._accumulateListen(175000, 180000, SP(0), app.now() + 1000);
  equal(w.getListenedMs(), before, 'a seek was counted as listening');
  equal(app.row(SP(0)).status, 'served',
    'scrubbing to the end minted a listen');
});

// ── Leaving a track by a route DIG does not drive ──────────────────────────

test('Spotify moving on by itself settles the track it left', async () => {
  // The hole the old ledger fell through: nextTrack() was the ONLY thing that
  // ever demoted a row, so an AirPods double-tap, a Connect auto-advance or the
  // Spotify app's own next button left the outgoing track sitting on whatever
  // its dispatch had written. 237 rows on 2026-08-18 were 'listened' with no
  // measurement of any kind because of it.
  const app = await session();
  app.win.playCurrentTrack();
  await app.tick(5000, 5000);
  equal(app.row(SP(0)).status, 'served');

  app.movedTo(SP(3));
  await app.tick(4000, 5000);

  equal(app.row(SP(0)).status, 'skipped',
    'the track Spotify walked away from kept claiming a listen');
});

test('a track heard through, then left externally, keeps its listen', async () => {
  const app = await session();
  app.win.playCurrentTrack();
  await app.tick(40000, 30000);
  equal(app.row(SP(0)).status, 'listened');

  app.movedTo(SP(3));
  await app.tick(4000, 5000);

  equal(app.row(SP(0)).status, 'listened',
    'moving on from a song you heard is not a skip');
});

test('the accumulator does not carry across an external handoff', async () => {
  // The outgoing track's listened-ms used to keep accruing against the incoming
  // one, so a long play followed by a handoff would promote the NEW track
  // instantly on evidence that belonged to the old one.
  const app = await session();
  app.win.playCurrentTrack();
  await app.tick(40000, 30000);

  app.movedTo(SP(3));
  await app.tick(3000, 5000);

  const arrived = app.row(SP(3));
  if (arrived) {
    equal(arrived.status, 'served',
      'the incoming track inherited the outgoing track’s listening time');
  }
});

// ── The rules the rest of the app depends on ───────────────────────────────

test('saved and disliked outrank any measurement', async () => {
  const app = await session();
  const w = app.win;
  w.history = [{ id: SP(0), artist: 'a', track: 't', status: 'saved', played_pct: 2 }];
  w.onListenMilestone(SP(0), 95);
  equal(w.history[0].status, 'saved',
    'a measurement overwrote what the listener said');

  w.history = [{ id: SP(0), artist: 'a', track: 't', status: 'disliked' }];
  w.onPlaybackLeft(SP(0), 3, 900);
  equal(w.history[0].status, 'disliked');
});

test('an explicit status coming back off does not mint a listen', async () => {
  // Un-hearting a track wrote a flat 'listened'. So the way to give a song a
  // full listen it never had was to like it and then change your mind.
  const app = await session();
  const w = app.win;
  equal(w._statusFromPct(null), 'served');
  equal(w._statusFromPct(12), 'served');
  equal(w._statusFromPct(95), 'listened');
});

test('served is the floor of the rank, so anything at all outranks it', async () => {
  const app = await session();
  const R = app.win.STATUS_RANK;
  assert(R.served < R.skipped, 'served must lose to a recorded skip');
  assert(R.skipped < R.listened);
  assert(R.listened < R.saved);
  equal(R.saved, R.disliked, 'the two explicit statuses toggle each other');
});

test('the ledger draws served as its own thing, not as a play', async () => {
  const app = await session();
  const w = app.win;
  w.DATA = { known: [] };
  w.currentView = 'list';
  w.currentFilter = 'all';
  w.history = [
    { id: SP(0), artist: 'A', track: 'served one', status: 'served' },
    { id: SP(1), artist: 'B', track: 'heard one', status: 'listened' },
  ];
  w.renderFeed();
  const html = app.el('feed').innerHTML;
  assert(html.includes('served one'), 'the served row did not render');
  // ▶ appears once, against the row that earned it.
  equal((html.match(/▶/g) || []).length, 1,
    'the play glyph is being drawn over a track nobody heard');
});

run();
