/**
 * Sweeps the surfaces the playback tests never touch, under module semantics.
 *
 * The browser code is loaded as `<script type="module">`, and modules are
 * always STRICT. The difference is not academic: assigning to an undeclared
 * variable is a silent implicit global in sloppy mode and a ReferenceError in
 * strict. A file that has spent years as one sloppy inline script can carry any
 * number of those, and each one is invisible until the line runs — so a map tab
 * or a feed filter nobody clicked during testing is exactly where one hides.
 *
 * So this calls things. Broadly and shallowly: enough of the app to execute the
 * top of most subsystems once, which is all a strict-mode landmine needs.
 *
 *   node tests/test_boot_smoke.mjs
 */
import { loadApp, appScript, codeOnly, test, run, assert } from './harness.mjs';

/** A session with enough data loaded that the render paths have work to do. */
async function booted() {
  const app = await loadApp({ isIOS: false });
  const w = app.win;
  const tracks = Array.from({ length: 60 }, (_, i) => ({
    id: 'sp' + String(i).padStart(20, '0'),
    name: `Track ${i}`,
    artist: `Artist ${i % 7}`,
    source: i % 3 === 0 ? 'bandcamp' : 'spotify',
    genres: [['jazz', 'techno', 'fado', 'highlife'][i % 4]],
    region: ['Japan', 'Brazil', 'Mali', 'Portugal'][i % 4],
    origin_region: ['Japan', 'Brazil', 'Mali', 'Portugal'][i % 4],
    year: 1970 + (i % 50),
    duration_ms: 180000,
  }));
  w.allDiscovery = tracks;
  w.allTracksPool = tracks.slice();
  w.history = tracks.slice(0, 12).map((t, i) => ({
    ...t, status: ['listened', 'saved', 'skipped'][i % 3], time: 1750000000000 + i,
  }));
  return app;
}

/** Calls `fn`, returning the error if it threw. Records nothing else. */
function attempt(w, name, ...args) {
  const fn = w[name];
  if (typeof fn !== 'function') return `${name} is not reachable`;
  try {
    const out = fn(...args);
    if (out && typeof out.catch === 'function') out.catch(() => {});
    return null;
  } catch (e) {
    return `${name}: ${e && e.message}`;
  }
}

test('booting the app raises nothing under module semantics', async () => {
  const app = await loadApp({ isIOS: true });
  // The harness swallows nothing on load: a throw during evaluation would have
  // rejected loadApp before this line.
  assert(typeof app.win.playCurrentTrack === 'function', 'the app did not evaluate');
});

test('the guest path boots too', async () => {
  const app = await loadApp({ guest: true });
  assert(app.win.DIG_GUEST === true, 'guest mode is not detected from the cookie');
  assert(typeof app.win.Player === 'object', 'Player is missing in guest mode');
});

test('the map renders, switches dimension, and toggles world context', async () => {
  const app = await booted();
  const w = app.win;
  const errs = [
    attempt(w, 'gatherMapData'),
    attempt(w, 'getWorldEstimates'),
    attempt(w, 'renderMap'),
    attempt(w, 'setMapDim', 'genre'),
    attempt(w, 'setMapDim', 'year'),
    attempt(w, 'setMapDim', 'region'),
    attempt(w, 'toggleWorldContext'),
  ].filter(Boolean);
  assert(!errs.length, errs.join(' | '));
});

test('the feed renders and filters', async () => {
  const app = await booted();
  const w = app.win;
  const errs = [attempt(w, 'renderFeed')].filter(Boolean);
  for (const f of ['all', 'saved', 'skipped', 'listened']) {
    w.currentFilter = f;
    const e = attempt(w, 'renderFeed');
    if (e) errs.push(`filter=${f} ${e}`);
  }
  assert(!errs.length, errs.join(' | '));
});

test('the picker and its helpers run over a real pool', async () => {
  const app = await booted();
  const w = app.win;
  const errs = [
    attempt(w, '_pickDiscoveryStratified'),
    attempt(w, '_peekNextContextTracks', 8),
    attempt(w, 'extractGenre', w.allDiscovery[0]),
    attempt(w, 'extractYear', 'something 1998 else'),
    attempt(w, 'inferRegionFromGenres', ['jazz']),
    attempt(w, 'vibeColor', ['warm'], 200),
    attempt(w, 'vibeColorBright', ['warm'], 200),
  ].filter(Boolean);
  assert(!errs.length, errs.join(' | '));
});

test('the map controls are wired by delegation, not by a global onclick', async () => {
  const app = await booted();
  const w = app.win;
  attempt(w, 'renderMap');
  // The generated markup must carry data attributes; an inline onclick would
  // resolve `setMapDim` against the global scope at click time, which a module
  // does not populate — it would fail only when someone clicked.
  assert(!/onclick="[a-zA-Z_$]/.test(codeOnly(appScript())),
    'generated markup still carries an inline handler that needs a global');
});

test('mobile controls and media-session handlers attach', async () => {
  const app = await booted();
  const handlers = app.win.navigator.mediaSession._handlers;
  for (const action of ['play', 'pause', 'nexttrack', 'previoustrack']) {
    assert(typeof handlers[action] === 'function',
      `mediaSession '${action}' handler never attached`);
  }
});

test('the big play button is wired without an inline handler', async () => {
  const app = await booted();
  const btn = app.el('big-play-btn');
  assert((btn._listeners.click || []).length > 0,
    'nothing listens on #big-play-btn — the markup lost onclick and gained nothing');
});

test('the queue builders are trapped in a promise callback, not top level', async () => {
  const app = await booted();
  // Not a defect this test is asking anyone to fix — a record of a real trap.
  // 467 lines, including eight functions written at column 0, live inside
  // `loadHistory().then(() => { … })`. They READ as top-level and are not: they
  // cannot be called before history loads, they are invisible to any module
  // boundary drawn around them, and a future extraction that moves one out of
  // that closure changes when it exists. If this ever starts failing because
  // they became reachable, that is the trap being fixed — delete the test.
  const trapped = ['diversityShuffle', 'buildDiscoveryQueue', 'fetchDiscovery',
    'isJunkTrack', 'upgradeDiscoveryQueue'];
  const reachable = trapped.filter((n) => typeof app.win[n] === 'function');
  assert(!reachable.length,
    `now reachable, so the closure was opened up: ${reachable.join(', ')}`);
});

await run('boot smoke');
