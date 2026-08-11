/**
 * Stage 2 of sample-not-sync: the client never downloads the full pool.
 *
 * The old boot pulled /discovery with no limit — every unheard track, ~17 MB
 * raw and growing daily with ingestion — just to hold a working set the
 * picker samples 1,000 rows from anyway. Now boot upgrades to a
 * coverage-weighted WORKING SET (?limit=8000) and tops it up when the
 * picker's unheard-eligible count runs low. Download size is constant no
 * matter how large the pool grows.
 *
 * These also lock the scope seam the harness itself caught: the refill
 * implementation lives inside the loadHistory().then boot closure (where
 * fetchDiscovery and the queue machinery are), while nextTrack is top-level
 * and calls it through a hook armed at boot. An unarmed hook is a silent
 * no-op, so the arming is asserted here.
 *
 *   node tests/test_working_set_refill.mjs
 */
import { loadApp, test, run, assert } from './harness.mjs';

const REGION = 'Testland';

function tracks(n, offset = 0) {
  const out = [];
  for (let i = 0; i < n; i++) {
    out.push({
      id: 't' + (offset + i), name: 'n' + (offset + i), artist: 'a' + (offset + i),
      genres: ['g'], region: REGION, source: 'bandcamp',
    });
  }
  return out;
}

/** Boot the app with a /discovery route that answers by requested limit. */
async function boot(discoveryHandler) {
  const a = await loadApp({
    isIOS: true,                               // skips the Web Playback SDK boot
    routes: { '/discovery': discoveryHandler } // registered before boot fetches
  });
  await a.flush();
  a.tick(3000);        // DISCOVERY_UPGRADE_DELAY_MS — releases the upgrade fetch
  await a.flush();
  return a;
}

const limitOf = (url) => Number((url.match(/limit=(\d+)/) || [, 0])[1]);

test('boot never requests the full pool — bootstrap then working set only', async () => {
  const calls = [];
  const a = await boot((url) => {
    const limit = limitOf(url);
    calls.push(limit);
    return { [REGION]: tracks(Math.min(limit || 99999, 8000)) };
  });
  assert(calls.length >= 2, `expected bootstrap + working-set fetches, saw ${calls}`);
  assert(calls.every((l) => l > 0),
    `a limit-less /discovery fetch is the full ~17 MB pool download this `
    + `stage exists to kill — saw limits ${calls}`);
  assert(calls.some((l) => l === 8000),
    `no working-set (limit=8000) upgrade fetch happened — session would be `
    + `stuck on the ${calls[0]}-track bootstrap: ${calls}`);
});

test('the refill hook is armed by boot — not the top-level no-op', async () => {
  const a = await boot((url) => ({ [REGION]: tracks(Math.min(limitOf(url), 8000)) }));
  const src = String(a.win.maybeRefillWorkingSet);
  assert(!/^\(\)\s*=>\s*\{\}$/.test(src.trim()),
    'maybeRefillWorkingSet is still the placeholder no-op after boot — the '
    + 'boot closure never armed it, so the working set can silently run dry');
});

test('a low eligible count triggers a refill that merges only fresh tracks', async () => {
  let refillServed = false;
  const a = await boot((url) => {
    const limit = limitOf(url);
    if (limit === 8000 && refillServed === 'armed') {
      refillServed = true;
      // 100 rows the client already holds + 7900 genuinely new ones.
      return { [REGION]: tracks(100).concat(tracks(7900, 50000)) };
    }
    return { [REGION]: tracks(Math.min(limit, 8000)) };
  });
  refillServed = 'armed';
  const before = a.win.allTracksPool.length;
  const fetchesBefore = a.fetches.length;

  a.win.maybeRefillWorkingSet(999);   // below DISCOVERY_LOW_WATER (1000)
  await a.flush();

  assert(refillServed === true, 'no refill fetch was made at eligibleN=999');
  const after = a.win.allTracksPool.length;
  assert(after === before + 7900,
    `pool must grow by exactly the fresh rows (dedup by id): `
    + `${before} -> ${after}, expected ${before + 7900}`);
  assert(a.logged('working set refilled').length === 1,
    'the refill must narrate itself — a silent pool change is how silent '
    + 'failures start');
  assert(a.fetches.length > fetchesBefore, 'refill made no request');
});

test('a healthy working set never refills', async () => {
  const a = await boot((url) => ({ [REGION]: tracks(Math.min(limitOf(url), 8000)) }));
  const n = a.fetches.length;
  a.win.maybeRefillWorkingSet(5000);   // far above low water
  await a.flush();
  assert(a.fetches.length === n,
    'eligibleN=5000 must not fetch — refills are for a LOW working set');
});

test('a short refill answer backs off instead of hammering every pick', async () => {
  let discoveryCalls = 0;
  const a = await boot((url) => {
    discoveryCalls++;
    // Server has almost nothing unheard left: far fewer rows than requested.
    return { [REGION]: tracks(Math.min(limitOf(url), 8000), discoveryCalls * 10000) };
  });
  // Note: the boot working set was already short (8000 requested, 8000 served
  // — actually served in full above; force the short case now).
  a.route('/discovery', (url) => {
    discoveryCalls++;
    return { [REGION]: tracks(5, discoveryCalls * 10000) };
  });
  const beforeCalls = discoveryCalls;
  a.win.maybeRefillWorkingSet(1);      // triggers a refill; server sends 5 rows
  await a.flush();
  assert(discoveryCalls === beforeCalls + 1, 'first low-water call must fetch');
  a.win.maybeRefillWorkingSet(1);      // still low — but the server is drained
  await a.flush();
  assert(discoveryCalls === beforeCalls + 1,
    'a drained server must not be re-fetched on the very next pick — '
    + 'backoff exists so "pool outrun" costs one request, not one per skip');
});

await run('working-set refill (sample-not-sync stage 2)');
