/**
 * The picker has to know WHEN a track was made.
 *
 * Every axis in _coverageWeight got sharper over 2026 — genre, country, then
 * artist, then album — and the era distribution got worse as they did. The
 * picker reaches into rare countries and genres, and the rare-country tail of
 * this pool is overwhelmingly recent: Bandcamp's location-tag crawl is 72% of
 * the 2020s and 8% of the 1980s. Measured 2026-08-18 over the last 300 served
 * picks: 1.0% 1980s, 1.7% 1990s, 5.0% 2000s, 67.0% 2020s — matching the pool's
 * own era mix to within 1.06x on every decade. The picker was not amplifying
 * the skew. It was blind to it, and reproducing supply exactly.
 *
 * Two things are being pinned here, and the second is the one that was got
 * wrong first:
 *
 *   1. The target is NOT uniform. Every other axis water-fills toward "even",
 *      which is right for country and genre and wrong for decades — the
 *      listener asked for markedly more 80s/90s/2000s while being explicit
 *      that they are not asking to hear as much 1940s as 2020s.
 *
 *   2. The weight divides out SUPPLY, not play history. A feedback term
 *      reading only what the listener has heard converges to a blend two
 *      thirds weighted to the pool's own shape — it reached 21.6% for the
 *      80s-2000s against a 35.5% target. You cannot fix an imbalance that
 *      lives in the population by weighting the sample alone.
 *
 *   node tests/test_era_balance.mjs
 */
import { loadApp, test, run, assert, equal } from './harness.mjs';

// The real unheard pool, 2026-08-18. Using the actual numbers because the
// clamp's whole job is to be sized against a real catalogue's shape.
const POOL = {
  '1950s': 220, '1960s': 371, '1970s': 685, '1980s': 908,
  '1990s': 2288, '2000s': 4375, '2010s': 17256, '2020s': 49152,
};
// What the listener had actually been getting.
const RECENT = {
  '1950s': 1, '1970s': 3, '1980s': 5, '1990s': 10,
  '2000s': 30, '2010s': 128, '2020s': 323,
};

async function picker({ pool = POOL, recent = RECENT } = {}) {
  const app = await loadApp({ isIOS: false });
  app.win.userCoverage = {
    genres: {}, countries: {}, artists: {}, albums: {},
    decades: { ...recent }, decadesPool: { ...pool },
  };
  app.win._refreshEraMix();
  return app;
}

/** Share of picks each decade would win, if era were the only term. */
function drawnMix(w, pool) {
  const total = Object.values(pool).reduce((a, b) => a + b, 0);
  const raw = {}; let Z = 0;
  for (const d of Object.keys(pool)) {
    raw[d] = (pool[d] / total) * w._eraWeight({ _year: d.slice(0, 4) });
    Z += raw[d];
  }
  const out = {};
  for (const d of Object.keys(raw)) out[d] = (raw[d] / Z) * 100;
  return out;
}

test('the target is deliberately not uniform', async () => {
  const app = await picker();
  const T = app.win.ERA_TARGET;
  assert(T['2020s'] > T['1940s'] * 10,
    'a uniform target is exactly what the listener said they did not want');
  assert(T['1990s'] > T['1960s'],
    'the 90s were asked for specifically; the 60s were not');
  assert(T['1980s'] + T['1990s'] + T['2000s'] > 35,
    'the three decades this was built for do not add up to a real share');
});

test('a decade that is 1% of the pool still gets served at its target', async () => {
  // THE POINT OF THE WHOLE TERM. The 1980s is 1.2% of what is left to play.
  const app = await picker();
  const mix = drawnMix(app.win, POOL);
  assert(mix['1980s'] > 3.5,
    `1980s would still be served at only ${mix['1980s'].toFixed(1)}% — the `
    + 'weight is not correcting for supply');
  const old = mix['1980s'] + mix['1990s'] + mix['2000s'];
  assert(old > 30, `80s-2000s only reaches ${old.toFixed(1)}%, target is ~35%`);
  assert(mix['2020s'] < 40, `2020s still takes ${mix['2020s'].toFixed(1)}%`);
});

test('nothing is ever weighted past the headroom', async () => {
  // The ratchet guard. A decade at zero recent plays and a sliver of pool is
  // the worst case, and it is exactly the shape that ran away on the country
  // axis before the source window was added.
  const app = await picker({
    pool: { '1930s': 1, '2020s': 100000 },
    recent: { '2020s': 500 },
  });
  const w = app.win._eraWeight({ _year: '1935' });
  assert(w <= app.win.ERA_SUPPLY_HEADROOM + 1e-9,
    `weight ran to ${w}x — a decade with one track would be served forever`);
});

test('a decade with nothing left to play makes no claim', async () => {
  // Not the same as a decade being under target. There is nothing to serve, so
  // absorbing weight here would only take it from a decade that CAN pay.
  const app = await picker({
    pool: { '2010s': 100, '2020s': 900 },
    recent: { '2020s': 500 },
  });
  equal(app.win._eraWeight({ _year: '1975' }), 1,
    'an empty decade is bidding for picks it cannot fill');
});

test('an unknown year is neutral, not a decade', async () => {
  const app = await picker();
  equal(app.win._eraWeight({ _year: 'unknown' }), 1);
  equal(app.win._eraWeight({}), 1);
  equal(app.win._decadeOf({ _year: '198' }), null, 'a partial year is not a decade');
  equal(app.win._decadeOf({ _year: '1987' }), '1980s');
});

test('the term relaxes as ingestion fills a decade in', async () => {
  // The clamp is meant to lift on its own. Same target, ten times the 1980s
  // supply, and the weight should come down rather than keep pushing.
  const thin = await picker();
  const thick = await picker({ pool: { ...POOL, '1980s': POOL['1980s'] * 10 } });
  const a = thin.win._eraWeight({ _year: '1985' });
  const b = thick.win._eraWeight({ _year: '1985' });
  assert(b < a, `weight did not relax with supply: ${a}x then ${b}x`);
  // ...but the decade is still served MORE, because there is more of it.
  const mixThick = drawnMix(thick.win, { ...POOL, '1980s': POOL['1980s'] * 10 });
  assert(mixThick['1980s'] > drawnMix(thin.win, POOL)['1980s'],
    'more supply produced fewer plays');
});

test('it is wired into the weight the real draw uses', async () => {
  // _coverageWeight is what stage 2 of _pickDiscoveryStratified samples on. If
  // the era term is not inside it, everything above is a private opinion.
  const app = await picker();
  const w = app.win;
  w.allTracksPool = [];
  w._refreshRecentSourceMix();
  const base = { id: 'x', artist: 'A', name: 'n', genres: ['g'], _genre: 'g',
                 region: 'R', origin_region: 'R' };
  const old = w._coverageWeight({ ...base, _year: '1985' });
  const now = w._coverageWeight({ ...base, _year: '2024' });
  assert(old > now * 2,
    `_coverageWeight ignores era: 1985 ${old} vs 2024 ${now}`);
});

run();
