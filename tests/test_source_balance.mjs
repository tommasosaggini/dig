/**
 * The picker must not let one player take the whole session.
 *
 * Reported 2026-08-14, twice, because the first answer was wrong: "am I not
 * getting only bandcamp results?" The measurement said yes and the first pass
 * had understated it — counting mode='external' rows (the listener's OWN
 * Spotify listening, which DIG did not choose) put Spotify at 27%. Filtered to
 * what DIG actually served: 97 of the last 100 picks were Bandcamp, 41 of them
 * in one unbroken run.
 *
 * It was not scarcity — 24,150 unheard Spotify tracks were sitting in the pool.
 * Every weight in _coverageWeight is blind to the source, and blindness is not
 * neutrality when the catalogue is shaped like this one: Spotify's half lives
 * in the big countries (US 21k unheard rows, UK 6.5k, Germany 2.4k, Japan
 * 2.3k), while the Bandcamp ingestion crawls by location tag and is the only
 * thing that ever populated the long tail — 138 of 260 regions in that
 * listener's unheard pool were >=90% Bandcamp, 73 held no Spotify row at all.
 * So the strongest term in the function, 1/(1 + plays in this country), had
 * quietly become "prefer Bandcamp", and it RATCHETS: each play raises that
 * country's count and pushes the next pick further into the tail, which is
 * more Bandcamp-only still. 81% over the last 200 picks, 98% over the last 50.
 *
 * The fix is a rolling window rather than the lifetime counts every other term
 * uses, and these pin that distinction — lifetime would have inverted the
 * fault, since the account was ~10k Bandcamp plays deep and 1/(1+n) would have
 * excluded Bandcamp outright.
 *
 *   node tests/test_source_balance.mjs
 */
import { loadApp, test, run, assert } from './harness.mjs';

/** A pool track. `bc:` ids are the ingestion's Bandcamp form. */
const T = (id, artist, country) => ({
  id, artist, name: 'n', genres: ['g'], _genre: 'g',
  region: country, origin_region: country, duration_ms: 180000,
});
const BC = (n, artist, country) => T(`bc:${n}:${n}`, artist, country);
const SP = (n, artist, country) => T(`sp${n}`, artist, country);

/** A history entry as addToHistory writes it. */
const H = (id) => ({
  id, artist: `a${id}`, track: 'n', region: 'R', status: 'listened',
  source: String(id).startsWith('bc:') ? 'bandcamp' : 'spotify',
});

async function app(historyIds = []) {
  const a = await loadApp({ isIOS: false });
  a.win.allTracksPool = [];
  a.win.userCoverage = { genres: {}, countries: {}, artists: {}, albums: {} };
  a.win.history = historyIds.map(H);
  return a;
}

test('a run of one source makes the other worth more', async () => {
  // Twenty Bandcamp plays and nothing else — the reported state.
  const a = await app(Array.from({ length: 20 }, (_, i) => `bc:${i}:${i}`));
  a.win._refreshRecentSourceMix();
  const w = a.win._coverageWeight;

  const bandcamp = w(BC(99, 'X', 'Samoa'));
  const spotify = w(SP(99, 'Y', 'Samoa'));

  assert(spotify > bandcamp,
    'after twenty straight Bandcamp plays a Spotify track scores no higher '
    + 'than another Bandcamp one — this is the whole bug: every other term is '
    + 'source-blind, and the country term the picker leans on hardest is, on '
    + 'this catalogue, a Bandcamp preference wearing a geography label');
  // Has to be enough to actually turn a draw. The country and artist terms
  // span orders of magnitude; a few percent here would be present but inert.
  assert(spotify / bandcamp > 3,
    `starved source preferred only ${(spotify / bandcamp).toFixed(2)}x — too `
    + 'weak to break a 41-pick run');
});

test('an even recent mix leaves the other terms in charge', async () => {
  // The term is an incentive, not a quota: once the window is balanced it must
  // go flat, or it becomes the new thing overriding country and genre.
  const ids = [];
  for (let i = 0; i < 10; i++) { ids.push(`bc:${i}:${i}`); ids.push(`sp${i}`); }
  const a = await app(ids);
  a.win._refreshRecentSourceMix();
  const w = a.win._coverageWeight;

  const ratio = w(SP(99, 'Y', 'Samoa')) / w(BC(99, 'X', 'Samoa'));
  assert(Math.abs(ratio - 1) < 0.01,
    `even mix still tilts the draw (${ratio.toFixed(3)}x) — the balance term `
    + 'must stop applying pressure once there is nothing to correct');
});

test('the window forgets, so a played-out account is not locked out', async () => {
  // The account that reported this is ~10k Bandcamp plays deep. Lifetime
  // counts would have flipped the complaint rather than fixed it: 1/(1+10000)
  // excludes a source outright and permanently. Only the last SOURCE_WINDOW
  // plays may count.
  const ids = [];
  for (let i = 0; i < 20; i++) ids.push(`sp${i}`);         // newest: Spotify
  for (let i = 0; i < 400; i++) ids.push(`bc:${i}:${i}`);  // ancient: Bandcamp
  const a = await app(ids);
  const mix = a.win._refreshRecentSourceMix();
  const w = a.win._coverageWeight;

  assert((mix.bandcamp || 0) + (mix.spotify || 0) <= 20,
    `window counted ${JSON.stringify(mix)} — more than SOURCE_WINDOW entries`);
  assert(!mix.bandcamp,
    `four hundred plays past the window still counted (${JSON.stringify(mix)}) `
    + '— the window is not a window');
  assert(w(BC(99, 'X', 'Samoa')) > w(SP(99, 'Y', 'Samoa')),
    'four hundred ancient Bandcamp plays still suppress Bandcamp even though '
    + 'the recent window is entirely Spotify — lifetime counts would have '
    + 'inverted this fault rather than fixed it');
});

test('source is derived the same way the history writer derives it', async () => {
  // The balance term counts what addToHistory persisted. If the two disagreed
  // about what a track was, the picker would balance against a tally of a
  // session that did not happen. _trackSource is the single derivation.
  const a = await app([]);
  const src = a.win._trackSource;
  assert(src({ id: 'bc:1:2' }) === 'bandcamp', 'bc: id form not read as Bandcamp');
  assert(src({ id: 'x', source: 'bandcamp' }) === 'bandcamp', 'explicit source ignored');
  assert(src({ id: 'x' }) === 'spotify', 'a track with neither marker is Spotify');
  assert(src(null) === 'spotify', '_trackSource must survive a null track');
});

test('the picker draws both sources from a one-sided window', async () => {
  // End to end through the real picker, not just the weight: a pool split
  // evenly between the two, a window of nothing but Bandcamp, and every
  // Bandcamp track in a country the listener has never touched — i.e. the
  // exact shape that produced the 41-pick run. Spotify must still get drawn.
  const a = await app(Array.from({ length: 20 }, (_, i) => `bc:${i}:${i}`));
  const pool = [];
  for (let i = 0; i < 200; i++) pool.push(BC(i, `bandcamper${i}`, `Tiny${i}`));
  for (let i = 0; i < 200; i++) pool.push(SP(i, `spotifier${i}`, 'United States'));
  a.win.allTracksPool = pool;
  a.win.playedIds = new Set();
  // The country term alone would hand every draw to the Bandcamp side.
  a.win.userCoverage.countries = { 'United States': 1400 };

  let spotify = 0;
  for (let i = 0; i < 200; i++) {
    const pick = a.win._pickDiscoveryStratified();
    if (pick && a.win._trackSource(pick.track) === 'spotify') spotify++;
  }
  assert(spotify > 0,
    'two hundred picks from a half-Spotify pool returned no Spotify track at '
    + 'all — the reported failure, reproduced');
  assert(spotify > 20,
    `only ${spotify}/200 picks were Spotify — the balance term is being `
    + 'swamped by the country weighting it exists to offset');
});

run();
