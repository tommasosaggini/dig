/**
 * Stage 3 of sample-not-sync: taste seeding no longer depends on the pool.
 *
 * seedTasteSignals used to match the permanent ledger (liked/disliked across
 * all sessions) against the client-side pool by id or "artist - name" key.
 * Under sample-not-sync the client holds an 8k working set, so a liked track
 * is USUALLY not in it — which would have silently reduced the taste model
 * to whatever fraction of the ledger happened to land in the sample. The
 * server now resolves each liked/disliked entry against the real pool
 * (db_get_ledger ships a `seed` object on the entry) and the client falls
 * back to it when the pool has no match.
 *
 *   node tests/test_taste_seed_serverside.mjs
 */
import { loadApp, test, run, assert } from './harness.mjs';

const POOL_TRACK = {
  id: 'pool1', name: 'In The Set', artist: 'Local Hero',
  genres: ['highlife'], region: 'Ghana', source: 'bandcamp',
};

const LEDGER = {
  known: [],
  liked: [
    {
      // NOT in the working set — only the server-resolved seed knows it.
      track: 'faraway artist - faraway song',
      seed: {
        id: 'sp123', genres: ['gnawa'], region: 'North Africa',
        origin_region: 'Morocco', query: '',
        labels: { energy: 'high', mood: 'ecstatic trance' },
      },
    },
    {
      // IS in the working set — the richer pool row must win over the seed.
      track: 'local hero - in the set',
      seed: { id: 'WRONG-if-used', genres: ['x'], region: 'x', query: '', labels: {} },
    },
  ],
  disliked: [
    {
      track: 'faraway bore - dull song',
      seed: {
        id: 'sp999', genres: ['corporate ambient'], region: 'Unknown',
        origin_region: null, query: '', labels: { energy: 'low', mood: 'flat' },
      },
    },
  ],
};

async function app() {
  const a = await loadApp({
    isIOS: true,
    routes: {
      '/discovery': () => ({ Ghana: [POOL_TRACK] }),
      '/ledger': () => LEDGER,
    },
  });
  await a.flush();
  return a;
}

test('a liked track outside the working set still seeds taste', async () => {
  const a = await app();
  const sig = a.win.tasteSignals.find((s) => s.id === 'sp123');
  assert(sig,
    'a liked track absent from the 8k working set produced no taste signal — '
    + 'seeding is still pool-dependent, which sample-not-sync silently breaks');
  assert(sig.region === 'Morocco',
    `seed region must prefer the MusicBrainz origin, got ${sig && sig.region}`);
  assert(sig.genre === 'gnawa', `seed genre must flow through extractGenre, got ${sig && sig.genre}`);
  assert(sig.energy === 'high' && sig.mood === 'ecstatic',
    `labels must derive like pool rows (first mood word), got ${sig && sig.energy}/${sig && sig.mood}`);
  assert(sig.action === 'save' && sig.strength > 0, 'liked = positive signal');
  assert(sig.trackIndex < 0, 'ledger seeds sit at virtual pre-session indices');
});

test('a disliked track outside the working set seeds negatively', async () => {
  const a = await app();
  const sig = a.win.tasteSignals.find((s) => s.id === 'sp999');
  assert(sig, 'disliked entry with a seed produced no signal');
  assert(sig.action === 'dislike' && sig.strength < 0, 'disliked = negative signal');
});

test('a pool match still wins over the shipped seed', async () => {
  const a = await app();
  const sig = a.win.tasteSignals.find((s) => s.id === 'pool1');
  assert(sig,
    'the in-pool liked track must seed from the pool row (matched by key)');
  assert(!a.win.tasteSignals.some((s) => s.id === 'WRONG-if-used'),
    'the seed must be a FALLBACK — a pool row carries every derivation '
    + 'already and must not be shadowed by the shipped metadata');
});

await run('server-resolved taste seeds (sample-not-sync stage 3)');
