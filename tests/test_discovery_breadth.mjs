/**
 * The discovery picker's job is breadth. These check it against the axis a
 * listener actually perceives: the artist.
 *
 * Reported 2026-08-01 from the phone: "I am fed an extremely narrow pool of
 * artists... Otim Alpha, whom I've listened already 4 or 5 songs from only via
 * DIG. This is ridiculous given how many artists Spotify contains."
 *
 * It was not the pool. The pool holds 31,684 artists over 41,971 tracks. It was
 * the weighting: it scored tracks by how little the listener had heard of their
 * GENRE and COUNTRY, and had no artist term at all. Spotify's taxonomy is
 * hyper-specific — 1,894 of the pool's 3,736 genres (51%) belong to exactly one
 * artist, and "acholi music" is five tracks of which four are Otim Alpha — so
 * half the time "explore a rare genre" resolved to "play that artist again",
 * and nothing counted artists, so nothing could tell the two apart.
 *
 * Measured against the real pool, replaying the real picker: of 1,500 picks
 * made from that listener's actual accumulated coverage, 32.5% landed on an
 * artist they had already heard. Drawing uniformly at random from the same
 * unheard pool would have given 26.6%. The discovery weighting was WORSE THAN
 * CHANCE at the one thing it exists to do. With the artist term: 19.1%.
 *
 *   node tests/test_discovery_breadth.mjs
 */
import { loadApp, test, run, assert } from './harness.mjs';

/** A pool track. Same shape the picker reads. */
const T = (id, artist, genre, country) => ({
  id, artist, name: 'n', genres: [genre], _genre: genre,
  region: country, origin_region: country, duration_ms: 180000,
});

async function app() {
  const a = await loadApp({ isIOS: true });
  // A pool is needed for the in-country genre-mass divisor to be computable;
  // one track per (country, genre) keeps that divisor at 1 so these tests are
  // measuring the artist term and nothing else.
  a.win.allTracksPool = [];
  a.win.userCoverage = { genres: {}, countries: {}, artists: {} };
  return a;
}

test('an artist you have heard is worth less than one you have not', async () => {
  const a = await app();
  const w = a.win._coverageWeight;
  a.win.userCoverage.artists = { 'otim alpha': 4 };

  const heard = w(T('a', 'Otim Alpha', 'acholi music', 'Uganda'));
  const fresh = w(T('b', 'Anyone Else', 'acholi music', 'Uganda'));

  assert(heard < fresh,
    'a track by an artist the listener has heard four times scores no lower '
    + 'than one by an artist they have never heard — this is the whole bug: '
    + 'the picker read "rare genre" as adventurous when half of all genres in '
    + 'the pool belong to a single artist');
  // Four plays must cost real ground, not a rounding difference. Unsoftened
  // 1/(1+plays) puts it at a fifth; anything near 1 means the term is present
  // but too weak to change a draw.
  assert(heard / fresh < 0.3,
    `four prior plays only cut the weight to ${(heard / fresh).toFixed(2)}x — `
    + 'too weak to change which track gets drawn');
});

test('one play already moves the odds', async () => {
  // The complaint was not about hearing an artist five times, it was about
  // hearing them at all, repeatedly. The first repeat is the one to prevent.
  const a = await app();
  const w = a.win._coverageWeight;
  const before = w(T('a', 'Otim Alpha', 'acholi music', 'Uganda'));
  a.win.userCoverage.artists = { 'otim alpha': 1 };
  const after = w(T('a', 'Otim Alpha', 'acholi music', 'Uganda'));
  assert(after <= before / 1.9,
    `one play moved the weight only ${(before / after).toFixed(2)}x — the `
    + 'listener notices their second track by an artist, not their fifth');
});

test('a collaborator counts as having been heard', async () => {
  // "Otim Alpha, Umoja" is the same voice. Taking the MIN across names would
  // let a heard artist ride back in on an unknown feature credit — and the
  // pool has exactly that row next to the four solo ones.
  const a = await app();
  const w = a.win._coverageWeight;
  a.win.userCoverage.artists = { 'otim alpha': 4 };
  const collab = w(T('a', 'Otim Alpha, Umoja', 'acholi music', 'Uganda'));
  const solo = w(T('b', 'Umoja', 'acholi music', 'Uganda'));
  assert(collab < solo,
    'a collaboration scored as freely as an unheard artist — a heard name '
    + 'must not be laundered through a feature credit');
});

test('genre and country breadth still drive the weight', async () => {
  // The artist term must not have swallowed the axes it was added beside.
  const a = await app();
  const w = a.win._coverageWeight;
  a.win.userCoverage.genres = { jazz: 200 };
  a.win.userCoverage.countries = { France: 300 };
  const worn = w(T('a', 'X', 'jazz', 'France'));
  const fresh = w(T('b', 'Y', 'gqom', 'Lesotho'));
  assert(worn < fresh / 5,
    'a heavily-played genre and country no longer lose to an unexplored one');
});

test('the picker and the look-ahead weigh tracks the same way', async () => {
  // They were two hand-copied implementations and had already drifted. The
  // look-ahead is what Spotify auto-advances through on a LOCKED phone, with
  // no JS of ours running — so a fix that landed only in the picker would
  // leave the phone doing the old thing, which is the harder case to notice.
  const src = a_src;
  const defs = src.match(/function _coverageWeight\s*\(/g) || [];
  assert(defs.length === 1, `_coverageWeight is defined ${defs.length} times`);
  const inline = src.match(/const _gap = \(t\) => \{|function _gap\(t\) \{/g) || [];
  assert(inline.length === 0,
    `${inline.length} hand-rolled copies of the weighting remain — that is `
    + 'exactly how the look-ahead drifted away from the picker before');
  const uses = src.match(/_gap = _coverageWeight/g) || [];
  assert(uses.length === 2,
    `expected the picker and the look-ahead to share it, found ${uses.length}`);
});

import { readFileSync } from 'node:fs';
const a_src = readFileSync(new URL('../web/js/app.js', import.meta.url), 'utf8');

run('discovery breadth');
