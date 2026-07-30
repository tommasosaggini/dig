/**
 * Regression lock for the Bandcamp play() race (worklist row 161:
 * "AbortError: The operation was aborted", Player.play returned ok=false).
 *
 * There is ONE <audio> element. bandcamp.play() awaits a resolve fetch before
 * assigning src, so a user skipping mid-load starts a second play() that steals
 * the element; the first one's play() promise then rejects with AbortError and
 * the old code filed that as a playback failure. It isn't — it's the queue
 * working. These tests pin that a superseded attempt is silent and, crucially,
 * that it never overwrites the newer track's src.
 *
 * "Silent" includes the RETURN VALUE, which is why these expect SUPERSEDED and
 * not false. playCurrentTrack answers false with a 700ms retry of whatever is
 * current by then — the newer track — and a strike against the 3-fail circuit
 * breaker, so returning false made three fast skips stop playback outright with
 * "playback error — try refreshing". Only a genuine failure may return false.
 *
 * The play() body is EXTRACTED FROM web/app.html at run time and executed
 * against stubs, so this tests the shipped source, not a copy of it.
 *
 *   node tests/test_bandcamp_supersede.mjs
 */
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const html = readFileSync(join(ROOT, 'web/app.html'), 'utf8');

// ── Extract `async play(track) { … }` by brace-matching from app.html ──
const start = html.indexOf('    async play(track) {');
if (start < 0) throw new Error('could not find bandcamp play() in web/app.html');
let depth = 0, end = -1;
for (let i = html.indexOf('{', start); i < html.length; i++) {
  if (html[i] === '{') depth++;
  else if (html[i] === '}' && --depth === 0) { end = i + 1; break; }
}
const playSrc = html.slice(start, end);
if (!/_playSeq/.test(playSrc)) throw new Error('play() has no _playSeq guard — fix reverted?');

// The sentinel is declared at module scope in app.html, outside the extracted
// slice. Read its value from the source rather than hardcoding it, so a rename
// there surfaces here instead of silently passing against a stale literal.
const SUP_M = html.match(/const SUPERSEDED = '([^']+)'/);
if (!SUP_M) throw new Error('could not find the SUPERSEDED sentinel in web/app.html');
const SUPERSEDED = SUP_M[1];

// ── Stubs: only what play() actually touches ──────────────────────────────
function makeHarness({ resolveDelay = 0, playDelay = 0, playRejects = null } = {}) {
  const logs = [];
  const audio = {
    _src: null, paused: true, readyState: 4, currentSrc: '',
    set src(v) { this._src = v; }, get src() { return this._src; },
    play() {
      return new Promise((res, rej) =>
        setTimeout(() => (playRejects ? rej(playRejects) : res()), playDelay));
    },
  };
  const bandcamp = {
    audio, _playSeq: 0, _dur: 0, _loadedId: null, _unlocked: true,
    init() {}, _clearStallWatch() {}, _armStallWatch() {},
  };
  const ctx = {
    bandcamp, activeSource: null,
    spotify: { stop() {} },
    clientLog: (tag, msg, data, opts) => logs.push({ tag, msg, data, opts }),
    paintArt: () => {},
    _startPoll: () => {},
    // Echo back the id that was asked for, so a test can prove which track's
    // URL actually landed on the element.
    fetch: (u) => new Promise(res => setTimeout(() => res({
      json: async () => ({
        ok: true,
        url: 'https://cdn/' + decodeURIComponent(new URL(u, 'http://x').searchParams.get('id')),
        art: null,
      }),
    }), resolveDelay)),
  };
  // Build the real method inside a scope that supplies those names.
  const factory = new Function(
    'bandcamp', 'spotify', 'clientLog', 'paintArt', '_startPoll', 'fetch', 'ctx',
    'SUPERSEDED',
    `let activeSource = null;
     const o = { ${playSrc} };
     return (t) => o.play(t);`
  );
  bandcamp.play = factory(bandcamp, ctx.spotify, ctx.clientLog, ctx.paintArt,
                          ctx._startPoll, ctx.fetch, ctx, SUPERSEDED);
  return { bandcamp, audio, logs };
}

let failures = 0;
const ok = (cond, m) => { if (!cond) { console.error('FAIL: ' + m); failures++; process.exitCode = 1; } };
// Only claim a check passed if nothing inside that block failed.
let mark = 0;
const done = (m) => { if (failures === mark) console.log('ok   ' + m); mark = failures; };

// ── 1. A lone play() still works ──────────────────────────────────────────
{
  const { bandcamp, audio, logs } = makeHarness();
  const r = await bandcamp.play({ id: 'bc:1', name: 'A' });
  ok(r === true, 'an uncontested play() should return true');
  ok(audio.src === 'https://cdn/bc:1', `expected the requested track, got ${audio.src}`);
  ok(!logs.some(l => l.msg.includes('superseded')), 'a lone play must not report superseded');
  done('uncontested play() loads and returns true');
}

// ── 2. Skip during resolve: loser must not touch the element ──────────────
{
  const { bandcamp, audio, logs } = makeHarness({ resolveDelay: 20 });
  const first = bandcamp.play({ id: 'bc:OLD', name: 'old' });
  await new Promise(r => setTimeout(r, 5));            // user skips mid-resolve
  const second = bandcamp.play({ id: 'bc:NEW', name: 'new' });
  const [r1, r2] = await Promise.all([first, second]);

  ok(r1 === SUPERSEDED, `the superseded play() should return SUPERSEDED, got ${r1}`);
  ok(r1 !== false, 'it must NOT return false — that triggers the retry/circuit breaker');
  ok(r2 === true, 'the winning play() should return true');
  ok(audio.src === 'https://cdn/bc:NEW', `element must hold the NEW track, got ${audio.src}`);
  ok(bandcamp._loadedId === 'bc:NEW', `_loadedId must be the NEW track, got ${bandcamp._loadedId}`);
  const sup = logs.filter(l => l.msg.includes('superseded'));
  ok(sup.length === 1, `expected one superseded log, got ${sup.length}`);
  ok(sup[0]?.opts?.transient === true, 'superseded log must be marked transient');
  done('skip during resolve → loser silent, newer track owns the element');
}

// ── 3. Skip during load: AbortError must not be filed as a failure ─────────
{
  const abort = Object.assign(new Error('The operation was aborted.'), { name: 'AbortError' });
  const { bandcamp, logs } = makeHarness({ playDelay: 20, playRejects: abort });
  const first = bandcamp.play({ id: 'bc:OLD', name: 'old' });
  await new Promise(r => setTimeout(r, 5));            // skip while play() pends
  bandcamp._playSeq++;                                  // a newer attempt claims it
  const r1 = await first;

  ok(r1 === SUPERSEDED, `a superseded play() should return SUPERSEDED, got ${r1}`);
  ok(r1 !== false, 'it must NOT return false — that triggers the retry/circuit breaker');
  ok(!logs.some(l => l.msg === 'audio.play rejected'),
     'a superseded AbortError must NOT be logged as "audio.play rejected"');
  ok(logs.some(l => l.msg.includes('superseded') && l.opts?.transient),
     'it should log a transient superseded event instead');
  done('skip during load → AbortError not filed as a playback failure');
}

// ── 4. A genuine failure is still reported ────────────────────────────────
{
  const boom = Object.assign(new Error('no supported source'), { name: 'NotSupportedError' });
  const { bandcamp, logs } = makeHarness({ playRejects: boom });
  const r = await bandcamp.play({ id: 'bc:1', name: 'A' });
  ok(r === false, 'a real playback failure returns false');
  ok(logs.some(l => l.msg === 'audio.play rejected'),
     'a real failure with no newer attempt MUST still be reported');
  done('genuine play failure still reported');
}

if (!process.exitCode) console.log('\nall bandcamp supersede checks passed');
