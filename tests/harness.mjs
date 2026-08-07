/**
 * Runs the WHOLE shipped browser script from web/app.html against synthetic
 * browser globals, so tests can drive real behaviour instead of asserting that
 * a substring is present.
 *
 * Why this exists
 * ───────────────
 * Every test in this repo before it matched strings against web/app.html. That
 * cannot catch a logic error and it cannot survive a refactor: one test pinned
 * the *name* of a constant, `LEGIT_ADVANCE_MIN_MS`, so it passed for a rule
 * that silently undid the user's AirPods skip, and failed the moment that rule
 * was corrected. Static assertions are a fine lock on an invariant you have
 * already reasoned out. They are not a test.
 *
 * Why not jsdom
 * ─────────────
 * The repo has no package.json and the deploy is scp, so adding a node_modules
 * tree to run tests would be the largest structural change in the change. The
 * script also touches canvas, the Spotify SDK and a dozen network endpoints on
 * load — under jsdom that is a swamp of stubbing regardless. A permissive
 * synthetic DOM gets to the same place with no dependency, and keeps the tests
 * runnable with a bare `node tests/…`.
 *
 * The deal this makes
 * ───────────────────
 * The DOM here AUTO-VIVIFIES: any element, any property, any method call
 * succeeds and returns another stub. That is deliberate — the alternative is
 * enumerating hundreds of DOM touches that have nothing to do with playback —
 * but it means the harness cannot catch a DOM bug, only a logic one. Anything
 * a test needs to *observe* must be recorded explicitly: see `fetches`,
 * `deepLinks` and `clientLogs` below. If a test would pass against a stub that
 * does nothing, it is not testing anything.
 *
 *   import { loadApp } from './harness.mjs'
 *   const app = await loadApp({ isIOS: true })
 *   app.route('/api/play', () => ({ error: 'spotify_404', no_device: true }))
 */
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));

const IOS_UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) '
  + 'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1';
const MAC_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
  + '(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36';

const IMPORT_RE =
  /^import\s+(?:([\s\S]*?)\s+from\s+)?['"](\.\/[^'"]+)['"];?[ \t]*$/gm;

/** `{ a, b as c }` / `Name` / `* as ns` → the local names it binds. */
function importedNames(clause) {
  if (!clause) return [];
  const braced = clause.match(/\{([\s\S]*)\}/);
  const names = [];
  if (braced) {
    for (const part of braced[1].split(',')) {
      const t = part.trim();
      if (t) names.push((t.split(/\s+as\s+/).pop() || t).trim());
    }
  }
  const bare = clause.replace(/\{[\s\S]*\}/, '').replace(/,/g, '').trim();
  if (bare && !bare.startsWith('*')) names.push(bare);
  return names.filter(Boolean);
}

/** Names a module makes available: `export function f`, `export { a, b }`, … */
export function exportedNames(src) {
  const names = [];
  for (const m of src.matchAll(/^export\s+(?:async\s+)?(?:function\*?|class|const|let|var)\s+([A-Za-z_$][\w$]*)/gm)) {
    names.push(m[1]);
  }
  for (const m of src.matchAll(/^export\s*\{([^}]*)\}/gm)) {
    for (const part of m[1].split(',')) {
      const t = part.trim();
      if (t) names.push((t.split(/\s+as\s+/).pop() || t).trim());
    }
  }
  return names;
}

/**
 * The whole browser module graph, flattened into one strict script in
 * dependency order, with import/export syntax removed.
 *
 * FLATTENING IS A COMPROMISE, and worth naming. Real module semantics would
 * mean vm.SourceTextModule behind --experimental-vm-modules, where only
 * EXPORTED bindings are reachable — which would be more faithful and would
 * also put every piece of app state a test drives (`allDiscovery`, `dIdx`)
 * behind an export it has no other reason to have. Flattening keeps the tests
 * driving the app the way the app drives itself.
 *
 * What it cannot catch is a broken import — a name imported but never
 * exported resolves fine once everything shares a scope. So that specific gap
 * is closed by checking it directly, here, where the graph is already parsed.
 */
export function appScript() {
  const html = readFileSync(join(ROOT, 'web/app.html'), 'utf8');
  const entries = [...html.matchAll(/<script[^>]*\ssrc="(\/js\/[^"?]+)/g)]
    .map((m) => m[1].replace(/^\//, ''));
  if (!entries.length) throw new Error('web/app.html loads no /js/ module');

  const seen = new Set();
  const ordered = [];

  const visit = (rel) => {
    if (seen.has(rel)) return;
    seen.add(rel);
    const src = readFileSync(join(ROOT, 'web', rel), 'utf8');
    const dir = rel.slice(0, rel.lastIndexOf('/'));
    for (const m of src.matchAll(IMPORT_RE)) {
      const dep = `${dir}/${m[2].replace(/^\.\//, '')}`;
      visit(dep);   // depth first: a dependency is emitted before its importer
      const depSrc = readFileSync(join(ROOT, 'web', dep), 'utf8');
      const has = new Set(exportedNames(depSrc));
      for (const name of importedNames(m[1])) {
        if (!has.has(name)) {
          throw new Error(
            `${rel} imports { ${name} } from ${m[2]}, which does not export it`);
        }
      }
    }
    ordered.push(src.replace(IMPORT_RE, '').replace(/^export\s+/gm, ''));
  };

  entries.forEach(visit);
  return ordered.join('\n;\n');
}

/**
 * `src` with line comments and block comments removed.
 *
 * The comments in this codebase quote the very constructs the tests forbid —
 * the fix and the note explaining it name the same thing — so a bare substring
 * search finds the explanation and reports the bug it prevents. Block comments
 * matter as much as line ones now that each module opens with a docstring
 * saying what it must not do.
 */
export function codeOnly(src) {
  return src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^[ \t]*\/\/.*$/gm, '');
}

/**
 * `const` and `let` at the top level of a vm script are LEXICAL — they never
 * become properties of the sandbox, so `Player`, `dIdx` and the 112 others are
 * invisible from outside while `function` declarations are not. Rather than
 * edit the shipped file to export them, append a block that runs in that same
 * scope and hands the bindings out through accessors.
 *
 * `let`/`var` get a setter as well, so a test can place the app in a state
 * (a queue, a dIdx) instead of driving it there through the UI. `const` gets a
 * getter only — assigning to one throws, and a harness that swallowed that
 * would be lying about what the app permits.
 */
function scopeEpilogue(src) {
  const seen = new Map();   // name -> 'const' | 'let'
  const re = /^(const|let|var)\s+/gm;
  for (let m; (m = re.exec(src));) {
    // Scan to the end of the logical declaration, ignoring separators nested
    // in initialisers so `let a = {x: 1}, b;` yields both names.
    let depth = 0, i = re.lastIndex, end = src.length;
    for (; i < src.length; i++) {
      const c = src[i];
      if ('([{'.includes(c)) depth++;
      else if (')]}'.includes(c)) depth--;
      else if (c === ';' && depth === 0) { end = i; break; }
      else if (c === '\n' && depth === 0
               && /^\s*(?:const|let|var|function|\/\/)/.test(src.slice(i + 1, i + 40))) {
        end = i; break;   // unterminated line followed by a new statement
      }
    }
    const decl = src.slice(re.lastIndex, end);
    // Split declarators on top-level commas, then take the identifier.
    let d = 0, part = '';
    const parts = [];
    for (const c of decl) {
      if ('([{'.includes(c)) d++;
      else if (')]}'.includes(c)) d--;
      if (c === ',' && d === 0) { parts.push(part); part = ''; } else part += c;
    }
    parts.push(part);
    for (const p of parts) {
      const name = (p.split('=')[0] || '').trim();
      if (/^[A-Za-z_$][\w$]*$/.test(name) && !seen.has(name)) {
        seen.set(name, m[1]);
      }
    }
  }
  const entries = [...seen].map(([name, kind]) => (
    kind === 'const'
      ? `  get ${name}() { return ${name}; },`
      : `  get ${name}() { return ${name}; }, set ${name}(v) { ${name} = v; },`
  ));
  return `\n;globalThis.__scope = {\n${entries.join('\n')}\n};\n`;
}

/**
 * An object that answers every property access with another one of itself and
 * every call with the same, so unrelated DOM work neither throws nor needs
 * enumerating. `classList` and `style` are given real backing so the few tests
 * that care about visible state can read it.
 */
function stubElement(id) {
  const classes = new Set();
  const target = {
    id: id || '',
    tagName: 'DIV',
    textContent: '',
    innerHTML: '',
    value: '',
    checked: false,
    dataset: {},
    style: {},
    children: [],
    classList: {
      add: (c) => classes.add(c),
      remove: (c) => classes.delete(c),
      toggle: (c, on) => (on === undefined
        ? (classes.has(c) ? classes.delete(c) : classes.add(c))
        : (on ? classes.add(c) : classes.delete(c))),
      contains: (c) => classes.has(c),
    },
    _classes: classes,
    _listeners: {},
    addEventListener(type, fn) { (this._listeners[type] ||= []).push(fn); },
    removeEventListener() {},
    dispatchEvent(ev) {
      for (const fn of this._listeners[ev && ev.type] || []) fn(ev);
      return true;
    },
    // Enough of HTMLMediaElement for the media-session anchor and Bandcamp.
    // The string-valued ones are declared rather than left to the Proxy: the
    // playback code does `(a.currentSrc || a.src || '').slice(0, 5)` to tell a
    // real stream from the silent `data:` unlock primer, and an auto-vivified
    // stub is not a string — the guard would throw instead of answering.
    // A real play() promise settles when OUTPUT STARTS, not when the call is
    // made — so a stream that never delivers leaves it pending indefinitely.
    // Resolving instantly made the harness unable to express the single most
    // damaging playback failure we have: a first load that stalls, holding the
    // caller's dispatch open. Default stays instant (every existing test relies
    // on it); set `el._stall = true` to hold the promise open instead.
    play() {
      this.paused = false;
      if (!this._stall) return Promise.resolve();
      return new Promise((_, reject) => { this._rejectPlay = reject; });
    },
    // Interrupting the load rejects a pending play() with AbortError — the
    // spec's "The play() request was interrupted by a new load request". This
    // is the contract the stall watchdog's teardown depends on, so the stub has
    // to honour it or the test that covers it proves nothing.
    pause() { this.paused = true; this._abortPendingPlay(); },
    load() { this._abortPendingPlay(); },
    _abortPendingPlay() {
      const reject = this._rejectPlay;
      if (!reject) return;
      this._rejectPlay = null;
      const err = new Error('The play() request was interrupted by a new load request.');
      err.name = 'AbortError';
      reject(err);
    },
    _stall: false,
    _rejectPlay: null,
    paused: true,
    currentTime: 0,
    duration: 0,
    src: '',
    currentSrc: '',
    error: null,
    networkState: 0,
    readyState: 0,
  };
  return new Proxy(target, {
    get(t, k) {
      if (k in t) return t[k];
      if (typeof k === 'symbol') return undefined;
      // Unknown property: a callable that is also further-navigable, since the
      // caller may want either and we cannot tell which from here.
      const fn = () => stubElement();
      return new Proxy(fn, {
        get: (f, kk) => (kk in f ? f[kk] : stubElement()),
        apply: () => stubElement(),
      });
    },
    set(t, k, v) { t[k] = v; return true; },
    has() { return true; },
  });
}

/**
 * Boot the app script.
 *
 * @param {object} opts
 * @param {boolean} opts.isIOS       present an iPhone user agent
 * @param {boolean} opts.guest       set the dig_mode=guest cookie
 * @param {number}  opts.now         starting value for the fake clock, ms
 */
export async function loadApp(opts = {}) {
  const { isIOS = false, guest = false, now = 1_750_000_000_000 } = opts;

  const elements = new Map();
  const audios = [];   // every <audio> the app created, in creation order
  const getEl = (id) => {
    if (!elements.has(id)) elements.set(id, stubElement(id));
    return elements.get(id);
  };

  // ── Observable side effects ───────────────────────────────────────────────
  const fetches = [];      // every URL the app requested, in order
  const deepLinks = [];    // every window.location.href = 'spotify:…'
  const clientLogs = [];   // {tag, msg, data} — the app's own telemetry
  const routes = [];       // [{test, handler}] most-recently-added wins

  function route(pattern, handler) {
    routes.unshift({ pattern, handler });
  }
  // The app probes /api/devices at BOOT, before any test has registered its
  // routes, so the unrouted default is now load-bearing. Neither obvious answer
  // is neutral: `{}` records "Spotify is absent" as a fact in every test, and a
  // working device sets everSawDevice everywhere (which the banner-copy tests
  // read). So the default FAILS — the probe's catch path touches no state — and
  // a test that cares about device liveness says so explicitly.
  route('/api/devices', () => { throw new Error('this test registered no /api/devices route'); });
  function respond(url) {
    for (const r of routes) {
      const hit = typeof r.pattern === 'string' ? url.startsWith(r.pattern)
        : r.pattern instanceof RegExp ? r.pattern.test(url)
        : r.pattern(url);
      if (hit) return r.handler(url);
    }
    return {};
  }

  // ── Fake clock ────────────────────────────────────────────────────────────
  // Real timers would make every test a race. Time only moves when a test says
  // so, and advancing it runs the callbacks that were due.
  let clock = now;
  let seq = 0;
  const timers = new Map();   // id -> {at, fn, every}

  const setTimeoutFn = (fn, ms = 0) => {
    const id = ++seq;
    timers.set(id, { at: clock + ms, fn, every: null });
    return id;
  };
  const setIntervalFn = (fn, ms = 0) => {
    const id = ++seq;
    timers.set(id, { at: clock + ms, fn, every: Math.max(1, ms) });
    return id;
  };
  const clearTimerFn = (id) => { timers.delete(id); };

  /**
   * Move the clock, firing due callbacks in time order. Awaits microtasks.
   *
   * Bounded, and it throws rather than warns: a runaway loop in the app is the
   * single most valuable thing this harness can find, and the first version of
   * this function let one allocate until node died with a 4 GB heap dump. A
   * test that hangs the machine reports nothing.
   */
  async function tick(ms = 0, maxSteps = 20000) {
    const until = clock + ms;
    let steps = 0;
    for (;;) {
      checkStalled();
      if (++steps > maxSteps) {
        throw new Error(
          `timer loop did not settle: ${maxSteps} callbacks in ${ms}ms of app time `
          + `(${timers.size} still pending) — the app is looping`);
      }
      let next = null;
      for (const [id, t] of timers) {
        if (t.at <= until && (next === null || t.at < timers.get(next).at)) next = id;
      }
      if (next === null) break;
      const t = timers.get(next);
      clock = t.at;
      if (t.every) t.at = clock + t.every; else timers.delete(next);
      try { t.fn(); } catch (e) { /* a throwing timer is the app's business */ }
      await flush();
    }
    clock = until;
    await flush();
    checkStalled();   // the storm may have started during that last flush
  }

  function checkStalled() {
    if (!stalled) return;
    throw new Error(
      `the app made ${fetches.length} requests without settling — it is looping `
      + `through promise callbacks (last: ${fetches[fetches.length - 1].url})`);
  }

  /** Let pending promise callbacks run without moving the clock. */
  const flush = () => new Promise((r) => setImmediate(r));

  // ── Browser globals ───────────────────────────────────────────────────────
  const storage = new Map();
  const localStorage = {
    getItem: (k) => (storage.has(k) ? storage.get(k) : null),
    setItem: (k, v) => storage.set(k, String(v)),
    removeItem: (k) => storage.delete(k),
    clear: () => storage.clear(),
    key: (i) => [...storage.keys()][i] ?? null,
    get length() { return storage.size; },
  };

  const documentListeners = {};
  const document = new Proxy({
    cookie: guest ? 'dig_mode=guest' : '',
    visibilityState: 'visible',
    hidden: false,
    title: '',
    readyState: 'complete',
    documentElement: stubElement('html'),
    body: stubElement('body'),
    head: stubElement('head'),
    getElementById: (id) => getEl(id),
    querySelector: (sel) => getEl(sel),
    querySelectorAll: () => [],
    getElementsByClassName: () => [],
    getElementsByTagName: () => [],
    createElement: (tag) => {
      const el = stubElement();
      el.tagName = String(tag).toUpperCase();
      // Audio elements are kept because the playback code creates them itself
      // rather than reading them out of the markup — the Bandcamp backend and
      // the media-session anchor each build their own — so a test that needs to
      // raise an `error` or an `ended` on one has no other way to reach it.
      if (el.tagName === 'AUDIO') audios.push(el);
      return el;
    },
    createElementNS: () => stubElement(),
    createTextNode: () => stubElement(),
    addEventListener: (type, fn) => { (documentListeners[type] ||= []).push(fn); },
    removeEventListener: () => {},
    dispatchEvent: (ev) => {
      for (const fn of documentListeners[ev && ev.type] || []) fn(ev);
      return true;
    },
    _listeners: documentListeners,
  }, {
    get: (t, k) => (k in t ? t[k] : stubElement()),
    set: (t, k, v) => { t[k] = v; return true; },
  });

  const windowListeners = {};
  const locationTarget = {
    href: 'https://diiiiiiiig.xyz/',
    protocol: 'https:', host: 'diiiiiiiig.xyz', hostname: 'diiiiiiiig.xyz',
    pathname: '/', search: '', hash: '', origin: 'https://diiiiiiiig.xyz',
    replace() {}, reload() {}, assign() {},
  };
  const location = new Proxy(locationTarget, {
    set(t, k, v) {
      // The deep link IS an href assignment. This is the single most important
      // thing the harness observes: "was the user thrown into Spotify?"
      if (k === 'href') deepLinks.push(String(v));
      t[k] = v;
      return true;
    },
    get: (t, k) => t[k],
  });

  const navigator = {
    userAgent: isIOS ? IOS_UA : MAC_UA,
    platform: isIOS ? 'iPhone' : 'MacIntel',
    language: 'en-GB',
    maxTouchPoints: isIOS ? 5 : 0,
    onLine: true,
    mediaSession: {
      metadata: null,
      playbackState: 'none',
      _handlers: {},
      setActionHandler(action, fn) { this._handlers[action] = fn; },
      setPositionState() {},
    },
    sendBeacon: () => true,
    clipboard: { writeText: () => Promise.resolve() },
  };

  // A runaway that recurses through PROMISE callbacks — play → fail → advance →
  // play — never yields to the timer queue, so the step bound in tick() never
  // gets a turn and node dies of a 4 GB heap instead of reporting a failure.
  // Every such loop has to touch the network, so the network is where to stand.
  //
  // Rejecting would not work: the app catches its own fetch errors, and a
  // rejection just feeds the next iteration. A promise that never settles parks
  // the loop instead, which hands control back to tick() to fail properly.
  let stalled = false;
  const FETCH_BUDGET = 3000;

  const fetchFn = (url, init) => {
    const u = String(url);
    fetches.push({ url: u, init });
    if (fetches.length > FETCH_BUDGET) {
      stalled = true;
      return new Promise(() => {});
    }
    let body;
    try { body = respond(u); } catch (e) { return Promise.reject(e); }
    if (body instanceof Error) return Promise.reject(body);
    const status = (body && body.__status) || 200;
    return Promise.resolve({
      ok: status >= 200 && status < 300,
      status,
      url: u,
      headers: { get: () => null },
      json: () => Promise.resolve(body),
      text: () => Promise.resolve(JSON.stringify(body)),
      blob: () => Promise.resolve({}),
      arrayBuffer: () => Promise.resolve(new ArrayBuffer(0)),
    });
  };

  const sandbox = {
    document, navigator, location, localStorage,
    sessionStorage: localStorage,
    fetch: fetchFn,
    console: { log() {}, warn() {}, error() {}, info() {}, debug() {} },
    setTimeout: setTimeoutFn,
    setInterval: setIntervalFn,
    clearTimeout: clearTimerFn,
    clearInterval: clearTimerFn,
    requestAnimationFrame: (fn) => setTimeoutFn(() => fn(clock), 16),
    cancelAnimationFrame: clearTimerFn,
    queueMicrotask: (fn) => Promise.resolve().then(fn),
    Date: new Proxy(Date, {
      construct: (T, args) => (args.length ? new T(...args) : new T(clock)),
      get: (T, k) => (k === 'now' ? () => clock : T[k]),
    }),
    performance: { now: () => clock - now },
    Audio: function Audio() { return stubElement(); },
    Image: function Image() { return stubElement(); },
    AudioContext: function AudioContext() { return stubElement(); },
    webkitAudioContext: function webkitAudioContext() { return stubElement(); },
    MediaMetadata: function MediaMetadata(init) { Object.assign(this, init); },
    URL: globalThis.URL,
    URLSearchParams: globalThis.URLSearchParams,
    Blob: function Blob() { return {}; },
    FormData: function FormData() { return { append() {} }; },
    Headers: globalThis.Headers,
    AbortController: globalThis.AbortController,
    IntersectionObserver: function IO() {
      return { observe() {}, unobserve() {}, disconnect() {} };
    },
    ResizeObserver: function RO() {
      return { observe() {}, unobserve() {}, disconnect() {} };
    },
    matchMedia: () => ({ matches: false, addListener() {}, addEventListener() {} }),
    getComputedStyle: () => ({ getPropertyValue: () => '' }),
    alert() {}, confirm: () => true, prompt: () => null,
    scrollTo() {}, open: () => null,
    addEventListener: (type, fn) => { (windowListeners[type] ||= []).push(fn); },
    removeEventListener: () => {},
    dispatchEvent: (ev) => {
      for (const fn of windowListeners[ev && ev.type] || []) fn(ev);
      return true;
    },
    Spotify: undefined,   // the SDK never loads; the Connect path is what we test
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  sandbox.self = sandbox;
  sandbox.top = sandbox;
  sandbox.parent = sandbox;

  const context = vm.createContext(sandbox);
  const src = appScript();
  // STRICT, because the shipped code is loaded as a module and modules always
  // are. The difference is not academic: assigning to an undeclared variable is
  // a silent implicit global in sloppy mode and a ReferenceError in strict, so
  // a harness that ran sloppy would pass a build the browser refuses.
  //
  // It is not a perfect stand-in — top-level `function` declarations still
  // become globals in a strict SCRIPT and would not in a module — but that
  // divergence is what makes the app's own functions reachable to drive, and it
  // errs toward the harness seeing more than the browser does, never less.
  vm.runInContext('"use strict";\n' + src + scopeEpilogue(src), context,
    { filename: 'web/js/app.js' });

  // One view over both halves of the app's scope: sandbox globals (function
  // declarations, anything var-like) and the lexical bindings the epilogue
  // exposed. Tests should not have to know which kind a name happens to be.
  const scope = sandbox.__scope || {};
  const win = new Proxy(sandbox, {
    get: (t, k) => (k in scope ? scope[k] : t[k]),
    set(t, k, v) {
      if (k in scope) {
        const d = Object.getOwnPropertyDescriptor(scope, k);
        if (!d.set) throw new Error(`${String(k)} is a const — the app cannot assign to it either`);
        scope[k] = v;
        return true;
      }
      t[k] = v;
      return true;
    },
    has: (t, k) => k in scope || k in t,
  });

  // Capture the app's own telemetry. clientLog is how the app narrates its
  // decisions, so a test can assert on the decision rather than on its effects.
  const realClientLog = sandbox.clientLog;
  sandbox.clientLog = (tag, msg, data) => {
    clientLogs.push({ tag, msg, data });
    try { return realClientLog && realClientLog(tag, msg, data); } catch (e) {}
  };

  await flush();

  return {
    /** The script's global scope — every top-level binding is reachable here. */
    win,
    /** Element by id, auto-created; same object the app sees. */
    el: getEl,
    /** Every <audio> the app built, in creation order. */
    audios,
    route,
    fetches,
    deepLinks,
    clientLogs,
    tick,
    flush,
    now: () => clock,
    /** URLs requested since the last call, for asserting on one action. */
    since(n) { return fetches.slice(n); },
    /**
     * Every /api/play URL, in order. Lives here rather than in one fixture
     * because "what did DIG actually dispatch, and in what order" is the
     * question most of these tests are really asking.
     */
    playUrls: () => fetches.filter((f) => f.url.startsWith('/api/play'))
      .map((f) => f.url),
    /** The first track id of each dispatch — the one DIG meant to play. */
    playedIds: () => fetches.filter((f) => f.url.startsWith('/api/play'))
      .map((f) => decodeURIComponent(
        (f.url.match(/tracks=([^&]*)/) || [, ''])[1].split('%2C')[0].split(',')[0])),
    /** Fired when the app asked for a client-log line matching `msg`. */
    logged(msg) {
      return clientLogs.filter((l) => String(l.msg).includes(msg));
    },
    /** Simulate a document event (visibilitychange, etc.). */
    emit(type, ev = {}) {
      document.dispatchEvent({ type, ...ev });
      sandbox.dispatchEvent({ type, ...ev });
    },
  };
}

// ── Tiny test runner, matching the style of the .py suites ──────────────────
const tests = [];
export function test(name, fn) { tests.push([name, fn]); }

export async function run(label) {
  let failed = 0;
  for (const [name, fn] of tests) {
    try {
      await fn();
      console.log(`ok   ${name}`);
    } catch (e) {
      failed++;
      console.log(`FAIL ${name}: ${e && e.message}`);
      if (process.env.VERBOSE) console.log(e && e.stack);
    }
  }
  if (failed) {
    console.log(`\n${failed} failed`);
    process.exit(1);
  }
  console.log(`all ${label} passed (${tests.length} tests)`);
}

export function assert(cond, msg) {
  if (!cond) throw new Error(msg || 'assertion failed');
}
export function equal(a, b, msg) {
  if (a !== b) throw new Error(`${msg || 'not equal'}: ${JSON.stringify(a)} !== ${JSON.stringify(b)}`);
}
