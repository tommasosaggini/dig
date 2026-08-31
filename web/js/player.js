/**
 * Playback. Wraps Spotify (Web SDK on desktop, Connect on iPhone) and Bandcamp
 * behind one interface, so the rest of the app never branches on source.
 *
 * ── The seam ──────────────────────────────────────────────────────────────
 * This module REPORTS and ASKS; it does not reach. Five things it needs from
 * whoever owns the queue arrive through `Player.wire()`, and until they do
 * they are inert no-ops. That is what lets the queue import the player without
 * the player importing the queue back.
 *
 * The five were previously bare references to `currentTrack`, `playCurrentTrack`,
 * `_tryConsumePendingPlay`, `allDiscovery` and `dIdx` — three of them written
 * `typeof x === 'function' ? x() : null`, which is not defensiveness but a
 * load-order guess: this code ran inside one 7,500-line script where those
 * might or might not have been defined yet. A declared seam answers that
 * question instead of hedging it.
 *
 * `rebaseQueueTo` is deliberately one call rather than a findIndex and an
 * assignment. Reaching into `allDiscovery` and `dIdx` from here is exactly the
 * coupling that made the queue impossible to reason about — the player would
 * move the queue's cursor and the queue would never know why.
 */
import { DIG_IS_IOS, DIG_GUEST } from './env.js';
import { clientLog, dbg } from './log.js';
import { SpotifyDevice } from './device.js';
import { paintArt, setArtFallback, paintTrackInfo, digPaintProgressInstant, pbarLog, markSkip }
  from './ui.js';

/** How many extra tracks ride along on a Connect play. See Player.play. */
export const DIG_CONNECT_LOOKAHEAD = 24;

/**
 * What the player needs from the queue. Filled in by Player.wire(); the
 * defaults keep the player usable — and testable — with no queue at all.
 *
 * Named `queue` rather than `host` because there will be more than one of these
 * across the app, and two modules both calling their collaborator `host` is a
 * duplicate declaration the moment anything flattens them into one scope.
 */
const queue = {
  /** The track the app currently intends to be playing, or null. */
  currentTrack: () => null,
  /** Re-dispatch that track. Used to recover from silent playback. */
  playCurrentTrack: () => {},
  /** Follow the queue to `trackId` if it is in it. Returns whether it was. */
  rebaseQueueTo: () => false,
  /** A play was requested before the player was ready — take it now. */
  tryConsumePendingPlay: () => {},
  /** The next `k` tracks, for the Connect look-ahead context. */
  peekNextContext: () => [],
  /** Tracks ahead of the cursor the listener has not heard. */
  upcomingUnheard: () => [],
  /** Was this track played recently enough to be on the navigation stack? */
  wasRecentlyPlayed: () => false,
  /**
   * Repaint the heart / dislike buttons for `trackId`. The poll owns the card
   * on a natural advance, and a card is title + cover + REACTIONS — reported
   * 2026-08-05: liked a Kali Uchis track, it ended by itself, and the next
   * song came up with the heart still filled. That is the same "half a card"
   * defect the 2026-08-02 audit found between title and cover; the buttons
   * were simply never part of the set. The state lives in app.js history, so
   * the poll asks rather than deriving it.
   */
  repaintReactions: () => {},
  /**
   * Spotify moved to `trackId` by itself — make the queue agree and record the
   * play. Returns the track object that is now current.
   */
  adoptExternalTrack: (trackId, opts) => (opts && opts.stub) || { id: trackId },
};

// Wraps Spotify + Bandcamp (+ future sources) behind one interface.
// The rest of the app only talks to `Player`.

// Third outcome of a play attempt, alongside true (playing) and false (this
// track will not play). Means: a newer play() took the audio element while this
// one was still loading. The caller must do nothing — no retry, no skip, no
// failure counter — because the newer track is already playing correctly.
const SUPERSEDED = 'superseded';

// Fourth outcome: the track was handed to the Spotify app via deep link and we
// do NOT know whether it started. This used to return plain `true` — commented
// "treat as success since Spotify app will play it" — which is only true when
// the phone is unlocked and Spotify is alive. On a locked phone the deep link
// does nothing, and the false success meant playCurrentTrack never ran its
// failure path, so DIG sat on a silent track forever instead of moving to one
// that plays. The caller must confirm playback actually began.
const DEEPLINK = 'deeplink';

// Fifth outcome: this track will not play and no retry will change that —
// Spotify has no device and the one-shot handshake is already spent. Distinct
// from plain `false`, which means "failed, and a warm-up retry is worth a go".
//
// The distinction exists because ONE OWNER MOVES THE QUEUE, and it is
// playCurrentTrack. Player.play used to advance by itself here (calling
// _onTrackEnd) and ALSO return false, so the caller handled the same failure a
// second time: its 700ms "device may be warming up" retry fired against a dIdx
// the first advance had already moved, dispatching the NEXT track instead of
// re-trying this one — which failed the same way, advanced again, and armed
// another retry. Measured 2026-08-01, 22:16:05 → 22:16:09: one 404 became three
// Spotify tracks dispatched and abandoned in 3.7s, each painting its title on
// the way past. That is the "titles come and go" of a failed handshake.
//
// So Player.play now only ever REPORTS. Anything that reads like "I already did
// the caller's job" belongs on this list as a fact instead.
const UNPLAYABLE = 'unplayable';
// How long to give the Spotify app to come up and start before giving up on it.
const _DEEPLINK_CONFIRM_MS = 6000;

// How long to let a just-woken Spotify app settle before re-issuing a play it
// answered with a 5xx. The server already sleeps 0.4s after transferring to a
// sleeping device, which is enough for one that was merely backgrounded and
// not enough for one that iOS had evicted: measured 2026-07-31, a cold-started
// app still 502'd 4.9s after the wake.
const _WOKEN_DEVICE_SETTLE_MS = 1500;

/**
 * A TIMER IS NOT A PLAN WHEN THE PAGE CAN BE FROZEN.
 *
 * The 5xx retry above waits _WOKEN_DEVICE_SETTLE_MS and then re-issues. If the
 * page goes hidden inside that gap, iOS freezes the timer and the retry never
 * happens — not late, never. Measured 2026-08-02: the page went hidden at
 * 06:16:48.217, the retry was announced at 06:16:50.496, and the log recorded
 * NOTHING for the next three minutes. Bandcamp was already paused for the
 * handover, so that is silence with no way out but a reload.
 *
 * Awaiting visibility instead would leave Player.play pending indefinitely and
 * wedge the skip that the listener would reach for. So: abandon the attempt,
 * report UNPLAYABLE like any other failure, and arm a ONE-SHOT re-dispatch for
 * the moment the page can run again. Same lesson as _confirmDeepLink — never
 * judge, and now never schedule, while hidden.
 */
let _rearmed = false;
function _retryWhenVisible(why) {
  if (_rearmed) return;
  _rearmed = true;
  document.addEventListener('visibilitychange', function _onVis() {
    if (document.visibilityState !== 'visible') return;
    document.removeEventListener('visibilitychange', _onVis);
    _rearmed = false;
    clientLog('connect', 'retrying the play the freeze swallowed', { why });
    queue.playCurrentTrack();
  });
}

// Bandcamp plays in-browser through the <audio> backend and never touches
// Connect, so it is unaffected by the Spotify app's state. Same test the
// look-ahead invariant uses — keep them in step.
function _isBandcampTrack(t) {
  return !!t && (t.source === 'bandcamp' || String(t.id || '').startsWith('bc:'));
}

// The source a track will actually play from, as a string. Three places were
// each deriving this their own way — addToHistory's persisted `source` field,
// _isBandcampTrack here, and now the picker's source-balance term — and a
// picker that disagreed with the history writer about what it had just served
// would balance against the wrong tally. Defined ON TOP of _isBandcampTrack
// rather than beside it so the two can never drift apart.
//
// The 'bc:' id form is the fallback because a track stub built from a search
// result may carry no .source at all, and that form is assigned at ingestion —
// it is the one thing every Bandcamp track is guaranteed to have.
function _trackSource(t) {
  if (!t) return 'spotify';
  if (_isBandcampTrack(t)) return 'bandcamp';
  const s = String(t.source || '').trim().toLowerCase();
  return s || 'spotify';
}

const Player = (() => {
  let activeSource = null;   // 'spotify' | 'bandcamp'
  // Consecutive Bandcamp streams that failed to load. A RUN, not a total —
  // cleared the moment output starts. See the 'error' handler: without a bound,
  // a bad minute at the CDN walks the whole queue in silence.
  let _bandcampErrorRun = 0;
  const _BANDCAMP_ERROR_RUN_LIMIT = 5;
  let _onTrackEnd = null;
  let _onStateChange = null;

  // ── Spotify backend ──
  // Surface a clear, clickable "reconnect Spotify" affordance and halt the
  // silent auth-retry loop. Used when the SDK can't authenticate — almost
  // always a scope-narrowed token (missing `streaming`). /reconnect forces a
  // fresh consent dialog that re-grants the full scope set.
  function _promptReconnect(msg) {
    const s = document.getElementById('player-status');
    if (s) {
      s.textContent = (msg || 'reconnect Spotify') + ' →';
      s.style.cursor = 'pointer';
      s.title = 'Your Spotify connection is missing playback permission. Click to reconnect.';
      s.onclick = () => { window.location.href = '/reconnect'; };
    }
    const banner = document.getElementById('reconnect-banner');
    if (banner && !banner.classList.contains('visible')) {
      banner.classList.add('visible');
      clientLog('spotify', 'reconnect banner shown');
    }
  }

  // Drop the live player so init()'s guard will build ONE fresh replacement.
  // Extracted because there are now three ways a player can be dead — auth
  // rejected it, connect() refused, or 'ready' never arrived — and every one
  // of them has to leave EXACTLY this state behind or the guard either skips
  // forever (player still set) or leaks a second listener set (the storm the
  // guard exists to prevent). One teardown, one meaning.
  function _teardownPlayer(why) {
    clientLog('spotify', 'tearing down dead player', { why });
    try { spotify.player && spotify.player.disconnect(); } catch (e) {}
    spotify.player = null;
    spotify.ready = false;
    spotify.deviceId = null;
    spotify._lastReadyId = null;
    if (spotify._readyWatchdog) {
      clearTimeout(spotify._readyWatchdog);
      spotify._readyWatchdog = null;
    }
  }

  // Status strings that describe something IN PROGRESS. Once audio is out of
  // the speakers every one of them is a lie, so any of them may be cleared by
  // whichever source actually started playing.
  //
  // A whitelist, not a blanket clear, because #player-status is shared with
  // STICKY mode labels — 'tailored', 'AI mix', '🛫 …' — that must survive.
  // The old Spotify-side clear guarded only against 'tailored' and would have
  // wiped the other two.
  const _TRANSIENT_STATUS = [
    'loading tracks', 'connecting spotify', 'offline', '...', '…',
    'having trouble loading tracks',
  ];

  // Clear the status iff it is one of those. Called from BOTH backends the
  // moment output actually starts.
  //
  // Reported 2026-08-17 as "I still see 'loading tracks' although I'm using
  // the tool normally", and it was true — 7,991 tracks were loaded and
  // playing. The label was set by handlePlay() during the one moment the pool
  // was empty, and every path that cleared it ran through the Spotify SDK:
  // `player_state_changed` (never fires for Bandcamp) and consume(), which
  // returns at `if (!Player.isReady())` BEFORE reaching its clear. So a
  // listener on a healthy Bandcamp-only session was told the app was still
  // loading, forever. A status owned by one backend cannot describe an app
  // with two.
  function _clearTransientStatus() {
    const s = document.getElementById('player-status');
    if (!s) return;
    const txt = (s.textContent || '').trim().toLowerCase();
    if (!txt) return;
    if (_TRANSIENT_STATUS.some(t => txt.includes(t))) s.textContent = '';
  }

  // Arm (or re-arm) the "no device announced" detector. Used at connect time
  // and again whenever a live device goes away, because those are the same
  // question — is a 'ready' coming? — asked at two different moments.
  function _armReadyWatchdog(why) {
    if (spotify._readyWatchdog) clearTimeout(spotify._readyWatchdog);
    spotify._readyWatchdog = setTimeout(() => {
      spotify._readyWatchdog = null;
      if (spotify.ready) return;               // it arrived; nothing to do
      clientLog('spotify', 'ready never arrived — rebuilding',
        { why, afterMs: SDK_READY_TIMEOUT_MS, rebuilds: spotify._readyRebuilds || 0 });
      _teardownPlayer(why);
      _scheduleSdkRebuild(why);
    }, SDK_READY_TIMEOUT_MS);
  }

  // Rebuild once, after a beat, and only so many times. The counter is what
  // keeps this from becoming the init storm: a token or account that can never
  // register a device would otherwise rebuild forever. Cleared on a successful
  // 'ready', so a session that recovers gets its full allowance back later.
  function _scheduleSdkRebuild(why) {
    spotify._readyRebuilds = (spotify._readyRebuilds || 0) + 1;
    if (spotify._readyRebuilds > SDK_READY_MAX_REBUILDS) {
      clientLog('spotify', 'SDK will not register a device — prompting reconnect',
        { why, attempts: spotify._readyRebuilds });
      _promptReconnect('reconnect Spotify');
      return;
    }
    setTimeout(() => spotify.init(), 2000);
  }

  // Wake anything waiting for a device. Called from the 'ready' listener on
  // BOTH of its paths — including the duplicate-device_id early return, since
  // a reconnect re-announces the SAME id (measured: the id survived a full
  // page reload), and a waiter that only heard about NEW ids would hang
  // through exactly the recovery it was waiting for.
  function _notifyReadyWaiters(device_id) {
    const waiters = spotify._readyWaiters || [];
    spotify._readyWaiters = [];
    waiters.forEach(fn => { try { fn(device_id); } catch (e) {} });
  }

  // Resolve with a device_id once one is registered, or null if none arrives
  // in `ms`. There is no SDK call that answers "are you registered?" — 'ready'
  // is the only signal — so waiting for the event is the whole mechanism.
  function _awaitReady(ms) {
    if (spotify.ready && spotify.deviceId) return Promise.resolve(spotify.deviceId);
    if (ms <= 0) return Promise.resolve(null);
    return new Promise(resolve => {
      let done = false;
      const timer = setTimeout(() => {
        if (done) return;
        done = true;
        resolve(null);
      }, ms);
      (spotify._readyWaiters || (spotify._readyWaiters = [])).push(id => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        resolve(id);
      });
    });
  }

  // Put OUR OWN device back, after Spotify has told us it is gone.
  //
  // Why this exists. There was already a repair path for a dead device —
  // 'not_ready' → _armReadyWatchdog → _teardownPlayer → rebuild — and it has
  // never run. Not once: `SDK not_ready event` appears ZERO times in the whole
  // retained server log. Spotify drops an idle Connect device server-side
  // without the SDK emitting anything, so `spotify.ready` and `_sdkRegistered`
  // both stay true and play() dispatches at a device that no longer exists.
  // The repair was gated on an event that never arrives.
  //
  // The 404 body "Device not found" IS the missing signal: it is Spotify
  // stating the fact, observed rather than predicted. So it drives the repair.
  //
  // Measured 2026-08-31 12:41 JST: 25 minutes of Bandcamp with the tab mostly
  // hidden, then a skip to a Spotify track — five 404s across three tracks in
  // four seconds, `server_devices: []`, and the only advice on screen was to
  // go open Spotify on another machine. Reloading the page fixed it instantly,
  // because a reload calls player.connect() — which is all this does, without
  // costing the queue, the history, or the user's place in it.
  const DEVICE_RECOVERY_BUDGET_MS = 10000;

  function _recoverOwnDevice(why) {
    // Single-flight. That incident produced five failures in four seconds; N
    // concurrent skips must share ONE reconnect, not race N of them into the
    // same player.
    if (spotify._recovering) return spotify._recovering;

    spotify._recovering = (async () => {
      try {
      clientLog('spotify', 'device gone — putting ours back', {
        why, deadDeviceId: (spotify.deviceId || '').slice(0, 12),
        rebuilds: spotify._readyRebuilds || 0,
      });
      // Spotify has told us it is not there, so this is fact, not suspicion.
      // Clearing it also keeps the 'ready' dedup from swallowing the
      // re-announcement of the same device_id.
      spotify.ready = false;
      spotify._lastReadyId = null;

      const deadline = Date.now() + DEVICE_RECOVERY_BUDGET_MS;

      // Cheap path first: re-register the player object we still hold. This is
      // what a page reload does, minus the reload.
      if (spotify.player) {
        try {
          const ok = await spotify.player.connect();
          clientLog('spotify', 'recovery: player.connect() returned', { ok });
          if (ok) {
            const id = await _awaitReady(deadline - Date.now());
            if (id) {
              clientLog('spotify', 'recovery: device re-registered', {
                device_id: (id || '').slice(0, 12),
                tookMs: DEVICE_RECOVERY_BUDGET_MS - (deadline - Date.now()),
              });
              return id;
            }
          }
        } catch (e) {
          clientLog('spotify', 'recovery: connect() threw', { err: String(e).slice(0, 160) });
        }
      }

      // Expensive path: the bounded rebuild that already exists. Its counter
      // is what stops an account that can never register a device from
      // spinning, and a successful 'ready' clears it — so this borrows the
      // limit rather than inventing a second one.
      clientLog('spotify', 'recovery: reconnect did not register — rebuilding');
      _teardownPlayer(why);
      _scheduleSdkRebuild(why);
      const id = await _awaitReady(Math.max(0, deadline - Date.now()));
      clientLog('spotify', 'recovery: rebuild ' + (id ? 'registered' : 'did not register in budget'),
        { device_id: (id || '').slice(0, 12) });
      return id;
      } catch (e) {
        // Never reject. The caller is the 404 handler, and a rejection there
        // would skip the fallback-to-another-device path entirely — turning a
        // failed recovery into a worse outcome than no recovery at all.
        clientLog('spotify', 'recovery threw', { err: String(e).slice(0, 200) });
        return null;
      }
    })();

    spotify._recovering.then(() => { spotify._recovering = null; });
    return spotify._recovering;
  }

  // How long a freshly-connected player may go without a 'ready' event before
  // we call it dead. The SDK gives no failure callback for this: connect()
  // resolves true and then simply never announces a device, so silence is the
  // only symptom. Observed 2026-08-17 — a player sat with hasPlayer:true and
  // ready:false while the app logged "bail: Player not ready (waiting for SDK
  // ready event)" for 70s against an event that was never coming. 15s is well
  // past a normal registration (~350ms measured) without being so short that a
  // slow network gets torn down mid-handshake.
  const SDK_READY_TIMEOUT_MS = 15000;
  // Bounded, because the init guard was added precisely because unbounded
  // rebuilds produced N stacked players. Two attempts, then leave it alone and
  // let the user reconnect deliberately.
  const SDK_READY_MAX_REBUILDS = 2;

  const spotify = {
    player: null, deviceId: null, token: null, ready: false, _lastArtUrl: null,

    async init() {
      // Idempotency latch + durable guard. On mobile, SDK reconnects and the
      // play-retry paths fired init() dozens of times in a burst; each call
      // built a BRAND-NEW Spotify.Player with its own listeners (the old ones
      // were never torn down). A single underlying 'ready' then triggered N
      // stacked handlers → N reconciles + N conflicting player_state_changed
      // updates fighting over the UI — the device thrash, the "opens Spotify
      // doing nothing", and the glitchy back-and-forth skip. One player, one
      // listener set, always. _initing serialises concurrent entry; the live
      // spotify.player is the durable "already built" guard.
      if (spotify._initing || spotify.player) {
        clientLog('spotify', 'init: skipped (player already up)',
          { hasPlayer: !!spotify.player, initing: !!spotify._initing });
        return;
      }
      spotify._initing = true;
      try { await spotify._doInit(); }
      finally { spotify._initing = false; }
    },

    async _doInit() {
      const status = document.getElementById('player-status');
      status.textContent = '...';
      try {
        const res = await fetch('/token');
        const data = await res.json();
        if (data.error) {
          if (data.auth_url) {
            status.textContent = 'login →';
            status.style.cursor = 'pointer';
            status.onclick = () => { window.location.href = '/login'; };
          } else {
            status.textContent = '';
          }
          return;
        }
        spotify.token = data.access_token;
        if (data.needs_reauth) {
          // Token can't drive the SDK (missing `streaming` et al). Don't build
          // a player that would only fire authentication_error on a loop —
          // prompt a forced-consent reconnect up front.
          clientLog('spotify', 'token missing SDK scopes — prompting reconnect',
            { missing: data.missing_scopes });
          _promptReconnect('reconnect Spotify');
          return;
        }
        clientLog('spotify', 'init: token fetched OK',
          { ua: (navigator.userAgent || '').slice(0, 120), isIOS: DIG_IS_IOS });
      } catch(e) {
        status.textContent = '!';
        clientLog('spotify', 'init: token fetch threw', { err: String(e).slice(0, 200) });
        return;
      }

      spotify.player = new Spotify.Player({
        name: 'DIG',
        getOAuthToken: async cb => {
          clientLog('spotify', 'getOAuthToken called by SDK');
          try {
            const r = await fetch('/token');
            const d = await r.json();
            if (d.access_token) {
              spotify.token = d.access_token;
              cb(spotify.token);
              if (d.needs_reauth) {
                clientLog('spotify', 'getOAuthToken: token missing SDK scopes', { missing: d.missing_scopes });
                _promptReconnect('reconnect Spotify');
              } else {
                clientLog('spotify', 'getOAuthToken returned fresh token');
              }
            } else if (d.auth_url) {
              clientLog('spotify', 'getOAuthToken: no token, redirecting to login');
              window.location.href = '/login';
            } else {
              cb(spotify.token);
              clientLog('spotify', 'getOAuthToken: returned cached token (refresh empty)');
            }
          } catch(e) {
            cb(spotify.token);
            clientLog('spotify', 'getOAuthToken threw', { err: String(e).slice(0, 200) });
          }
        },
        volume: 0.8,
      });

      spotify.player.addListener('ready', ({ device_id }) => {
        // Trust the SDK 'ready' event as the readiness signal — that is its
        // contract (the device is registered with Spotify's backend and can
        // receive commands). We do NOT gate playback on the REST device list:
        // that check is racy — the 'ready' device_id can lag, or differ from
        // the id /me/player/devices reports for the SAME player — and gating
        // on it previously diverted playback to a phantom device, leaving the
        // tab silent. Instead we go ready immediately and rely on play()'s
        // REACTIVE handling: a real 404 NO_ACTIVE_DEVICE triggers transfer +
        // retry (wakes the device), and a real "Device not found" triggers
        // fallback to another device. The poll below is ADVISORY only.
        //
        // Dedup: the SDK re-announces 'ready' with the SAME device_id on every
        // reconnect (frequent on mobile). Re-running reconcile/consume each time
        // is the storm we just killed at the init layer — guard it here too.
        if (spotify.ready && spotify._lastReadyId === device_id) {
          clientLog('spotify', 'ready: duplicate (same device, ignoring)', { device_id: (device_id || '').slice(0, 12) });
          // Ignoring the event for state purposes is right; ignoring it for a
          // waiter is not. Anything blocked on "is a device registered?" has
          // its answer.
          _notifyReadyWaiters(device_id);
          return;
        }
        spotify._lastReadyId = device_id;
        spotify.deviceId = device_id;
        spotify.ready = true;
        spotify._sdkRegistered = true;
        spotify._fallbackDeviceId = null;
        spotify._authFails = 0;   // healthy auth — clear the reconnect-prompt counter
        // The device registered, so the watchdog has nothing to catch. Cancel
        // it or it fires mid-session and tears down a WORKING player.
        if (spotify._readyWatchdog) {
          clearTimeout(spotify._readyWatchdog);
          spotify._readyWatchdog = null;
        }
        spotify._readyRebuilds = 0;
        status.textContent = 'ready';
        setTimeout(() => { if (status.textContent === 'ready') status.textContent = ''; }, 3000);
        console.log('Spotify SDK ready event:', device_id);
        clientLog('firstplay', 'Spotify SDK ready event fired (trusted)', { device_id });
        _notifyReadyWaiters(device_id);
        _reconcileSdkDevice(device_id);
        queue.tryConsumePendingPlay('sdk-ready');
      });

      // ADVISORY device reconcile (non-gating). Polls /me/player/devices a few
      // times to confirm our in-browser player ('DIG') is visible and — if it
      // appears under a DIFFERENT id than the 'ready' event reported (a known
      // Spotify quirk) — adopts that API-visible id so play/transfer target
      // the right device. It NEVER sets _sdkRegistered=false and NEVER forces
      // fallback mode: a genuinely dead SDK device is detected reactively by
      // play()'s 404 handler, not predicted here. Worst case it does nothing
      // and play() proceeds on the trusted SDK id.
      async function _reconcileSdkDevice(device_id) {
        const POLL_MS = 500, MAX_POLLS = 6;
        for (let i = 1; i <= MAX_POLLS; i++) {
          await new Promise(r => setTimeout(r, POLL_MS));
          if (spotify.deviceId !== device_id) return;  // newer ready event / already adopted
          let devices;
          try {
            const r = await fetch('https://api.spotify.com/v1/me/player/devices',
              { headers: { 'Authorization': `Bearer ${spotify.token}` } });
            if (!r.ok) continue;
            devices = (await r.json()).devices || [];
          } catch (e) {
            clientLog('spotify', 'reconcile poll threw', { err: String(e).slice(0,160) });
            continue;
          }
          if (devices.some(d => d.id === device_id)) {
            {
              clientLog('spotify', `reconcile: device confirmed after ${i * POLL_MS}ms`,
                { device_id, server_devices: devices.map(d => ({ name: d.name, id: (d.id || '').slice(0,12), active: d.is_active })) });
            }
            return;  // API agrees on our id — nothing to reconcile
          }
          // Our id isn't listed — is our player present under a different id?
          // Only adopt an API-listed DIG device when it is ACTIVE. A stale,
          // inactive 'DIG' (a leftover from a previous session, is_active:false)
          // must NEVER replace the fresh, live 'ready' id — adopting that ghost
          // was routing every play to a dead device (the "opens Spotify, nothing
          // plays" + the unbearable delay). A genuinely dead SDK device is
          // caught reactively by play()'s 404 handler; we don't predict it here.
          const adopt = devices.find(d => (d.name || '') === 'DIG' && d.is_active && d.id !== device_id);
          if (adopt) {
            {
              clientLog('spotify', 'reconcile: adopting DIG device id from API (SDK ready id differed)',
                { sdk_ready_id: device_id, api_id: (adopt.id || '').slice(0,12),
                  server_devices: devices.map(d => ({ name: d.name, id: (d.id || '').slice(0,12), active: d.is_active })) });
            }
            spotify.deviceId = adopt.id;  // play/transfer now target the right device
            return;
          }
          // Not listed under any id yet — keep polling. play() stays on the
          // trusted SDK id and will transfer-to-activate on its first 404.
        }
        {
          clientLog('spotify', `reconcile: DIG device not yet listed after ${MAX_POLLS * POLL_MS}ms (staying on trusted SDK id)`,
            { device_id });
        }
      }
      spotify.player.addListener('not_ready', () => {
        status.textContent = 'offline';
        // Clearing `ready` is the whole point. This handler used to only log,
        // so `spotify.ready` stayed TRUE after the device went offline and
        // play() would happily issue commands at a device that was no longer
        // there. The player object is deliberately NOT torn down here: 'not_ready'
        // is routine on mobile (backgrounding the tab) and the SDK re-announces
        // 'ready' by itself on reconnect, which the ready handler already
        // dedups. Tearing down on every backgrounding would rebuild the player
        // constantly for no reason.
        spotify.ready = false;
        clientLog('spotify', 'SDK not_ready event');
        // …but "usually re-announces" is not "always", and a device that dies
        // MID-SESSION was the hole left by the first version of this fix.
        // Measured 2026-08-17, an hour after shipping it: a session whose
        // device died logged `Player.play: spotify NOT ready — bailing,
        // deviceId: null` on every Spotify track for 15 minutes. Nothing could
        // recover it — play() bails without reconnecting, init()'s guard still
        // sees a live `spotify.player` and skips, and the connect-time watchdog
        // had already been cancelled by the earlier successful 'ready'. So the
        // listener re-arms it: the SDK gets its usual chance to come back on
        // its own, and if it does not, this rebuilds instead of bailing
        // forever. Same bounded counter — a flapping device cannot spin.
        _armReadyWatchdog('not_ready with no re-announce');
      });
      spotify.player.addListener('initialization_error', ({ message }) => {
        status.textContent = 'init err';
        clientLog('spotify', 'initialization_error', { message });
        console.error(message);
      });
      spotify.player.addListener('account_error', ({ message }) => {
        status.textContent = 'need Premium';
        clientLog('spotify', 'account_error', { message });
        console.error(message);
      });
      spotify.player.addListener('playback_error', ({ message }) => {
        console.error('Spotify playback error:', message);
        clientLog('spotify', 'playback_error', { message });
      });

      spotify.player.addListener('player_state_changed', state => {
        if (!state) {
          clientLog('spotify', 'state_changed: null state (device deactivated?)');
          return;
        }
        if (activeSource !== 'spotify') return;
        const pos = state.position, dur = state.duration;

        // Log the SDK's view of "currently playing" track on transitions —
        // detected by track-id change since last log.
        const sdkId = state.track_window?.current_track?.id;
        if (sdkId && sdkId !== spotify._lastLoggedSdkId) {
          spotify._lastLoggedSdkId = sdkId;
          const sdkName = state.track_window.current_track.name;
          const sdkArtists = (state.track_window.current_track.artists || []).map(a => a.name).join(', ');
          {
            clientLog('spotify', 'state_changed: track is now', {
              id: sdkId, name: (sdkName || '').slice(0,40), artist: sdkArtists.slice(0,40),
              paused: state.paused, position: pos, duration: dur,
            });
          }
        }

        document.getElementById('btn-play').textContent = state.paused ? '▶' : '❚❚';
        document.getElementById('mc-play').textContent = state.paused ? '▶' : '❚❚';
        if (!state.paused) { Player._clearStuckTimer(); _clearTransientStatus(); }
        _updateProgress(pos, dur, sdkId);
        void syncMediaSessionPlayback();

        // ── UI title/artist re-sync from the actually-playing track ──
        // playCurrentTrack() sets the UI from DIG's expected dIdx track. If
        // Spotify ends up playing a different track (AirPods skip advancing
        // through Spotify's queue, prequeue mismatch, or any other cause),
        // the UI lies. When the SDK reports a STABLE playing track that
        // differs from what DIG thinks, sync the UI to what's actually
        // playing — and rebase dIdx to that track if it's in our queue, so
        // the next skip advances correctly.
        const sdkTrack = state.track_window?.current_track;
        if (sdkTrack && sdkTrack.id && !state.paused) {
          const msSincePlay = Date.now() - (Player._lastPlayStarted || 0);
          if (msSincePlay > 2000) {  // past the transition window
            const expected = queue.currentTrack();
            if (expected && expected.id !== sdkTrack.id) {
              const sdkArtists = (sdkTrack.artists || []).map(a => a.name).join(', ');
              paintTrackInfo(sdkTrack.name || '', sdkArtists);
              // Follow the queue to what is ACTUALLY playing, if we know it.
              const rebased = queue.rebaseQueueTo(sdkTrack.id);
              {
                clientLog('play', 'UI re-synced from SDK (mismatch detected)', {
                  expected: `${expected.artist} — ${expected.name}`,
                  actual: `${sdkArtists} — ${sdkTrack.name}`,
                  rebased,
                });
              }
            }
          }
        }

        // Album art — only update DOM if the URL actually changed
        const images = state.track_window?.current_track?.album?.images;
        if (images && images.length > 0) {
          const url = images[0].url;
          if (url !== spotify._lastArtUrl) {
            spotify._lastArtUrl = url;
            paintArt(url, 'spotify-sdk');
            if ('mediaSession' in navigator && navigator.mediaSession.metadata) {
              navigator.mediaSession.metadata.artwork = [{ src: url, sizes: '300x300', type: 'image/jpeg' }];
            }
          }
        }
        // Track ended detection. Two SDK signatures occur in practice:
        //   (a) paused=true, position == 0, previous_tracks populated
        //   (b) paused=true, position ≈ duration (Spotify left it parked at end)
        // Original code only caught (a); (b) silently dropped, leaving the
        // queue stuck. Both should fire onTrackEnd to advance to DIG's next.
        // Guard against the SDK's known transition false-positive: during a
        // play(), state_changed briefly fires paused=true,position=0 BEFORE
        // the new audio buffer starts — so require msSincePlay > 2000 to
        // dodge that race.
        if (state.paused) {
          const dur  = state.duration || 0;
          const pos  = state.position || 0;
          const hadPrev = (state.track_window?.previous_tracks?.length || 0) > 0;
          const posAtZero = pos === 0 && hadPrev;
          const posAtEnd  = dur > 0 && pos >= dur - 1500;
          const msSincePlay = Date.now() - (Player._lastPlayStarted || 0);
          const eligible = msSincePlay > 2000;
          clientLog('trackend-sdk', 'paused state seen', {
            pos, dur, hadPrev, posAtZero, posAtEnd,
            msSincePlay: Math.round(msSincePlay), eligible,
            willFire: !!(eligible && (posAtZero || posAtEnd)),
            wired: !!_onTrackEnd,
          });
          if (eligible && (posAtZero || posAtEnd)) {
            clientLog('spotify', 'track ended (firing onTrackEnd)', {
              via: posAtZero ? 'position-zero' : 'position-at-duration',
              position: pos, duration: dur, msSincePlay,
            });
            if (_onTrackEnd) _onTrackEnd();
          } else if (posAtZero || posAtEnd) {
            // Within 2s of play() — spurious during transition. Ignore.
            console.log(`[DIG] ignoring spurious track-end ${msSincePlay}ms after play()`);
          }
        }
      });

      spotify.player.addListener('authentication_error', ({ message } = {}) => {
        status.textContent = 'auth err';
        clientLog('spotify', 'authentication_error', { message });
        // Tear the dead player down so init()'s guard rebuilds ONE fresh player
        // (otherwise the guard sees spotify.player still set and skips, or we'd
        // leak a second player + its listeners — the very storm we just fixed).
        _teardownPlayer('authentication_error');
        // A scope-narrowed token fails auth on EVERY init, so a blind 3s retry
        // loops forever (observed: 1000+ authentication_errors from a single
        // user across 3 weeks). After a couple of consecutive failures, stop
        // and prompt a forced-consent reconnect instead of hammering. The
        // counter is reset on a successful 'ready'.
        spotify._authFails = (spotify._authFails || 0) + 1;
        if (spotify._authFails >= 2) {
          clientLog('spotify', 'auth failing repeatedly — prompting reconnect', { fails: spotify._authFails });
          _promptReconnect('reconnect Spotify');
          return;
        }
        setTimeout(() => spotify.init(), 3000);
      });

      clientLog('spotify', 'calling player.connect()');
      const connectOk = await spotify.player.connect();
      clientLog('spotify', `player.connect() returned ${connectOk}`);

      // The return value used to be logged and dropped. A false here means the
      // SDK refused to connect at all, so no 'ready' is coming and the player
      // object is inert — but init()'s guard only checks that the object
      // EXISTS, so leaving it in place made the failure permanent for the rest
      // of the session.
      if (!connectOk) {
        _teardownPlayer('connect() returned false');
        _scheduleSdkRebuild('connect-refused');
        return;
      }

      // connect() resolving true does NOT mean the device registered. The SDK
      // announces that separately via 'ready', and when that never arrives it
      // does so silently — no error, no callback, nothing to catch. So the only
      // way to notice is to wait and check.
      _armReadyWatchdog('ready-timeout');
    },

    async play(trackId) {
      // Cleared on every attempt; set true only when Spotify refuses the track
      // with a 403 "Restriction violated" (market-restricted / greyed-out /
      // unavailable). The caller uses this to skip instantly instead of running
      // the device-warmup retry, which a restriction will never clear.
      spotify._lastPlayRestricted = false;
      if (!spotify.deviceId || !spotify.token) {
        clientLog('play', 'spotify.play: missing deviceId or token', { deviceId: spotify.deviceId, hasToken: !!spotify.token });
        console.warn('[DIG] spotify.play: no deviceId or token', { deviceId: spotify.deviceId, hasToken: !!spotify.token });
        return false;
      }
      clientLog('play', 'spotify.play: enter', {
        id: trackId, deviceId: (spotify.deviceId || '').slice(0, 12),
        sdkRegistered: spotify._sdkRegistered, activated: spotify._activated,
        fallbackDeviceId: (spotify._fallbackDeviceId || '').slice(0, 12),
        path: (spotify._sdkRegistered === false && spotify._fallbackDeviceId) ? 'FALLBACK /api/play' : 'SDK direct',
      });
      bandcamp.stop();   // silence the <audio> backend when handing off to Spotify
      activeSource = 'spotify';
      spotify._lastArtUrl = null;

      // SDK-broken fast path: verification flagged the SDK device as
      // unregistered. Route the play through the server-side /api/play
      // (which does transfer + play correctly) targeting whichever real
      // device Spotify knows about — usually a phone or desktop app.
      if (spotify._sdkRegistered === false && spotify._fallbackDeviceId) {
        clientLog('play', 'fallback to /api/play (SDK device broken)',
          { id: trackId, fallback: spotify._fallbackDeviceId });
        try {
          const url = `/api/play?track=${encodeURIComponent(trackId)}&device=${encodeURIComponent(spotify._fallbackDeviceId)}`;
          const r = await fetch(url);
          const data = await r.json();
          if (data.ok) {
            clientLog('play', 'fallback /api/play OK', { id: trackId });
            document.getElementById('btn-play').textContent = '❚❚';
            document.getElementById('mc-play').textContent = '❚❚';
            _startPoll();
            return true;
          } else {
            clientLog('play', 'fallback /api/play FAILED', { id: trackId, err: data.error, detail: (data.detail||'').slice(0,200) });
            // Re-check the device list — fallback might be stale (phone closed)
            try {
              const dr = await fetch('https://api.spotify.com/v1/me/player/devices',
                { headers: { 'Authorization': `Bearer ${spotify.token}` } });
              const dd = await dr.json();
              const devs = dd.devices || [];
              const next = devs.find(d => d.id !== spotify.deviceId);
              if (next && next.id !== spotify._fallbackDeviceId) {
                spotify._fallbackDeviceId = next.id;
                clientLog('play', 'fallback device updated, retrying', { name: next.name });
                const url2 = `/api/play?track=${encodeURIComponent(trackId)}&device=${encodeURIComponent(next.id)}`;
                const r2 = await fetch(url2);
                const d2 = await r2.json();
                if (d2.ok) {
                  document.getElementById('btn-play').textContent = '❚❚';
                  document.getElementById('mc-play').textContent = '❚❚';
                  _startPoll();
                  return true;
                }
              } else {
                spotify._fallbackDeviceId = null;
                const ps = document.getElementById('player-status');
                if (ps) ps.textContent = 'Open Spotify on phone/desktop, play anything for 2s, then skip';
              }
            } catch (e) { /* ignore */ }
            return false;
          }
        } catch (e) {
          clientLog('play', 'fallback /api/play threw', { err: String(e).slice(0,200) });
          return false;
        }
      }

      // Web Playback SDK requires activateElement() on a user gesture for
      // browser autoplay policy (Chrome). Without it the SDK device gets
      // marked active server-side but the audio element refuses to start
      // and Spotify's API may keep returning 404 NO_ACTIVE_DEVICE. Safe to
      // call repeatedly. Cache the success so we don't refetch every play.
      if (!spotify._activated && spotify.player && spotify.player.activateElement) {
        try {
          await spotify.player.activateElement();
          spotify._activated = true;
          clientLog('spotify', 'activateElement OK');
        } catch (e) {
          clientLog('spotify', 'activateElement failed', { err: String(e).slice(0, 120) });
        }
      }

      // Helper: PUT /me/player/play with the current trackId
      const _playReq = async (token) => fetch(
        `https://api.spotify.com/v1/me/player/play?device_id=${spotify.deviceId}`,
        {
          method: 'PUT',
          headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ uris: [`spotify:track:${trackId}`] }),
        });

      // Helper: transfer playback to our SDK device. The Web Playback SDK
      // registers a device but Spotify often leaves it is_active=false;
      // calling /me/player/play directly then 404s with NO_ACTIVE_DEVICE.
      const _transfer = async (token) => fetch(
        'https://api.spotify.com/v1/me/player',
        {
          method: 'PUT',
          headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ device_ids: [spotify.deviceId], play: false }),
        });

      try {
        let resp = await _playReq(spotify.token);

        if (resp.status === 401) {
          // Token expired — refresh and retry once
          try {
            const tr = await fetch('/token');
            const td = await tr.json();
            if (td.access_token) {
              spotify.token = td.access_token;
              resp = await _playReq(spotify.token);
            } else if (td.auth_url) {
              window.location.href = '/login';
              return false;
            }
          } catch (e) { /* fall through */ }
        }

        // 404: could be NO_ACTIVE_DEVICE (transfer needed) or
        // "Device not found" (SDK device never registered). Inspect the body.
        if (resp.status === 404) {
          const body404 = await resp.clone().text().catch(() => '');
          const isDeviceNotFound = /device\s*not\s*found/i.test(body404);
          {
            clientLog('play', '404 — checking cause', {
              id: trackId, isDeviceNotFound, bodySnippet: body404.slice(0, 120),
            });
          }

          if (isDeviceNotFound) {
            // Our own device first. Handing the session to a phone is a
            // consolation prize, and 10 of the 11 times this branch has ever
            // run there was no other device to hand it to — `fallback: null`,
            // and the user was told to go and open Spotify somewhere else.
            const recoveredId = await _recoverOwnDevice('play 404 device-not-found');
            if (recoveredId) {
              // _playReq reads spotify.deviceId at call time, so this already
              // targets the re-registered device.
              resp = await _playReq(spotify.token);
              clientLog('play', 'retry after device recovery',
                { id: trackId, device_id: (recoveredId || '').slice(0, 12), status: resp.status });
              if (resp.ok) {
                spotify._sdkRegistered = true;
                spotify._fallbackDeviceId = null;
                document.getElementById('btn-play').textContent = '❚❚';
                document.getElementById('mc-play').textContent = '❚❚';
                _startPoll();
                return true;
              }
              // Recovered a device and STILL could not play on it: fall through
              // and let the rest of this handler have its turn.
            }

            // SDK device is broken on Spotify's side. Mark it and try to
            // re-route through any other device the user has active.
            spotify._sdkRegistered = false;
            try {
              const dr = await fetch('https://api.spotify.com/v1/me/player/devices',
                { headers: { 'Authorization': `Bearer ${spotify.token}` } });
              const dd = await dr.json();
              const devs = dd.devices || [];
              const next = devs.find(d => d.id !== spotify.deviceId);
              {
                clientLog('play', 'Device not found — looking for fallback',
                  { server_devices: devs.map(d => ({ name: d.name, active: d.is_active })),
                    fallback: next ? next.name : null });
              }
              if (next) {
                spotify._fallbackDeviceId = next.id;
                const url = `/api/play?track=${encodeURIComponent(trackId)}&device=${encodeURIComponent(next.id)}`;
                const r2 = await fetch(url);
                const d2 = await r2.json();
                if (d2.ok) {
                  clientLog('play', 'fallback after Device-not-found OK', { device: next.name });
                  document.getElementById('btn-play').textContent = '❚❚';
                  document.getElementById('mc-play').textContent = '❚❚';
                  _startPoll();
                  return true;
                } else {
                  clientLog('play', 'fallback after Device-not-found FAILED', { err: d2.error });
                }
              } else {
                const ps = document.getElementById('player-status');
                if (ps) ps.textContent = 'Open Spotify on phone/desktop, play anything for 2s, then skip';
              }
            } catch (e) {
              clientLog('play', 'device-not-found fallback threw', { err: String(e).slice(0,200) });
            }
            return false;
          }

          // NO_ACTIVE_DEVICE — SDK device exists but Spotify hasn't activated
          // it. Transfer to wake it up, then retry the play.
          clientLog('play', 'NO_ACTIVE_DEVICE — transfer + retry', { id: trackId });
          let tStatus = '?';
          try {
            const tResp = await _transfer(spotify.token);
            tStatus = tResp.status;
            if (!tResp.ok) {
              const tBody = await tResp.text().catch(() => '');
              clientLog('play', 'transfer FAILED', { status: tStatus, body: tBody.slice(0, 120) });
            }
          } catch (e) {
            clientLog('play', 'transfer threw', { err: String(e).slice(0, 120) });
          }
          // Spotify needs a moment to actually register the activation
          await new Promise(r => setTimeout(r, 600));
          resp = await _playReq(spotify.token);
          {
            clientLog('play', 'retry after transfer', { transferStatus: tStatus, retryStatus: resp.status });
          }
        }

        if (!resp.ok) {
          if (resp.status === 401) {
            window.location.href = '/login';
            return false;
          }
          const body = await resp.text().catch(() => '');
          // 403 "Restriction violated" = Spotify won't play this track here
          // (market-restricted / greyed-out / unavailable). Flag it so the
          // caller skips immediately rather than burning a 700ms retry on a
          // failure that will never clear.
          if (resp.status === 403 && /restriction|not\s*available|market|unavailable/i.test(body)) {
            spotify._lastPlayRestricted = true;
          }
          clientLog('play', `spotify.play FAILED ${resp.status}`, { body: body.slice(0, 200), id: trackId, restricted: !!spotify._lastPlayRestricted });
          console.error(`[DIG] spotify.play FAILED: ${resp.status}`, body);
          return false;
        }
        document.getElementById('btn-play').textContent = '❚❚'; document.getElementById('mc-play').textContent = '❚❚';
        clientLog('play', 'spotify.play OK', { id: trackId, status: resp.status });
        _startPoll();
        return true;
      } catch (err) {
        clientLog('play', 'spotify.play threw', { id: trackId, err: String(err).slice(0, 200) });
        console.error('[DIG] spotify.play fetch error:', err);
        return false;
      }
    },

    async togglePlay() { if (spotify.player) await spotify.player.togglePlay(); },
    async pause() { if (spotify.player) await spotify.player.pause(); },
    async resume() { if (spotify.player) await spotify.player.resume(); },

    async seek(posMs) { if (spotify.player) await spotify.player.seek(posMs); },

    async getState() {
      if (!spotify.player) return null;
      const s = await spotify.player.getCurrentState();
      if (!s) return null;
      const cur = s.track_window && s.track_window.current_track;
      return { position: s.position, duration: s.duration, paused: s.paused,
               trackId: cur ? cur.id : null };
    },

    stop() {
      if (spotify.player) spotify.player.pause().catch(() => {});
    },
  };

  // ── Bandcamp backend ──
  // Full-track playback through a plain HTML5 <audio> element. This works on
  // iOS WebKit (where the Spotify Web Playback SDK does not) — verified on
  // iPhone incl. lock-screen background play + auto-advance. Bandcamp stream
  // URLs are signed and expire in hours, so we NEVER store one: we resolve a
  // FRESH full-track URL per play from /api/bandcamp/resolve (zero Spotify
  // quota). Pool id form: 'bc:<band_id>:<track_id>', source 'bandcamp'.
  const bandcamp = {
    audio: null, ready: false, _dur: 0, _stallTimer: null,
    // Bumped on every play() call. There is one <audio> element, so a second
    // play() started while the first is still awaiting its resolve/play steals
    // the element out from under it — the loser's play() promise rejects with
    // AbortError. Comparing the captured seq tells a superseded attempt (the
    // user skipped: expected, the newer track is fine) apart from a real
    // playback failure, which is the only one worth reporting.
    //
    // A superseded attempt returns SUPERSEDED, never false. `false` means "this
    // track will not play", and playCurrentTrack answers that by retrying after
    // 700ms and counting toward the 3-strike circuit breaker — which, once the
    // element already belongs to a newer track, restarts the song the user just
    // skipped TO and trips "playback error — try refreshing" after three fast
    // skips. The loser of the race must report nothing at all.
    _playSeq: 0,

    // A Bandcamp CDN stall fires NO 'error' event and play() has already
    // resolved, so a wedged stream hangs silently forever with no recovery.
    // Arm a watchdog on stalled/waiting; if output hasn't resumed (position
    // advanced) within the window, skip the track like a natural end. Cleared
    // by 'playing', a new play(), or stop().
    _armStallWatch() {
      if (bandcamp._stallTimer) return;   // already watching this stall
      const startPos = bandcamp.audio ? bandcamp.audio.currentTime : 0;
      bandcamp._stallTimer = setTimeout(() => {
        bandcamp._stallTimer = null;
        if (activeSource !== 'bandcamp' || !bandcamp.audio) return;
        const advanced = bandcamp.audio.currentTime > startPos + 0.25;
        if (advanced || bandcamp.audio.paused) return;
        clientLog('bandcamp', 'stall watchdog → skip (stream never recovered)',
          { pos: Math.round((bandcamp.audio.currentTime || 0) * 1000),
            readyState: bandcamp.audio.readyState, networkState: bandcamp.audio.networkState });
        // TEAR THE ELEMENT OFF THE DEAD STREAM BEFORE ADVANCING.
        //
        // When the stall happens on a track's FIRST load, `await audio.play()`
        // in play() below is still pending — a play() promise settles when
        // output starts, and output never started. That await holds the caller's
        // dispatch open, and app.js holds _playLock for the whole call. So the
        // skip this watchdog is firing hit "BLOCKED by _playLock" at age ~9.7s
        // and did nothing: the queue cursor advanced, no track was dispatched,
        // and the session went silent until the user touched the page. The lock's
        // 15s stale-clear cannot save it either — this watchdog fires at 9s, so
        // it is ALWAYS inside the stale window. Deterministic, not a race: 5 of
        // the 8 watchdog firings in the week to 2026-08-07 died exactly this way.
        //
        // Detaching the source rejects that pending play() with AbortError (the
        // spec's "interrupted by a new load request"), which the catch below
        // reports as SUPERSEDED once the seq bump marks this attempt abandoned —
        // the same path a mid-load user skip already takes. app.js releases the
        // lock on SUPERSEDED and ignores the rest, so the advance lands.
        //
        // It also buries the zombie. Left attached, the dead element can start
        // playing minutes later when the tab is foregrounded and data finally
        // arrives — on 2026-08-07 that resolved a 159s-pending play() and played
        // a track the queue had long moved past ("queue cursor disagrees with
        // the audio", 40 times in the same week).
        bandcamp._playSeq++;
        bandcamp._loadedId = null;
        try {
          bandcamp.audio.pause();
          bandcamp.audio.removeAttribute('src');
          bandcamp.audio.load();
        } catch (e) {}
        // The rejection propagates through the awaits as microtasks, all of
        // which drain before any macrotask. A plain setTimeout is therefore
        // enough to guarantee the lock is released before we ask for the next
        // track — advancing synchronously here would re-hit the same block.
        setTimeout(() => { if (_onTrackEnd) _onTrackEnd(); }, 0);
      }, 9000);
    },
    _clearStallWatch() { if (bandcamp._stallTimer) { clearTimeout(bandcamp._stallTimer); bandcamp._stallTimer = null; } },

    init() {
      if (bandcamp.audio) return;
      const a = document.createElement('audio');
      a.preload = 'auto';
      a.setAttribute('playsinline', '');
      a.addEventListener('loadedmetadata', () => { bandcamp._dur = (a.duration || 0) * 1000; });
      // The one audio event that reported NOTHING, which is why "autoplay
      // doesn't work" has never been diagnosable: both ways it can fail —
      // never firing at all (page frozen by iOS), or firing and being dropped
      // by a guard — look identical from the server, and they need different
      // fixes. Log before the guard, and say which branch was taken.
      /**
       * Is the element actually holding this track's stream?
       *
       * Between `activeSource = 'bandcamp'` at the top of play() and the src
       * assignment after the resolve fetch — 1,348 ms for Witch's Book — the
       * element still carries whatever was there before: the previous track,
       * or the silent `data:` unlock primer. An `error` or an `ended` in that
       * window is not this track failing or finishing, and treating it as one
       * advanced the queue out from under a track that then played perfectly.
       *
       * Observed 2026-08-01 03:21:05, first play of a session: the primer
       * raised errCode 4 (SRC_NOT_SUPPORTED) 35 ms after play() started, the
       * error handler called _onTrackEnd, the picker moved dIdx to a Spotify
       * track, and 1.3 s later Witch's Book began. Audio on one track, cursor
       * on another — so every progress paint was suppressed as a mismatch and
       * THE BAR SAT AT ZERO for the whole song while the clock ran behind it.
       *
       * _loadedId already existed for precisely this window; the handlers just
       * never consulted it.
       */
      const _holdsRealStream = () => {
        const src = (a.currentSrc || a.src || '');
        if (!src || src.slice(0, 5) === 'data:') return false;
        return !!bandcamp._loadedId;
      };

      a.addEventListener('ended', () => {
        clientLog('bandcamp', 'audio ended', {
          activeSource,
          wired: !!_onTrackEnd,
          vis: document.visibilityState,
          posMs: Math.round((a.currentTime || 0) * 1000),
          durMs: Math.round((a.duration || 0) * 1000),
          willAdvance: activeSource === 'bandcamp' && !!_onTrackEnd,
        });
        if (!_holdsRealStream()) {
          clientLog('bandcamp', 'ended ignored — element is not on this stream yet',
            { src: (a.currentSrc || a.src || '').slice(0, 40), loadedId: bandcamp._loadedId });
          return;
        }
        if (activeSource === 'bandcamp' && _onTrackEnd) _onTrackEnd();
      });
      a.addEventListener('error', () => {
        if (activeSource !== 'bandcamp') return;
        console.warn('[DIG] bandcamp audio error — skipping');
        // Log the full audio-element state, not just the src: error code +
        // network/ready state pinpoint whether it was a load failure, a
        // decode error, or (as in the unlock-clobber bug) the src being
        // swapped out from under a live stream.
        clientLog('bandcamp', 'audio error → skip', {
          src: (a.currentSrc || a.src || '').slice(0, 80),
          errCode: a.error && a.error.code, errMsg: (a.error && a.error.message || '').slice(0, 120),
          networkState: a.networkState, readyState: a.readyState, unlocked: !!bandcamp._unlocked,
          holdsRealStream: _holdsRealStream(), loadedId: bandcamp._loadedId || null,
        });
        if (!_holdsRealStream()) {
          clientLog('bandcamp', 'error ignored — element is not on this stream yet',
            { src: (a.currentSrc || a.src || '').slice(0, 40) });
          return;
        }
        // BOUND THE WALK. Every error advances, and with Spotify already
        // unreachable the queue ahead is Bandcamp too — so a CDN having a bad
        // minute walks the listener through track after track in silence, at
        // roughly the speed of the resolve call. Same defect the harness found
        // on the Spotify side (_UNPLAYABLE_RUN_LIMIT); it was never bounded
        // here, and this is the path that runs when Spotify is already gone.
        //
        // Reset by output actually starting, so this counts a RUN and not a
        // total: one bad stream an hour must never accumulate into a stop.
        if (++_bandcampErrorRun >= _BANDCAMP_ERROR_RUN_LIMIT) {
          clientLog('bandcamp', 'too many streams failed in a row — stopping', {
            run: _bandcampErrorRun,
          });
          const s = document.getElementById('player-status');
          if (s) s.textContent = 'having trouble loading tracks — press play to retry';
          _bandcampErrorRun = 0;
          return;
        }
        if (_onTrackEnd) _onTrackEnd();
      });
      a.addEventListener('play',  () => { const p = document.getElementById('btn-play'); if (p) p.textContent = '❚❚'; const m = document.getElementById('mc-play'); if (m) m.textContent = '❚❚'; });
      a.addEventListener('pause', () => { const p = document.getElementById('btn-play'); if (p) p.textContent = '▶'; const m = document.getElementById('mc-play'); if (m) m.textContent = '▶'; });
      // 'playing' = audio is actually producing output (distinguishes a
      // resolved play() promise from a silent/stuck element). 'stalled'/'waiting'
      // = the stream stopped feeding data — catches network stalls that would
      // otherwise look like a hang with no error event.
      // Disarm the stuck watchdog here, mirroring what the Spotify SDK's
      // player_state_changed listener does on `!state.paused`. Without this the
      // bandcamp path never disarms, and the watchdog's `state.paused` test
      // cannot tell "never started" from "the user pressed pause" — a guest who
      // hit space within 8s of a track starting got auto-skipped instead
      // (observed twice in one session, 2026-08-08).
      a.addEventListener('playing', () => { bandcamp._clearStallWatch(); Player._clearStuckTimer(); _bandcampErrorRun = 0; _clearTransientStatus(); if (activeSource === 'bandcamp') clientLog('bandcamp', 'audio playing (output started)', { pos: Math.round((a.currentTime || 0) * 1000), readyState: a.readyState }); });
      a.addEventListener('stalled', () => { if (activeSource === 'bandcamp') { clientLog('bandcamp', 'audio stalled (no data)', { pos: Math.round((a.currentTime || 0) * 1000), networkState: a.networkState, readyState: a.readyState }); bandcamp._armStallWatch(); } });
      a.addEventListener('waiting', () => { if (activeSource === 'bandcamp') { clientLog('bandcamp', 'audio waiting (buffer underrun)', { pos: Math.round((a.currentTime || 0) * 1000), readyState: a.readyState }); bandcamp._armStallWatch(); } });
      // Keep it in the DOM — matches the proven bctest.html setup (more reliable
      // for iOS background audio + MediaSession than a detached element).
      try { document.body.appendChild(a); } catch (e) {}
      bandcamp.audio = a;
      bandcamp.ready = true;

      // ── Why a track can end in the background and nothing follows ────────
      // Two causes, indistinguishable from the server because both produce
      // silence, and they need opposite fixes:
      //   (a) iOS froze this page, so 'ended' never ran — DIG cannot advance
      //       itself and the fix has to be recovery on resume;
      //   (b) the page kept running and something else swallowed the advance —
      //       a guard, a wedged _playLock — which is a bug we can fix directly.
      // A heartbeat separates them: if these stop the moment the screen locks
      // and resume on unlock, it is (a). If they continue through, it is (b).
      // 15s is well under the shortest track and ~10 lines per song.
      setInterval(() => {
        if (!bandcamp.audio || bandcamp.audio.paused) return;
        if (activeSource !== 'bandcamp') return;
        const pos = bandcamp.audio.currentTime || 0;
        const dur = bandcamp.audio.duration || 0;
        clientLog('heartbeat', 'bandcamp audio alive', {
          posMs: Math.round(pos * 1000), durMs: Math.round(dur * 1000),
          leftMs: dur ? Math.round((dur - pos) * 1000) : null,
          vis: document.visibilityState, readyState: bandcamp.audio.readyState,
        });
      }, 15000);

      // Page lifecycle, beacon-delivered. 'hidden' then a long gap then
      // 'visible' with a matching wall-clock jump IS the proof of a freeze —
      // the client timestamps in `at` are what make that readable, since the
      // server only sees when a batch arrived.
      const _lifecycle = (what) => () => clientLog('lifecycle', what, {
        vis: document.visibilityState,
        audioPaused: bandcamp.audio ? bandcamp.audio.paused : null,
        posMs: bandcamp.audio ? Math.round((bandcamp.audio.currentTime || 0) * 1000) : null,
        activeSource,
      });
      document.addEventListener('visibilitychange', _lifecycle('visibilitychange'));
      // Read Spotify the INSTANT the page can run again. Playback died at 0:08
      // while the page was hidden for 34s, and the first state read afterwards
      // came 9s after it woke and returned null — so nothing recorded what
      // Spotify looked like closest to the moment it stopped. This costs one
      // extra call per foreground, and it is the only window in which the
      // answer still exists.
      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState !== 'visible') return;
        if (!Player.spotifyState) return;
        Player.spotifyState().then((st) => {
          clientLog('connect', 'state on becoming visible', st ? {
            trackId: st.trackId, paused: st.paused, position: st.position,
            duration: st.duration, deviceId: st.deviceId,
            deviceActive: st.deviceActive, contextUri: st.contextUri,
            contextType: st.contextType, shuffle: st.shuffle,
          } : { state: null, meaning: 'no active device / nothing playing' });
          // AND THEN USE IT. This read the truth and threw it away — it was one
          // log line. Meanwhile the session-sync repainted the card from stored
          // state, so a phone that had been locked through six native advances
          // came back showing the track from before the lock, frozen at 0:00,
          // labelled "playing on another device".
          //
          // Measured 2026-08-02, and every line of it is in the log:
          //   10:23:37  DIG dispatches "morning clouds"   (last thing it knew)
          //             …screen locked, Spotify advances natively 6-7 times…
          //   10:29:34  app opened -> session-sync paints "morning clouds" 0:00
          //   10:31:29  THIS handler reads 5IatwXu8…, playing, 125925ms in
          //             — and did nothing with it
          //   10:33:03  the poll finally starts, but only because a skip did it
          // Three and a half minutes of the wrong card while the answer sat in
          // a log line. Freezing is not something DIG can prevent; coming back
          // and asking is the whole recovery, so the answer has to be used.
          if (!st || st.paused || !st.trackId) return;
          const belief = (Player._connectBelief && Player._connectBelief()) || {};
          if (st.trackId === belief.trackId && belief.playing) return;  // agreed already
          clientLog('connect', 'came back to a different song — following Spotify', {
            was: belief.trackId, now: st.trackId, name: st.trackName,
            posMs: Math.round(st.position || 0),
          });
          // Same adoption the poll performs for an external skip: put the card
          // and the queue on the track Spotify is really playing. adoptPlaying
          // repaints, restarts the poll and re-installs DIG's look-ahead, so
          // the next advance is a DIG pick rather than wherever Spotify was.
          queue.adoptExternalTrack(st.trackId, {
            fromLookahead: Player._connectContextTracks
              && Player._connectContextTracks[st.trackId],
            stub: { id: st.trackId, name: st.trackName || '', artist: st.artistName || '' },
          });
          Player.adoptPlaying(st);
        }).catch((e) => {
          // NOT a silent catch. The empty one here hid a ReferenceError for the
          // whole life of the recovery above: the feature never ran, no log
          // line appeared, and from the outside it looked like a design that
          // simply did nothing. A handler that swallows its own bugs is worse
          // than no handler, because it also swallows the evidence.
          clientLog('connect', 'becoming-visible handler failed',
            { err: String(e && e.message || e).slice(0, 140) });
        });
      });
      window.addEventListener('pagehide', _lifecycle('pagehide'));
      window.addEventListener('pageshow', _lifecycle('pageshow'));
      // Safari fires these when it actually suspends/wakes a tab — the direct
      // answer to (a) vs (b) when the browser is willing to say so.
      document.addEventListener('freeze', _lifecycle('freeze'));
      document.addEventListener('resume', _lifecycle('resume'));
      // iOS unlock: an <audio> element can only be play()ed programmatically
      // (e.g. a Spotify→Bandcamp auto-advance, where there's no fresh tap and
      // the prior sound came from the Spotify app, not our page) AFTER it has
      // played once inside a user gesture. Prime it on the first gesture with a
      // ~50ms silent clip so every later gesture-less play just works.
      const SILENT = 'data:audio/wav;base64,UklGRrQBAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YZABAACAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgA';
      const unlock = () => {
        if (bandcamp._unlocked) return;
        // NEVER prime over a real stream: if a Bandcamp track is already loaded
        // (currentSrc is an http(s) URL, not the data: primer), the element is
        // already unlocked by that play — setting src=SILENT here would replace
        // the playing track with silence and trigger an erroneous skip.
        const cur = a.currentSrc || a.src || '';
        if (cur && cur.slice(0, 5) !== 'data:') {
          bandcamp._unlocked = true;
        } else {
          try { a.src = SILENT; const p = a.play(); if (p && p.then) p.then(() => { try { a.pause(); a.currentTime = 0; } catch (e) {} }).catch(() => {}); } catch (e) {}
          bandcamp._unlocked = true;
        }
        document.removeEventListener('pointerdown', unlock, true);
        document.removeEventListener('touchend', unlock, true);
      };
      document.addEventListener('pointerdown', unlock, true);
      document.addEventListener('touchend', unlock, true);
    },

    async play(track) {
      const _t0 = performance.now();
      bandcamp.init();
      const seq = ++bandcamp._playSeq;
      const superseded = () => seq !== bandcamp._playSeq;
      // Log the play attempt up front WITH the unlock state — on iOS, an
      // un-primed element silently fails a gesture-less auto-advance, so
      // knowing `unlocked` at dispatch time is the single most useful signal.
      clientLog('bandcamp', 'play: enter', {
        id: track.id, name: track.name, unlocked: !!bandcamp._unlocked,
        hadSrc: !!(bandcamp.audio && bandcamp.audio.currentSrc), prevSource: activeSource,
      });
      // Silence the other sources before we take over.
      try { spotify.stop(); } catch (e) {}
      bandcamp._clearStallWatch();   // a new track invalidates any prior stall watch
      activeSource = 'bandcamp';
      // No stream of ours is loaded until the src assignment below. Leaving the
      // PREVIOUS track's id here would let a late `ended` from it pass the
      // guard and advance the queue for a track that is already gone.
      bandcamp._loadedId = null;
      bandcamp._dur = 0;
      // Resolve a fresh full-track stream URL (expires — never cache it).
      let url = null, art = null;
      const _tResolve = performance.now();
      try {
        const r = await fetch('/api/bandcamp/resolve?id=' + encodeURIComponent(track.id));
        const d = await r.json();
        if (d && d.ok && d.url) {
          url = d.url; art = d.art || null;
          clientLog('bandcamp', 'resolve OK', {
            id: track.id, queue: (url.split('/')[2] || '').slice(0, 40), dur: d.duration,
            streamable: d.streamable, resolveMs: Math.round(performance.now() - _tResolve),
            // Whether the resolver could supply a cover, and whether we will
            // use it: the fallback only fires for a pool row that has none.
            resolvedArt: !!art, hadPoolArt: !!track.art,
          });
        }
        else clientLog('bandcamp', 'resolve failed', { id: track.id, err: d && d.error });
      } catch (e) {
        clientLog('bandcamp', 'resolve threw', { id: track.id, err: String(e).slice(0, 120) });
      }
      if (!url) return false;
      // A newer play() won the element while we were resolving — leave it alone,
      // or we'd overwrite the track the user actually asked for.
      if (superseded()) {
        clientLog('bandcamp', 'play superseded during resolve', { id: track.id }, { transient: true });
        return SUPERSEDED;
      }
      // The resolver's cover is offered ALWAYS, not only when the pool row had
      // none. "The row has a URL" is not "a cover is on screen": a dead pool
      // URL is still a URL, and gating on it threw away a working cover for
      // every stale row (0.7% of Bandcamp rows — ~90 tracks in this pool).
      // setArtFallback only ever gets read if the painted one fails.
      if (art) setArtFallback(art);
      if (art && !track.art) {
        paintArt(art, 'bandcamp-resolve-fallback');
      }
      try {
        bandcamp.audio.src = url;
        // Mark which track the element now holds. Until this line runs, the
        // <audio> element still carries the PREVIOUS track's currentTime, so a
        // poll landing during the resolve fetch must report the old id (→ the
        // _updateProgress guard suppresses it) instead of painting stale %.
        bandcamp._loadedId = track.id;
        await bandcamp.audio.play();
        // play() promise resolved — confirm with element state. A resolved
        // promise does NOT guarantee audible output (cf. the silent-tab class
        // of bug); the 'playing'/'stalled' events above tell the rest.
        clientLog('bandcamp', 'audio.play resolved', {
          id: track.id, paused: bandcamp.audio.paused, readyState: bandcamp.audio.readyState,
          playMs: Math.round(performance.now() - _t0),
        });
      } catch (e) {
        // AbortError here almost always means the user skipped mid-load: our
        // src assignment was replaced by the next track's. That is the queue
        // working, not a playback failure, so don't file it as one.
        if (superseded()) {
          clientLog('bandcamp', 'play superseded during load', { id: track.id }, { transient: true });
          return SUPERSEDED;
        }
        clientLog('bandcamp', 'audio.play rejected', { id: track.id, err: String(e).slice(0, 120), unlocked: !!bandcamp._unlocked });
        return false;
      }
      if (superseded()) return SUPERSEDED;   // a newer track owns the element now
      _startPoll();
      return true;
    },

    togglePlay() { if (!bandcamp.audio) return; if (bandcamp.audio.paused) bandcamp.audio.play().catch(() => {}); else bandcamp.audio.pause(); },
    pause()  { if (bandcamp.audio) bandcamp.audio.pause(); },
    resume() { if (bandcamp.audio) bandcamp.audio.play().catch(() => {}); },
    seek(posMs) { if (bandcamp.audio && isFinite(posMs)) { try { bandcamp.audio.currentTime = Math.max(0, posMs) / 1000; } catch (e) {} } },
    getState() {
      if (!bandcamp.audio) return null;
      return {
        position: (bandcamp.audio.currentTime || 0) * 1000,
        duration: bandcamp._dur || ((bandcamp.audio.duration || 0) * 1000),
        paused: bandcamp.audio.paused,
        trackId: bandcamp._loadedId || null,
      };
    },
    stop() {
      bandcamp._clearStallWatch();
      if (bandcamp.audio) { try { bandcamp.audio.pause(); } catch (e) {} }
      // Relinquish the active-source claim. Critical on iOS: the Connect
      // Player.play override calls stop() when switching to a Spotify track,
      // but it lives OUTSIDE this IIFE and can't reach the private
      // activeSource. Without this, activeSource stays 'bandcamp', so
      // Player.getState() keeps reading THIS now-paused <audio> element and
      // the poll reports its frozen position → the progress bar sticks (e.g.
      // jammed at 0:28) while Spotify is actually playing the new track. The
      // guard avoids stomping a bandcamp/spotify claim.
      if (activeSource === 'bandcamp') activeSource = null;
    },
  };

  // ── Shared helpers ──
  let _pollInterval = null;
  // How long the painted track may disagree with the queue cursor before we
  // stop trusting the cursor. Long enough to cover a dispatch beat, short
  // enough that a real desync is visible within one bar-length of silence.
  const _PBAR_MISMATCH_GRACE_MS = 4000;
  let _mismatchSince = 0;
  let _mismatchReported = false;
  let _prevPos = 0;                   // previous poll's position (for forward-progress detection)
  let _listenedMs = 0;                // accumulator: real ms the user actually listened
  let _currentTrackForListen = null;  // track id this accumulator belongs to

  // WHAT COUNTS AS HAVING LISTENED TO SOMETHING
  // ───────────────────────────────────────────
  // Spotify counts a play as a stream at 30 seconds, and
  // scripts/import_spotify_export.py already reuses that number, so DIG's
  // 'listened' means the same thing wherever it is written. The percentage is
  // the escape hatch for a track shorter than the threshold — four fifths of a
  // 22-second interlude is a listen by any reading, and 30s of it does not
  // exist to be measured.
  //
  // Both are read off _listenedMs, which counts FORWARD PLAYBACK ONLY (see the
  // seek guard in _accumulateListen). Scrubbing to the end does not earn either.
  const LISTEN_STREAM_MS = 30000;
  const LISTEN_DEEP_PCT = 80;
  let _listenMilestoneFired = false;  // once per track: the promotion is not a repeat event
  let _listenDurMs = 0;               // duration of the track the accumulator belongs to
  let _lastSampleAt = 0;              // wall clock of the last sample fed in

  function _resetListenAccumulator(trackId) {
    _listenedMs = 0;
    _prevPos = 0;
    _currentTrackForListen = trackId;
    _listenMilestoneFired = false;
    _listenDurMs = 0;
    _lastSampleAt = 0;
  }
  // Public so playCurrentTrack can call it on new track start
  window._resetListenAccumulator = _resetListenAccumulator;
  // Public because the connect-poll lives in a different closure in this file
  // and is the ONLY thing that sees playback on Spotify Connect. Same reason
  // _resetListenAccumulator is public: one meter, reachable by every feeder.
  window._accumulateListen = (...a) => _accumulateListen(...a);

  // ── THE METER ──────────────────────────────────────────────────────────────
  //
  // ONE accumulator, fed by every path that learns where playback is. That is
  // the whole point of it living here rather than inside a poll: it used to be
  // spliced into _updateProgress, which only ever runs under the Web Playback
  // SDK, so on Spotify Connect — the iOS lane, and any desktop session driving
  // a phone or a speaker — NOTHING measured anything, ever. getPlayedPct()
  // returned null for those sessions for their entire life. That is the real
  // reason 1,687 of 3,755 'listened' rows on 2026-08-18 carried no played_pct:
  // not tabs closed mid-song, but a meter that was never wired to the path the
  // listener was actually on.
  //
  // Feed it from a REAL sample only. The connect-poll's 250ms interpolator
  // extrapolates position between polls to keep the bar gliding; feeding that
  // in would count seconds twice and invent listening during a stall.
  function _accumulateListen(posMs, durMs, trackId, nowMs) {
    if (!durMs || !trackId) return;

    // The accumulator belongs to whatever is ACTUALLY playing. playCurrentTrack
    // resets it on dispatch, which covered every track DIG itself started and
    // nothing else. Spotify moves on by itself constantly — Connect
    // auto-advance at the end of a track, an AirPods double-tap, the lock
    // screen — and none of those paths touched it, so the outgoing track's
    // listened-ms kept accruing against the incoming one.
    if (trackId !== _currentTrackForListen) {
      if (_currentTrackForListen && typeof window.onPlaybackLeft === 'function') {
        // Bookkeeping must never take playback with it — see the localStorage
        // quota incident in app.js's addToHistory.
        try {
          window.onPlaybackLeft(_currentTrackForListen, window.getPlayedPct(), _listenedMs);
        } catch (e) {
          clientLog('listen', 'leave handler threw — playback kept',
                    { err: String((e && e.message) || e).slice(0, 200) });
        }
      }
      _resetListenAccumulator(trackId);
    }
    _listenDurMs = durMs;

    // PLAYBACK CANNOT OUTRUN THE CLOCK, and that — not a fixed window — is what
    // separates it from a seek. The old rule accepted a delta of 50–2000ms,
    // which was written for a 500ms SDK poll and silently rejected every sample
    // from anything slower: the connect-poll sits at 9s mid-track, so even once
    // it was feeding this, not one millisecond would have been counted. A tab
    // throttled in the background has the same problem in the same direction.
    //
    // So the allowance is the time that actually elapsed between samples. Real
    // playback fits inside it; a scrub forward does not. The tolerance covers
    // timer jitter and the propagation lag on a Connect device.
    const elapsed = _lastSampleAt ? (nowMs - _lastSampleAt) : null;
    const allowance = elapsed == null ? 2000 : elapsed + 750;
    const delta = posMs - _prevPos;
    if (delta > 50 && delta <= allowance) _listenedMs += delta;
    _prevPos = posMs;
    _lastSampleAt = nowMs;

    // THE ONLY THING THAT PROMOTES A TRACK TO 'listened'.
    //
    // A dispatch writes 'served' and stays there until this fires, because a
    // dispatch is not evidence that anyone heard anything. Firing here rather
    // than at the end of a track is what makes the promotion independent of HOW
    // a track ends: closing the tab, walking away, a Connect handoff and a skip
    // all leave the accumulator's verdict already recorded.
    if (!_listenMilestoneFired &&
        (_listenedMs >= LISTEN_STREAM_MS ||
         (_listenedMs / durMs) * 100 >= LISTEN_DEEP_PCT)) {
      _listenMilestoneFired = true;
      const pct = Math.min(100, (_listenedMs / durMs) * 100);
      clientLog('listen', 'stream threshold reached', {
        id: String(trackId).slice(0, 12),
        ms: Math.round(_listenedMs), pct: Math.round(pct),
      });
      try {
        if (typeof window.onListenMilestone === 'function') {
          window.onListenMilestone(trackId, pct);
        }
      } catch (e) {
        clientLog('listen', 'milestone handler threw — playback kept',
                  { err: String((e && e.message) || e).slice(0, 200) });
      }
    }
  }

  function _updateProgress(posMs, durMs, trackId) {
    if (!durMs) return;
    // Skip-transition guard: for a beat after we dispatch a new track, the SDK
    // keeps reporting the OLD track (with its old position). Painting that would
    // bounce the bar to the just-skipped track's % before the new track loads
    // (the "0 → old% → 0" glitch). Only paint when the SDK's reported track is
    // the one DIG currently intends to show; otherwise hold the reset-to-0.
    // NO id means we cannot verify, which is not the same as verified-ok. The
    // guard used to be `if (trackId)`, so an empty id skipped the check and
    // painted whatever the element happened to hold — observed 2026-08-01
    // 03:29:22 as a 37.3% paint 355 ms into a fresh track, the bar flashing to
    // the previous song's position on the way past. Bandcamp reports no id
    // during the resolve window by design; unverifiable means hold the zero.
    if (!trackId) {
      pbarLog('unverified — no track id', (posMs / durMs) * 100, {});
      return;
    }
    {
      const intended = queue.currentTrack();
      if (intended && intended.id && trackId !== intended.id) {
        // TIME-BOX IT. This is a guard against a transition BEAT, but it had no
        // bound — so when the cursor and the audio genuinely diverged, it
        // suppressed every paint for the rest of the track and the bar sat at
        // zero while the clock ran behind it. Reported 2026-08-01 on Witch's
        // Book: the audio and the artwork were both right, the cursor was not,
        // and the only symptom was a dead progress bar.
        //
        // Past a beat, the audio is the fact and the cursor is the thing that
        // is wrong. Paint what is playing and say so loudly, rather than
        // hiding a desync behind a bar that looks merely broken.
        _mismatchSince = _mismatchSince || Date.now();
        if (Date.now() - _mismatchSince < _PBAR_MISMATCH_GRACE_MS) {
          pbarLog('SDK-suppressed', (posMs / durMs) * 100,
            { trackId: (trackId || '').slice(0, 10), intended: (intended.id || '').slice(0, 10) });
          return;
        }
        if (!_mismatchReported) {
          _mismatchReported = true;
          clientLog('pbar', 'queue cursor disagrees with the audio — painting the audio', {
            playing: trackId, cursor: intended.id,
            cursorName: (intended.name || '').slice(0, 40),
            ms: Date.now() - _mismatchSince,
          });
        }
      } else {
        _mismatchSince = 0;
        _mismatchReported = false;
      }
    }
    pbarLog('SDK-paint', (posMs / durMs) * 100, { trackId: (trackId || '').slice(0, 10) });
    // The SDK's own samples into the shared meter. This path polls fast, so its
    // deltas are small; _accumulateListen sizes the seek guard off the elapsed
    // time rather than assuming that cadence, because the connect-poll feeds
    // the same meter at 9s.
    _accumulateListen(posMs, durMs, trackId, Date.now());
    document.getElementById('player-progress-fill').style.width = ((posMs / durMs) * 100) + '%';
    document.getElementById('player-time').textContent = _msToTime(posMs);
    // Mirror to mobile player (skip while user is dragging)
    const mcp = document.getElementById('mc-progress');
    if (mcp && !mcp.classList.contains('dragging')) {
      const f = document.getElementById('mc-progress-fill');
      if (f) f.style.width = ((posMs / durMs) * 100) + '%';
      const cur = document.getElementById('mc-time-cur');
      const tot = document.getElementById('mc-time-tot');
      if (cur) cur.textContent = _msToTime(posMs);
      if (tot) tot.textContent = _msToTime(durMs);
    }
  }

  // Returns 0–100 = (real seconds listened / track length) — robust against seeking.
  //
  // Against _listenDurMs, the duration of the track the ACCUMULATOR is holding.
  // It used to divide by _lastDur, which was only ever written on the SDK path,
  // so on Spotify Connect this returned null for every track in every session —
  // which is how played_pct ended up 45% empty, and why every reader downstream
  // had to invent its own policy for a missing measurement.
  window.getPlayedPct = function () {
    if (!_listenDurMs) return null;
    return Math.min(100, Math.max(0, (_listenedMs / _listenDurMs) * 100));
  };
  // Diagnostic — useful for the firstplay/skip telemetry
  window.getListenedMs = function () { return _listenedMs; };

  // ONE definition of the threshold, shared with app.js. Two copies of a number
  // this load-bearing is how the skip path and the milestone path end up
  // disagreeing about whether the same play was a listen.
  window.LISTEN_STREAM_MS = LISTEN_STREAM_MS;
  window.LISTEN_DEEP_PCT = LISTEN_DEEP_PCT;

  function _msToTime(ms) {
    const s = Math.floor(ms / 1000);
    return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0');
  }

  // ── Audio-health probe — AN OBSERVER, AND ONLY AN OBSERVER ────────────────
  //
  // It answers "is sound actually coming out", because Spotify's 204 /
  // `paused:false` does not: a phantom device reports healthy playback in
  // perfect silence. That is worth knowing, so it is logged.
  //
  // IT MUST NEVER COMMAND PLAYBACK. It did between 2026-07-30 and today, and
  // the cost was the bug this comment exists to prevent coming back:
  //
  //   03:59:17.984  audio-probe  recovery: re-dispatching current track
  //   03:59:17.986  pbar         instant-snap pct 0        ← song back to 0:00
  //   03:59:17.987  art          cleared to placeholder    ← cover gone
  //
  // Eight of those in ninety minutes on 2026-08-05, every one on a track that
  // was playing. The recovery was written on 2026-07-30 for the desktop SDK
  // path, where three things were true that are false on Connect: getState was
  // a real-time local read (it is now a polled /me/player snapshot that
  // legitimately freezes during dispatch propagation, tab suspension and
  // resume); _startPoll restarted on every spotify.play, reseeding the
  // baseline (on Connect it is never restarted); and a re-dispatch went
  // through the SDK rather than as transfer-then-play.
  //
  // That last one is decisive, and Invariant A already says why: the hazard is
  // the MECHANISM. A play command goes out as transfer-then-play and the
  // transfer pauses the device — which is what cost "Ari Ari" on 2026-08-03.
  // Invariant A blocks re-commanding a track Spotify is already playing except
  // for playback DIG started itself, and the recovery walked through exactly
  // that door to do exactly that damage.
  //
  // So: if a real stall pattern shows up in these WARNs, fix it with something
  // that does not command — a seek on the no-transfer path, which Invariant A
  // already recognises as harmless. Do not put the re-dispatch back.
  function _startPoll() {
    if (_pollInterval) clearInterval(_pollInterval);
    let _probeLastPos = -1, _probeStalled = 0, _probeWarned = false,
        _probeTrack = null, _probeStallStartedAt = 0, _probeLastSampleAt = 0;
    _pollInterval = setInterval(async () => {
      const state = await Player.getState();
      if (state) {
        _updateProgress(state.position, state.duration, state.trackId);
        const _sinceLastSampleMs = _probeLastSampleAt
          ? Date.now() - _probeLastSampleAt : null;
        _probeLastSampleAt = Date.now();
        const _ctx = () => ({
          track: (state.trackId || '').slice(0, 10),
          position: state.position, duration: state.duration,
          // A sample count is not a duration, and reading it as one leaves the
          // log unable to answer the first question about any stall. This loop
          // ASKS for 500ms; a hidden tab does not have to grant it. On
          // 2026-08-06 the WARN below fired at six samples and 89 SECONDS —
          // 15s per sample — and nothing recorded says whether the audio froze
          // for three seconds or for a minute and a half, nor whether it was
          // the audio that stopped or this timer that was throttled. These two
          // numbers separate those.
          stalledMs: _probeStallStartedAt ? Date.now() - _probeStallStartedAt : 0,
          sinceLastSampleMs: _sinceLastSampleMs,
          // How long ago DIG dispatched. A "stall" a second or two after a
          // play is propagation lag — /me/player is still reporting the
          // OUTGOING track, whose position is frozen because Spotify has
          // already left it. Recorded rather than acted on, so the two can
          // finally be told apart in the log.
          sinceDispatchMs: Date.now() - (Player._lastPlayStarted || 0),
          // iOS suspends JS on a hidden tab, so a gap here is the tab being
          // away, not the audio stopping. That is what produced the 03:59:17
          // false stall above.
          vis: (typeof document !== 'undefined' && document.visibilityState) || null,
          deviceActive: state.deviceActive != null ? state.deviceActive : null,
          activeSource,
          sdkRegistered: spotify._sdkRegistered, activated: spotify._activated,
          deviceId: (state.deviceId || spotify.deviceId || '').slice(0, 12),
          fallback: (spotify._fallbackDeviceId || '').slice(0, 12),
        });
        // A stall that CLEARS ON ITS OWN was never a stall. Logging the
        // resolution — and how long it lasted — is what turns these WARNs from
        // a count into evidence: a real silence never clears without the user,
        // a propagation artefact clears in a second or two. Without this the
        // 162 detections of 2026-08-04 read as 162 real stalls, which is the
        // reading that put the re-dispatch on this path in the first place.
        const _clear = (why) => {
          if (_probeWarned) {
            clientLog('audio-probe', 'stall CLEARED on its own — was not silence',
              Object.assign({ why, stalledMs: Date.now() - _probeStallStartedAt }, _ctx()));
          }
          _probeStalled = 0; _probeWarned = false; _probeLastPos = state.position;
        };
        if (state.trackId !== _probeTrack) {
          // A DIFFERENT track is on the wire, so every sample before this one
          // measured something else. Without this the baseline survived a
          // skip: the outgoing track left `_probeLastPos` at, say, 104807, the
          // incoming one reported 0 → 3000, and "position not advancing" was
          // true six polls running because 3000 is not greater than 104807.
          // The probe asks "is THIS track advancing", so it has to be keyed to
          // the track it is asking about.
          _clear('track-changed');
          _probeTrack = state.trackId;
        } else if (state.paused) {
          _clear('paused');
        } else if (state.position > _probeLastPos + 5) {
          _clear('advancing');   // advancing = real audio
        } else {
          if (_probeStalled === 0) _probeStallStartedAt = Date.now();
          _probeStalled++;
          if (_probeStalled >= 6 && !_probeWarned) {
            _probeWarned = true;
            clientLog('audio-probe',
              'WARN: reported playing but position not advancing (possibly SILENT)',
              Object.assign({ samples: _probeStalled }, _ctx()));
          }
        }
      }
      void syncMediaSessionPlayback();
    }, 500);
  }

  // ── Media-session anchor ────────────────────────────────────────────────
  // `navigator.mediaSession.playbackState` is ADVISORY in Chrome. The state
  // the OS actually sees — and uses to decide whether a headset gesture means
  // "play" or "next track" — is derived from a real media element in the
  // frame tree. On the Spotify path our audio lives in the SDK's CROSS-ORIGIN
  // iframe, so this page owns no element: the OS session belongs to the SDK,
  // and once it goes stale (pause → idle → resume) the OS keeps reporting
  // "paused" no matter what we set. An AirPods double-tap then arrives as the
  // `play` action instead of `nexttrack` — and `play` while already playing
  // is a no-op, so skip looks dead. Re-setting metadata/handlers on a track
  // change does NOT recover it; the state was never ours to set.
  //
  // So own an element. A silent looping clip in the TOP frame plays exactly
  // while DIG is logically playing, so the OS state is derived from something
  // we control and cannot drift from the player.
  //
  // Not used when Bandcamp is active (that path already owns a real <audio>)
  // nor on iOS, where Spotify Connect plays inside the Spotify app — the Now
  // Playing session belongs there and hijacking it would break the lock
  // screen controls that currently work.
  const msAnchor = {
    el: null, wanted: false, _gestureHooked: false, _srcUrl: null,

    // 6 s of 8 kHz mono 8-bit silence. Built at runtime: Chrome only grants a
    // media session to media longer than ~5 s, and a 6 s clip as a base64
    // literal would be ~48 KB of source for no benefit.
    _src() {
      if (msAnchor._srcUrl) return msAnchor._srcUrl;
      const SR = 8000, n = SR * 6;
      const buf = new ArrayBuffer(44 + n), view = new DataView(buf);
      const tag = (off, s) => { for (let i = 0; i < s.length; i++) view.setUint8(off + i, s.charCodeAt(i)); };
      tag(0, 'RIFF'); view.setUint32(4, 36 + n, true); tag(8, 'WAVEfmt ');
      view.setUint32(16, 16, true); view.setUint16(20, 1, true); view.setUint16(22, 1, true);
      view.setUint32(24, SR, true); view.setUint32(28, SR, true);
      view.setUint16(32, 1, true); view.setUint16(34, 8, true);
      tag(36, 'data'); view.setUint32(40, n, true);
      new Uint8Array(buf, 44).fill(128);   // 128 = silence in unsigned 8-bit PCM
      msAnchor._srcUrl = URL.createObjectURL(new Blob([buf], { type: 'audio/wav' }));
      return msAnchor._srcUrl;
    },

    init() {
      if (msAnchor.el) return msAnchor.el;
      const a = document.createElement('audio');
      a.src = msAnchor._src();
      a.loop = true;
      a.preload = 'auto';
      // MUST stay unmuted at volume > 0 — Chrome ignores muted elements when
      // deciding who owns the media session. The samples are silence, so
      // nothing is audible.
      a.muted = false;
      a.volume = 1;
      a.setAttribute('playsinline', '');
      a.setAttribute('aria-hidden', 'true');
      try { document.body.appendChild(a); } catch (e) {}
      msAnchor.el = a;
      return a;
    },

    /** Reconcile the anchor with the logical player state. Idempotent. */
    apply(playing) {
      const want = !!playing && !DIG_IS_IOS && activeSource === 'spotify';
      msAnchor.wanted = want;
      const a = want ? msAnchor.init() : msAnchor.el;
      if (!a) return;
      if (want) {
        if (a.paused) {
          const p = a.play();
          if (p && p.catch) p.catch(() => msAnchor._retryOnGesture());
        }
      } else if (!a.paused) {
        try { a.pause(); } catch (e) {}
      }
    },

    // Autoplay policy can refuse the first play() when the anchor is started
    // outside a gesture (the 500 ms poll, an auto-advance). Re-arm on the next
    // real interaction instead of hammering play() twice a second.
    _retryOnGesture() {
      if (msAnchor._gestureHooked) return;
      msAnchor._gestureHooked = true;
      const go = () => {
        msAnchor._gestureHooked = false;
        document.removeEventListener('pointerdown', go, true);
        document.removeEventListener('keydown', go, true);
        if (msAnchor.wanted && msAnchor.el && msAnchor.el.paused) {
          const p = msAnchor.el.play();
          if (p && p.catch) p.catch(() => {});
        }
      };
      document.addEventListener('pointerdown', go, true);
      document.addEventListener('keydown', go, true);
    },
  };

  /** Keep lock screen / AirPods / Control Center in sync with the active backend. */
  async function syncMediaSessionPlayback() {
    if (!('mediaSession' in navigator)) return;
    if (!activeSource) {
      msAnchor.apply(false);
      try { navigator.mediaSession.playbackState = 'none'; } catch (e) {}
      return;
    }
    const st = activeSource === 'bandcamp' ? bandcamp.getState()
             : await spotify.getState();
    if (!st) {
      msAnchor.apply(false);
      try { navigator.mediaSession.playbackState = 'none'; } catch (e) {}
      return;
    }
    // Anchor first: the element IS the OS-facing state, playbackState only
    // annotates it.
    msAnchor.apply(!st.paused);
    try {
      navigator.mediaSession.playbackState = st.paused ? 'paused' : 'playing';
    } catch (e) {}
    if (!navigator.mediaSession.setPositionState) return;
    const durSec = st.duration / 1000;
    if (!durSec || durSec <= 0 || !isFinite(durSec)) return;
    let pos = st.position / 1000;
    pos = Math.min(Math.max(pos, 0), Math.max(durSec - 0.05, 0));
    try {
      navigator.mediaSession.setPositionState({
        duration: durSec,
        playbackRate: st.paused ? 0 : 1,
        position: pos,
      });
    } catch (e) {
      try {
        navigator.mediaSession.setPositionState({
          duration: durSec,
          playbackRate: 1,
          position: pos,
        });
      } catch (e2) {
        try { navigator.mediaSession.setPositionState(null); } catch (e3) {}
      }
    }
  }

  // ── Public API ──
  return {
    async init() {
      bandcamp.init();
      if (DIG_GUEST) return;   // guests: Bandcamp only, no Spotify
      await spotify.init();
    },

    onTrackEnd(fn) { _onTrackEnd = fn; },

    isReady() {
      return spotify.ready || (DIG_GUEST && bandcamp.ready);
    },
    isSpotifyReady() { return spotify.ready; },
    isPlaying() { return this._playing; },

    get spotifyReady() { return spotify.ready; },
    get spotifyDeviceId() { return spotify.deviceId; },
    // Restricted-track flag, exposed so playCurrentTrack (outside this IIFE)
    // can read it without a ReferenceError on bare `spotify`.
    get spotifyLastRestricted() { return spotify._lastPlayRestricted; },

    _playing: false,
    _stuckTimer: null,

    _clearStuckTimer() {
      if (this._stuckTimer) { clearTimeout(this._stuckTimer); this._stuckTimer = null; }
    },

    _startStuckTimer(track) {
      this._clearStuckTimer();
      this._stuckTimer = setTimeout(async () => {
        // Check if anything is actually playing
        const state = await Player.getState();
        const stuck = !state || state.paused;
        {
          clientLog('play', stuck ? 'STUCK auto-skip after 8s' : 'stuck timer fired but state ok',
            { id: track.id, name: (track.name || '').slice(0,40),
              hasState: !!state, paused: state ? state.paused : null,
              position: state ? state.position : null });
        }
        if (stuck) {
          console.warn(`[DIG] STUCK detected: "${track.artist} - ${track.name}" [${track.source||'spotify'}] id=${track.id} — no playback after 8s, auto-skipping`);
          if (_onTrackEnd) _onTrackEnd();
        }
      }, 8000);
    },

    async play(track) {
      this._clearStuckTimer();
      // Guard against concurrent playback calls
      if (this._playing) {
        clientLog('play', 'Player.play: stopping prior in-flight play before new');
        spotify.stop();
        await new Promise(r => setTimeout(r, 100));
      }
      this._playing = true;
      this._lastPlayStarted = Date.now();
      try {
        const source = track.source || 'spotify';
        if (source === 'bandcamp' || String(track.id || '').startsWith('bc:')) {
          const ok = await bandcamp.play(track);
          clientLog('play', `bandcamp.play returned ${ok}`, { id: track.id });
          // Pass the sentinel through untouched — collapsing it to false here
          // would re-create the spurious retry it exists to prevent.
          if (ok === SUPERSEDED) return SUPERSEDED;
          if (!ok) return false;
          this._startStuckTimer(track);
          return true;
        }
        if (source === 'youtube' || String(track.id || '').startsWith('yt:')) {
          console.warn('[DIG] YouTube playback disabled, skipping');
          clientLog('play', 'Player.play: YouTube disabled — skipping', { id: track.id });
          return false;
        }
        let trackId = track.id;
        if (typeof trackId !== 'string' || !trackId) {
          // Refuse to dispatch a malformed track — sending `spotify:track:undefined`
          // returns a 400 from Spotify and the retry loop just thrashes.
          clientLog('play', 'Player.play: BAD trackId — refusing', { id: track.id, name: track.name, artist: track.artist, source });
          console.warn('[DIG] Player.play: bad trackId, skipping', track);
          return false;
        }
        console.log(`[DIG] Player.play: "${track.artist} - ${track.name}" source=${source} id=${trackId}`);
        clientLog('play', 'Player.play: dispatch', { source, id: trackId, deviceId: spotify.deviceId, sdkReady: spotify.ready });

        {
          if (!spotify.ready) {
            console.warn(`[DIG] Spotify not ready, cannot play "${track.name}"`);
            clientLog('play', 'Player.play: spotify NOT ready — bailing', { id: trackId, deviceId: spotify.deviceId });
            return false;
          }
          const ok = await spotify.play(trackId);
          clientLog('play', `spotify.play returned ${ok}`, { id: trackId });
          if (!ok) return false;
          // The SDK stuck-timer queries the local Web Playback SDK's state.
          // When playback was routed via the Connect fallback (the SDK device
          // failed to register and we POSTed /api/play to a different device),
          // the SDK has no state and getState() always returns null, which
          // produces a false "STUCK" every 8s and auto-skips every track.
          // Skip the timer in fallback mode — the Connect poller has its own
          // dead-playback detection (4 consecutive empty /api/state polls).
          if (!(spotify._sdkRegistered === false && spotify._fallbackDeviceId)) {
            this._startStuckTimer(track);
          }
          return true;
        }
      } finally {
        this._playing = false;
      }
    },

    // A deliberate pause is not a stall, so it disarms the stuck watchdog. The
    // 'playing' listener already does that once output starts, but the listener
    // can press pause while the stream is still buffering — before 'playing'
    // fires — and then nothing else would.
    //
    // These two are the LIVE pause path on desktop only: `if (DIG_IS_IOS)`
    // (1763-3475) replaces Player.togglePlay/Player.pause with Connect versions.
    // Putting the clear there instead would have fixed nothing, because that
    // same block also replaces Player.play — and the two _startStuckTimer calls
    // live in the Player.play HERE, so the watchdog is only ever armed off iOS.
    async togglePlay() {
      this._clearStuckTimer();
      if (activeSource === 'bandcamp') bandcamp.togglePlay();
      else await spotify.togglePlay();
    },

    async pause() {
      this._clearStuckTimer();
      if (activeSource === 'bandcamp') bandcamp.pause();
      else await spotify.pause();
    },

    async resume() {
      if (activeSource === 'bandcamp') bandcamp.resume();
      else await spotify.resume();
    },

    async seekRelative(ms) {
      const _t0 = performance.now();
      clientLog('seek', 'seekRelative: enter (SDK path)', { ms, source: activeSource });
      const state = await this.getState();
      const _tState = performance.now();
      if (!state) {
        console.warn('[DIG playback] seekRelative: no state', { ms, activeSource });
        clientLog('seek', 'seekRelative: BAIL no state', { ms, getStateMs: Math.round(_tState - _t0) });
        return;
      }
      const dur = state.duration;
      if (!dur || dur < 1000 || !isFinite(dur)) {
        // Spotify SDK often reports duration: 0 briefly after a track change; seeking then clamps to 0 → "restart".
        console.warn('[DIG playback] seekRelative: skipped (no duration yet)', { ms, dur, pos: state.position });
        clientLog('seek', 'seekRelative: BAIL no duration', { ms, dur, pos: state.position, getStateMs: Math.round(_tState - _t0) });
        return;
      }
      const target = Math.max(0, Math.min(dur, state.position + ms));
      console.log('[DIG playback] seekRelative', { ms, from: state.position, to: target, dur });
      if (activeSource === 'bandcamp') bandcamp.seek(target);
      else await spotify.seek(target);
      // Snap the bar to the seek target now, bypassing the glide transition.
      if (dur) digPaintProgressInstant((target / dur) * 100);
      clientLog('seek', 'seekRelative: done (SDK)', { ms, from: state.position, to: target, getStateMs: Math.round(_tState - _t0), totalMs: Math.round(performance.now() - _t0) });
    },

    async seekTo(posMs) {
      const state = await this.getState();
      if (!state) return;
      const dur = state.duration;
      if (!dur || dur < 1000 || !isFinite(dur)) {
        console.warn('[DIG playback] seekTo: skipped (no duration yet)', { posMs, dur });
        return;
      }
      const clamped = Math.max(0, Math.min(dur, posMs));
      console.log('[DIG playback] seekTo', { posMs: clamped, dur });
      if (activeSource === 'bandcamp') bandcamp.seek(clamped);
      else await spotify.seek(clamped);
    },

    async getState() {
      if (activeSource === 'bandcamp') return bandcamp.getState();
      return await spotify.getState();
    },

    get activeSource() { return activeSource; },
    // Writable because the iOS Connect block lives OUTSIDE this closure (it
    // starts after the `})()` below), so it cannot assign the private variable
    // however much it wants to. Bandcamp sets it from in here via _bandcamp,
    // and the desktop SDK path sets it in spotify.play — Connect was the one
    // caller with no way to say "Spotify is the source now", so on iPhone
    // activeSource sat at null for the entire session. That is not cosmetic:
    // the audio-health probe's recovery is gated on it, so the one platform
    // where playback silently stalls is the one where nothing recovered it.
    set activeSource(v) { activeSource = v; },

    // Bandcamp delegators — the iOS block replaces Player.play/getState/etc.
    // with Spotify-Connect versions, so those overrides delegate here for
    // bc: tracks (Bandcamp uses the same <audio> path on iOS and desktop).
    _bandcamp: {
      init:   () => bandcamp.init(),
      play:   (t) => bandcamp.play(t),
      toggle: () => bandcamp.togglePlay(),
      pause:  () => bandcamp.pause(),
      resume: () => bandcamp.resume(),
      seek:   (ms) => bandcamp.seek(ms),
      getState: () => bandcamp.getState(),
      stop:   () => bandcamp.stop(),
      isActive: () => activeSource === 'bandcamp',
      isTrack: (t) => !!(t && (t.source === 'bandcamp' || String(t.id || '').startsWith('bc:'))),
    },

    syncMediaSession: () => syncMediaSessionPlayback(),
  };
})();

// ── iOS detection: Spotify Web Playback SDK doesn't work on iOS (WebKit limitation).
// On iOS we use Spotify Connect via server-side REST API instead.
// How many upcoming picks to hand Spotify as a play context on iOS so it
// auto-advances natively when a track ends (see Player.play in the iOS block).
// Context size = 1 (current) + DIG_CONNECT_LOOKAHEAD. Spotify keeps walking
// this list while the phone is LOCKED (our JS is frozen then), so the list
// length ≈ how long locked-screen playback lasts: ~24 tracks ≈ 1.5 h.
// Well under Spotify's (undocumented, ~hundreds) play-uris ceiling.

if (DIG_IS_IOS) {
  console.log('[DIG] iOS detected — using Spotify Connect mode (server-side REST API)');

  // Override Player methods to route through server API instead of SDK
  const _origInit = Player.init.bind(Player);
  const _origPlay = Player.play.bind(Player);

  // Track state for the Connect player
  let _connectPlaying = false;
  let _connectTrackId = null;
  // Consecutive tracks that came back "paused at 0" after a 204 OK. Reset by
  // any track that actually starts, so this counts a RUN and not a total —
  // one failure every twenty minutes is not the same situation as three in a
  // row, and only the second is worth abandoning Spotify over.
  let _silentFailures = 0;
  const _SILENT_FAILURE_RUN_LIMIT = 3;
  let _connectPollInterval = null;
  // Public setter so playCurrentTrack (outside this IIFE) can pin DIG's
  // intent the moment a new track is dispatched, BEFORE the network
  // round-trip. Closes the cover-flash race where a poll would fire
  // between playCurrentTrack's DOM update and connect.play's PUT-success
  // handler, see _connectTrackId is still OLD, see Spotify reports OLD,
  // see no mismatch, and overwrite the DOM with OLD values.
  Player._setConnectTrackId = function (id) { _connectTrackId = id; };
  /**
   * What DIG currently believes is playing on Connect.
   *
   * There is a setter here and there was no getter, so a caller outside this
   * closure that needed to COMPARE against DIG's belief reached for
   * `_connectTrackId` directly — which is not in scope, throws a ReferenceError
   * at runtime, and node --check cannot see. It was swallowed whole by a
   * `.catch(() => {})` on the visibility handler; the feature simply never ran
   * and nothing said so. Reading state across the seam needs a door too.
   */
  Player._connectBelief = function () {
    return { trackId: _connectTrackId, playing: _connectPlaying };
  };
  Player._getConnectTrackId = function () { return _connectTrackId; };

  Player.init = async function() {
    // Prime the in-page <audio> element on EVERY account, not just guests:
    // approved Spotify users still hit Bandcamp tracks (interleaved in the feed
    // / on auto-advance), and on iOS that <audio> must be unlocked by an early
    // gesture BEFORE the first gesture-less Spotify→Bandcamp handoff, or the
    // play fails. Doing it lazily inside bandcamp.play() registered the unlock
    // mid-playback, and the next touch clobbered the live stream with silence.
    try { Player._bandcamp.init(); } catch (e) {}
    // Guests have no Spotify — skip Connect entirely; Bandcamp plays via <audio>.
    if (DIG_GUEST) return;
    // On iOS, don't init the SDK. Instead, check if user is authenticated
    // and if Spotify app has an active device.
    const status = document.getElementById('player-status');
    try {
      const res = await fetch('/token');
      const data = await res.json();
      if (data.error) {
        if (data.auth_url) {
          status.textContent = 'login →';
          status.style.cursor = 'pointer';
          status.onclick = () => { window.location.href = '/login'; };
        }
        return;
      }
      // On iOS we DON'T pick a specific device. We let Spotify route the
      // play command to whatever device was last active (usually the phone's
      // Spotify app). Specifying a device_id was causing DIG to target the
      // laptop's SDK instead of the phone.
      Player._connectReady = true;
      Player._connectDeviceId = null; // null = let Spotify decide
      Player._connectDeviceName = 'Spotify';
      status.textContent = '';
      clientLog('connect', 'ready (no device pinned — Spotify decides routing)');
      // A REFRESH, not the first ask — app.js probes at boot, seconds earlier.
      // Rate-limited (probe, not probeNow) so it costs nothing when boot's
      // answer is still warm: two calls per open would be two calls against a
      // dev quota that locks out for ~24h, for an answer already in hand.
      if (DIG_IS_IOS && !DIG_GUEST) SpotifyDevice.probe('connect-ready');
      queue.tryConsumePendingPlay('connect-ready');
    } catch (e) {
      clientLog('connect', 'init failed', { err: String(e) });
      status.textContent = 'error';
    }
  };

  Player.isReady = function() { return DIG_GUEST ? true : !!Player._connectReady; };
  Player.isSpotifyReady = function() { return !!Player._connectReady; };
  Object.defineProperty(Player, 'spotifyReady', { get() { return !!Player._connectReady; } });

  /**
   * `opts.positionMs` — start this track PART WAY IN rather than at zero.
   *
   * Only the handshake return uses it, and it exists because that return had
   * no way to say "Spotify is already playing this, 45 seconds in". Dispatching
   * DIG's look-ahead is what installs the native auto-advance the locked screen
   * depends on, so it cannot be skipped — but at position 0 it yanked the song
   * back to the start, which reads as DIG having lost the listener's place.
   *
   * `opts.capturedAt` is when that position was READ. Roughly a second passes
   * between the read and the play landing, and without subtracting it the
   * takeover lands a second behind every time. Compensating for the client leg
   * only is deliberate: the server's transfer step adds more, and a small
   * UNDERSHOOT replays a moment already heard while an overshoot cuts audio
   * out — the errors are not symmetric.
   */
  Player.play = async function(track, opts) {
    // Bandcamp bypasses Spotify Connect entirely — it plays through the same
    // <audio> backend on iOS as on desktop. Stop Connect (the phone's Spotify
    // app keeps playing otherwise) and the poll, then hand off.
    if (Player._bandcamp && Player._bandcamp.isTrack(track)) {
      // Captured BEFORE _stopConnectPoll, which clears it.
      const spotifyWasPlaying = _connectPlaying;
      try { Player._stopConnectPoll && Player._stopConnectPoll(); } catch (e) {}
      // Silence Spotify ONLY when Spotify is the thing making sound. This used
      // to fire on every Bandcamp track unconditionally, and the comment that
      // stood here named the cost without drawing the conclusion: "this pause
      // is the exact moment the Connect device becomes reclaimable". A paused
      // backgrounded app is what iOS evicts, and an evicted app is why the next
      // Spotify pick finds no device and gets deep-linked into a cold Spotify
      // that opens the track page without playing it. Through a run of Bandcamp
      // tracks it was poking a dead app over and over for nothing — every one
      // of those calls answered `nothing_to_pause`, which is the proof it had
      // no work to do.
      // `_connectPlaying`, not the dispatched source: _lastDispatchedSource is
      // already the NEW track by the time Player.play runs, so a source test
      // here would never be true. This asks the direct question — is Connect
      // making sound right now — which is the only case a pause has any work
      // to do.
      if (spotifyWasPlaying) {
        const d = Player._connectDeviceId ? `?device=${encodeURIComponent(Player._connectDeviceId)}` : '';
        try { fetch('/api/pause' + d); } catch (e) {}  // fire-and-forget
        // Only NOW is the device reclaimable, so stop trusting the lease and
        // go ask. The probe resolves in ~300ms while this track plays for
        // minutes, so the next Spotify pick reads an answer, not a guess.
        SpotifyDevice.endangered('bandcamp-start');
      }
      Player._playing = true;
      Player._lastPlayStarted = Date.now();
      try { return await Player._bandcamp.play(track); }
      finally { Player._playing = false; }
    }
    // Switching back to a Spotify track — make sure any Bandcamp audio is silent.
    try { Player._bandcamp && Player._bandcamp.stop(); } catch (e) {}
    // Mirror of what spotify.play does on the desktop path (bandcamp.stop then
    // activeSource = 'spotify'). Connect could never say this — see the setter
    // — so the audio-probe's silent-recovery, gated on activeSource, was dead
    // on iOS: 162 stalls detected, 2 recovered, and both of those were desktop.
    // Saying it here rather than special-casing the probe keeps one meaning for
    // the variable, so every other reader of it is right on iOS too.
    try { Player.activeSource = 'spotify'; } catch (e) {}
    Player._playing = true;
    Player._lastPlayStarted = Date.now();
    const trackId = track.id || track;

    // INVARIANT A — NEVER COMMAND A TRACK SPOTIFY IS ALREADY PLAYING.
    //
    // A play command at a track that is already playing cannot improve
    // anything and can destroy everything: it goes out as transfer-then-play,
    // and 2026-08-02 06:44 is what that costs — 502, retry, 404, and DIG
    // deciding the phone was gone while the song was audibly playing on it.
    //
    // resumeSpotify has followed-not-dispatched since 2026-08-01, and it did
    // not save that episode: its adopt branch needs a state read, both its read
    // and its cached fallback came back empty 1.8s after a successful one, and
    // it fell through to the dispatch. That is the argument for putting the
    // rule HERE. One branch that adopts is a path that can be missed; a
    // precondition on the only door into Connect cannot be. Handshake return,
    // poll recovery, session sync and the ordinary queue all come through this
    // line.
    //
    // Deliberately NOT conditioned on how the caller arrived or on why the
    // state was read. If Spotify says it is playing this track, this call has
    // nothing left to do — say so and let the caller carry on.
    // "DIG DISPATCHED THIS" is the exception, and it is not the same question
    // as "this is the current Connect track". Both a dispatch and an adoption
    // set _connectTrackId; only a dispatch clears _adoptedTrackId. Reading the
    // wrong one made this rule swallow the context-jump recovery, which
    // re-asserts exactly the track it already sent — a deliberate correction
    // where the state saying "playing" is DIG's own echo, not evidence.
    //
    // So: skip the command for playback DIG did not start — the deep link's,
    // the Spotify app's, another device's — and never for its own.
    // THE ONE DELIBERATE EXCEPTION. Invariant A exists because a redundant
    // command can only fail; installing the look-ahead is not redundant — it
    // replaces Spotify's album with DIG's queue, which is the entire point of
    // the call and cannot be done by observing. The caller says so explicitly
    // rather than this trying to infer it, because "same track, but for a
    // different reason" is exactly the kind of thing an inference gets wrong.
    const digDispatchedThis = (_connectTrackId === trackId && _adoptedTrackId !== trackId);
    const already = Player.lastSpotifyState && Player.lastSpotifyState(15000);
    // THE EXCEPTION IS NOW CONDITIONAL ON BEING HARMLESS.
    //
    // The exception used to read `installLookahead` alone, justified as
    // "Invariant A exists because a redundant command can only fail; installing
    // the look-ahead is not redundant". That is not why Invariant A exists. It
    // exists because the command goes out as TRANSFER-then-play and the
    // transfer pauses the device — the hazard is the mechanism, not the
    // redundancy, so making the call non-redundant never made it safe. It cost
    // "Ari Ari" on 2026-08-03.
    //
    // Removing the exception outright is not the answer either: it would take
    // the look-ahead with it, and Spotify would go back to playing its own
    // album on the next advance (the "lil soda boy" report). What the invariant
    // is actually protecting is playback that currently works. So the exception
    // survives exactly as long as it cannot break that — i.e. only on the
    // no-transfer path. If some future caller sets installLookahead without it,
    // this blocks them, which is the correct default.
    const harmlessInstall = opts && opts.installLookahead && opts.noTransfer;
    if (already && !already.paused && already.trackId === trackId && !digDispatchedThis
        && !harmlessInstall) {
      clientLog('connect', 'already playing this — following, not commanding', {
        id: trackId, posMs: Math.round(already.position || 0),
        device: already.deviceId,
      });
      Player.adoptPlaying(already);
      SpotifyDevice.saw('already-playing');
      _startConnectPoll();
      Player._playing = false;
      return true;
    }

    // ── Multi-URI look-ahead context (iPhone natural-end auto-advance) ──
    // iOS Safari freezes our 1.5s connect-poll when the screen locks or the
    // app is backgrounded, so JS can't detect a track ending and dispatch the
    // next play — the queue stalls (the "next song doesn't start" bug). Instead
    // we hand Spotify a short context of DIG's upcoming picks; Spotify then
    // auto-advances natively, no JS required. A later UI-skip / onTrackEnd
    // sends a fresh context that fully REPLACES this one (we never touch
    // Spotify's additive /queue, so there's no stale-queue "bounce"). When the
    // poll IS alive it observes each advance via the track-changed handler,
    // which lazily splices the advanced-to context track into the nav queue.
    let contextIds = [trackId];
    try {
      const lookahead = (typeof queue.peekNextContext === 'function')
        ? queue.peekNextContext(DIG_CONNECT_LOOKAHEAD) : [];
      const ctxTracks = {};
      for (const nt of lookahead) {
        if (!nt || !nt.id || contextIds.includes(nt.id)) continue;
        // INVARIANT: the Spotify Connect context is Spotify-only. Spotify
        // validates EVERY uri in this array — a single Bandcamp id (bc:...)
        // makes it build "spotify:track:bc:..." and reject the WHOLE play
        // with 400 Invalid track uri, which then cascades into the deep-link
        // fallback (Spotify app reopens) on every play/skip. Bandcamp tracks
        // play via the <audio> backend, never through Connect, so they must
        // never enter this look-ahead. (When the screen is locked Spotify
        // natively auto-advances this Spotify-only subset, which is correct —
        // a Bandcamp handoff can't happen with JS frozen anyway.)
        if (nt.source === 'bandcamp' || String(nt.id).startsWith('bc:')) continue;
        contextIds.push(nt.id);
        ctxTracks[nt.id] = nt;
      }
      Player._connectContextTracks = ctxTracks;
    } catch (e) {
      contextIds = [trackId];
      Player._connectContextTracks = {};
      clientLog('connect', 'lookahead build failed', { err: String(e).slice(0, 120) });
    }
    Player._connectContextIds = contextIds;
    _lastQueuedId = contextIds[1] || null;  // for track-changed log categorisation

    // Aim at a device if ANYTHING knows one. Both sources had the id and both
    // discarded it: the probe returned a bare boolean, and /me/player's `device`
    // field was parsed away. So every play went out as `device=-`, which only
    // reaches a Spotify that is already active — the exact state a backgrounded
    // iPhone drops out of while still playing. Pinning it lets the server's
    // transfer-then-play wake the device instead of 404ing on nothing.
    if (!Player._connectDeviceId) {
      const known = SpotifyDevice.usableDeviceId();
      if (known) {
        clientLog('connect', 'aiming at the device the probe found', { id: known });
        Player._connectDeviceId = known;
      }
    }
    clientLog('connect', 'play', { id: trackId, device: Player._connectDeviceId, ctxLen: contextIds.length });

    async function _tryPlay(deviceId) {
      let posMs = Math.max(0, Math.round((opts && opts.positionMs) || 0));
      if (posMs && opts && opts.capturedAt) {
        posMs += Math.max(0, Date.now() - opts.capturedAt);
      }
      // no_transfer is an ASSERTION ABOUT THE DEVICE, so only a caller holding
      // a fresh state read may make it (see _installLookaheadOnAdopted). The
      // server skips the pause-then-play transfer on it; if the assertion is
      // wrong the play 404s and the server's wake-and-reissue recovery still
      // runs, which is strictly better than pausing a device that was playing.
      const url = `/api/play?tracks=${encodeURIComponent(contextIds.join(','))}` +
                  (deviceId ? `&device=${encodeURIComponent(deviceId)}` : '') +
                  (posMs ? `&position_ms=${posMs}` : '') +
                  ((opts && opts.noTransfer) ? '&no_transfer=1' : '');
      const r = await fetch(url);
      return await r.json();
    }

    // One extra attempt per play call, never a loop.
    let transientRetried = false;
    try {
      let data = await _tryPlay(Player._connectDeviceId);

      // Retry ONLY if we had pinned a device. The server now wakes a sleeping
      // device itself, so a device-less call that still failed has already
      // exhausted recovery — and retrying it unpinned (what this did before)
      // reissued a byte-identical request that could only fail the same way.
      // That no-op retry is why a backgrounded Spotify app meant dead playback.
      // 404 ONLY. A 404 means the device we named does not exist any more, so
      // dropping it and letting the server find another is right.
      //
      // A 5xx is the opposite situation and this used to catch it too, which
      // was actively destructive: 502 means Spotify FOUND the device and it
      // did not answer in time. Unpinning threw away the one piece of good
      // information we had, and the device-less retry could then only return
      // NO_ACTIVE_DEVICE — manufacturing "Spotify is gone" out of "Spotify was
      // busy". Measured 2026-08-01 06:55:5x: 502 on device 177ee437…, unpin,
      // 502, 404, Bandcamp — while the track was audibly still playing.
      // …but NOT for the look-ahead install. It is an optional context
      // takeover on a track already playing, so there is nothing to rescue by
      // finding another device — and a device-less play lets SPOTIFY choose
      // one, which is how a phone once put music on an idle web player on a
      // Mac in another building. It also drops `_connectDeviceId`, and the
      // next thing the listener does is press play, which then goes out
      // device-less too (`transport action=resume device=-`, 05:51:50).
      const optionalTakeover = !!(opts && opts.installLookahead);
      if (data.error && Player._connectDeviceId && /spotify_404/.test(data.error)
          && !optionalTakeover) {
        clientLog('connect', 'play failed on pinned device, retrying unpinned', { err: data.error });
        Player._connectDeviceId = null;
        data = await _tryPlay(null);
      }

      // A 5xx is NOT the same case as the device-less 404 above. It means
      // Spotify found a device and its gateway could not reach it — typically
      // one the server woke seconds earlier that is still coming up — so a
      // retry is not the byte-identical no-op that guard was written to avoid;
      // the app is a second further along each time.
      //
      // Measured 2026-07-31, Flos Virginum: the deep link DID create the
      // device (probe one second earlier: names ["iPhone"], usable 1), the
      // play 502'd 4.9s after the wake, and because nothing was pinned
      // CLIENT-side the guard above was false — so nothing retried at all and
      // DIG declared Spotify unreachable while the device sat there working.
      // That is "Spotify opens but nothing plays".
      if (data.error && /spotify_(500|502|503)/.test(data.error) && !transientRetried) {
        transientRetried = true;
        // Retry against the device the SERVER used, which it now reports on
        // failure. Retrying with Player._connectDeviceId sent nothing at all —
        // the client never had an id, because the server picks and wakes a
        // device inside its own 404 recovery. So the retry went out
        // device-less and 404'd against the very device that had just been
        // woken, and DIG called Spotify unreachable while the probe was
        // reporting the iPhone present one second earlier.
        const onDevice = data.device || Player._connectDeviceId || null;
        clientLog('connect', 'spotify 5xx — device is up but not answering yet, retrying once',
          { err: data.error, trackId, onDevice });
        await new Promise(r => setTimeout(r, _WOKEN_DEVICE_SETTLE_MS));
        // The settle may have been served by a frozen timer that only ran
        // because we just came back. Re-check rather than firing a play into a
        // page that is still hidden — see _retryWhenVisible.
        if (document.visibilityState === 'hidden') {
          clientLog('connect', 'hidden before the 5xx retry — deferring to the return',
            { trackId, onDevice });
          _retryWhenVisible('spotify-5xx');
          Player._playing = false;
          return UNPLAYABLE;
        }
        data = await _tryPlay(onDevice);
      }

      // The server may have woken a sleeping device to make this play land.
      // Pin it so the next play goes straight there instead of rediscovering it.
      //
      // ONLY on success. The failure payload now carries `device` too (so the
      // 5xx retry above can target it), and without this guard the client
      // latched the id from a FAILED play and kept sending it: 177ee437… came
      // back "Device not found" and DIG went on asking for it anyway. Pinning
      // is for a device that just proved it works.
      if (data.device && !data.error) {
        if (data.recovered) {
          clientLog('connect', 'server woke a sleeping device', { device: data.device_name });
        }
        Player._connectDeviceId = data.device;
      }

      // A 5xx THAT SURVIVED THE RETRY IS NOT A MISSING DEVICE. Spotify's
      // gateway could not reach a device it can see; the device is still
      // there and will answer again. Calling giveUp() here latched
      // provenUnreachable, which switches the SOURCE to Bandcamp and narrows
      // the picker away from Spotify for the rest of the session — a
      // permanent verdict from a transient error, and the thing that made
      // this feel unfixable: one busy moment and Spotify was written off.
      //
      // Keep the device pinned and say so. The track does not play, the
      // caller moves on, and the next dispatch tries the same device again.
      if (data.error && /spotify_(500|502|503)/.test(data.error)) {
        clientLog('connect', 'spotify 5xx persisted — transient, NOT declaring Spotify gone',
          { err: data.error, trackId, device: Player._connectDeviceId });
        Player._playing = false;
        return UNPLAYABLE;
      }

      // INVARIANT B — A FAILED CONTROL CALL IS NOT A DEAD DEVICE WHILE SPOTIFY
      // IS PLAYING. This is the one that ends the class rather than the case.
      //
      // 2026-08-02 06:44: the handshake landed, Spotify was playing "No Quiere
      // Novio" at 4602ms on device 177ee437 with deviceActive:true, DIG issued
      // a play at the track that was ALREADY PLAYING, got 502, retried, got
      // 404 — and read that 404 as "the device is gone". It gave up, switched
      // the source, and started Bandcamp over a stream that was working fine.
      // Reported, fairly, as three days of no progress: every fix so far has
      // removed one ROUTE to this, and there were always more routes.
      //
      // So the rule stops being about which error code and starts being about
      // the facts: the control plane failing tells us nothing about whether
      // audio is coming out of the phone. If a state read close to now says
      // Spotify is playing, this track did not start — and NOTHING ELSE is
      // known. lost()/giveUp() are claims about the DEVICE and are simply not
      // ours to make here.
      //
      // ASK, DO NOT ASSUME. The first cut of this read the CACHED state, and
      // that is exactly the kind of patch that has been failing here: after any
      // successful dispatch the cache says "playing", so every later failure
      // was suppressed and DIG could never give up at all. The cache cannot
      // tell independent playback from DIG's own echo.
      //
      // One /me/player read on a failure is affordable — plays fail rarely, and
      // the alternative verdict (giveUp) switches the source for the rest of
      // the session. If Spotify does not answer, we learn nothing and fall
      // through to the old behaviour, which is the safe direction.
      let stillPlaying = null;
      if (data.error) {
        try { stillPlaying = await Player.spotifyState(); } catch (e) { stillPlaying = null; }
      }
      if (data.error && stillPlaying && !stillPlaying.paused && stillPlaying.trackId) {
        clientLog('connect', 'play failed but Spotify is playing — NOT declaring it gone', {
          err: data.error, trackId,
          spotifyOn: stillPlaying.trackId, posMs: Math.round(stillPlaying.position || 0),
          device: stillPlaying.deviceId,
        });
        Player._playing = false;
        return UNPLAYABLE;
      }

      // AN OPTIONAL TAKEOVER IS NOT ENTITLED TO A VERDICT ON THE DEVICE.
      //
      // The look-ahead install runs on a track ALREADY PLAYING, adopted from
      // Spotify. All it buys is that the next natural advance comes from DIG's
      // queue instead of Spotify's album. If it fails, the correct outcome is
      // that we keep Spotify's context — not that Spotify is declared gone.
      //
      // It was declaring Spotify gone. 2026-08-05 05:51, "Burn It All Down",
      // started by the listener in Spotify and adopted correctly:
      //
      //   installing DIG's look-ahead over Spotify's album
      //   transport play … transfer_skipped reason=caller_says_device_is_playing
      //   transport play … play_status=404  devices_seen=0 devices=[]
      //   spotify device lost (play-no-device)
      //   spotify unreachable after handshake — falling back to Bandcamp
      //
      // The 404 is not news: a backgrounded iPhone stops ADVERTISING on
      // /me/player/devices while still playing perfectly well — the trap
      // _adoptPlayerState was written about — and /me/player had reported
      // deviceActive:true on that same device five seconds earlier.
      //
      // The guard above (ask /me/player, do not declare it gone if Spotify is
      // playing) is the general answer and it did not save this: by the time
      // it asked, the answer was no longer "playing". That is exactly why this
      // belongs here as a PRECONDITION rather than there as a rescue. The
      // install had no standing to reach this code at all.
      //
      // Note the .catch in _installLookaheadOnAdopted claimed invariant B
      // covered this. It never ran: Player.play RETURNS unplayable, it does
      // not throw, so the damage was done before the promise settled.
      if (data.error && optionalTakeover) {
        clientLog('connect', 'look-ahead install failed — keeping Spotify\'s context, '
          + 'and saying nothing about the device', { err: data.error, trackId });
        Player._playing = false;
        return UNPLAYABLE;
      }

      if (data.error) {
        SpotifyDevice.lost(data.no_device ? 'play-no-device' : 'play-' + data.error);
        console.warn('[DIG connect] play error after retry:', data);
        // NO AUTOMATIC APP SWITCH. This used to deep-link into Spotify on
        // the first no-device play of a session — which is the normal state of
        // picking up your phone, so the first thing DIG did on being opened was
        // throw the listener into a different app they had not asked for.
        //
        // Bandcamp plays in-browser and always works, so there is music within
        // a second either way. Going to Spotify is now a tap on the banner, and
        // the handshake it starts actually completes: see device.js, which
        // watches for the return, probes, and puts the track back on Spotify.
        SpotifyDevice.giveUp(data.error, { trackId });
        Player._playing = false;
        return UNPLAYABLE;
      }
      _connectPlaying = true;
      _connectTrackId = trackId;
      _adoptedTrackId = null;   // we are driving again; the context is installed
      // New dispatch: Spotify has not been seen on this track yet. Until a
      // poll says otherwise, a jump deep into the look-ahead is the app
      // resuming into the wrong slot rather than anyone asking for it.
      _connectTrackConfirmed = false;
      Player._lastPlayDispatchAt = Date.now();  // for poll-suppression window
      SpotifyDevice.saw('play-ok');
      _startConnectPoll();
      // Pre-queueing DISABLED 2026-04-29. Symptom: every UI-skip would
      // briefly play the new track (cover flashed) then bounce back to
      // the still-queued previous track — Spotify's native queue, having
      // a stale entry, was overriding our /api/play context. AirPods
      // skip still works because mediaSession's `nexttrack` handler is
      // independently registered and fires DIG's nextTrack() logic.
      // (Keep the function for future diagnostic re-enable; just don't call.)
      // void _prequeueNextTracks();
      return true;
    } catch (e) {
      console.error('[DIG connect] play threw:', e);
      Player._playing = false;
      return false;
    }
  };

  // ── Spotify queue pre-loading ───────────────────────────────────────────
  // Only pre-queue 1 track at a time into Spotify's native queue.
  // More than 1 causes staleness: AirPods skip plays the OLDEST queued
  // item (FIFO), which may be from a completely different queue position
  // if the user has been skipping via the UI in between.
  let _lastQueuedId = null;       // the one track sitting in Spotify's queue
  let _prequeueInFlight = false;
  // Bounds the re-issue loop in the context-jump handler below, so a track
  // Spotify simply refuses to play can't ping-pong forever.
  let _contextJumpRecoveries = 0;
  // Has Spotify been seen playing the CURRENT dispatch? Cleared on every new
  // play, set by the poll on confirmed arrival. Gates the context-jump guard
  // so it only ever second-guesses an unconfirmed track.
  let _connectTrackConfirmed = false;
  // The track we are FOLLOWING rather than driving (see Player.adoptPlaying).
  // Null whenever DIG dispatched, because a dispatch always carries the
  // look-ahead and Spotify is then walking DIG's queue, not its own.
  let _adoptedTrackId = null;

  async function _prequeueNextTracks() {
    if (_prequeueInFlight || !DIG_IS_IOS) return;
    _prequeueInFlight = true;
    try {
      // The queue knows what is ahead and what has been heard; this only
      // knows what Spotify will accept. Same invariant as the play context:
      // never hand a Bandcamp id to Spotify's native queue — it would 400 and
      // poison the AirPods skip — so ask for unheard SPOTIFY tracks only.
      for (const t of queue.upcomingUnheard({ spotifyOnly: true })) {
        if (t.id === _lastQueuedId) return; // already queued this one
        // Push to Spotify's native queue
        try {
          const r = await fetch(`/api/queue?track=${encodeURIComponent(t.id)}`);
          const data = await r.json();
          if (data.ok) {
            _lastQueuedId = t.id;
            console.log(`[DIG connect] queued next: ${t.artist} — ${t.name}`);
            clientLog('connect', 'queued next track', { id: t.id, name: t.name });
          }
        } catch (e) {}
        return; // only queue 1
      }
    } finally {
      _prequeueInFlight = false;
    }
  }

  // Let Bandcamp tracks (in-page <audio>) stop the Connect poll cleanly.
  Player._stopConnectPoll = function() {
    if (_connectPollInterval) { clearInterval(_connectPollInterval); _connectPollInterval = null; }
    _connectPlaying = false;
  };

  Player.togglePlay = async function() {
    if (Player._bandcamp && Player._bandcamp.isActive()) return Player._bandcamp.toggle();
    const d = Player._connectDeviceId ? `?device=${encodeURIComponent(Player._connectDeviceId)}` : '';
    if (_connectPlaying) {
      try { await fetch('/api/pause' + d); _connectPlaying = false; } catch(e) {}
    } else {
      try { await fetch('/api/resume' + d); _connectPlaying = true; } catch(e) {}
    }
  };

  Player.pause = async function() {
    if (Player._bandcamp && Player._bandcamp.isActive()) return Player._bandcamp.pause();
    const d = Player._connectDeviceId ? `?device=${encodeURIComponent(Player._connectDeviceId)}` : '';
    try { await fetch('/api/pause' + d); _connectPlaying = false; } catch(e) {}
  };

  Player.resume = async function() {
    if (Player._bandcamp && Player._bandcamp.isActive()) return Player._bandcamp.resume();
    const d = Player._connectDeviceId ? `?device=${encodeURIComponent(Player._connectDeviceId)}` : '';
    try { await fetch('/api/resume' + d); _connectPlaying = true; } catch(e) {}
  };

  // Token cache. Spotify access tokens live ~1h; the SDK refresh cycle
  // gives us a fresh one well before expiry. Caching client-side avoids
  // the 19+ second /token spikes seen when our server is busy refreshing
  // under load. Refresh proactively after ~50 min, on demand on any 401.
  let _cachedToken = { value: null, fetchedAt: 0 };
  const _TOKEN_TTL_MS = 50 * 60 * 1000;  // 50 min — well within Spotify's 1h
  let _tokenCacheHits = 0, _tokenCacheMisses = 0;
  async function _getToken({ force = false } = {}) {
    if (!force && _cachedToken.value &&
        (Date.now() - _cachedToken.fetchedAt) < _TOKEN_TTL_MS) {
      _tokenCacheHits++;
      // Log every 50th hit so we can confirm the cache is doing real work
      if (_tokenCacheHits % 50 === 0) {
        clientLog('token-cache', 'hit milestone', {
          hits: _tokenCacheHits, misses: _tokenCacheMisses,
          ageSec: Math.round((Date.now() - _cachedToken.fetchedAt) / 1000),
        });
      }
      return _cachedToken.value;
    }
    _tokenCacheMisses++;
    const _tFetch = performance.now();
    try {
      const r = await fetch('/token').then(x => x.json());
      const fetchMs = Math.round(performance.now() - _tFetch);
      if (r && r.access_token) {
        _cachedToken = { value: r.access_token, fetchedAt: Date.now() };
        clientLog('token-cache', 'miss → refetched', {
          force, fetchMs, hits: _tokenCacheHits, misses: _tokenCacheMisses,
        });
        return r.access_token;
      }
      clientLog('token-cache', 'miss → no token', {
        force, fetchMs, response_keys: Object.keys(r || {}),
      });
    } catch (e) {
      clientLog('token-cache', 'miss → fetch threw', {
        err: String(e).slice(0, 200), fetchMs: Math.round(performance.now() - _tFetch),
      });
    }
    return _cachedToken.value || null;  // fall back to stale token rather than null
  }
  // Expose for the connect-poller / external callers
  Player._getToken = _getToken;

  // Most-recent state snapshot from connect-poll (or external state events).
  // Used so seek/back/forward don't have to round-trip GET /me/player every
  // time — that endpoint is slow when a track has just started (Spotify's
  // own state hasn't propagated yet — we observed 22s+ blocking calls).
  let _lastStateAt = 0;
  let _lastState = null;
  const _STATE_FRESHNESS_MS = 4000;  // accept poll-state up to 4s old

  // Re-anchor the progress clock. The interpolator + poll read _lastState /
  // _lastStateAt as "position P as of wall-clock T"; calling this on a discrete
  // event (new track, seek) makes the clock reflect intent IMMEDIATELY instead
  // of waiting up to ~3s for the next poll. This is the missing re-anchor that
  // left the PREVIOUS track's progress lingering after a skip.
  // `trackId` is not optional in spirit: Object.assign carries the PREVIOUS
  // track's id forward, so an anchor placed at 0 for a new song produced a
  // clock that claimed to belong to the old one. Nothing read that id before,
  // so it went unnoticed; the interpolator now checks it, and a clock lying
  // about its track would freeze the bar for the whole propagation window.
  Player._anchorProgress = function (position, duration, paused, trackId) {
    _lastState = Object.assign({}, _lastState, {
      // AN ANCHOR IS AN INTENTION, NOT AN OBSERVATION.
      //
      // This writes into the same variable the poll fills from /me/player, and
      // lastSpotifyState() hands that variable to Invariant A as EVIDENCE of
      // what Spotify is doing. So DIG's own optimistic "snap the bar to 0" came
      // back as "Spotify says this track is at 0" and adoption followed it.
      // Measured 2026-08-04 01:50:04 — "already playing this — following, not
      // commanding, posMs: 0" 1.7s after a poll had read position 67779. That
      // is the song jumping back to 0:00 on returning to DIG.
      //
      // One variable, two meanings; the flag keeps them apart. The interpolator
      // still reads this (it WANTS intent — that is what makes the bar move the
      // instant we dispatch); only the "what is Spotify actually doing" reader
      // refuses it.
      _anchored: true,
      position: position,
      duration: (duration != null ? duration : (_lastState && _lastState.duration) || 0),
      paused: !!paused,
      trackId: (trackId !== undefined ? trackId
                : (_lastState && _lastState.trackId) || null),
    });
    _lastStateAt = Date.now();
    _interpLastPct = null;   // intentional re-anchor — reset the bounce baseline
  };
  Player._getStateCachedOrFresh = async function () {
    if (_lastState && (Date.now() - _lastStateAt) < _STATE_FRESHNESS_MS) {
      // Interpolate position by elapsed wall-clock time (only if playing)
      if (!_lastState.paused) {
        const interp = { ..._lastState };
        interp.position = (_lastState.position || 0) + (Date.now() - _lastStateAt);
        if (interp.duration) interp.position = Math.min(interp.duration, interp.position);
        return interp;
      }
      return _lastState;
    }
    return await Player.getState();
  };

  /**
   * Spotify is ALREADY playing what we want. Follow it; send nothing.
   *
   * This is the fix for the failure the handshake kept producing, and the
   * reason it kept producing it. Coming back from the deep link, Spotify is
   * playing the very track DIG asked it to open — and DIG then issued a play
   * command for that same track at that same position, purely to install its
   * look-ahead context. Measured 2026-08-01 06:55:54, two seconds after the
   * listener returned to DIG:
   *
   *   probe    count:1 usable:1 names:["iPhone"] active:true
   *   learned  device 177ee437… playing:true
   *   PUT /me/player          -> 500
   *   PUT /me/player/play     -> 502 "Bad gateway."
   *
   * 502 is Spotify saying the device did not acknowledge the command. The
   * iOS app had been foregrounded for eleven seconds and backgrounded for two;
   * it was playing perfectly and could not be commanded. DIG then unpinned the
   * device, retried with none, got 404 NO_ACTIVE_DEVICE, declared Spotify
   * unreachable and switched to Bandcamp — while Le beaujolais was still
   * playing. The listener saw it freeze at 7 seconds and a Bandcamp track
   * start over the top of it.
   *
   * Every layer of that was DIG's own doing. The command was unnecessary: the
   * desired state already held. So the rule is now the obvious one —
   *
   *   NEVER COMMAND SPOTIFY TO REACH A STATE OBSERVATION ALREADY SHOWS.
   *
   * The look-ahead context is not lost, only deferred: the next real dispatch
   * installs it, and that one happens when the listener skips or the track
   * ends, by which time the app is no longer two seconds off the foreground.
   */
  /**
   * The last /me/player state we read, if it is still fresh enough to trust.
   *
   * `/me/player` returns null TRANSIENTLY — repeatedly observed, including
   * 1.5s after a read carrying a full playing state. A caller that treats one
   * null as "Spotify is not playing" makes the wrong decision on a coin flip.
   * Measured 2026-08-01 12:13:09.212: trackId 6kvwyMeandp…, paused:false,
   * position 2512, deviceActive:true — and the handshake's own read 1.5s later
   * came back null, so it dispatched instead of adopting, and that dispatch
   * 500'd on the transfer, 502'd on the play, 404'd on the retry, and dropped
   * the listener onto Bandcamp while Spotify was playing.
   */
  Player.lastSpotifyState = function(maxAgeMs = 8000) {
    if (!_lastState || !_lastStateAt) return null;
    // An anchor is DIG's intent, not Spotify's answer. Returning one here let
    // Invariant A adopt our own "0" as the truth about a song that was 67s in.
    // Callers asking this question want evidence; "I don't know" is the honest
    // reply when all we hold is what we intended.
    if (_lastState._anchored) return null;
    return (Date.now() - _lastStateAt) <= maxAgeMs ? _lastState : null;
  };

  Player.adoptPlaying = function(state) {
    // Adoption covers exactly ONE track — the one the deep link started. DIG
    // sent no look-ahead context, so when this track ends Spotify continues
    // through its OWN (the album the link opened). Observed 07:25:41 ->
    // 07:32:00: "Lgm. Satria Sejati" adopted and followed cleanly for the full
    // 6m19s, then Spotify moved to "Lgm. Sumpah Pemuda" with ctxPos -1 — its
    // own next album track, not a DIG pick. Recording which track is the
    // adopted one is how the poll knows to hand control back at the boundary.
    _adoptedTrackId = state.trackId;
    _connectPlaying = true;
    _connectTrackId = state.trackId;
    // Confirmed by observation, not by hope: we are adopting what Spotify
    // SAYS it is playing, so the context-jump guard has nothing to second-
    // guess. Leaving this false makes the next poll treat the adopted track
    // as an unconfirmed dispatch and try to correct it.
    _connectTrackConfirmed = true;
    _lastState = state;
    _lastStateAt = Date.now();
    if (state.deviceId) Player._connectDeviceId = state.deviceId;
    // Spotify is the source now. Left as 'bandcamp', every source-aware call
    // — pause, resume, getState — routes to the paused <audio> element instead
    // of to the phone that is actually making sound. stop() is the existing
    // owner of that transition (it clears activeSource); assigning the
    // variable directly is not possible from here anyway, since it lives in
    // the main IIFE and this block is outside it.
    try { Player._bandcamp && Player._bandcamp.stop(); } catch (e) {}
    Player._lastPlayDispatchAt = Date.now();   // poll fast while we settle
    SpotifyDevice.saw('adopted-playing');
    clientLog('connect', 'adopted the track Spotify is already playing', {
      id: state.trackId, atMs: state.position, device: state.deviceId,
    });
    _startConnectPoll();
    _installLookaheadOnAdopted(state);
  };

  /**
   * TAKE THE QUEUE, NOT JUST THE TRACK.
   *
   * A handshake deep-links to `spotify:track:<id>`, and Spotify opens that
   * track inside its OWN ALBUM. Adopting follows the track and leaves the
   * album sitting behind it, so the very next advance — a double-tap, or the
   * track simply ending — plays the next album cut instead of DIG's pick.
   *
   * Reported 2026-08-02: "I double tapped to skip a song by lil soda boy and
   * got another lil soda boy song." The log is unambiguous —
   *
   *   10:21:10.3  adopted 6ljeYIIPOK3 "Plug Me In"  ctx spotify:album:5R58UNHW5v
   *   10:21:32.9  double-tap -> 4ZasdJcL "Courtside"  ctx: the SAME album
   *   10:21:32.9  "adopted track ended — DIG taking back control"  (190ms later)
   *
   * DIG reclaimed almost instantly and it did not matter: Spotify had already
   * started the wrong song. Reacting cannot win here, and with the screen
   * locked DIG cannot react at all — the look-ahead context IS the mechanism
   * that drives advances while JS is frozen. So install it.
   *
   * Re-issuing the play at the CURRENT position is what `opts.positionMs` was
   * built for; the comment on it says the dispatch "is the only reason the next
   * track is DIG's pick and not Spotify's album". The 08-01 adopt path traded
   * that away for smoothness. The trade is what the listener heard.
   *
   * Two guards, both facts rather than hopes:
   *  - only when the QUEUE agrees this is the current track, so the look-ahead
   *    we install actually follows from here;
   *  - once per track, so a re-adoption cannot re-issue in a loop.
   */
  let _lookaheadInstalledFor = null;
  function _installLookaheadOnAdopted(state) {
    if (!state || !state.trackId) return;
    if (_lookaheadInstalledFor === state.trackId) return;
    const cur = queue.currentTrack();
    if (!cur || cur.id !== state.trackId) {
      clientLog('connect', 'adopted, but the queue is not on this track — leaving Spotify\'s context',
        { adopted: state.trackId, queueOn: cur && cur.id });
      return;
    }
    _lookaheadInstalledFor = state.trackId;
    const sentPos = Math.round(state.position || 0);
    const sentAt = Date.now();
    clientLog('connect', 'installing DIG\'s look-ahead over Spotify\'s album', {
      id: state.trackId, atMs: sentPos,
    });
    // capturedAt lets Player.play add the round-trip back on, so the takeover
    // lands where the song actually is rather than a second behind it.
    Promise.resolve(Player.play(cur, {
      positionMs: Math.round(state.position || 0),
      capturedAt: Date.now(),
      installLookahead: true,
      // `state` IS a fresh read saying this device is playing this track — the
      // whole reason we are adopting it. That is the evidence no_transfer
      // requires, and without it this call is the one that stopped "Ari Ari":
      // the transfer paused the phone and the 502 that followed never restarted
      // it. Skipping it is also what makes the claim below true rather than
      // hopeful — a failed play leaves the song playing.
      noTransfer: true,
    })).then(async (ok) => {
      // MEASURE WHERE THE TAKEOVER ACTUALLY LANDED.
      //
      // The play carries position_ms computed BEFORE the request goes out, so a
      // slow round-trip makes it a rewind: Spotify applies a position the song
      // left behind while the call was in flight. Reported 2026-08-04 as "the
      // song went back like 10 seconds without stopping", and the only way to
      // see it in the log was subtracting two timestamps by hand —
      // dispatch atMs 2881 at 02:12:35.031, resolved 02:12:44.162, so the
      // takeover landed 9.1s behind a song that never stopped playing.
      //
      // capturedAt was supposed to cover this ("add the round-trip back on"),
      // but it is evaluated when the URL is built, so it adds ~0ms. A round
      // trip cannot be compensated before it is made. Until that is fixed
      // properly, the error is at least VISIBLE — sentAtMs vs where the song
      // really is, one line, no arithmetic.
      const roundTripMs = Date.now() - sentAt;
      let landedErrMs = null, truthPos = null;
      try {
        const after = await Player.spotifyState();
        if (after && after.trackId === state.trackId) {
          truthPos = Math.round(after.position || 0);
          // Where the song WOULD be had nothing interfered, minus where it is.
          // Positive = we rewound the listener.
          landedErrMs = Math.round(sentPos + roundTripMs - truthPos);
        }
      } catch (e) { /* the measurement must never break the install */ }
      clientLog('connect', 'look-ahead install result', {
        id: state.trackId, ok, roundTripMs, sentPos, truthPos, landedErrMs,
        rewound: landedErrMs != null && landedErrMs > 1500,
      });
    }).catch((e) => {
      // Only a THROW lands here, which a failed play is not — it returns
      // UNPLAYABLE. That mattered: this catch used to carry the claim that
      // "invariant B keeps a failed command from being read as a dead device",
      // and on 2026-08-05 a 404 here declared the device lost and dropped the
      // listener onto Bandcamp mid-song without this line ever running. The
      // guarantee now lives where it can hold — as a precondition in
      // Player.play, keyed on installLookahead.
      clientLog('connect', 'look-ahead install threw',
        { id: state.trackId, err: String(e).slice(0, 100) });
    });
  }

  Player.getState = async function() {
    if (Player._bandcamp && Player._bandcamp.isActive()) return Player._bandcamp.getState();
    return Player.spotifyState();
  };

  /**
   * Turn a /me/player body into DIG's state, and LEARN THE DEVICE FROM IT.
   *
   * `/me/player` and `/me/player/devices` answer different questions, and DIG
   * was only ever asking the second one. `/me/player/devices` lists devices
   * that are advertising themselves; a backgrounded iPhone stops advertising
   * while still playing perfectly well. `/me/player` reports what is ACTUALLY
   * playing, and its `device` field names the machine doing it.
   *
   * Measured 2026-08-01, four seconds apart:
   *
   *   06:43:34  connect-poll  gotState:true paused:false  59689/272554ms
   *   06:43:38  probe         count:0 usable:0 names:[]
   *
   * Spotify was a minute into the track. DIG read that state, threw away the
   * device that came with it, asked the other endpoint, was told "no devices",
   * declared Spotify unreachable, and fell back to Bandcamp mid-song. The play
   * that failed went out as `device=-` — DIG had no id to send because the only
   * place it ever learned one was an empty list, while the answer was sitting
   * in a response it had already parsed.
   *
   * So every state read now also refreshes the device: the id to aim the next
   * play at, and the evidence that Spotify is alive.
   */
  function _adoptPlayerState(s) {
    const dev = s.device || null;
    const out = {
      position: s.progress_ms || 0,
      duration: s.item?.duration_ms || 0,
      paused: !s.is_playing,
      trackId: s.item?.id || null,
      albumArt: s.item?.album?.images?.[0]?.url || null,
      trackName: s.item?.name || null,
      artistName: (s.item?.artists || []).map(a => a.name).join(', ') || null,
      deviceId: dev?.id || null,
      deviceName: dev?.name || null,
      // Instrumentation, not logic. `context` says whether Spotify is playing
      // OUR uri list or something of its own, and `deviceActive` distinguishes
      // "the device went away" from "it is there and stopped" — the exact
      // question left open when playback died at 0:08 on 2026-08-01 11:09 and
      // the only evidence was a null read 40s later.
      contextUri: s.context?.uri || null,
      contextType: s.context?.type || null,
      deviceActive: dev ? !!dev.is_active : null,
      // Read purely so it appears in the log. Shuffle decides where a
      // multi-uri play STARTS, which is how DIG spent 2026-08-05 11:03-11:15
      // playing a random one of its next 25 picks on every dispatch — and the
      // state read that would have said so was throwing this field away.
      // Nothing branches on it: the offset in /api/play makes the start index
      // explicit, so shuffle is now the listener's business and not ours.
      shuffle: s.shuffle_state ?? null,
    };
    if (out.deviceId) {
      // Pin it for the next play. Aiming explicitly is what lets the server's
      // transfer-then-play wake a device that has gone quiet; a device-less
      // play can only ever reach one Spotify already considers active, which
      // is exactly the state a backgrounded phone drops out of.
      if (Player._connectDeviceId !== out.deviceId) {
        clientLog('connect', 'learned device from player state', {
          id: out.deviceId, name: out.deviceName,
          was: Player._connectDeviceId, playing: !out.paused,
        });
        Player._connectDeviceId = out.deviceId;
      }
      // Spotify is demonstrably there. Saying so keeps the fallback from
      // firing and the "Spotify is asleep" banner from appearing over a
      // phone that is audibly playing.
      SpotifyDevice.saw('player-state');
    }
    _lastState = out; _lastStateAt = Date.now();
    return out;
  }

  /**
   * Spotify's own state, ASKED FOR DIRECTLY — never the active source's.
   *
   * getState answers "what is DIG playing", which is the right question almost
   * everywhere and the wrong one for the handshake: coming back from Spotify,
   * activeSource is still `bandcamp` (pausing does not change it), so getState
   * would report the paused Bandcamp track and the takeover would resume the
   * Spotify song at a Bandcamp position. Two different questions that happened
   * to share an answer while only one source could ever be live.
   */
  Player.spotifyState = async function() {
    // Poll Spotify's player state via token — includes album art for iOS
    try {
      const tok = await _getToken();
      if (!tok) return null;
      const r = await fetch('https://api.spotify.com/v1/me/player', {
        headers: { 'Authorization': 'Bearer ' + tok },
      });
      if (r.status === 401) {
        // Token expired between caching and use — force refresh once
        const fresh = await _getToken({ force: true });
        if (!fresh) return null;
        const r2 = await fetch('https://api.spotify.com/v1/me/player', {
          headers: { 'Authorization': 'Bearer ' + fresh },
        });
        if (r2.status === 204 || !r2.ok) return null;
        return _adoptPlayerState(await r2.json());
      }
      if (r.status === 204 || !r.ok) return null;
      return _adoptPlayerState(await r.json());
    } catch (e) { return null; }
  };

  Player.seekRelative = async function(ms) {
    if (Player._bandcamp && Player._bandcamp.isActive()) {
      const st = Player._bandcamp.getState();
      if (st) { Player._bandcamp.seek(Math.max(0, st.position + ms)); void Player.syncMediaSession(); }
      return;
    }
    const _t0 = performance.now();
    clientLog('seek', 'seekRelative: enter (Connect path)', { ms });
    try {
      // Use cached state if recent; only round-trip Spotify if we don't
      // have one. Cuts the typical seek from ~3s to ~500ms and eliminates
      // the 22s pathological case (when a track just started and Spotify
      // hasn't propagated state to its API yet).
      const st = await Player._getStateCachedOrFresh();
      const _tState = performance.now();
      if (!st) {
        clientLog('seek', 'seekRelative: BAIL no state (Connect)', { ms, getStateMs: Math.round(_tState - _t0) });
        return;
      }
      const target = Math.max(0, st.position + ms);
      const tok = await _getToken();
      const _tToken = performance.now();
      if (!tok) {
        clientLog('seek', 'seekRelative: BAIL no token (Connect)', { ms, getStateMs: Math.round(_tState - _t0), tokenMs: Math.round(_tToken - _tState) });
        return;
      }
      const seekResp = await fetch(`https://api.spotify.com/v1/me/player/seek?position_ms=${target}`, {
        method: 'PUT',
        headers: { 'Authorization': 'Bearer ' + tok },
      });
      const _tSeek = performance.now();
      // Optimistically advance our cached position so subsequent seeks
      // don't bounce off the old value before the next poll, and SNAP the bar
      // to the new spot now (instead of letting the CSS transition crawl there
      // over 0.4s — the "lags before catching up" you felt on a 15s skip).
      if (_lastState) {
        _lastState.position = target; _lastStateAt = Date.now();
        _interpLastPct = null;   // intentional seek — reset the bounce baseline
        if (_lastState.duration) digPaintProgressInstant((target / _lastState.duration) * 100);
      }
      clientLog('seek', 'seekRelative: done (Connect)', {
        ms, from: st.position, to: target, status: seekResp.status,
        getStateMs: Math.round(_tState - _t0),
        tokenMs: Math.round(_tToken - _tState),
        seekMs: Math.round(_tSeek - _tToken),
        totalMs: Math.round(_tSeek - _t0),
      });
    } catch (e) {
      clientLog('seek', 'seekRelative: THREW (Connect)', { ms, err: String(e).slice(0, 200), totalMs: Math.round(performance.now() - _t0) });
    }
  };

  let _connectLastArt = null;
  // Which track the card on screen currently belongs to. The poll used to
  // decide about the cover and the title separately, which is how they came to
  // disagree — see the paint block. Tracking the CARD is what lets them move
  // as one thing.
  let _connectPaintedTrackId = null;
  // Let playCurrentTrack tell the poll which cover it already painted (from the
  // prefetch cache), so the next poll won't redundantly re-set the same <img>.
  Player._noteArt = function (url) { _connectLastArt = url || null; };

  let _connectPollFailures = 0;

  // Smooth progress-bar interpolation between Connect polls. Connect polls
  // are spaced seconds apart, so the bar would visibly tick rather than
  // glide. This 250 ms ticker advances the fill using wall-clock elapsed
  // since the last poll; the next poll resets to truth.
  let _progressInterpolator = null;
  let _interpLastPct = null;   // last % the interpolator painted (bounce detector)
  // Connect polls land every ~1.5 s (measured gapMs 1501/1505/1510), so this is
  // roughly six missed polls: long enough that ordinary throttling while the
  // tab is backgrounded does not trip it, short enough that the bar can never
  // wander far from truth on its own.
  const _CLOCK_MAX_EXTRAPOLATION_MS = 10000;
  function _startProgressInterpolator() {
    if (_progressInterpolator) return;
    _progressInterpolator = setInterval(() => {
      if (!_lastState || _lastState.paused || !_lastState.duration) { _interpLastPct = null; return; }
      const mcp = document.getElementById('mc-progress');
      if (mcp && mcp.classList.contains('dragging')) return;  // don't fight user
      // The clock has to belong to the track we are actually on. `match` was
      // already computed here — but only inside the BAR JUMP log payload, so
      // the bar painted the previous song's position and duration anyway.
      // That is the "I skip and it resets, then jumps back to the old
      // timestamp" report: between the skip and the first poll for the new
      // track, _lastState still holds the old one. Observed 2026-07-31,
      // clockTrackId=6D75KWhmhK against intentTrackId=76HIAc01XF, match:false,
      // painted 100% → 34.6%.
      if (_connectTrackId && _lastState.trackId && _lastState.trackId !== _connectTrackId) {
        _interpLastPct = null;   // resuming later is not a "jump"
        return;
      }
      const elapsed = Date.now() - _lastStateAt;
      // Interpolation fills the ~1.5 s between polls. Past a few missed polls
      // it is not smoothing anything, it is inventing a position: with no
      // ceiling a 124-second-old clock (measured, same session) ran the bar to
      // the end of the track while the real poll kept repainting the true
      // position, so the bar flipped between the two — "25 seconds to 1:47 and
      // back". Freezing is the honest answer; the next poll re-anchors it.
      if (elapsed > _CLOCK_MAX_EXTRAPOLATION_MS) {
        _interpLastPct = null;
        return;
      }
      const interpPos = Math.min(_lastState.duration,
                                 (_lastState.position || 0) + elapsed);
      const pct = (interpPos / _lastState.duration) * 100;
      // BOUNCE DETECTOR: during steady play the bar should creep up by ~0.1%
      // per 250ms tick. A backwards move, or a jump >3% between ticks, means
      // the clock got re-anchored to a different position mid-track (the
      // stale-poll clobber, an unexpected poll, a duration change, etc).
      // Stays silent unless something actually misbehaves.
      if (_interpLastPct != null && (pct < _interpLastPct - 0.5 || Math.abs(pct - _interpLastPct) > 3)) {
        clientLog('progress', 'BAR JUMP', {
          fromPct: +_interpLastPct.toFixed(1), toPct: +pct.toFixed(1),
          interpPos: Math.round(interpPos), clockPos: Math.round(_lastState.position || 0),
          clockAgeMs: elapsed, duration: _lastState.duration,
          clockTrackId: (_lastState.trackId || '').slice(0, 10),
          intentTrackId: (_connectTrackId || '').slice(0, 10),
          sinceDispatchMs: Player._lastPlayDispatchAt ? (Date.now() - Player._lastPlayDispatchAt) : null,
          match: _lastState.trackId === _connectTrackId,
        });
      }
      _interpLastPct = pct;
      pbarLog('connect-interp', pct, { clockTrack: (_lastState.trackId || '').slice(0, 10),
        intent: (_connectTrackId || '').slice(0, 10), clockAgeMs: elapsed });
      const tb = document.getElementById('player-progress-fill');
      if (tb) tb.style.width = pct + '%';
      const f = document.getElementById('mc-progress-fill');
      if (f) f.style.width = pct + '%';
      const cur = document.getElementById('mc-time-cur');
      if (cur) {
        const s = Math.floor(interpPos / 1000);
        cur.textContent = `${Math.floor(s/60)}:${String(s%60).padStart(2,'0')}`;
      }
    }, 250);
  }

  // Cadence + drift telemetry for the connect-poll. Captures (a) gap
  // between successive polls (should be steady ~3 s; large jumps mean
  // the timer was throttled by the browser when the tab is backgrounded),
  // (b) how long getState took, (c) drift between our interpolated
  // progress and Spotify's authoritative position. Logged at most once
  // per N polls to keep volume sane.
  let _pollIdx = 0;
  let _pollLastFiredAt = 0;
  const _POLL_LOG_EVERY = 5;  // 1 in 5 polls — every ~15s of playback

  // ── How often to actually ASK Spotify ──────────────────────────────────────
  //
  // Every tick of this poll is a direct https://api.spotify.com/v1/me/player
  // call from the phone. It ran flat out at 1.5s, which is 2,400 calls an hour
  // from ONE listening phone — while the entire crawler fleet is held to 2,400
  // an hour by lib/spotify_gate. So the app's own playback was the largest
  // consumer of the app-wide quota, and the only one outside the gate.
  //
  // That quota is Development Mode and app-wide (Extended Quota is not
  // available to us), and it locks the whole app out for ~18h when tripped.
  // Measured 2026-08-01: 175 of the last 200 crawler runs aborted rate-limited,
  // 26,363 artists sat resolved and unqueryable, and the pool grew 317 tracks
  // in a fortnight while the listener played 362.
  //
  // Polling flat out was never needed. The progress bar is interpolated
  // locally every 250ms from the last known position, so between track
  // boundaries the poll corrects drift and nothing else. What it must NOT miss
  // is a change of state, and those are predictable: just after DIG dispatches
  // a track, and just before a track ends and Spotify natively advances onto
  // the next look-ahead entry. So the rate follows UNCERTAINTY — fast where
  // something is about to change, slow where nothing is.
  //
  // Steady playback drops from 40 calls/minute to under 7.
  const _POLL_FAST_MS     = 1500;   // a change is imminent or in flight
  const _POLL_STEADY_MS   = 9000;   // mid-track: only drift correction
  const _POLL_IDLE_MS     = 20000;  // paused or nothing playing
  const _POLL_DISPATCH_WINDOW_MS = 12000;  // confirm the track we just sent
  const _POLL_BOUNDARY_MS = 15000;  // native auto-advance is close
  let _pollLastAskedAt = 0;

  /** ms to wait before the next real call, given what we currently believe. */
  function _pollIntervalNow() {
    // A dispatch is unconfirmed until the poll has seen it. Everything that
    // decides whether Spotify landed on the right track (the context-jump
    // guard, _connectTrackConfirmed) depends on seeing it promptly.
    if (Player._lastPlayDispatchAt &&
        Date.now() - Player._lastPlayDispatchAt < _POLL_DISPATCH_WINDOW_MS) {
      return _POLL_FAST_MS;
    }
    if (!_lastState) return _POLL_FAST_MS;      // no belief yet — go and look
    // DISAGREEMENT IS THE DEFINITION OF UNCERTAINTY. If the last thing Spotify
    // told us is not the track DIG believes is playing, something is in motion
    // and the steady cadence is the wrong one to be on — whatever the cause,
    // the resolution should not wait 9s for the next look.
    if (_connectTrackId && _lastState.trackId && _lastState.trackId !== _connectTrackId) {
      return _POLL_FAST_MS;
    }
    if (_lastState.paused) return _POLL_IDLE_MS;
    const dur = _lastState.duration || 0;
    if (!dur) return _POLL_FAST_MS;             // can't reason about the end
    const pos = (_lastState.position || 0) + (Date.now() - _lastStateAt);
    // Near the end, Spotify advances by itself through the look-ahead context
    // — on a locked phone that happens with no JS of ours running at all, so
    // the poll is the only thing that will ever notice.
    if (dur - pos < _POLL_BOUNDARY_MS) return _POLL_FAST_MS;
    return _POLL_STEADY_MS;
  }

  function _startConnectPoll() {
    _startProgressInterpolator();
    if (_connectPollInterval) clearInterval(_connectPollInterval);
    _connectPollInterval = setInterval(async () => {
      // The timer still ticks at 1.5s — it is free — but the CALL is rate-
      // governed. Skipping here rather than restarting the interval keeps the
      // cadence able to go fast again the instant a dispatch or a track
      // boundary makes it matter.
      const _now = Date.now();
      const _dueIn = _pollIntervalNow();
      if (_now - _pollLastAskedAt < _dueIn) return;
      const _sinceLastAsk = _now - _pollLastAskedAt;
      _pollLastAskedAt = _now;
      const _pollIx = ++_pollIdx;
      const _pollFiredAt = Date.now();
      const _gapSinceLast = _pollLastFiredAt ? _pollFiredAt - _pollLastFiredAt : null;
      _pollLastFiredAt = _pollFiredAt;
      // Drift snapshot BEFORE getState (so we capture what the UI was showing)
      let _interpPos = null;
      if (_lastState && !_lastState.paused) {
        _interpPos = (_lastState.position || 0) + (_pollFiredAt - _lastStateAt);
      }
      // Snapshot the optimistic clock BEFORE getState — getState() caches its
      // result into _lastState unconditionally, so if this poll turns out to be
      // STALE (Spotify still reporting the previous track), we restore this and
      // keep the from-0 interpolation running instead of freezing the bar.
      const _clockBefore = _lastState ? Object.assign({}, _lastState) : null;
      const _clockAtBefore = _lastStateAt;
      const _tGet = performance.now();
      // SPOTIFY'S state, not the active source's. This is the CONNECT poll —
      // "what is DIG's current source doing" is the wrong question for it, and
      // it gave a catastrophic answer: after a handshake adoption activeSource
      // is still `bandcamp` (the track was paused, not stopped), so getState
      // returned the paused BANDCAMP state and the poll read Spotify as having
      // jumped to a bc: id. It then "corrected" that by dispatching, which is
      // the bounce this whole path was supposed to stop.
      const st = await Player.spotifyState();
      const _getStateMs = Math.round(performance.now() - _tGet);
      // Log every Nth poll, plus every poll where getState was slow OR drift was big
      const driftMs = (st && _interpPos != null && st.position != null)
        ? (st.position - _interpPos) : null;
      const shouldLog =
        (_pollIx % _POLL_LOG_EVERY === 0) ||
        _getStateMs > 2000 ||
        (driftMs != null && Math.abs(driftMs) > 1500);
      if (shouldLog) {
        clientLog('connect-poll', `tick #${_pollIx}`, {
          gapMs: _gapSinceLast, getStateMs: _getStateMs,
          gotState: !!st, paused: st ? st.paused : null,
          interpPos: _interpPos != null ? Math.round(_interpPos) : null,
          truthPos: st ? st.position : null,
          driftMs: driftMs != null ? Math.round(driftMs) : null,
          duration: st ? st.duration : null,
        });
      }
      if (!st) {
        _connectPollFailures++;
        // CRITICAL iPhone autoplay path: when a Spotify Connect track ends
        // on a phone, /me/player stops returning state altogether — it
        // doesn't report paused=true at position 0. So our paused-state
        // track-end detector NEVER fires on iPhone. Detect track-end here
        // by interpolating where we would have been when the state went
        // null. If that's within 3s of the last-known duration, the track
        // ended naturally → fire onTrackEnd. If it's mid-track, this is a
        // genuine pause / network drop / app backgrounded → don't advance.
        if (_connectPlaying && _connectPollFailures === 2 && _lastState
            && _lastState.duration && _lastState.position != null) {
          const interpPos = _lastState.position + (Date.now() - _lastStateAt);
          const remaining = _lastState.duration - interpPos;
          const trackEnded = remaining < 3000; // within 3 s of end
          clientLog('connect', 'null state — evaluating track-end', {
            failures: _connectPollFailures, trackId: _connectTrackId,
            lastPosition: _lastState.position, lastDuration: _lastState.duration,
            interpPos: Math.round(interpPos), remaining: Math.round(remaining),
            verdict: trackEnded ? 'track-ended' : 'genuine-pause-or-loss',
          });
          if (trackEnded) {
            clientLog('connect', 'track ended via null-state (firing onTrackEnd)', {
              trackId: _connectTrackId,
              interpPos: Math.round(interpPos), duration: _lastState.duration,
            });
            _connectPlaying = false;
            if (Player._onTrackEnd) Player._onTrackEnd();
            return;
          }
        }
        // Long-term silence: status hint for the user (genuine playback loss).
        if (_connectPlaying && _connectPollFailures >= 4) {
          clientLog('connect', 'playback appears dead — Spotify returns no state', { failures: _connectPollFailures, trackId: _connectTrackId });
          _connectPlaying = false;
          const s = document.getElementById('player-status') || document.getElementById('pc-region');
          if (s) s.textContent = 'playback lost — tap play';
        }
        return;
      }
      _connectPollFailures = 0;
      // Spotify answered with real state, so its app is awake and registered.
      // This is the cheap, continuous liveness signal — it keeps the lease
      // fresh for the whole of a Spotify track with no extra API calls.
      if (!st.paused) SpotifyDevice.saw('poll');
      // Spotify is on the track DIG asked for: the only real proof a re-issue
      // worked. Resetting on DISPATCH instead would defeat the bound below —
      // each re-issue returns ok, clears the counter, and can jump again
      // forever. Confirmed arrival is the one event that cannot loop.
      if (st.trackId && st.trackId === _connectTrackId) {
        _contextJumpRecoveries = 0;
        // Spotify is demonstrably on the track DIG asked for. Anything that
        // moves it from here is an actor, not a botched resume — see the
        // context-jump guard below, which stops second-guessing at this point.
        _connectTrackConfirmed = true;
      }

      if (st && st.trackId && _connectTrackId && st.trackId !== _connectTrackId) {
        clientLog('connect-poll', 'DIVERGENCE seen', {
          spotify: st.trackId, intent: _connectTrackId,
          cadenceMs: _dueIn, sinceLastAskMs: _sinceLastAsk,
          sinceDispatchMs: Player._lastPlayDispatchAt
            ? (Date.now() - Player._lastPlayDispatchAt) : null,
          adopted: _adoptedTrackId, position: st.position, duration: st.duration,
        });
      }

      // STALE-POLL GUARD (top-level): if Spotify's /me/player is reporting
      // a different trackId from what DIG just dispatched, AND that
      // dispatch was within the last 2.5s, treat the entire poll as stale.
      // Spotify takes ~1-2.5s to propagate PUT /me/player/play; without
      // this guard, the connect-poll repaints cover/title/progress with
      // the OLD track's data, producing a visible cover-flash. The next
      // poll catches up to ground truth.
      const sinceDispatch = Player._lastPlayDispatchAt
        ? (Date.now() - Player._lastPlayDispatchAt) : null;
      if (st.trackId && _connectTrackId && st.trackId !== _connectTrackId &&
          sinceDispatch != null && sinceDispatch < 2500) {
        clientLog('connect', 'stale-poll suppressed (top-level)', {
          dig_intent: _connectTrackId, spotify_reports: st.trackId,
          sinceDispatch_ms: sinceDispatch,
          poll_idx: _pollIx,
        });
        // getState() already cached this STALE old-track state into _lastState
        // (position ~where the previous track was), so the 250ms interpolator —
        // which reads _lastState directly — would advance the bar from the OLD
        // position, bouncing up then snapping back when Spotify finally
        // propagates the new track. RESTORE the optimistic clock we snapshotted
        // before getState: it's anchored at 0 with the known duration, so the
        // bar keeps gliding from 0 through the propagation window. (If we had no
        // prior clock, fall back to a hard 0/0 hold.)
        if (_clockBefore) { _lastState = _clockBefore; _lastStateAt = _clockAtBefore; }
        else if (Player._anchorProgress) Player._anchorProgress(0, 0, false, _connectTrackId);
        return;
      }

      // Feed the listen meter. THIS POLL IS THE ONLY THING THAT SEES PLAYBACK
      // ON CONNECT — there is no SDK here, so _updateProgress never runs and
      // for the whole life of this path nothing measured how much of anything
      // was heard. A real sample, not the 250ms interpolator below it: the
      // interpolator extrapolates between polls to keep the bar gliding, and
      // counting that would both double-count and invent listening during a
      // stall the poll has not noticed yet.
      // Fed while PAUSED too. A paused track's position does not advance, so
      // the delta rule declines to count it without needing to be told — and
      // keeping the samples coming is what stops the seek guard from going
      // slack: the allowance is sized off the gap since the last sample, so a
      // poll that stopped reporting during a five-minute pause would come back
      // willing to accept a five-minute jump as playback.
      if (st.duration && st.trackId) {
        window._accumulateListen(st.position, st.duration, st.trackId, Date.now());
      }
      // Update progress bar
      if (st.duration) {
        pbarLog('connect-poll', (st.position / st.duration) * 100,
          { stTrack: (st.trackId || '').slice(0, 10), intent: (_connectTrackId || '').slice(0, 10) });
        document.getElementById('player-progress-fill').style.width = ((st.position / st.duration) * 100) + '%';
        const mcp = document.getElementById('mc-progress');
        if (mcp && !mcp.classList.contains('dragging')) {
          const f = document.getElementById('mc-progress-fill');
          if (f) f.style.width = ((st.position / st.duration) * 100) + '%';
          const _toT = (ms) => { const s = Math.floor(ms/1000); const m = Math.floor(s/60); return `${m}:${String(s%60).padStart(2,'0')}`; };
          const cur = document.getElementById('mc-time-cur');
          const tot = document.getElementById('mc-time-tot');
          if (cur) cur.textContent = _toT(st.position);
          if (tot) tot.textContent = _toT(st.duration);
        }
      }
      // Update play/pause button
      const btn = document.getElementById('mc-play') || document.getElementById('btn-play');
      if (btn) btn.textContent = st.paused ? '▶' : '❚❚';
      _connectPlaying = !st.paused;
      // Album art + track info — update from what Spotify is ACTUALLY playing.
      // This corrects stale title/artist when the real track differs from
      // what DIG's pool data said (e.g., Spotify auto-advanced, or the play
      // targeted a different version).
      // ART AND TEXT TOGETHER — NEVER HALF A CARD. The title was painted
      // unconditionally and the cover only `if (st.albumArt)`, so a track
      // Spotify reports WITHOUT artwork moved the title and left the previous
      // song's sleeve sitting under it. That is the "cover doesn't match the
      // song" report, produced by DIG itself rather than by any race.
      //
      // The session-sync path already carries this rule and says so in a
      // comment; the poll never got it. Gating on the TRACK rather than on the
      // art's truthiness is what makes them move as one: a new track repaints
      // both (a missing cover becomes the placeholder, which is honest), and
      // the same track only repaints art, so a cover that arrives late still
      // lands without the title flickering.
      const cardChanged = st.trackId && st.trackId !== _connectPaintedTrackId;
      if (cardChanged) {
        _connectPaintedTrackId = st.trackId;
        _connectLastArt = st.albumArt || null;
        paintArt(st.albumArt || null, 'connect-poll');
        paintTrackInfo(st.trackName || null, st.artistName || null);
        // The reactions belong to the card. Leaving them behind is not
        // cosmetic: the heart is a claim about the song you are hearing, and a
        // stale one invites a tap that saves the wrong track.
        queue.repaintReactions(st.trackId);
      } else {
        if (st.albumArt && st.albumArt !== _connectLastArt) {
          _connectLastArt = st.albumArt;
          paintArt(st.albumArt, 'connect-poll-late-cover');
        }
        paintTrackInfo(st.trackName || null, st.artistName || null);
      }
      // Detect external skip (AirPods double-tap): if the track Spotify is
      // playing differs from what DIG last sent, the user skipped via
      // AirPods/Spotify. Sync DIG's state to match.
      if (st.trackId && st.trackId !== _connectTrackId) {
        // STALE-POLL GUARD: Spotify's /me/player endpoint takes ~1-2.5s
        // to reflect a PUT /me/player/play we just sent. Polls firing
        // inside that window report the OLD track id; without this
        // suppression, the connect-poll would interpret it as an
        // "external skip" and rewrite the UI to OLD's cover/title for
        // ~1.5s before catching up — exactly the cover-flash bug. If we
        // dispatched a play recently, log it as state-lag and SKIP all
        // the UI/state-mutating work that follows in this poll.
        const sinceDispatch = Player._lastPlayDispatchAt
          ? (Date.now() - Player._lastPlayDispatchAt) : null;
        if (sinceDispatch != null && sinceDispatch < 2500) {
          clientLog('connect', 'stale-poll suppressed', {
            from: _connectTrackId, to: st.trackId,
            sinceDispatch_ms: sinceDispatch,
            note: 'Spotify state lag after our /api/play — UI not touched',
          });
          // Also do NOT update _connectTrackId; keep it pinned to DIG's
          // intent so the next stale poll is also suppressed.
          return;
        }
        const msSinceLastPlay = Player._lastPlayStarted
          ? (Date.now() - Player._lastPlayStarted) : null;
        // Where the track Spotify is on sits in the look-ahead context we
        // handed it on the last play. -1 = not ours at all.
        const ctxIds = Player._connectContextIds || [];
        const ctxPos = st.trackId ? ctxIds.indexOf(st.trackId) : -1;

        // CONTEXT-JUMP GUARD. We hand Spotify DIG_CONNECT_LOOKAHEAD+1 track
        // URIs so a locked screen can auto-advance natively. The cost is that
        // the Spotify APP, when it wakes from suspension, can resume somewhere
        // ELSE inside that list instead of at index 0 — observed in prod on
        // 2026-07-31: DIG dispatched 3YuH2kBeqv…, Spotify came up on
        // 16wVEQXd8U…, which was position 13 of the very array we had just
        // sent. Treating that as an "external skip" is what produced the
        // user-visible bug: the UI paints the track DIG asked for, the audio
        // is a different one, and a beat later the title rewrites itself to
        // match Spotify.
        //
        // Distinguishing that from a legitimate move used to be done on
        // ELAPSED TIME — a step to position 1 counted as natural only if the
        // previous track had been playing 30s+. That clause was wrong, and it
        // broke the AirPods double-tap. On the Connect path the Spotify APP
        // owns the Now Playing session, so a double-tap never reaches DIG at
        // all (zero `media` events in the 6h to 2026-08-01 22:54, while the
        // lifecycle log shows activeSource null on all 18 visibility changes —
        // DIG holds no audio session to receive a remote command). The tap
        // goes to Spotify, Spotify steps to position 1 of OUR look-ahead, and
        // DIG saw "position 1, only 5.2s since the last play" and re-asserted
        // the old track. Observed at 22:47:51. The user's skip worked and DIG
        // undid it — indistinguishable, from the earbuds, from a dead button.
        //
        // The real discriminator is not WHEN the divergence happened but
        // whether Spotify was ever confirmed on DIG's track first. A waking
        // app resumes into the wrong slot INSTEAD of arriving where we sent
        // it; a user skips away FROM a track that was correctly playing. So
        // once arrival is confirmed, every later move is the user's and we
        // follow it — however fast, however many positions it covers.
        const isContextJump = ctxPos > 1 && !_connectTrackConfirmed;
        if (isContextJump && _contextJumpRecoveries < 2) {
          _contextJumpRecoveries++;
          clientLog('connect', 'context-jump — re-asserting DIG intent', {
            intent: _connectTrackId, spotify_landed_on: st.trackId,
            ctxPos, ctxLen: ctxIds.length,
            msSinceLastPlay, attempt: _contextJumpRecoveries,
          });
          const want = queue.currentTrack();
          if (want && want.id === _connectTrackId) {
            void Player.play(want);   // UI is already correct; make audio match it
            return;                   // do NOT repaint to Spotify's wrong track
          }
        }

        // Categorise the source of the change so we can tell, in the logs,
        // whether Spotify advanced through ITS own queue (a bug we control
        // by not pre-queueing), walked our look-ahead context, backed up to a
        // recently-played track (also bug-symptom), or the user genuinely
        // pressed AirPods/lock-screen next. The previous unconditional
        // "external skip (AirPods)" label hid the bouncing-skip bug.
        let category;
        if (st.trackId === _lastQueuedId) {
          category = 'spotify-queue-advance';   // Spotify pulled from /api/queue we sent
        } else if (ctxPos > 0) {
          category = 'context-advance';         // natural end → next of OUR look-ahead
        } else if (queue.wasRecentlyPlayed(st.trackId)) {
          category = 'spotify-history-bounce';  // Spotify reverted to a track we played earlier
        } else {
          category = 'external-skip';            // genuine AirPods / lock screen / Spotify mobile
        }
        // THE ADOPTED TRACK IS OVER — TAKE THE WHEEL BACK.
        //
        // Adoption is deliberately one track long. DIG sent no context for it
        // (that command is what kept 502ing at a just-backgrounded Spotify), so
        // whatever Spotify plays next is Spotify's own choice — the rest of the
        // album the deep link opened. Reported as "song finished naturally and
        // another Indonesian traditional song followed, so recommendations are
        // not being enforced", and that is exactly right: 07:32:00 moved from
        // "Lgm. Satria Sejati" to "Lgm. Sumpah Pemuda" at ctxPos -1.
        //
        // Now is the safe moment to dispatch, and the reason the dispatch was
        // deferred to here: Spotify has been playing for minutes rather than
        // seconds, so it answers commands again, and the interruption lands in
        // the first second of a track the listener did not want anyway.
        //
        // Any change counts, not just a natural end. An AirPods skip during an
        // adopted track means "next DIG song" too, and following Spotify to the
        // next album cut would be the same failure by another route.
        if (_adoptedTrackId && _connectTrackId === _adoptedTrackId) {
          clientLog('connect', 'adopted track ended — DIG taking back control', {
            adopted: _adoptedTrackId, spotifyWentTo: st.trackId,
            spotifyWentToName: st.trackName || null, category,
          });
          _adoptedTrackId = null;
          _connectTrackId = st.trackId;   // so this change is not re-judged
          if (Player._onTrackEnd) Player._onTrackEnd();
          return;
        }
        console.log(`[DIG connect] track-changed (${category}): ${_connectTrackId} → ${st.trackId}`);
        clientLog('connect', `track-changed: ${category}`, {
          from: _connectTrackId, to: st.trackId,
          // _lastState was overwritten by this tick's getState() before we got
          // here, so reading it gave the NEW name for both fields — every
          // track-changed line in the 48h to 2026-07-31 logged fromName ===
          // toName, which made the divergence impossible to read. _clockBefore
          // is the pre-getState snapshot, i.e. the track actually being replaced.
          fromName: (_clockBefore && _clockBefore.trackName) || null,
          toName: st.trackName || null,
          lastQueuedId: _lastQueuedId,
          ctxPos, ctxLen: ctxIds.length,
          msSinceLastPlay,
        });
        _connectTrackId = st.trackId;
        // We are adopting what Spotify is actually playing, so this track is
        // confirmed by construction — the next divergence from it is a fresh
        // event to judge on its own, not a continuation of this one.
        _connectTrackConfirmed = true;
        Player._lastPlayStarted = Date.now();
        // Spotify moved on its own; make the queue agree. One call, because
        // it is one decision: find the track, splice it in if it came from the
        // look-ahead we sent, move the cursor, and record the play. Doing that
        // from here meant the player reordered the queue and wrote history
        // while the queue had no idea either had happened.
        //
        // The fallback stub is for a track DIG does not have at all (Spotify
        // radio, or something pre-queued): the queue must not move its cursor
        // to a random slot, but prev still has to work, so it goes on the
        // navigation stack and nowhere else.
        const externalTrack = queue.adoptExternalTrack(st.trackId, {
          fromLookahead: Player._connectContextTracks
            && Player._connectContextTracks[st.trackId],
          stub: {
            id: st.trackId,
            name: st.trackName || '',
            artist: st.artistName || '',
          },
        });
        if (Player._connectContextTracks) delete Player._connectContextTracks[st.trackId];
        // Pre-queue disabled (see Player.connect.play comment).
        // void _prequeueNextTracks();
      }
      // SILENT PLAY FAILURE: Spotify says paused within 5s of our play, so the
      // 204 was a lie — the transport ack'd and the device never started.
      //
      // This used to answer with a deep link, and the comment that stood here
      // said so plainly: "the deep-link fallback below is what causes the iOS
      // symptom 'app jumps into Spotify as if no song was playing'". It is the
      // second of two places DIG navigated away without being asked, and the
      // more surprising one — it fires mid-listening, from a poll, with no
      // action from the listener at all.
      //
      // Bandcamp instead. It plays in-browser and always works, the banner
      // offers the trip to Spotify as a tap, and the handshake that tap starts
      // now completes (see device.js). Advancing through _onTrackEnd rather
      // than re-dispatching: the device just proved it will not play this, so
      // asking it again is the loop that ends a session in silence.
      // Anything actually playing ends the run. Counting a run rather than a
      // total is the point — without this reset it becomes a lifetime tally and
      // the third failure of a long, otherwise healthy session gives up.
      if (!st.paused && st.position > 1500 && _silentFailures) {
        clientLog('connect', 'playback confirmed — clearing the silent-failure run',
          { was: _silentFailures });
        _silentFailures = 0;
      }
      if (st.paused && _connectPlaying) {
        const msSincePlay = Date.now() - (Player._lastPlayStarted || 0);
        // POSITION, NOT JUST THE CLOCK. "Paused 2-8s after we played" was the
        // whole test, and a listener pressing pause three seconds into a song
        // matches it exactly — so the ordinary act of pausing wrote Spotify off
        // for the rest of the session. A play that never started sits at 0; a
        // person pauses somewhere. That is the difference, and it is a fact
        // rather than a window.
        const neverStarted = !st.position || st.position < 1500;
        if (msSincePlay > 2000 && msSincePlay < 8000 && _connectTrackId && neverStarted) {
          _silentFailures++;
          clientLog('connect', 'silent play failure — Spotify says paused after 204 OK', {
            ms: msSincePlay, trackId: _connectTrackId,
            position: st.position, duration: st.duration,
            lastQueuedId: _lastQueuedId, run: _silentFailures,
            visibility: document.visibilityState,
            ua: navigator.userAgent.slice(0, 80),
          });
          _connectPlaying = false;
          // NOT giveUp() on the first one. This poll just READ the device's
          // state, which is direct evidence Spotify is reachable — the exact
          // thing giveUp() asserts is false. One track failing to start is a
          // fact about the track; the device being gone is a different claim
          // and this is not the place that can make it. Same error as reading a
          // failed control call as a dead device (2026-08-02 06:44).
          //
          // A RUN of them is different, and that is a fact too: repeated
          // failures with nothing ever starting means the trip is not worth
          // continuing, so escalate on evidence rather than on the first sample.
          if (_silentFailures >= _SILENT_FAILURE_RUN_LIMIT) {
            SpotifyDevice.giveUp('silent-play-failures', {
              trackId: _connectTrackId, run: _silentFailures,
            });
          }
          if (Player._onTrackEnd) Player._onTrackEnd();
        } else if (!neverStarted && msSincePlay < 8000) {
          // Worth a line: this is what the old rule was destroying.
          clientLog('connect', 'paused shortly after play, but it HAD started — the listener paused', {
            ms: msSincePlay, position: st.position, trackId: _connectTrackId,
          });
        }
      }

      // Track ended detection. Original rule (`paused && position === 0`)
      // missed natural ends because Spotify often reports position ≈ duration
      // for a beat before resetting to 0, and the 1.5s poll cadence misses
      // the zero. We now treat both signatures as track-end:
      //   (a) paused, position == 0, msSincePlay > 5s
      //   (b) paused, position within 1500ms of duration, msSincePlay > 5s
      // and log every paused-while-was-playing observation so we can audit
      // why end-detection fired or didn't.
      if (st.paused && _connectTrackId && _connectPlaying) {
        const msSincePlay = Date.now() - (Player._lastPlayStarted || 0);
        const posAtZero  = st.position === 0;
        const posAtEnd   = (st.duration && st.position
                            && st.position >= st.duration - 1500);
        const eligible   = msSincePlay > 5000;
        const fire       = eligible && (posAtZero || posAtEnd);
        clientLog('connect', 'paused observed', {
          trackId: _connectTrackId, position: st.position, duration: st.duration,
          msSincePlay, posAtZero, posAtEnd, eligible, fire,
        });
        if (fire) {
          clientLog('connect', 'track ended (firing onTrackEnd)', {
            trackId: _connectTrackId, position: st.position, duration: st.duration,
            via: posAtZero ? 'position-zero' : 'position-at-duration',
          });
          _connectPlaying = false; // prevent re-firing
          if (Player._onTrackEnd) Player._onTrackEnd();
          else clientLog('connect', 'track ended but NO onTrackEnd handler wired');
        }
      } else if (!st.paused) {
        _connectPlaying = true;
      }
    }, 1500);
  }

  // Wire track-end callback. Set BOTH the Connect-poll handler
  // (Player._onTrackEnd, used by the Spotify Connect path below) AND the one
  // inside the Player IIFE that the Bandcamp <audio> 'ended' listener reads.
  //
  // This used to say `_onTrackEnd = fn` directly, and that is a different
  // variable. `_onTrackEnd` is declared with `let` inside `const Player =
  // (() => { … })()`, which closes ~1000 lines above this; here the name is
  // undeclared, so in sloppy mode the assignment quietly created a GLOBAL that
  // nothing ever reads. The IIFE's copy stayed null for the life of the page.
  //
  // It failed silently and asymmetrically, which is why it survived: Spotify
  // tracks auto-advance through Player._onTrackEnd (really set) while Bandcamp
  // tracks check the IIFE's copy and find nothing. Measured 2026-07-31, first
  // track after the 'ended' listener was given a log line:
  //   audio ended … activeSource=bandcamp wired=false vis=visible willAdvance=false
  // Foreground, page alive, event delivered, guard satisfied — and no handler.
  //
  // The only way in is the setter the IIFE exposes, so capture it before
  // replacing it.
  const _setInnerTrackEnd = Player.onTrackEnd.bind(Player);
  Player.onTrackEnd = function(fn) { Player._onTrackEnd = fn; _setInnerTrackEnd(fn); };

} else {
  // Desktop: normal Spotify SDK init
  // Collect the SDK's readiness from the shim in app.html's <head>, which
  // owns the callback because it is guaranteed to run before the SDK. Both
  // orders are covered and neither can fire twice: if the SDK was ready first
  // the flag is already set, otherwise the shim calls this handler, and the
  // handler clears itself either way.
  const _initSpotifyOnce = () => {
    window.__digOnSdkReady = null;
    Player.init();
  };
  if (window.__digSdkReady) _initSpotifyOnce();
  else window.__digOnSdkReady = _initSpotifyOnce;
}

// Fallback init if SDK doesn't fire (covers both iOS connect + desktop SDK timeout)
setTimeout(() => { if (!Player.isReady()) Player.init(); }, 2000);

/**
 * Hand the player its view of the queue. Called once, by the queue.
 *
 * Merged rather than replaced so a partial wiring (a test supplying only
 * currentTrack, say) keeps the inert defaults for everything else instead of
 * turning the rest into undefined-is-not-a-function at the worst moment.
 */
Player.wire = function (impl) {
  Object.assign(queue, impl || {});
};

export { Player, SUPERSEDED, DEEPLINK, UNPLAYABLE, _DEEPLINK_CONFIRM_MS,
         _isBandcampTrack, _trackSource };
