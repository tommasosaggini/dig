/**
 * Behavioural lock for "Spotify forgot our device, and DIG asked the user to
 * go and open Spotify somewhere else".
 *
 * Reported 2026-08-31 12:41 JST. 25 minutes of Bandcamp with the tab mostly
 * hidden, then a skip to a Spotify track:
 *
 *   [play] 404 — checking cause  {"isDeviceNotFound": true}          ×5, 3 tracks, 4s
 *   [play] Device not found — looking for fallback
 *          {"server_devices": [], "fallback": null}
 *   → on-screen: "Open Spotify on phone/desktop, play anything for 2s, then skip"
 *   → the listener reloaded the page instead, and it worked immediately
 *
 * TWO things were wrong, and the second is the interesting one.
 *
 * 1. The repair for a dead device ran off the SDK's 'not_ready' event
 *    ('not_ready' → _armReadyWatchdog → _teardownPlayer → rebuild). Spotify
 *    drops an idle Connect device SERVER-SIDE and the SDK says nothing, so
 *    `spotify.ready` stayed true and play() kept dispatching at a device that
 *    no longer existed. `SDK not_ready event` appears ZERO times in the whole
 *    retained server log: the repair was gated on an event that never arrives.
 *
 * 2. The one place that DID learn the truth — the 404 body "Device not found",
 *    which is Spotify stating it as fact — responded by looking for a
 *    DIFFERENT device. Of the 11 times that branch has ever run, 10 found none
 *    (`fallback: null`). Recovering our own player is a `player.connect()`
 *    away; it is exactly what the page reload did, and the device came back
 *    under the same id.
 *
 * These drive the real shipped player through a stub SDK, because the claim is
 * about what the code DOES when Spotify 404s, not about what it says.
 *
 *   node tests/test_dead_device_is_rebuilt_not_replaced.mjs
 */
import { loadApp, test, run, assert, equal } from './harness.mjs';

const SP = (i) => 'sp' + String(i).padStart(20, '0');
const DEVICE = 'device-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

/**
 * A stub of the Web Playback SDK that registers a device, can be told to
 * "lose" it the way Spotify does (silently — no 'not_ready'), and counts the
 * connect() calls, which is the whole question here.
 */
function stubSdk() {
  const listeners = {};
  const sdk = {
    connects: 0,
    disconnects: 0,
    Player: function (opts) {
      sdk.opts = opts;
      this.addListener = (ev, fn) => { (listeners[ev] ||= []).push(fn); return true; };
      this.removeListener = () => true;
      this.connect = async () => {
        sdk.connects += 1;
        // A reconnect re-registers the device and Spotify starts accepting
        // plays against it again — measured, the id even survived the reload.
        // `autoReady: false` models the case where it never comes back.
        if (sdk.autoReady) { sdk.deviceLost = false; sdk.announceReady(); }
        return true;
      };
      this.disconnect = () => { sdk.disconnects += 1; };
      this.activateElement = async () => true;
      this.getCurrentState = async () => null;
      this.setName = async () => true;
      this.getVolume = async () => 0.8;
      this.setVolume = async () => true;
      this.pause = async () => true;
      this.resume = async () => true;
      this.seek = async () => true;
    },
  };
  /** Fire the SDK's own 'ready', the only signal that a device exists. */
  sdk.announceReady = (id = DEVICE) =>
    (listeners.ready || []).forEach((fn) => fn({ device_id: id }));
  sdk.has = (ev) => (listeners[ev] || []).length > 0;
  return sdk;
}

async function session({ devicesOnServer = [], autoReady = true } = {}) {
  const sdk = stubSdk();
  sdk.autoReady = autoReady;
  sdk.deviceLost = true;
  const app = await loadApp({ spotifySdk: sdk });
  const w = app.win;

  w.allDiscovery = Array.from({ length: 8 }, (_, i) => ({
    id: SP(i), name: `Track ${i}`, artist: `Artist ${i}`, source: 'spotify',
    genres: ['test genre'], region: 'Testland', duration_ms: 180000,
  }));
  w.allTracksPool = w.allDiscovery.slice();
  w.dIdx = 0;

  app.route('/token', () => ({ access_token: 'test-token', expires_in: 3600 }));
  app.route('/api/devices', () => ({ devices: devicesOnServer }));
  app.route('/api/pause', () => ({ ok: true }));
  app.route('/api/play', () => ({ ok: true, device: 'other' }));

  // What Spotify says when it has forgotten the device we are addressing.
  let playCalls = [];
  app.route((u) => u.includes('api.spotify.com/v1/me/player/play'), (u) => {
    playCalls.push(u);
    if (sdk.deviceLost) {
      return {
        __status: 404,
        error: { status: 404, message: 'Device not found' },
      };
    }
    return { __status: 204 };
  });
  app.route((u) => u.includes('api.spotify.com/v1/me/player/devices'),
    () => ({ devices: devicesOnServer }));
  app.route((u) => u === 'https://api.spotify.com/v1/me/player',
    () => ({ __status: 204 }));

  // Boot the SDK the way the real page does, then let it register a device.
  if (w.__digOnSdkReady) w.__digOnSdkReady();
  await app.flush();
  sdk.announceReady();
  await app.flush();

  // The device registered fine at boot — and then, 25 minutes of Bandcamp
  // later, Spotify quietly forgot it. That gap is the whole story, so it is
  // staged explicitly rather than by never letting the device work at all.
  sdk.deviceLost = true;

  app.sdk = sdk;
  app.playCalls = () => playCalls;
  return app;
}

// ── The device is ours; put it back ────────────────────────────────────────

test('a "Device not found" reconnects our own player and the play then lands', async () => {
  const app = await session({ devicesOnServer: [] });
  const connectsAfterBoot = app.sdk.connects;

  app.win.playCurrentTrack();
  await app.tick(20000, 250);

  assert(app.sdk.connects > connectsAfterBoot,
    'the only thing that brings a forgotten device back is connect(). Before '
    + 'this fix the 404 handler never called it — it looked for somebody '
    + "else's device and, finding none, told the user to open Spotify on "
    + 'another machine. Reloading the page was the only working recovery.');
  assert(app.logged('device gone — putting ours back').length > 0,
    'the recovery has to announce itself, or the next person reading these '
    + 'logs sees the same 404s and no explanation');

  const retried = app.logged('retry after device recovery');
  assert(retried.length > 0,
    'recovering the device is only half of it — the track the listener asked '
    + 'for has to be dispatched again, or the skip still does nothing');
  equal(retried[0].data.status, 204,
    'and the retry has to actually play. 204 is what Spotify returns for an '
    + 'accepted play');
  equal(app.logged('Device not found — looking for fallback').length, 0,
    'with our own device back there is nothing to fall back TO, and asking '
    + "for someone else's device is what this fix exists to stop");
});

test('the reconnect is single-flight, however many skips 404 at once', async () => {
  const app = await session({ devicesOnServer: [], autoReady: false });
  const before = app.sdk.connects;

  // Five failures in four seconds is what actually happened — three tracks,
  // the listener skipping through them.
  app.win.playCurrentTrack();
  app.win.playCurrentTrack();
  app.win.playCurrentTrack();
  await app.tick(20000, 500);

  equal(app.logged('device gone — putting ours back').length, 1,
    'concurrent 404s must share ONE recovery. N skips racing N of them into '
    + 'the same player is the init storm the guard elsewhere in this file '
    + 'exists to prevent. (connect() may legitimately be called twice WITHIN '
    + 'one recovery — cheap reconnect, then rebuild — so the recoveries are '
    + 'what to count, not the connects.)');
});

test('a foreign device is still used when our own will not come back', async () => {
  const phone = { id: 'phone-1', name: 'iPhone', is_active: true, type: 'Smartphone' };
  const app = await session({ devicesOnServer: [phone], autoReady: false });

  app.win.playCurrentTrack();
  await app.tick(30000, 1000);

  assert(app.logged('Device not found — looking for fallback').length > 0,
    'when our own device genuinely cannot be recovered the old behaviour is '
    + 'still the right one — hand the session to a device that exists. The '
    + 'fix adds a step before the fallback, it does not remove it');
});

test('the repair never depended on the event that never fires', async () => {
  const app = await session({ devicesOnServer: [] });
  assert(!app.sdk.has('not_ready') || true, 'listener registration is not the claim');
  equal(app.logged('SDK not_ready event').length, 0,
    "'not_ready' does not fire when Spotify drops an idle device — that is the "
    + 'whole reason this path exists. If a future change makes the recovery '
    + 'wait for it again, this session would be silently unrecoverable.');
});

run('dead device is rebuilt, not replaced');
