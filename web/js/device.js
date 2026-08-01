/**
 * Is the phone's Spotify playback reachable right now, and what do we do about it.
 *
 * ── The problem ───────────────────────────────────────────────────────────
 * On iPhone the Spotify APP is the playback device; DIG only remote-controls
 * it over Connect. Starting a Bandcamp track pauses that playback, and a paused,
 * backgrounded playback is eligible for iOS to reclaim — at which point its Connect
 * device deregisters COMPLETELY. `/me/player/devices` then returns nothing, so
 * the server's wake-a-sleeping-device recovery has nothing to wake, the play
 * 404s, and DIG falls back to a deep link that yanks the user into Spotify.
 *
 * Measured over the 48h of prod logs to 2026-07-31, bucketed by the source of
 * the PREVIOUS track: bandcamp→spotify failed 4 of 6 (67%), spotify→spotify
 * 1 of 13 (8%). So pausing for Bandcamp is clearly the trigger.
 *
 * WHEN it dies is NOT predictable, and nothing here may pretend otherwise: in
 * the same logs a 1s Bandcamp run lost the device while a 535s one kept it,
 * and the gap since the last successful play separates the two groups no
 * better (123s failed, 1372s fine). iOS reclaims the playback on its own schedule.
 *
 * ── Why this is one object ────────────────────────────────────────────────
 * It was nine module-level flags — a lease, two probe timestamps, an in-flight
 * guard, a handshake latch, an provenUnreachable latch — written from six places,
 * including two that reached in and assigned `_spotifyDeviceLeaseUntil = 0`
 * directly. Every bug in this area was fixed by adding a flag and a branch, so
 * the invariants lived in comments and nothing could enforce them.
 *
 * Now the state is private and the only way to move it is to report a FACT:
 * `saw()` something worked, `lost()` a play failed for want of a device,
 * `endangered()` something just made it reclaimable. Callers say what
 * happened; this decides what it means.
 *
 * ── The split that makes it robust ────────────────────────────────────────
 *   * the /api/devices PROBE is ground truth — it asks Spotify directly;
 *   * the LEASE is only a cheap gate on how often we bother to ask, refreshed
 *     by evidence already collected (a play that lands, a poll that sees
 *     playback). It is NOT a model of the iOS suspend window — there isn't one.
 * So LEASE_MS is not load-bearing for correctness: too short costs one extra
 * probe, too long costs one deep link. It only needs to be short enough that a
 * lapse is noticed within a track or two.
 */
import { DIG_IS_IOS, DIG_GUEST } from './env.js';
import { clientLog } from './log.js';

const LEASE_MS = 45000;          // ~1-2 skips; see above, NOT a suspend timer
const PROBE_MIN_GAP_MS = 30000;  // ceiling on probe rate (Spotify's dev quota)

/**
 * What this module needs from the playback. Filled in by SpotifyDevice.wire().
 * One entry, because completing a handshake means playing something, and
 * choosing what to play is not this module's job.
 */
const playback = {
  /** Spotify is reachable again — put the current track on it. */
  resumeSpotify: () => {},
  /**
   * A Spotify track id worth opening to start the handshake, or null.
   *
   * Needed because the handshake link has to name a TRACK — see
   * beginHandshake. By the time the banner is tappable the queue has walked
   * past Spotify onto Bandcamp, so "the current track" is the wrong answer;
   * the app finds the next Spotify pick instead.
   */
  spotifyTrackToOpen: () => null,
  /** Stop making sound. Spotify is about to need the audio session. */
  releaseAudio: () => {},
  /** The handshake failed — go back to playing whatever we were playing. */
  resumeLocal: () => {},
};

export function wireSpotifyDevice(impl) {
  Object.assign(playback, impl || {});
}

let leaseUntil = 0;
let lastProbeAt = 0;
let probeInFlight = false;
// Has a usable device been seen AT ALL this session? Distinguishes "Spotify
// isn't running" from "Spotify went to sleep" — the same banner for both told
// a first-time listener their Spotify had gone to sleep when it had never been
// awake, which is the copy reading as nonsense rather than as instruction.
let everSawDevice = false;
// We sent the user to Spotify and are expecting them back. Without this the
// handshake was a ONE-WAY TRIP: the button opened Spotify and nothing watched
// for the return, so the listener came back to DIG still on Bandcamp with the
// banner still up. Nothing was broken in Spotify — DIG simply never looked.
let awaitingReturn = false;
// Telemetry only now: has the listener tapped through to Spotify this
// session? There is no automatic handshake left for it to gate.
let handshakeUsed = false;
// Named for what it MEANS, not for what it gates. `unavailable` collided
// with the ordinary English word all over the playback — including inside a
// 403-matching regex — which made the one-owner check below unwritable.
let provenUnreachable = false;   // set by a failed play, never by a timer

/** Show or hide the plain-language notice. Idempotent; logs only on change. */
function setAsleepNotice(on) {
  const b = document.getElementById('spotify-asleep-banner');
  if (!b) return;
  if (on === b.classList.contains('visible')) return;
  b.classList.toggle('visible', !!on);
  if (on) _setBannerCopy();
  clientLog('device', on ? 'asleep notice shown' : 'asleep notice cleared',
    on ? { everSawDevice } : undefined);
}

/**
 * The devices the SERVER would actually play to — the same order
 * _pick_playback_device uses in server.py: the caller's own, else active, else
 * a phone. The caller's-own clause is omitted because this path is iOS-only
 * and iOS pins no device, so it could never match.
 *
 * Counting `devices.length` was the bug: an idle 'DIG' web player on a Mac at
 * home proved the phone's Spotify was reachable. Measured 2026-07-31 — the
 * probe returned {count:1, names:['DIG'], active:false}, the lease was revived,
 * the notice cleared, a Spotify track was picked, and the server correctly had
 * nothing to play it on, so DIG deep-linked and Spotify reopened on the phone.
 * The two sides have to agree on what a usable device is, or the client keeps
 * promising what the server refuses.
 */
function usableOf(devices) {
  return devices.filter((x) => x && x.id && !x.is_restricted
    && (x.is_active || x.type === 'Smartphone'));
}

export const SpotifyDevice = {
  // ── What is known ───────────────────────────────────────────────────────

  /** Seen working recently. A guess, and cheap; the probe is the truth. */
  isProbablyLive() {
    return Date.now() < leaseUntil;
  },

  /**
   * Proven unreachable: a play actually failed for want of a device AND the
   * one-shot handshake was already spent.
   *
   * This is the gate on whether Bandcamp takes over, and it is deliberately a
   * FACT rather than a forecast. It used to be "the 45s lease has lapsed",
   * which is a guess about the future, and the guess was expensive: measured
   * 2026-07-31, a lapsed lease withheld 18,213 Spotify tracks and played 24
   * consecutive Bandcamp ones while the banner sat there. A lapsed lease means
   * "not seen lately", not "gone".
   */
  isUnavailable() {
    return provenUnreachable;
  },


  /** Milliseconds of lease left, for the dispatch log. */
  leaseMs() {
    return Math.max(0, leaseUntil - Date.now());
  },

  // ── Reporting facts ─────────────────────────────────────────────────────

  /**
   * Evidence that it works: a play that landed, a poll that saw playback, a
   * probe that found a usable device. Spotify is the default source, so it
   * takes a proven failure to leave it and a single fact to come back.
   */
  saw(reason) {
    everSawDevice = true;
    const wasDead = !this.isProbablyLive();
    leaseUntil = Date.now() + LEASE_MS;
    if (provenUnreachable) {
      provenUnreachable = false;
      clientLog('device', 'spotify reachable again — resuming Spotify picks', { reason });
    }
    if (wasDead) {
      clientLog('device', 'spotify device alive', { reason });
      setAsleepNotice(false);
    }
  },

  /** A play failed for want of a device. */
  lost(reason) {
    if (!this.isProbablyLive()) return;   // already known dead — don't re-log
    leaseUntil = 0;
    clientLog('device', 'spotify device lost', { reason });
  },

  /**
   * Something just made the device reclaimable — in practice, the /api/pause
   * that starts a Bandcamp track. Stop trusting the lease and go ask.
   *
   * Without this a SHORT Bandcamp track leaves the lease looking fresh, the
   * next Spotify track dispatches blind, and the user is deep-linked anyway.
   * Probing here rather than at pick time is free: it resolves in ~300ms while
   * this track plays for minutes, so the next pick reads an answer, not a guess.
   */
  endangered(reason) {
    leaseUntil = 0;
    this.probe(reason);
  },

  // ── Asking ──────────────────────────────────────────────────────────────

  /**
   * Fire-and-forget liveness check, rate-limited.
   *
   * Deliberately NOT awaited: the picker is synchronous, so a probe started on
   * this pick refreshes the lease in time for the NEXT one. That costs at most
   * one pick made on stale information and keeps the picker free of async
   * surgery.
   */
  probe(why) {
    if (probeInFlight) return Promise.resolve(null);
    if (Date.now() - lastProbeAt < PROBE_MIN_GAP_MS) return Promise.resolve(null);
    probeInFlight = true;
    lastProbeAt = Date.now();
    return fetch('/api/devices')
      .then((r) => r.json())
      .then((d) => {
        const devices = (d && d.devices) || [];
        const usable = usableOf(devices);
        clientLog('device', 'probe', {
          why, count: devices.length, usable: usable.length,
          names: devices.map((x) => x && x.name).slice(0, 4),
          active: devices.some((x) => x && x.is_active),
        });
        if (usable.length) {
          everSawDevice = true;
          this.saw('probe');
        } else {
          leaseUntil = 0;
        }
        return usable.length > 0;
      })
      .catch((e) => {
        clientLog('device', 'probe failed', { why, err: String(e).slice(0, 120) });
        return null;
      })
      .finally(() => { probeInFlight = false; });
  },

  /** The state just changed and we were told so — skip the rate limit. */
  probeNow(why) {
    lastProbeAt = 0;
    return this.probe(why);
  },

  /**
   * While Bandcamp is standing in, keep asking whether Spotify is back. A
   * no-op otherwise, so the picker can call it unconditionally.
   */
  pollForReturn() {
    if (!DIG_IS_IOS || DIG_GUEST || !provenUnreachable) return;
    this.probe('pick');   // clears the latch through saw() when it returns
  },

  // ── Decisions ───────────────────────────────────────────────────────────


  /**
   * Proven unreachable after the handshake was spent. Bandcamp takes over.
   *
   * Announcing it here rather than at the call site is the point of the whole
   * object: the latch, the notice and the log line are one event, and when
   * they were three statements next to each other one of them drifted.
   */
  giveUp(reason, detail) {
    provenUnreachable = true;
    setAsleepNotice(true);
    clientLog('device', 'spotify unreachable after handshake — falling back to Bandcamp',
      Object.assign({ reason }, detail || {}));
  },

  /** Show or hide the notice directly (the picker mirrors the latch to it). */
  showAsleepNotice(on) {
    setAsleepNotice(on);
  },
};

/**
 * The banner used to state the remedy — "open Spotify and hit play" — and give
 * no way to do it, which is what "DIG is not even trying" looks like from the
 * outside. A deep link on a TAP is the same action DIG refuses to take on its
 * own; the difference is that the user asked for it.
 */
/**
 * THE HANDSHAKE, as a round trip.
 *
 * Opening Spotify is the only way a Connect device can come into existence, so
 * the trip out is unavoidable. The trip BACK was missing entirely: the button
 * navigated to `spotify:` and nothing watched for the listener's return, so
 * they came back to DIG still on Bandcamp with the banner still up. Nothing
 * was ever broken in Spotify — DIG just never looked again.
 *
 * It also probed BEFORE navigating, which cannot work: at that moment the playback
 * is not running and the device it is asking about does not exist yet. The
 * probe belongs on the way back, when the answer has changed.
 */
function beginHandshake(reason) {
  awaitingReturn = true;
  handshakeUsed = true;
  // OPEN A TRACK, NOT THE APP. `spotify:` alone launches Spotify to whatever
  // screen it was on and plays nothing — and a Spotify that is running but has
  // never played registers a Connect device that the API LISTS and cannot
  // actually control. Measured 2026-08-01 03:44: the probe found
  // count:1 usable:1 names:["iPhone"] active:false, DIG played to it, and
  // Spotify answered 404 "Device not found" even after the server woke it.
  //
  // `spotify:track:<id>` makes Spotify start playing, which is what turns a
  // listed device into a real one. That is why the old link worked and why the
  // plain form looked like the handshake was broken: from the outside nothing
  // opened and nothing played, and from the API side the device was a ghost.
  const trackId = playback.spotifyTrackToOpen();
  clientLog('device', 'handshake: opening Spotify', { reason, trackId });
  // LET GO OF THE AUDIO SESSION BEFORE LEAVING. iOS grants it to ONE app, and
  // DIG was keeping it for the entire trip: measured 2026-08-01 04:21:17, the
  // <audio> element went on producing output while the app was hidden and the
  // listener was inside Spotify (pos 24436 -> 26630 across `vis: hidden`).
  //
  // That single fact explains every shape this failure took. Usually Spotify
  // could not take the session at all, so the album opened and nothing played.
  // Once it did win the session, returning to DIG handed it straight back —
  // the listener watched the Spotify track stop and the Bandcamp track carry
  // on. Either way Spotify stops playing, a Spotify that is not playing drops
  // its Connect registration, and the play that follows gets 404 "Device not
  // found" against a device the probe had just seen ALIVE 8s earlier.
  //
  // So the handshake is a HANDOVER, not a detour: DIG goes quiet on the way
  // out and only makes sound again if the handshake failed (finishHandshake).
  playback.releaseAudio();
  window.location.href = trackId ? `spotify:track:${trackId}` : 'spotify:';
}

/**
 * Back from Spotify. Ask whether a device appeared, and if it did, put the
 * music back on it — finding the device and not using it would leave the
 * listener exactly where they started, having done what we asked.
 */
async function finishHandshake() {
  awaitingReturn = false;
  // Spotify needs a moment after foregrounding before /me/player/devices lists
  // it; probing the instant we regain visibility reads the pre-launch state and
  // reports failure for a handshake that actually worked.
  await new Promise((r) => setTimeout(r, 1200));
  const live = await SpotifyDevice.probeNow('handshake-return');
  clientLog('device', 'handshake result', { live: !!live });
  if (live) {
    setAsleepNotice(false);
    // Stays silent locally: resumeSpotify puts the music on the DEVICE, and
    // resuming the <audio> here would take the session straight back off it.
    await playback.resumeSpotify();
  } else {
    // Say so rather than leaving the banner sitting there implying nothing
    // happened. A handshake that genuinely failed is worth naming.
    //
    // And start the sound again — beginHandshake stopped it to make room for a
    // Spotify that never arrived, so without this a failed handshake leaves
    // the listener in silence, which is strictly worse than not trying.
    playback.resumeLocal();
    _setBannerCopy();
  }
}

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState !== 'visible' || !awaitingReturn) return;
  void finishHandshake();
});

/**
 * The banner says which of two situations this is, because they need different
 * things from the listener and the single old line said the wrong one half the
 * time: "Spotify went to sleep" told a listener whose Spotify had never been
 * open that it had nodded off, and "hit play" told them to do something the
 * button does for them.
 */
function _setBannerCopy() {
  const el = document.getElementById('spotify-asleep-copy');
  if (!el) return;
  el.textContent = everSawDevice
    ? 'Spotify went to sleep — playing Bandcamp for now.'
    : 'Spotify isn\'t running — playing Bandcamp for now.';
}

(function wireWakeButton() {
  const btn = document.getElementById('spotify-wake-btn');
  if (!btn) return;
  btn.addEventListener('click', () => {
    clientLog('device', 'user tapped Wake Spotify',
      { handshakeUsed, provenUnreachable, everSawDevice });
    beginHandshake('user-tap');
  });
})();
