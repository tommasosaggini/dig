/**
 * Painting the now-playing surfaces: cover art, title, progress bar.
 *
 * A leaf on purpose. The player and the queue both paint, and if either owned
 * these the other would have to import it — which is the import cycle that
 * makes "extract the player" impossible. Everything here talks to the DOM and
 * to the log, and to nothing else in the app.
 *
 * Each function writes BOTH surfaces — the top bar and the big player page —
 * because they are the same state shown twice, and every past bug here was one
 * of them being updated without the other.
 */
import { clientLog } from './log.js';

// ── Progress-bar tracer ─────────────────────────────────────────────────────
// Set on every track start; every progress-bar paint within 5s of it logs its
// source, the % it is painting, and the track ids involved — so the server log
// shows the exact paint sequence on a skip, and which site bounces on which
// path. Deliberately time-boxed: outside that window it costs nothing.
let skipAt = 0;

/** A track just started — open the 5s tracing window. */
export function markSkip() {
  skipAt = Date.now();
}

export function pbarLog(src, pct, data) {
  if (!skipAt || Date.now() - skipAt > 5000) return;
  clientLog('pbar', src, Object.assign(
    { pct: Math.round((pct || 0) * 10) / 10, msSinceSkip: Date.now() - skipAt },
    data || {}));
}

/**
 * Jump the bar to `pct` with no animation.
 *
 * The transition is stripped and restored around a forced reflow, so the new
 * width commits before the transition comes back — otherwise the browser
 * coalesces both writes and animates from the old position, which on a skip
 * looks like the bar sliding backwards through the previous track.
 */
export function digPaintProgressInstant(pct) {
  pct = Math.max(0, Math.min(100, pct || 0));
  pbarLog('instant-snap', pct, {});
  const ids = ['player-progress-fill'];
  const mcp = document.getElementById('mc-progress');
  // Never fight the user's thumb: while the mobile bar is being dragged it owns
  // its own width.
  if (!(mcp && mcp.classList.contains('dragging'))) ids.push('mc-progress-fill');
  for (const id of ids) {
    const el = document.getElementById(id);
    if (!el) continue;
    const prev = el.style.transition;
    el.style.transition = 'none';
    el.style.width = pct + '%';
    void el.offsetWidth;        // force reflow so the snap commits before restore
    el.style.transition = prev;
  }
}

/**
 * Album art: mini player-art + big-art (with like/dislike overlays), toggling
 * .has-art. A falsy url gives the ♫ placeholder. Was four copies of this exact
 * template before.
 */
let _lastPaintedArt = null;
// A second cover for the CURRENT track, from a source that resolved it live.
// Only ever read on the failure path — the pool's cover stays first choice
// while it works, because it is already on screen by the time this is set.
let _artFallback = null;

/**
 * Offer a second cover for the track being painted now.
 *
 * Set it and forget it: if the cover on screen is fine, this is never read.
 * It exists because the two facts arrive in the wrong order — the pool's cover
 * is painted at dispatch and the resolver's arrives ~600ms later, by which time
 * the dead one may or may not have failed yet. Handing the fallback to the
 * failure path itself removes the race in both directions: fail first and the
 * handler uses it, fail later and the handler is still holding it.
 */
export function setArtFallback(url) {
  _artFallback = url || null;
  // Already fallen back to the placeholder before this arrived? Then the
  // failure has happened and nothing further will fire — paint it now.
  const ba = document.getElementById('big-art');
  if (_artFallback && ba && !ba.classList.contains('has-art') && _lastPaintedArt) {
    clientLog('art', 'resolver cover arrived after the pool cover had failed',
      { alt: String(_artFallback).slice(0, 90) });
    paintArt(_artFallback, 'resolve-after-failure');
  }
}

export function paintArt(url, why) {
  // Cover art has SIX writers — dispatch, the Spotify SDK, the Connect poll,
  // the Bandcamp resolve fallback, and two session-sync paths — and nothing
  // recorded which one ran. So "the cover went missing" could not be told from
  // "the cover was never set": a later writer blanking a good URL and a first
  // writer finding none look identical on screen and identical in the log.
  //
  // Only CHANGES are logged, so a poll repainting the same URL every 1.5s stays
  // quiet and an overwrite stands out.
  const next = url || '';
  if (next !== _lastPaintedArt) {
    clientLog('art', next ? 'painted' : 'cleared to placeholder', {
      why: why || 'untagged',
      url: next.slice(0, 100) || null,
      wasShowing: _lastPaintedArt ? _lastPaintedArt.slice(0, 60) : null,
    });
    _lastPaintedArt = next;
    // A NEW COVER MEANS A NEW TRACK'S FALLBACK. Carrying the last one over is
    // the same stale-writer bug the onerror guard above exists for, except it
    // would paint the PREVIOUS song's sleeve over the current one — worse than
    // the placeholder, because it looks correct.
    _artFallback = null;
  }
  const OVERLAYS = '<div class="art-overlay heart" id="heart-overlay">♥</div><div class="art-overlay nah" id="nah-overlay">✕</div>';
  const pa = document.getElementById('player-art');
  const ba = document.getElementById('big-art');
  if (url) {
    if (pa) pa.innerHTML = `<img src="${url}">`;
    if (ba) { ba.innerHTML = `<img src="${url}">` + OVERLAYS; ba.classList.add('has-art'); }
    // A URL that fails to load looks exactly like no URL at all: the same
    // empty square. Without this, "no cover art" could mean the track carried
    // none, or carried one the CDN refused — opposite problems, and nothing
    // told them apart. onerror rather than a HEAD probe: this is the actual
    // load the user is looking at, not a guess about it.
    // A cover URL can 404 while the row still carries its id: measured
    // 2026-08-01 over 150 random Bandcamp rows, 0.7% are gone from the CDN.
    // Rare, but "rare" over a 12,653-track pool is still ~90 tracks, and
    // Juggler was one of them — 404, 54 bytes of text/html. Left
    // alone the browser renders its own broken-image glyph, which is neither
    // the artwork nor DIG's placeholder — it just looks like a fault.
    //
    // So fall back to the same ♫ the no-art path shows, and say so. The log
    // line is the point: a dead URL and a missing URL look identical on screen,
    // and only one of them is fixable at ingest.
    for (const host of [pa, ba]) {
      const img = host && host.querySelector && host.querySelector('img');
      if (!img) continue;
      img.onerror = () => {
        // A STALE onerror MUST NOT BLANK THE CURRENT COVER. Replacing innerHTML
        // does not cancel the old <img>'s in-flight load: it can fail a second
        // later and run this handler with `host` now containing the NEXT
        // track's artwork. Observed 2026-08-01 04:09:13 — Juggler's dead cover
        // errored 1.5s after 127 Bpm's good one had been painted, and cleared
        // it to the placeholder. The paint log said "painted" and the screen
        // said otherwise, which is the whole reason that log exists.
        //
        // `next` is captured per paint, so this compares against the paint that
        // created THIS handler rather than reading the DOM — no dependency on
        // isConnected or on the element still being reachable.
        //
        // NOT COVERED BY A TEST, deliberately: reproducing it needs a DOM that
        // parses innerHTML so querySelector returns the element the handler was
        // attached to, and the harness stub does not. A test was written, did
        // not fail when the guard was removed, and was deleted — one that
        // cannot fail is worse than none, because it reads as coverage.
        if (_lastPaintedArt !== next) return;
        // A SECOND COVER BEATS A PLACEHOLDER. The Bandcamp resolve returns a
        // fresh cover for the same track, and it was being thrown away: the
        // fallback in player.js asked "did the pool row have art" when the
        // question is "is a cover on screen". A DEAD pool URL is still a URL,
        // so the good one never got used. Found 2026-08-02 by driving the real
        // app — "03MF (Original Mix)" showed ♫ while /api/bandcamp/resolve was
        // answering a2055129838_10.jpg, which loads fine.
        if (_artFallback && _artFallback !== url) {
          const alt = _artFallback;
          if (host === ba) {
            clientLog('art', 'cover failed — using the resolver\'s cover instead',
              { dead: String(url).slice(0, 90), alt: alt.slice(0, 90) });
          }
          host.innerHTML = host === ba
            ? `<img src="${alt}">` + OVERLAYS : `<img src="${alt}">`;
          if (host === ba) ba.classList.add('has-art');
          return;
        }
        if (host === ba) {
          clientLog('art', 'cover failed to load — falling back to placeholder',
            { url: String(url).slice(0, 120) });
        }
        host.innerHTML = host === ba
          ? '<div class="no-art">♫</div>' + OVERLAYS
          : '<div class="no-art">♫</div>';
        if (host === ba) ba.classList.remove('has-art');
      };
    }
  } else {
    if (pa) pa.innerHTML = '<div class="no-art">♫</div>';
    if (ba) { ba.innerHTML = '<div class="no-art">♫</div>' + OVERLAYS; ba.classList.remove('has-art'); }
  }
}

/**
 * Track title + artist into both the top bar and the big player page.
 *
 * A null/undefined arg leaves that field untouched — for callers that only know
 * one of the two, or that should skip an empty value rather than blank it.
 */
export function paintTrackInfo(name, artist) {
  if (name != null) {
    const a = document.getElementById('player-track'); if (a) a.textContent = name;
    const b = document.getElementById('pc-track'); if (b) b.textContent = name;
  }
  if (artist != null) {
    const a = document.getElementById('player-artist'); if (a) a.textContent = artist;
    const b = document.getElementById('pc-artist'); if (b) b.textContent = artist;
  }
}
