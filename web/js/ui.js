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
