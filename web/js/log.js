/**
 * Client telemetry: mirrors important events to the server so a phone session
 * shows up in `docker logs dig`.
 *
 * This is the only reason the iPhone bugs of 2026-07/08 were diagnosable at
 * all — no console is attached to a locked phone in someone's pocket, and the
 * failures that matter most are precisely the ones that happen while the page
 * is frozen. Two details below exist because of that and should not be
 * simplified away: `at` leads the payload, and a hidden page uses sendBeacon.
 *
 * Imports nothing. Everything imports it.
 */

/** Append to the on-page debug pane, if one is open, and to the console. */
export function dbg(msg) {
  const el = document.getElementById('debug');
  if (el) el.textContent += msg + '\n';
  console.log('[DIG]', msg);
}

/**
 * Send one event to /api/client-log, which prints it as
 * `[CLIENT <tag>] user=<id> <msg> data={…}`.
 *
 * `transient` marks a condition already recovered from, so the server keeps it
 * out of the platform error worklist.
 */
export function clientLog(tag, msg, data, { transient = false } = {}) {
  try {
    const now = new Date();
    const ts = now.toISOString();
    console.log(`[DIG ${tag}] ${ts} ${msg}`, data ?? '');
    // `at` FIRST, always. The server stamps these on arrival, so a batch that
    // was held while the phone was locked and flushed on unlock is
    // indistinguishable from a burst that happened now — which is exactly the
    // question worth answering about a track that ended in the background.
    // Insertion order survives JSON.stringify, and the server truncates the
    // rendered payload at 300 chars, so this has to lead.
    const body = JSON.stringify({
      tag, msg, transient,
      data: Object.assign({ at: ts.slice(11, 23) }, data || {}),
    });
    // A frozen page's fetch — keepalive or not — is simply dropped, which is
    // why the whole track-end path was invisible. sendBeacon is queued by the
    // browser and delivered even after the page stops running.
    const hidden = document.visibilityState === 'hidden';
    if (hidden && navigator.sendBeacon) {
      navigator.sendBeacon('/api/client-log',
        new Blob([body], { type: 'application/json' }));
      return;
    }
    fetch('/api/client-log', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      keepalive: true,
    }).catch(() => {});
  } catch (e) {}
}
