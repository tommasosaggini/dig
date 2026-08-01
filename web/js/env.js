/**
 * What kind of session this is. Decided once, synchronously, at load.
 *
 * These are read on the play path — the picker consults DIG_IS_IOS on every
 * pick — so they cannot be async, and they cannot be re-derived per call. They
 * are also the two facts most likely to be wanted by a module that has no
 * business importing anything else, which is why they live alone.
 */

/**
 * The Spotify Web Playback SDK does not run in iOS Safari. On iPhone the
 * Spotify APP is the playback device and DIG only remote-controls it over
 * Connect — a different enough path that several behaviours branch on this.
 *
 * `!window.MSStream` excludes old IE11 on Windows Phone, which lied about
 * being an iPhone in its user agent.
 */
export const DIG_IS_IOS =
  /iPhone|iPad|iPod/.test(navigator.userAgent) && !window.MSStream;

/**
 * An anonymous listener: no Spotify account, so Bandcamp only.
 *
 * Read from a cookie the server sets rather than from /me, because this has to
 * be known BEFORE Player.init runs — an async answer would arrive after the
 * decision it exists to make. The cookie is deliberately JS-readable; the
 * signed identity cookie next to it is not.
 */
export const DIG_GUEST = /(?:^|;\s*)dig_mode=guest/.test(document.cookie || '');
