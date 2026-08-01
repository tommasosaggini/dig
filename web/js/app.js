import { DIG_IS_IOS, DIG_GUEST } from './env.js';
import { clientLog, dbg } from './log.js';
import { SpotifyDevice } from './device.js';

// ── Default deadline on every request ────────────────────────────────────────
// Nothing in this app used to time out. That was not cosmetic: Player.play()
// awaits these fetches, so ONE hung request left its promise permanently
// unsettled — neither .then nor .catch ran, _playLock was never released, and
// every later play was silently blocked ("next song doesn't play"). Killing a
// stalled request converts that dead session into an ordinary rejection the
// existing .catch handlers already recover from.
//
// Wrapping fetch itself, rather than ~20 call sites, is deliberate: several
// play-path calls build their URL dynamically (/api/play), and new call sites
// would otherwise silently reopen the hole.
(() => {
  if (typeof AbortSignal === 'undefined' || !AbortSignal.timeout) return;  // pre-iOS 16
  const _rawFetch = window.fetch.bind(window);
  const DEFAULT_MS = 15000;
  const SLOW_MS    = 120000;
  // Endpoints that legitimately take far longer than DEFAULT_MS. Two kinds:
  //   • the full pool — ~1.9 MB gzipped over mobile
  //   • the LLM endpoints — a non-streaming Sonnet call with max_tokens=4000
  //     generating 8-20 recs with prose reasons (lib/ai_recommend.py). Those
  //     routinely run 20-50s; at the 15s default they were aborted every time
  //     and AI-Mix / Journey silently never filled their queues, because both
  //     call sites swallow the rejection with a bare console.error.
  const SLOW_PREFIXES = ['/discovery', '/api/ai-recommend', '/api/journey'];
  // Stream resolve sits ON the play path, so it can't have the 120s deadline —
  // that would re-wedge playback for two minutes. But it can't have the 15s one
  // either: prod logs from 29/07 show the server answering resolve in 186-583ms
  // while the CLIENT measured 15981ms and 23290ms for the same requests. The
  // gap is queueing, not work, and at DEFAULT_MS those two plays would have been
  // aborted outright instead of arriving late. Long enough to outlast a
  // congestion spike, short enough that a truly dead request still frees the
  // player within one track.
  const RESOLVE_MS = 45000;
  const RESOLVE_PREFIXES = ['/api/bandcamp/resolve', '/api/soundcloud/resolve'];
  window.fetch = function (input, init) {
    init = init || {};
    // Never override a caller's own signal, and leave keepalive beacons
    // (client-log) alone — those are fire-and-forget on page teardown.
    if (init.signal || init.keepalive) return _rawFetch(input, init);
    const url = typeof input === 'string' ? input : ((input && input.url) || '');
    let ms = DEFAULT_MS;
    if (SLOW_PREFIXES.some(p => url.indexOf(p) === 0) || url.indexOf('catalog.json') >= 0) ms = SLOW_MS;
    else if (RESOLVE_PREFIXES.some(p => url.indexOf(p) === 0)) ms = RESOLVE_MS;
    return _rawFetch(input, Object.assign({}, init, { signal: AbortSignal.timeout(ms) }));
  };
})();

// ===== STATE =====
let DATA, DISC, GENRE_MAP = null;
let allDiscovery = [];  // all discovery tracks, shuffled
let dIdx = 0;
let currentFilter = 'all';
let currentView = 'list'; // 'list' or 'map'
let _feedLimit = 400;       // current visible cap; user can press "View More" to expand
const _FEED_PAGE = 400;
let showWorldContext = true;

// ── Tailored mode: taste gravity ──
let tailoredMode = false;
let allTracksPool = [];  // flat array of all tracks (for dynamic picking)
// Per-user genre + country play counts, used by the Discovery picker to
// penalise over-played cells (ARCHITECTURE.md Principle 1: breadth first).
// Loaded once at startup from /api/coverage, then incremented locally on
// each play so the picker's view of "what I've heard" stays current.
let userCoverage = { genres: {}, countries: {} };
let playedIds = new Set(); // tracks already played this session

// Playback navigation stack — a linear list of tracks ACTUALLY HEARD this
// session (DIG-dispatched plays AND Spotify-external advances), used by
// prevTrack so "previous" means "the song you were just hearing", not
// "queue index minus one". Pure dIdx-1 was wrong because the diversity-
// shuffled queue + coverage-aware picker + external Spotify queue
// advances all break the assumption that adjacent indices were heard
// adjacently.
//
//   playedStack[0..n-1] = chronological order (oldest → newest)
//   playedCursor        = index of currently-playing track (top of stack
//                         under normal forward play; lower when user has
//                         pressed prev one or more times)
let playedStack = [];
let playedCursor = -1;
// Set to true while navigating via prev/forward through the stack so the
// resulting playCurrentTrack doesn't push the same track again.
let _navFromStack = false;
const _PLAYED_STACK_MAX = 200;
// Session taste signals: { id, genre, region, energy, mood, genres[], action, trackIndex, strength }
let tasteSignals = [];
let _pendingLedger = null;   // ledger response, retained for the post-upgrade re-seed
let _tailoredTrackCount = 0; // monotonic counter of tracks played in tailored mode


function recordTasteSignal(track, action, pct) {
  // Weighted by listen completeness (Instagram-style).
  //   save     → strong like, regardless of pct (user explicit)
  //   dislike  → strong negative, regardless of pct (explicit)
  //   listened → +0.30 if pct ≥70, +0.15 if 30–70, +0 if <30
  //   skip     → -0.40 if pct <10 (instant skip), -0.10 if 10–50, +0.10 if ≥70 (sat through, then moved on)
  const playedPct = (pct == null && typeof getPlayedPct === 'function') ? getPlayedPct() : pct;
  let strength = 0;
  if (action === 'save') strength = 1.0;
  else if (action === 'dislike') strength = -0.8;
  else if (action === 'listened') {
    if (playedPct == null) strength = 0.15;
    else if (playedPct >= 70) strength = 0.30;
    else if (playedPct >= 30) strength = 0.15;
    else strength = 0;
  } else if (action === 'skip') {
    if (playedPct == null) strength = -0.20;
    else if (playedPct < 10) strength = -0.40;
    else if (playedPct < 50) strength = -0.10;
    else if (playedPct >= 70) strength = 0.10; // listened most then moved on
    else strength = -0.05;
  }
  if (strength === 0) return;

  if (action !== 'dislike' && action !== 'skip') _tailoredTrackCount++;

  tasteSignals.push({
    id: track.id,
    genre: (track._genre || '').toLowerCase(),
    genres: (track.genres || []).map(g => g.toLowerCase()),
    region: track.region,
    energy: track._energy,
    mood: track._mood,
    action,
    strength,
    trackIndex: _tailoredTrackCount,
  });

  // Keep only last 100 signals (enough for ~90-track decay window)
  if (tasteSignals.length > 100) tasteSignals = tasteSignals.slice(-100);
}

// ── Taste dimension decomposition ─────────────────────────────────────────
// Pre-loaded from server (/api/taste-profile) which JOINs user_history saves
// with the tracks table — covers ALL 638+ saves, not just those currently
// in the client-side pool (which misses most).
let _serverTasteProfile = null;
let _serverFeelPairs = null;  // co-occurrence pairs: "energy|mood" → count

async function _loadTasteProfile() {
  try {
    const r = await fetch('/api/taste-profile');
    const data = await r.json();
    if (data.profile) {
      _serverTasteProfile = data.profile;
      _serverFeelPairs = data.feel_pairs || {};
      const pairCount = Object.keys(_serverFeelPairs).length;
      console.log(`[DIG tailored] taste profile loaded: ${data.saves_matched} saves, ${pairCount} feel-pairs`,
                   Object.fromEntries(Object.entries(data.profile).map(([k, v]) => [k, Object.keys(v).length + ' values'])));
    }
  } catch (e) {
    console.warn('[DIG tailored] taste profile load failed:', e);
  }
}

function _buildTasteProfile() {
  // Use server-provided profile if available (covers all historical saves)
  if (_serverTasteProfile) {
    // Merge with session-only tasteSignals for real-time responsiveness
    const profile = JSON.parse(JSON.stringify(_serverTasteProfile)); // deep copy
    for (const sig of tasteSignals) {
      if (sig.strength <= 0) continue;
      const w = sig.strength;
      if (sig.energy && sig.energy !== 'unknown')
        profile.energies[sig.energy] = (profile.energies[sig.energy] || 0) + w;
      if (sig.mood && sig.mood !== 'unknown')
        profile.moods[sig.mood] = (profile.moods[sig.mood] || 0) + w;
      if (sig.genre && sig.genre !== 'unknown')
        profile.genres[sig.genre] = (profile.genres[sig.genre] || 0) + w;
      if (sig.region)
        profile.regions[sig.region] = (profile.regions[sig.region] || 0) + w;
    }
    return profile;
  }
  // Fallback: build from local data only (sparse but functional)
  const profile = { energies: {}, moods: {}, genres: {}, regions: {}, decades: {} };
  for (const sig of tasteSignals) {
    if (sig.strength <= 0) continue;
    const w = sig.strength;
    if (sig.energy && sig.energy !== 'unknown') profile.energies[sig.energy] = (profile.energies[sig.energy] || 0) + w;
    if (sig.mood && sig.mood !== 'unknown') profile.moods[sig.mood] = (profile.moods[sig.mood] || 0) + w;
    if (sig.genre && sig.genre !== 'unknown') profile.genres[sig.genre] = (profile.genres[sig.genre] || 0) + w;
    if (sig.region) profile.regions[sig.region] = (profile.regions[sig.region] || 0) + w;
  }
  return profile;
}

// Rotating dimension anchors for tailored mode. Each track uses a DIFFERENT
// anchor dimension, cycling continuously. Anchor weights adapt based on
// user engagement — dimensions that produce saves get boosted, dimensions
// that produce skips decay. Over time the rotation naturally spends more
// time on the dimensions that work for this user.
const ANCHOR_DIMS = ['energy', 'mood', 'genre', 'texture', 'lateral'];
let _anchorIdx = 0;
// Adaptive weights per dimension — start equal, evolve with engagement.
// Higher weight = more likely to be selected as the anchor for the next pick.
let _anchorWeights = { energy: 1.0, mood: 1.0, genre: 1.0, texture: 1.0, lateral: 1.0 };
const ANCHOR_BOOST = 0.3;   // added to weight when a save happens under this anchor


// Called when user saves a track in tailored mode — strong boost
function _boostCurrentAnchor() {
  const dim = ANCHOR_DIMS[_anchorIdx];
  _anchorWeights[dim] += ANCHOR_BOOST;
}

// Adjust anchor weight based on played_pct — continuous signal, not just save/skip
function _adjustAnchorFromPct(pct, action) {
  const dim = ANCHOR_DIMS[_anchorIdx];
  if (pct == null) return;

  if (action === 'listened') {
    // Track ended naturally — boost proportional to how much was heard
    if (pct >= 90) _anchorWeights[dim] += 0.20;      // nearly full listen
    else if (pct >= 70) _anchorWeights[dim] += 0.12;  // deep listen
    else if (pct >= 50) _anchorWeights[dim] += 0.05;  // moderate
  } else if (action === 'skip') {
    // Skip — penalize proportional to how quickly
    if (pct < 5) _anchorWeights[dim] = Math.max(0.3, _anchorWeights[dim] - 0.15);   // instant skip = strong negative
    else if (pct < 15) _anchorWeights[dim] = Math.max(0.3, _anchorWeights[dim] - 0.08);
    else if (pct < 30) _anchorWeights[dim] = Math.max(0.3, _anchorWeights[dim] - 0.03);
    // Skip after 50%+ = mild — they gave it a fair chance
    else if (pct >= 50) _anchorWeights[dim] += 0.02; // slight positive actually
  }
}

// Per-artist AND per-genre AND per-energy pool concentration for inverse weighting
let _artistPoolCount = null;
let _genrePoolCount = null;
let _countryGenreCount = null;  // {country: {genre: n, __total: N}} — per-country genre mix
function _getArtistPoolCount() {
  if (_artistPoolCount) return _artistPoolCount;
  _artistPoolCount = {};
  _genrePoolCount = {};
  _countryGenreCount = {};
  for (const t of allTracksPool) {
    const a = (t.artist || '').split(',')[0].trim().toLowerCase();
    if (a) _artistPoolCount[a] = (_artistPoolCount[a] || 0) + 1;
    const g = t._genre || 'unknown';
    _genrePoolCount[g] = (_genrePoolCount[g] || 0) + 1;
    const c = t.origin_region || t.region || 'Unknown';
    if (!_countryGenreCount[c]) _countryGenreCount[c] = { __total: 0 };
    _countryGenreCount[c][g] = (_countryGenreCount[c][g] || 0) + 1;
    _countryGenreCount[c].__total += 1;
  }
  return _artistPoolCount;
}
function _getGenrePoolCount() { if (!_genrePoolCount) _getArtistPoolCount(); return _genrePoolCount; }

// Share (0..1) that `genre` occupies of its country's slice of the pool.
// This is the lever against the collection skew where a single traditional
// genre dominates a country (rebetiko = 38% of Greece, throat singing = 79%
// of Mongolia, chinese classical+erhu+guqin > 50% of China). The global
// anti-concentration term can't see this — rebetiko is only moderately common
// pool-wide — so without it a country collapses to its stereotype.
function _countryGenreShare(country, genre) {
  const cg = _getArtistPoolCount() && _countryGenreCount[country];
  if (!cg || !cg.__total) return 0;
  return (cg[genre] || 0) / cg.__total;
}
// Raw count of `genre` within `country`'s slice of the pool (cache-backed).
function _countryGenreCountOf(country, genre) {
  _getArtistPoolCount();  // ensure the per-country map is built
  const cg = _countryGenreCount[country];
  return (cg && cg[genre]) || 0;
}


// All artists from a comma-separated string, lowercased + trimmed.
// Used by the cooldown so a featured collaborator (Valentina Lisitsa
// appearing in "Rachmaninoff, Lisitsa" then "Nyman, Lisitsa") doesn't
// silently sneak past the per-primary check.
function _allArtists(a) {
  if (!a) return [];
  return a.split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
}

// Map<artist, gap> — gap = 0 means the very last played track had this
// artist (in any position), 1 means the one before, etc. Only the smallest
// (most recent) gap is stored per artist. Beyond `lookback` the artist
// isn't recorded. Used to compute a progressively-decaying cooldown
// penalty over EVERY collaborator, not just the primary.
function _recentArtistGapMap(lookback) {
  const m = new Map();
  const end = Math.min(history.length, lookback);
  for (let i = 0; i < end; i++) {
    for (const a of _allArtists(history[i].artist)) {
      if (a && !m.has(a)) m.set(a, i);
    }
  }
  return m;
}

// Linear-decay cooldown penalty over ALL artists in the candidate's
// artist string — returns the MAX penalty across them, so a track
// featuring an artist who appeared 1 play ago gets the full hit even
// if the primary is "fresh".
function _artistCooldownPenalty(gapMap, artistStr,
                                maxPenalty = 3, decayWindow = 80) {
  let worst = 0;
  for (const a of _allArtists(artistStr)) {
    const gap = gapMap.get(a);
    if (gap === undefined) continue;
    if (gap >= decayWindow) continue;
    const p = maxPenalty * (1 - gap / decayWindow);
    if (p > worst) worst = p;
  }
  return worst;
}


function pickNextTrack() {
  // Full-taste-profile scoring: every candidate is scored against the user's
  // known saves/skips on EVERY dimension at once (genre is the dominant
  // signal, mood secondary, energy/region tertiary). No anchor rotation —
  // the audit showed it ignored the taste profile 80% of the time.
  if (!allTracksPool.length) return null;

  const heardIds = new Set(history.map(h => h.id));
  const available = allTracksPool.filter(t => !heardIds.has(t.id) && !playedIds.has(t.id));
  if (!available.length) {
    // Pool exhausted for this user. Never silently re-suggest — surface that
    // we're out and let the caller handle UX (status message / mode switch).
    return null;
  }

  // Build taste profile from signals (server-loaded + session)
  const rawTaste = _buildTasteProfile();

  // Normalize each dimension to [-1, 1] by its own max|value|. Without this,
  // raw save-counts make energy (5 buckets) swamp genre (100+ buckets): the
  // top energy bucket can sit at ~1600 while the top genre bucket is ~33.
  // After normalization every dim contributes ≤ its configured weight.
  function _normDim(d) {
    if (!d) return {};
    let mx = 0;
    for (const k in d) { const a = Math.abs(d[k]); if (a > mx) mx = a; }
    if (!mx) return {};
    const out = {};
    for (const k in d) out[k] = d[k] / mx;
    return out;
  }
  const taste = {
    genres: _normDim(rawTaste.genres),
    moods: _normDim(rawTaste.moods),
    energies: _normDim(rawTaste.energies),
    regions: _normDim(rawTaste.regions),
  };

  // Set of the user's positively-weighted top genres — tracks completely
  // outside this cluster get a small "genre alignment gate" penalty so we
  // stop wandering into yodeling/turbo-folk/zamba territory.
  const topGenreSet = new Set(
    Object.entries(rawTaste.genres || {})
      .filter(([,w]) => w > 0)
      .sort((a,b) => b[1] - a[1])
      .slice(0, 30)
      .map(([g]) => g)
  );

  // Recent tracks for short-horizon diversity (avoid back-to-back repeats)
  const recentWindow = 10;
  const recent = [];
  for (let i = Math.max(0, dIdx - recentWindow); i < dIdx && i < allDiscovery.length; i++) {
    recent.push(allDiscovery[i]);
  }
  const recentRegions = new Set(recent.map(t => t.region));
  const recentGenres = new Set(recent.map(t => t._genre));
  // Artist cooldown uses a wider lookback (up to 200 plays) than region/
  // genre because artist repeats within ~80 plays still read as "why already
  // again?" Penalty decays linearly from 3 at gap 0 → 0 at gap 80.
  const artistGapMap = _recentArtistGapMap(200);

  // Energy diversity: count energy levels in recent picks. Under-represented
  // levels get a boost, over-represented ones get docked. User wants variety
  // on the energy axis even though their saves skew moderate — so energy is
  // *not* a taste-matching dimension, it's an exploration dimension.
  const recentEnergyCounts = {};
  for (const r of recent) {
    const e = r._energy || 'unknown';
    recentEnergyCounts[e] = (recentEnergyCounts[e] || 0) + 1;
  }
  const recentTotal = Math.max(1, recent.length);

  const apc = _getArtistPoolCount();
  const gpc = _getGenrePoolCount();

  const sampleSize = Math.min(400, available.length);
  const candidates = available.length <= sampleSize
    ? available
    : available.sort(() => Math.random() - 0.5).slice(0, sampleSize);

  let bestScore = -Infinity, bestTrack = candidates[0];
  let bestBreakdown = null;

  for (const t of candidates) {
    // Primary taste match: genre is the single most discriminating signal
    // for this user — score aggressively from it.
    const primaryGenre = t._genre || 'unknown';
    let genreScore = (taste.genres[primaryGenre] || 0) * 4;

    // Secondary genres (track.genres[1..2]) — Spotify often tags multiple
    const extraGenres = (t.genres || []).slice(1, 3);
    for (const g of extraGenres) {
      const gl = (g || '').toLowerCase();
      if (gl) genreScore += (taste.genres[gl] || 0) * 1.5;
    }

    // Lateral genre boost via GENRE_MAP.neighbors (soft similarity).
    // Chillwave → dream pop etc. Only activates when the map is loaded.
    if (GENRE_MAP && GENRE_MAP.neighbors) {
      // Check user's top 5 genres for neighbor-of-a-favorite match
      const topGenres = Object.entries(taste.genres)
        .filter(([,w]) => w > 0)
        .sort((a,b) => b[1] - a[1])
        .slice(0, 5);
      for (const [sg, w] of topGenres) {
        const neigh = GENRE_MAP.neighbors[sg];
        if (!neigh) continue;
        const idx = neigh.indexOf(primaryGenre);
        if (idx >= 0 && idx < 10) {
          genreScore += w * (1 - idx * 0.08);  // 1.0 → 0.2 for 10 neighbors
        }
      }
    }

    // When genre gives no signal — especially Bandcamp's coarse tags
    // ("experimental"/"rock"/"electronic") that never match the user's
    // fine-grained saved genres — lean harder on mood. Bandcamp is 100%
    // mood/energy/feel-labeled, so mood is the bridge that makes it
    // tailorable instead of random.
    const genreKnown = genreScore > 0.3;
    const moodScore = (taste.moods[t._mood] || 0) * (genreKnown ? 2 : 3.5);
    const regionScore = (taste.regions[t.region] || 0) * 1;  // global taste steers harder (was 0.5)

    // Energy = inverse-frequency exploration bonus. Under-represented energy
    // in the last ~10 picks gets a small nudge; over-represented a small dock.
    // No taste bias toward the user's saved-energy distribution (user wants the
    // full energy spectrum). Kept modest (±0.8) so this taste-BLIND term can't
    // drown the genre/mood affinity signals — at ×5 it used to dominate every
    // pick, which is the main reason tailored mode felt random.
    const myEnergy = t._energy || 'unknown';
    const share = (recentEnergyCounts[myEnergy] || 0) / recentTotal;
    const energyScore = (0.4 - share) * 2;  // share=0 → +0.8; share=0.8 → −0.8

    let score = genreScore + moodScore + energyScore + regionScore;

    // Genre alignment gate — if primary genre is outside the user's top-30
    // saved genres AND no lateral neighbor match, dock the score. Soft, not
    // a hard filter: a great mood+energy match can still surface something
    // surprising, but zamba/yodeling shouldn't be beating chillwave.
    const inCluster = topGenreSet.has(primaryGenre) || extraGenres.some(g => topGenreSet.has((g||'').toLowerCase()));
    // Don't bury a coarse-genre track that the user's mood profile actually
    // likes — that mood match is how Bandcamp picks earn their place.
    if (!inCluster && genreScore < 0.3 && moodScore < 1) score -= 3;

    // Penalty for unknown/missing labels — these tracks had bad/no metadata
    // and can't meaningfully match the user's taste profile.
    if (!t._mood || t._mood === 'unknown') score -= 4;
    if (!t._energy || t._energy === 'unknown') score -= 4;
    if (primaryGenre === 'unknown') score -= 6;

    // Short-horizon diversity (prevent back-to-back same region/genre)
    if (recentRegions.has(t.region)) score -= 2;
    if (recentGenres.has(primaryGenre)) score -= 3;
    // Artist cooldown with linear decay: heavy penalty if just played,
    // fully lifted at gap 80. Deep discographies stay reachable across a
    // long session without clustering.
    score -= _artistCooldownPenalty(artistGapMap, t.artist);

    // Quality signal from engagement history (saved/deep-listened boost,
    // skipped/disliked penalty). Small coefficient — dominant signal is taste.
    const quality = t._quality || 0;
    if (quality > 0) score += Math.min(quality, 10) * 0.5;
    else if (quality < 0) score += Math.max(quality, -10) * 0.3;

    // Anti-concentration: dampen over-represented artists/genres
    const primaryArtist = (t.artist || '').split(',')[0].trim().toLowerCase();
    const artistConc = apc[primaryArtist] || 1;
    const genreConc = gpc[primaryGenre] || 1;
    score *= (1 / Math.sqrt(artistConc)) * (1 / Math.pow(genreConc, 0.2));

    // Within-country flattening guard. The pool over-represents one
    // traditional genre per non-Western country (a collection-pipeline skew),
    // so a country keeps surfacing the same stereotype. Penalize a track whose
    // genre is dominant *within its own country*, while leaving the long tail
    // (the country's modern/rare scenes) untouched. Only genres above a 12%
    // in-country share are docked, scaled by how far past that they sit:
    // rebetiko (0.38 of Greece) → −3.1; csárdás (0.20) → −1.0; Mongolia throat
    // singing (0.79) → −8. A country with a single genre (every track at
    // share 1) is docked uniformly, so no in-country pick is distorted.
    const trackCountry = t.origin_region || t.region || 'Unknown';
    const cgShare = _countryGenreShare(trackCountry, primaryGenre);
    if (cgShare > 0.12) score -= (cgShare - 0.12) * 12;

    // Small random nudge for tiebreaking (and to surface variety when
    // many tracks tie near the top)
    score += Math.random() * 0.5;

    if (score > bestScore) {
      bestScore = score;
      bestTrack = t;
      bestBreakdown = {
        genre: Math.round(genreScore * 10) / 10,
        mood: Math.round(moodScore * 10) / 10,
        energy: Math.round(energyScore * 10) / 10,
        region: Math.round(regionScore * 10) / 10,
      };
    }
  }

  playedIds.add(bestTrack.id);
  {
    clientLog('tailored', 'pick', {
      artist: (bestTrack.artist || '').slice(0, 30),
      track: (bestTrack.name || '').slice(0, 30),
      energy: bestTrack._energy,
      mood: bestTrack._mood,
      region: bestTrack.region,
      genre: bestTrack._genre,
      score: Math.round(bestScore * 10) / 10,
      parts: bestBreakdown,
    });
  }
  return bestTrack;
}

// Session tracking
let history = []; // { track, artist, id, region, status: 'listened'|'skipped'|'saved', time }

async function loadHistory() {
  // Try server first, fall back to localStorage
  try {
    const res = await fetch('/history');
    const data = await res.json();
    if (Array.isArray(data) && data.length > 0) {
      history = data;
      localStorage.setItem('dig-history', JSON.stringify(history));
      return;
    }
  } catch(e) {}
  try { history = JSON.parse(localStorage.getItem('dig-history') || '[]'); } catch(e) { history = []; }
}
// Track the last successfully-synced payload so we can skip POSTs that
// don't actually carry new state. With a ~7,000-entry history that
// serialises to ~640 KB, posting on every save was the dominant network
// cost and was getting truncated mid-string on flaky mobile uplinks
// (cascading into JSONDecodeError + BrokenPipe storms server-side).
let _lastSyncedHistorySig = null;
let _historySyncInFlight = false;

function _historySignature(arr) {
  // Cheap signature: length + last 5 entries' (id, status). Avoids
  // hashing 640 KB on every keystroke; sufficient because addToHistory
  // touches only the head/most-recent entries.
  if (!arr || !arr.length) return '0:';
  const tail = arr.slice(0, 5).map(h => `${h.id}:${h.status}`).join('|');
  return `${arr.length}:${tail}`;
}

function saveHistory() {
  localStorage.setItem('dig-history', JSON.stringify(history));
  clearTimeout(saveHistory._timer);
  saveHistory._timer = setTimeout(() => {
    if (_historySyncInFlight) return;          // last POST still going — skip
    const sig = _historySignature(history);
    if (sig === _lastSyncedHistorySig) return; // nothing changed since last sync
    _historySyncInFlight = true;
    const payload = JSON.stringify(history);
    fetch('/history', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: payload,
    }).then(r => {
      if (r.ok) _lastSyncedHistorySig = sig;
    }).catch(() => {}).finally(() => {
      _historySyncInFlight = false;
    });
  }, 4000);  // 4s — large payload + mobile network; let edits coalesce
}
// Mode the track was played in — matches the values sent to clientLog/mode on
// next-track calls. "discovery" is the default exploration mode (no toggles).
function currentMode() {
  if (journeyMode) return 'journey';
  if (aiMixMode) return 'aimix';
  if (tailoredMode) return 'tailored';
  return 'discovery';
}

function addToHistory(track, status, pct) {
  // pct: 0–100 played percentage at the moment this status was set.
  // Captured so future taste signals can weight by listen completeness.
  const playedPct = (pct == null && typeof getPlayedPct === 'function') ? getPlayedPct() : pct;
  const mode = currentMode();
  const existing = history.find(h => h.id === track.id);
  // Status priority — explicit user actions (saved/disliked) are STICKY and
  // never get silently downgraded by automatic events (listened/skipped).
  // Saved↔disliked may toggle each other (later wins, since they're both
  // intentional user actions). Listened upgrades skipped. Skipped is floor.
  const STATUS_RANK = { saved: 5, disliked: 5, listened: 2, skipped: 1 };
  if (existing) {
    const newRank = STATUS_RANK[status] || 0;
    const oldRank = STATUS_RANK[existing.status] || 0;
    // Only update status if the new one is equal or higher priority. This
    // prevents the relisten-of-a-saved-track bug where 'listened' was
    // clobbering the user's 'saved'.
    if (newRank >= oldRank) {
      existing.status = status;
    }
    existing.time = Date.now();
    if (playedPct != null) existing.played_pct = playedPct;
    // Only stamp mode on first insert — preserve the mode the track was
    // originally surfaced in, even if the user later saves/dislikes it from
    // a different mode.
    if (existing.mode == null) existing.mode = mode;
  } else {
    history.unshift({
      track: track.name, artist: track.artist, id: track.id,
      region: track.region || '', status, time: Date.now(),
      played_pct: playedPct, mode,
      // Persist the playback source explicitly (server stores it; falls back to
      // inferring from a 'bc:' id if absent). Bandcamp tracks may lack .source
      // on a stub, so derive it from the id form as the fallback.
      source: track.source || (String(track.id || '').startsWith('bc:') ? 'bandcamp' : 'spotify'),
    });
    // Increment local coverage counters so the Discovery picker's
    // freshness penalty reflects this play immediately (server snapshot
    // catches up on next page load).
    if (track.genres) for (const g of track.genres) {
      userCoverage.genres[g] = (userCoverage.genres[g] || 0) + 1;
    }
    const _country = track.origin_region || track.region;
    if (_country) userCoverage.countries[_country] = (userCoverage.countries[_country] || 0) + 1;
  }
  saveHistory();
  // Also save to server ledger
  fetch(`/listened?track=${encodeURIComponent(track.artist + ' - ' + track.name)}`).catch(() => {});
  renderFeed();
  renderMap();
}

// ===== HEART ANIMATION =====
function triggerOverlay(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.remove('pop');
  void el.offsetWidth;
  el.classList.add('pop');
  setTimeout(() => el.classList.remove('pop'), 800);
}

// ===== UNIFIED PLAYER =====
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
// Bounded so a run of unplayable Spotify tracks can't walk the whole queue.
let _deepLinkAdvances = 0;

// How long to let a just-woken Spotify app settle before re-issuing a play it
// answered with a 5xx. The server already sleeps 0.4s after transferring to a
// sleeping device, which is enough for one that was merely backgrounded and
// not enough for one that iOS had evicted: measured 2026-07-31, a cold-started
// app still 502'd 4.9s after the wake.
const _WOKEN_DEVICE_SETTLE_MS = 1500;
let _lastDispatchedSource = null;   // for the source-transition log line

// Bandcamp plays in-browser through the <audio> backend and never touches
// Connect, so it is unaffected by the Spotify app's state. Same test the
// look-ahead invariant uses — keep them in step.
function _isBandcampTrack(t) {
  return !!t && (t.source === 'bandcamp' || String(t.id || '').startsWith('bc:'));
}

const Player = (() => {
  let activeSource = null;   // 'spotify' | 'bandcamp'
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
          return;
        }
        spotify._lastReadyId = device_id;
        spotify.deviceId = device_id;
        spotify.ready = true;
        spotify._sdkRegistered = true;
        spotify._fallbackDeviceId = null;
        spotify._authFails = 0;   // healthy auth — clear the reconnect-prompt counter
        status.textContent = 'ready';
        setTimeout(() => { if (status.textContent === 'ready') status.textContent = ''; }, 3000);
        console.log('Spotify SDK ready event:', device_id);
        clientLog('firstplay', 'Spotify SDK ready event fired (trusted)', { device_id });
        _reconcileSdkDevice(device_id);
        if (typeof _tryConsumePendingPlay === 'function') _tryConsumePendingPlay('sdk-ready');
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
        clientLog('spotify', 'SDK not_ready event');
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
        if (!state.paused) { Player._clearStuckTimer(); const s = document.getElementById('player-status'); if (s && !s.textContent.includes('tailored')) s.textContent = ''; }
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
            const expected = (typeof currentTrack === 'function') ? currentTrack() : null;
            if (expected && expected.id !== sdkTrack.id) {
              const sdkArtists = (sdkTrack.artists || []).map(a => a.name).join(', ');
              paintTrackInfo(sdkTrack.name || '', sdkArtists);
              // Rebase dIdx if the actual track is in our queue
              const idxInQ = allDiscovery.findIndex(t => t.id === sdkTrack.id);
              if (idxInQ >= 0) { dIdx = idxInQ; }
              {
                clientLog('play', 'UI re-synced from SDK (mismatch detected)', {
                  expected: `${expected.artist} — ${expected.name}`,
                  actual: `${sdkArtists} — ${sdkTrack.name}`,
                  rebased: idxInQ >= 0,
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
            paintArt(url);
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
        try { spotify.player && spotify.player.disconnect(); } catch (e) {}
        spotify.player = null; spotify.ready = false; spotify.deviceId = null; spotify._lastReadyId = null;
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
        if (!advanced && !bandcamp.audio.paused) {
          clientLog('bandcamp', 'stall watchdog → skip (stream never recovered)',
            { pos: Math.round((bandcamp.audio.currentTime || 0) * 1000),
              readyState: bandcamp.audio.readyState, networkState: bandcamp.audio.networkState });
          if (_onTrackEnd) _onTrackEnd();
        }
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
      a.addEventListener('ended', () => {
        clientLog('bandcamp', 'audio ended', {
          activeSource,
          wired: !!_onTrackEnd,
          vis: document.visibilityState,
          posMs: Math.round((a.currentTime || 0) * 1000),
          durMs: Math.round((a.duration || 0) * 1000),
          willAdvance: activeSource === 'bandcamp' && !!_onTrackEnd,
        });
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
        });
        if (_onTrackEnd) _onTrackEnd();
      });
      a.addEventListener('play',  () => { const p = document.getElementById('btn-play'); if (p) p.textContent = '❚❚'; const m = document.getElementById('mc-play'); if (m) m.textContent = '❚❚'; });
      a.addEventListener('pause', () => { const p = document.getElementById('btn-play'); if (p) p.textContent = '▶'; const m = document.getElementById('mc-play'); if (m) m.textContent = '▶'; });
      // 'playing' = audio is actually producing output (distinguishes a
      // resolved play() promise from a silent/stuck element). 'stalled'/'waiting'
      // = the stream stopped feeding data — catches network stalls that would
      // otherwise look like a hang with no error event.
      a.addEventListener('playing', () => { bandcamp._clearStallWatch(); if (activeSource === 'bandcamp') clientLog('bandcamp', 'audio playing (output started)', { pos: Math.round((a.currentTime || 0) * 1000), readyState: a.readyState }); });
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
            id: track.id, host: (url.split('/')[2] || '').slice(0, 40), dur: d.duration,
            streamable: d.streamable, resolveMs: Math.round(performance.now() - _tResolve),
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
      // Cover fallback only if the pool row didn't already carry art.
      if (art && !track.art) {
        paintArt(art);
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
  let _lastPos = 0, _lastDur = 0;     // latest polled position/duration (ms)
  let _prevPos = 0;                   // previous poll's position (for forward-progress detection)
  let _listenedMs = 0;                // accumulator: real ms the user actually listened
  let _currentTrackForListen = null;  // track id this accumulator belongs to

  function _resetListenAccumulator(trackId) {
    _listenedMs = 0;
    _prevPos = 0;
    _currentTrackForListen = trackId;
  }
  // Public so playCurrentTrack can call it on new track start
  window._resetListenAccumulator = _resetListenAccumulator;

  function _updateProgress(posMs, durMs, trackId) {
    if (!durMs) return;
    // Skip-transition guard: for a beat after we dispatch a new track, the SDK
    // keeps reporting the OLD track (with its old position). Painting that would
    // bounce the bar to the just-skipped track's % before the new track loads
    // (the "0 → old% → 0" glitch). Only paint when the SDK's reported track is
    // the one DIG currently intends to show; otherwise hold the reset-to-0.
    if (trackId) {
      const intended = (typeof currentTrack === 'function') ? currentTrack() : null;
      if (intended && intended.id && trackId !== intended.id) {
        _pbarLog('SDK-suppressed', (posMs / durMs) * 100,
          { trackId: (trackId || '').slice(0, 10), intended: (intended.id || '').slice(0, 10) });
        return;
      }
    }
    _pbarLog('SDK-paint', (posMs / durMs) * 100, { trackId: (trackId || '').slice(0, 10) });
    _lastPos = posMs; _lastDur = durMs;
    // Accumulate real listening time: only count if position advanced 100ms–2000ms
    // (a normal poll interval). Big jumps = user seeking; rewind = also a seek.
    const delta = posMs - _prevPos;
    if (delta > 50 && delta < 2000) {
      _listenedMs += delta;
    }
    _prevPos = posMs;
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
  window.getPlayedPct = function () {
    if (!_lastDur) return null;
    return Math.min(100, Math.max(0, (_listenedMs / _lastDur) * 100));
  };
  // Diagnostic — useful for the firstplay/skip telemetry
  window.getListenedMs = function () { return _listenedMs; };

  function _msToTime(ms) {
    const s = Math.floor(ms / 1000);
    return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0');
  }

  function _startPoll() {
    if (_pollInterval) clearInterval(_pollInterval);
    let _probeLastPos = -1, _probeStalled = 0, _probeWarned = false;
    _pollInterval = setInterval(async () => {
      const state = await Player.getState();
      if (state) {
        _updateProgress(state.position, state.duration, state.trackId);
        // Audio-health probe — the definitive "is sound actually coming out"
        // signal. If the SDK reports playing (not paused) but the position
        // does not advance across ~3s of polls, the audio is silent (phantom
        // device, or local element never activated). Surface it once so the
        // failure is observable in the server log instead of inferred.
        if (state.paused) {
          _probeStalled = 0; _probeWarned = false; _probeLastPos = state.position;
        } else if (state.position > _probeLastPos + 5) {
          _probeStalled = 0; _probeWarned = false; _probeLastPos = state.position;  // advancing = real audio
        } else {
          _probeStalled++;
          if (_probeStalled >= 6 && !_probeWarned) {
            _probeWarned = true;
            clientLog('audio-probe',
              'WARN: SDK says playing but position not advancing (likely SILENT)',
              { position: state.position, sdkRegistered: spotify._sdkRegistered,
                activated: spotify._activated, deviceId: (spotify.deviceId || '').slice(0, 12),
                fallback: (spotify._fallbackDeviceId || '').slice(0, 12) });
            // Recovery: a phantom/silent SDK device almost always clears if we
            // re-dispatch the current track once (re-issues the play to the
            // device). Guard to a single attempt per track so a genuinely dead
            // device doesn't loop — if it's still silent after this, we leave
            // it for the user rather than thrash the queue.
            const _cur = (typeof currentTrack === 'function') ? currentTrack() : null;
            if (activeSource === 'spotify' && _cur && _cur._silentRecovered !== true
                && typeof playCurrentTrack === 'function') {
              _cur._silentRecovered = true;
              clientLog('audio-probe', 'recovery: re-dispatching current track', { id: _cur.id });
              playCurrentTrack();
            }
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

    async togglePlay() {
      if (activeSource === 'bandcamp') bandcamp.togglePlay();
      else await spotify.togglePlay();
    },

    async pause() {
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
const DIG_CONNECT_LOOKAHEAD = 24;

// Non-destructively compute the next `k` distinct discovery picks WITHOUT
// advancing real playback/queue state — the iOS Connect look-ahead context.
//
// This mirrors _pickDiscoveryStratified's two stages (anti-cluster hard
// filter → coverage-gap weighted sample) but BATCHES them: the expensive
// full-pool scan runs ONCE, then we draw k picks from it. Per-pick work is
// only over the ≤1000 sample, so k=24 costs about the same as a single
// picker call (no per-skip lag). We grow the anti-cluster sets as we pick so
// the look-ahead list stays internally diverse, just like real sequential
// play. Only normal discovery mode supports look-ahead; curated/async modes
// (journey, ai-mix, tailored) return [] → single-track context.
function _peekNextContextTracks(k) {
  try {
    if (journeyMode || aiMixMode || tailoredMode) return [];
    if (!allTracksPool || !allTracksPool.length) return [];
    const heardIds = new Set(history.map(h => h.id));
    const eligible = allTracksPool.filter(t =>
      t && t.id && !heardIds.has(t.id) && !playedIds.has(t.id));
    if (!eligible.length) return [];

    // Seed anti-cluster sets from the last few real plays (same as the picker).
    const RECENT_N = 6;
    const recentArtists = new Set();
    const recentCountries = new Set();
    const recentTopGenres = new Set();
    for (let i = 0; i < Math.min(RECENT_N, history.length); i++) {
      const h = history[i];
      if (!h) continue;
      for (const a of _allArtists(h.artist)) recentArtists.add(a);
      if (h.region) recentCountries.add(h.region);
      const pm = allTracksPool.find(t => t.id === h.id);
      if (pm && pm.genres && pm.genres.length) recentTopGenres.add(pm.genres[0]);
    }
    const _passes = (t, artists, countries, genres) => {
      for (const a of _allArtists(t.artist)) if (artists.has(a)) return false;
      const country = t.origin_region || t.region;
      if (country && country !== 'Unknown' && countries.has(country)
          && (userCoverage.countries[country] || 0) > 20) return false;
      const topGenre = (t.genres && t.genres[0]) || null;
      if (topGenre && genres.has(topGenre)
          && (userCoverage.genres[topGenre] || 0) > 20) return false;
      return true;
    };
    const _gap = (t) => {
      let minGenrePlays = Infinity;
      for (const g of (t.genres || [])) {
        const p = userCoverage.genres[g] || 0;
        if (p < minGenrePlays) minGenrePlays = p;
      }
      if (!isFinite(minGenrePlays)) minGenrePlays = 50;
      const country = t.origin_region || t.region;
      const countryPlays = (!country || country === 'Unknown')
        ? 200 : (userCoverage.countries[country] || 0);
      let w = Math.pow((1 / (1 + minGenrePlays)) * (1 / (1 + countryPlays)), 0.6);
      // Within-country flattening guard — mirrors _pickDiscoveryStratified.
      // Divide by the genre's in-country track count (softened) so a country's
      // dominant traditional genre can't win the look-ahead context by sheer
      // count. Critical on iOS: this context is what auto-advances on the phone.
      const cgCount = _countryGenreCountOf(country, t._genre);
      if (cgCount > 1) w /= Math.pow(cgCount, 0.8);
      return w;
    };

    // Stage-1 filter ONCE against the seed recent-sets (picker's behaviour).
    const filtered = eligible.filter(t => _passes(t, recentArtists, recentCountries, recentTopGenres));
    const basePool = filtered.length >= 50 ? filtered : eligible;
    const SAMPLE = 1000;
    const _sampleOf = (arr) => {
      if (arr.length <= SAMPLE) return arr;
      const s = [], step = arr.length / SAMPLE;
      for (let i = 0; i < SAMPLE; i++) s.push(arr[Math.floor(i * step + Math.random() * step)]);
      return s;
    };

    // Grow these as we pick so successive look-ahead tracks stay diverse.
    const usedArtists = new Set(recentArtists);
    const usedCountries = new Set(recentCountries);
    const usedGenres = new Set(recentTopGenres);
    const pickedIds = new Set();
    const out = [];
    for (let n = 0; n < k; n++) {
      let sample = _sampleOf(basePool).filter(t =>
        t && t.id && !pickedIds.has(t.id) && _passes(t, usedArtists, usedCountries, usedGenres));
      if (!sample.length) {
        // Anti-cluster exhausted the sample — relax to anything unpicked.
        sample = _sampleOf(basePool).filter(t => t && t.id && !pickedIds.has(t.id));
      }
      if (!sample.length) break;
      let total = 0;
      const weights = sample.map(t => { const w = _gap(t); total += w; return w; });
      let chosen;
      if (total <= 0) {
        chosen = sample[Math.floor(Math.random() * sample.length)];
      } else {
        let r = Math.random() * total;
        chosen = sample[0];
        for (let i = 0; i < sample.length; i++) { r -= weights[i]; if (r <= 0) { chosen = sample[i]; break; } }
      }
      if (!chosen || !chosen.id) break;
      out.push(chosen);
      pickedIds.add(chosen.id);
      for (const a of _allArtists(chosen.artist)) usedArtists.add(a);
      const cc = chosen.origin_region || chosen.region;
      if (cc && cc !== 'Unknown') usedCountries.add(cc);
      const cg = (chosen.genres && chosen.genres[0]) || null;
      if (cg) usedGenres.add(cg);
    }
    return out;
  } catch (e) {
    return [];
  }
}


if (DIG_IS_IOS) {
  console.log('[DIG] iOS detected — using Spotify Connect mode (server-side REST API)');

  // Override Player methods to route through server API instead of SDK
  const _origInit = Player.init.bind(Player);
  const _origPlay = Player.play.bind(Player);

  // Track state for the Connect player
  let _connectPlaying = false;
  let _connectTrackId = null;
  let _connectPollInterval = null;
  // Public setter so playCurrentTrack (outside this IIFE) can pin DIG's
  // intent the moment a new track is dispatched, BEFORE the network
  // round-trip. Closes the cover-flash race where a poll would fire
  // between playCurrentTrack's DOM update and connect.play's PUT-success
  // handler, see _connectTrackId is still OLD, see Spotify reports OLD,
  // see no mismatch, and overwrite the DOM with OLD values.
  Player._setConnectTrackId = function (id) { _connectTrackId = id; };
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
      if (typeof _tryConsumePendingPlay === 'function') _tryConsumePendingPlay('connect-ready');
    } catch (e) {
      clientLog('connect', 'init failed', { err: String(e) });
      status.textContent = 'error';
    }
  };

  Player.isReady = function() { return DIG_GUEST ? true : !!Player._connectReady; };
  Player.isSpotifyReady = function() { return !!Player._connectReady; };
  Object.defineProperty(Player, 'spotifyReady', { get() { return !!Player._connectReady; } });

  Player.play = async function(track) {
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
    Player._playing = true;
    Player._lastPlayStarted = Date.now();
    const trackId = track.id || track;

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
      const lookahead = (typeof _peekNextContextTracks === 'function')
        ? _peekNextContextTracks(DIG_CONNECT_LOOKAHEAD) : [];
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

    clientLog('connect', 'play', { id: trackId, device: Player._connectDeviceId, ctxLen: contextIds.length });

    async function _tryPlay(deviceId) {
      const url = `/api/play?tracks=${encodeURIComponent(contextIds.join(','))}` +
                  (deviceId ? `&device=${encodeURIComponent(deviceId)}` : '');
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
      if (data.error && Player._connectDeviceId && /spotify_(404|502|500)/.test(data.error)) {
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

      if (data.error) {
        SpotifyDevice.lost(data.no_device ? 'play-no-device' : 'play-' + data.error);
        console.warn('[DIG connect] play error after retry:', data);
        // THE HANDSHAKE, and it is spent ONCE per session. Opening the Spotify
        // app is the only way a Connect device can come into existence, so the
        // first failure is worth one interruption — after that the device
        // either exists (and everything since plays through it) or Spotify is
        // genuinely unreachable, and repeating the deep link is what produced
        // "Spotify reopens every song". The second failure therefore falls back
        // to Bandcamp and offers the banner's Wake button instead, so any
        // further trip into Spotify is the user's decision, not ours.
        if (SpotifyDevice.handshakeSpent()) {
          SpotifyDevice.giveUp(data.error, { trackId });
          Player._playing = false;
          // No _onTrackEnd() here. Advancing beats stalling in silence, but
          // advancing is the CALLER's job — doing it here as well is what
          // burned three tracks in 3.7s. Report the fact; it moves the queue.
          return UNPLAYABLE;
        }
        SpotifyDevice.spendHandshake();
        clientLog('connect', 'handshake: opening Spotify once to create a device',
          { err: data.error, trackId });
        window.location.href = `spotify:track:${trackId}`;
        Player._playing = false;
        // After launching Spotify, wait then re-init Connect so the
        // now-active device is found for future commands.
        setTimeout(async () => {
          Player._connectReady = true;
          _connectPlaying = true;
          _connectTrackId = trackId;
          _startConnectPoll();
          clientLog('connect', 'post-deep-link re-init');
        }, 3000);
        // NOT `true`. Whether this plays depends on the Spotify app actually
        // coming up, which it won't if the phone is locked. The caller confirms.
        return DEEPLINK;
      }
      _connectPlaying = true;
      _connectTrackId = trackId;
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

  async function _prequeueNextTracks() {
    if (_prequeueInFlight || !DIG_IS_IOS) return;
    _prequeueInFlight = true;
    try {
      // Find the very next unheard track after current position
      const heardIds = new Set(history.map(h => h.id));
      for (let i = dIdx + 1; i < allDiscovery.length; i++) {
        const t = allDiscovery[i];
        if (!t || !t.id) continue;
        if (heardIds.has(t.id)) continue;
        // Same invariant as the play context: never hand a Bandcamp id to
        // Spotify's native queue (it'd 400 / poison the AirPods-skip). Skip
        // past Bandcamp tracks to pre-queue the next genuine Spotify one.
        if (t.source === 'bandcamp' || String(t.id).startsWith('bc:')) continue;
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

  Player.getState = async function() {
    if (Player._bandcamp && Player._bandcamp.isActive()) return Player._bandcamp.getState();
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
        const s = await r2.json();
        const out = {
          position: s.progress_ms || 0,
          duration: s.item?.duration_ms || 0,
          paused: !s.is_playing,
          trackId: s.item?.id || null,
          albumArt: s.item?.album?.images?.[0]?.url || null,
          trackName: s.item?.name || null,
          artistName: (s.item?.artists || []).map(a => a.name).join(', ') || null,
        };
        _lastState = out; _lastStateAt = Date.now();
        return out;
      }
      if (r.status === 204 || !r.ok) return null;
      const s = await r.json();
      const out = {
        position: s.progress_ms || 0,
        duration: s.item?.duration_ms || 0,
        paused: !s.is_playing,
        trackId: s.item?.id || null,
        albumArt: s.item?.album?.images?.[0]?.url || null,
        trackName: s.item?.name || null,
        artistName: (s.item?.artists || []).map(a => a.name).join(', ') || null,
      };
      _lastState = out; _lastStateAt = Date.now();
      return out;
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
      _pbarLog('connect-interp', pct, { clockTrack: (_lastState.trackId || '').slice(0, 10),
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
  function _startConnectPoll() {
    _startProgressInterpolator();
    if (_connectPollInterval) clearInterval(_connectPollInterval);
    _connectPollInterval = setInterval(async () => {
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
      const st = await Player.getState();
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

      // Update progress bar
      if (st.duration) {
        _pbarLog('connect-poll', (st.position / st.duration) * 100,
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
      if (st.albumArt && st.albumArt !== _connectLastArt) {
        _connectLastArt = st.albumArt;
        paintArt(st.albumArt);
      }
      paintTrackInfo(st.trackName || null, st.artistName || null);
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
          const want = currentTrack();
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
        } else if (playedStack.some(p => p.id === st.trackId)) {
          category = 'spotify-history-bounce';  // Spotify reverted to a track we played earlier
        } else {
          category = 'external-skip';            // genuine AirPods / lock screen / Spotify mobile
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
        // Build a track object — prefer the queue version (richer metadata),
        // fall back to a stub from the SDK state for non-DIG tracks.
        let inQueueIdx = allDiscovery.findIndex(t => t.id === st.trackId);
        // Spotify auto-advanced into a track from the look-ahead context we
        // sent (natural end). Splice that pick into the nav queue right after
        // the current position so the rebase + history-record below treat it
        // as a first-class DIG play. Lazy (only the track actually reached)
        // keeps allDiscovery from growing by the full look-ahead each play.
        if (inQueueIdx < 0 && Player._connectContextTracks && Player._connectContextTracks[st.trackId]) {
          const insertAt = Math.min(allDiscovery.length, Math.max(0, (typeof dIdx === 'number' ? dIdx : -1) + 1));
          allDiscovery.splice(insertAt, 0, Player._connectContextTracks[st.trackId]);
          delete Player._connectContextTracks[st.trackId];
          inQueueIdx = insertAt;
          clientLog('connect', 'context auto-advance spliced into queue', { id: st.trackId, at: insertAt });
        }
        let externalTrack;
        if (inQueueIdx >= 0) {
          dIdx = inQueueIdx;
          externalTrack = allDiscovery[inQueueIdx];
          addToHistory(externalTrack, 'listened');
        } else {
          // Spotify advanced into a track DIG doesn't have (radio / pre-
          // queued). Don't move dIdx (would point at random pool slot).
          // Push a stub onto the navigation stack so prev still works.
          externalTrack = {
            id: st.trackId,
            name: st.trackName || '',
            artist: st.artistName || '',
          };
        }
        _pushPlayed(externalTrack, 'external');
        // Pre-queue disabled (see Player.connect.play comment).
        // void _prequeueNextTracks();
      }
      // Detect silent play failure: if Spotify says paused within 5s of our
      // play command, the 204 was a lie — playback didn't actually start.
      // Auto-retry with a deep link as fallback.
      // NOTE: the deep-link fallback below is what causes the iOS symptom
      // "app jumps into Spotify as if no song was playing" — it's our last
      // resort when the Spotify Connect transport ack'd 204 but the actual
      // device never started. We log entry, the precondition window, AND
      // the actual deep-link launch so we can correlate against transport
      // 404/502 signals server-side.
      if (st.paused && _connectPlaying) {
        const msSincePlay = Date.now() - (Player._lastPlayStarted || 0);
        if (msSincePlay > 2000 && msSincePlay < 8000 && _connectTrackId) {
          clientLog('connect', 'silent play failure — Spotify says paused after 204 OK', {
            ms: msSincePlay, trackId: _connectTrackId,
            position: st.position, duration: st.duration,
            lastQueuedId: _lastQueuedId,
            visibility: document.visibilityState,
            ua: navigator.userAgent.slice(0, 80),
          });
          clientLog('connect', 'firing spotify: deep-link fallback (will navigate away)', {
            trackId: _connectTrackId,
          });
          // Try deep link as last resort
          window.location.href = `spotify:track:${_connectTrackId}`;
          _connectPlaying = false;
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

// ===== PLAYER LOGIC =====
function currentTrack() { return allDiscovery[dIdx]; }

/** Structured logs for playback / queue debugging (filter console by `[DIG`). */
function playDbg(event, extra = {}) {
  try {
    console.log('[DIG playback]', event, Object.assign({
      dIdx,
      queueLen: allDiscovery.length,
      playingId: currentTrack()?.id,
    }, extra));
  } catch (e) {}
}

// ── Album-art prefetch ───────────────────────────────────────────────────
// The /discovery track dicts carry no cover URL, so the art used to appear
// only when the ~1.5–3s /me/player poll (Connect/iOS) finally returned it —
// the "next song's album takes a second, feels cheap" gap. Real players
// prefetch the next track's art. We batch-resolve cover URLs for upcoming
// picks via GET /v1/tracks (up to 50 ids in one call), cache them, AND warm
// the browser's image cache (new Image().src) so the cover is already decoded
// and paints the instant we switch tracks.
const _artCache = new Map();          // trackId -> cover URL ('' = resolved, no art)
const _durCache = new Map();          // trackId -> duration_ms (from the same /v1/tracks call)
const _artInflight = new Set();

async function _digSpotifyToken() {
  try { if (window.Player && Player._getToken) return await Player._getToken(); } catch (e) {}
  return null;  // SDK/desktop path gets art fast via player_state_changed anyway
}

async function prefetchAlbumArt(tracks) {
  const ids = [];
  for (const t of (tracks || [])) {
    if (!t || !t.id) continue;
    if ((t.source || 'spotify') !== 'spotify') continue;
    if (t.id.startsWith('yt:')) continue;
    if (_artCache.has(t.id) || _artInflight.has(t.id)) continue;
    ids.push(t.id);
    if (ids.length >= 50) break;            // Spotify caps /v1/tracks at 50 ids
  }
  if (!ids.length) return;
  const tok = await _digSpotifyToken();
  if (!tok) return;
  ids.forEach(id => _artInflight.add(id));
  try {
    const r = await fetch('https://api.spotify.com/v1/tracks?ids=' + ids.join(','),
      { headers: { 'Authorization': 'Bearer ' + tok } });
    if (!r.ok) return;
    const data = await r.json();
    for (const tr of (data.tracks || [])) {
      if (!tr || !tr.id) continue;
      const url = (tr.album && tr.album.images && tr.album.images[0] && tr.album.images[0].url) || '';
      _artCache.set(tr.id, url);
      if (tr.duration_ms) _durCache.set(tr.id, tr.duration_ms);   // lets the bar start at 0 instantly
      if (url) { const im = new Image(); im.src = url; }   // warm the image cache
    }
  } catch (e) {
    clientLog('art', 'prefetch threw', { err: String(e).slice(0, 120) });
  } finally {
    ids.forEach(id => _artInflight.delete(id));
  }
}

// Paint the progress fill to an exact % RIGHT NOW, bypassing the CSS width
// transition. That 0.4s transition is what makes the bar GLIDE between the
// steady playback ticks — good. But it also makes a discrete JUMP (a 15s seek,
// or the reset to 0 on a new track) visibly crawl to its target over 0.4s.
// Real players snap on jumps and glide only during continuous play; this gives
// us the snap. We kill the transition, set the width, force a reflow so the
// snap actually commits, then restore the transition for the next tick.
// ── Progress-bar tracer (temporary, for the skip-bounce investigation) ──
// Set on every track start; every progress-bar paint within 5s of it logs its
// source, the % it's painting, and the track ids involved — so the server log
// shows the exact paint sequence on a skip (which site bounces, on which path).
let _pbarSkipAt = 0;
function _pbarLog(src, pct, data) {
  if (!_pbarSkipAt || Date.now() - _pbarSkipAt > 5000) return;
  clientLog('pbar', src, Object.assign(
    { pct: Math.round((pct || 0) * 10) / 10, msSinceSkip: Date.now() - _pbarSkipAt },
    data || {}));
}

function digPaintProgressInstant(pct) {
  pct = Math.max(0, Math.min(100, pct || 0));
  _pbarLog('instant-snap', pct, {});
  const ids = ['player-progress-fill'];
  const mcp = document.getElementById('mc-progress');
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

// Album art: mini player-art + big-art (with like/dislike overlays), toggling
// .has-art. Falsy url -> the ♫ placeholder. Was 4 copies of this exact template.
function paintArt(url) {
  const OVERLAYS = '<div class="art-overlay heart" id="heart-overlay">♥</div><div class="art-overlay nah" id="nah-overlay">✕</div>';
  const pa = document.getElementById('player-art');
  const ba = document.getElementById('big-art');
  if (url) {
    if (pa) pa.innerHTML = `<img src="${url}">`;
    if (ba) { ba.innerHTML = `<img src="${url}">` + OVERLAYS; ba.classList.add('has-art'); }
  } else {
    if (pa) pa.innerHTML = '<div class="no-art">♫</div>';
    if (ba) { ba.innerHTML = '<div class="no-art">♫</div>' + OVERLAYS; ba.classList.remove('has-art'); }
  }
}

// Track title + artist into both the top bar and the big player page.
// A null/undefined arg leaves that field untouched (callers that only know one,
// or that should skip an empty value).
function paintTrackInfo(name, artist) {
  if (name != null) {
    const a = document.getElementById('player-track'); if (a) a.textContent = name;
    const b = document.getElementById('pc-track'); if (b) b.textContent = name;
  }
  if (artist != null) {
    const a = document.getElementById('player-artist'); if (a) a.textContent = artist;
    const b = document.getElementById('pc-artist'); if (b) b.textContent = artist;
  }
}

let _playLock = false;
let _playLockSince = 0;
// Bumped on every acquisition. A dispatch only releases the lock if it still
// owns it: after a stale-clear (below) the abandoned attempt can still settle
// later, and an unconditional `_playLock = false` in its .then/.catch would
// release the lock belonging to the play that replaced it — letting two
// dispatches run at once. The 15s stale-clear and the 15s fetch deadline expire
// at the same moment, so that collision is the common case, not a rare one.
let _playLockSeq = 0;
// Upper bound on a legitimate Player.play() round-trip. Above the ~2-8s a slow
// Spotify search / Connect PUT can take, low enough that a wedged session
// recovers within one track-end. Mirrors _nextInFlight's 12s stale-clear.
const _PLAY_LOCK_STALE_MS = 15000;
let _consecutivePlayFails = 0;
let _consecutiveRestricted = 0; // consecutive 403 "Restriction violated" skips
// Bounds the walk when Spotify is unreachable. An UNPLAYABLE moves to the next
// track with no delay — deliberately, since a 700ms warm-up retry cannot fix a
// missing device — so nothing else paces it. Normally it trips never: setting
// SpotifyDevice.giveUp switches off the Spotify-only narrowing in the picker, so
// within a track or two the queue yields Bandcamp, which plays in-browser and
// always works. It exists for the stretch of queue where that is not true,
// which would otherwise be walked at network speed and in silence.
let _consecutiveUnplayable = 0;
const _UNPLAYABLE_RUN_LIMIT = 6;
let _hasPlayedThisSession = false;
// After a deep link, confirm the Spotify app really started this track. If it
// didn't — locked phone, Spotify killed, user dismissed the app-switch — advance
// rather than sitting silent on a track that will never play. Bounded, because a
// run of unplayable Spotify tracks must not walk the whole queue unattended.
function _confirmDeepLink(t) {
  const id = t && t.id;
  // Per deep link, not global: the confirm can run twice (timer, then
  // visibility) and the queue only needs reclaiming once.
  let contextReclaimed = false;
  clientLog('connect', 'deep link dispatched — confirming playback', { id });

  async function _check(via) {
    // Compare by id, not identity: a pool upgrade rebuilds the track objects.
    const cur = currentTrack();
    if (!cur || cur.id !== id) return;            // user moved on; nothing to do
    // What Spotify ACTUALLY reports, logged in full. This check used to be
    // `st && !st.paused` — "something is playing on this account" — with no
    // comparison to the track we linked to, so a Spotify already playing
    // anything at all (another track, another device, a laptop in another
    // room) counted as "deep link confirmed playing". Every historical
    // success rate computed from that line is therefore suspect, including the
    // 7-of-7 I quoted; this records enough to tell a real success from a
    // coincidence, and to see what a cold Spotify does after the link.
    let st = null, err = null;
    try {
      st = await Player.getState();
    } catch (e) { err = String(e).slice(0, 120); }
    const spotifyTrack = st && st.trackId ? String(st.trackId) : null;
    const matched = spotifyTrack === id;
    const playing = !!(st && !st.paused);
    clientLog('connect', 'deep-link check', {
      id, via, gotState: !!st, playing, paused: st ? !!st.paused : null,
      spotifyTrack, matched, posMs: st ? Math.round(st.position || 0) : null,
      vis: document.visibilityState, err,
    });
    // One device read per deep link, deliberately un-rate-limited: whether the
    // app registered a device after being opened is THE question here, and it
    // is one call. (Spotify's dev quota — see reference_dig_spotify_quota.)
    SpotifyDevice.probeNow('deeplink-confirm');
    if (playing && matched) {
      _deepLinkAdvances = 0;
      SpotifyDevice.saw('deeplink');
      clientLog('connect', 'deep link confirmed playing', { id, via });
      // RECLAIM THE QUEUE. A `spotify:track:` link opens the track inside its
      // OWN ALBUM's context, so Spotify's up-next is that album and not DIG's.
      // When the track ends Spotify simply walks down the album and the picker
      // never runs: observed 2026-07-31, "Bear Day" was followed by "Float On
      // Baby" — same artist — with ctxPos -1, meaning the track Spotify chose
      // was not among the 19 DIG thought it had queued.
      //
      // A normal Connect play sends DIG's URI list; the deep link cannot,
      // because it only happens when that play failed. Now the device
      // demonstrably exists — Spotify is playing through it this instant — so
      // re-issue the play and the context becomes ours. It costs the few
      // seconds already elapsed and buys back the entire queue.
      if (!contextReclaimed) {
        contextReclaimed = true;
        const onNow = currentTrack();
        if (onNow && onNow.id === id && Player && Player.play) {
          clientLog('connect', 'reclaiming the queue from the album context', { id });
          Promise.resolve(Player.play(onNow)).catch(() => {});
        }
      }
      return;
    }
    if (playing && !matched) {
      // Spotify is alive but on something else. The device exists, which is
      // all the handshake needed — take it over through the API rather than
      // linking again, and the user stays in DIG.
      clientLog('connect', 'spotify playing a different track — taking it over via API',
        { id, spotifyTrack, via });
      _deepLinkAdvances = 0;
      SpotifyDevice.saw('deeplink-other-track');
      playCurrentTrack();
      return;
    }
    if (_deepLinkAdvances >= 3) {
      clientLog('connect', 'deep link never started — giving up after 3', { id, via });
      _deepLinkAdvances = 0;
      const s = document.getElementById('player-status') || document.getElementById('pc-region');
      if (s) s.textContent = 'open Spotify, then tap play';
      return;
    }
    _deepLinkAdvances++;
    // A COLD Spotify does not play from a deep link. It opens the track's page
    // and stops there — "Spotify opens on the album of the song, but nothing
    // plays" — and registers no Connect device while it sits idle.
    //
    // Measured 2026-07-31 across every deep link of the day: all 7 fired while
    // at least one device was listed started playing; the single one fired at
    // `count: 0` did not. So the link is not unreliable, it is cold-start
    // sensitive — and crucially that first link leaves the app RESIDENT, which
    // is the state that works.
    //
    // Skipping to a different track here threw that warm-up away and then hit
    // the very next Spotify track with the same missing device, which is how
    // one cold start ended a session on Bandcamp. Retry the SAME track instead:
    // by now Spotify is warm, so the normal Connect path plays it invisibly if
    // a device appeared, and one further link plays it if not.
    if (_deepLinkAdvances === 1) {
      clientLog('connect', 'deep link opened a cold Spotify — retrying the same track', { id, via });
      SpotifyDevice.saw('deeplink-warm');   // it is warm now; do not write Spotify off
      playCurrentTrack();
      return;
    }
    clientLog('connect', 'deep link never started — advancing', { id, via, attempt: _deepLinkAdvances });
    nextTrack(true);
  }

  // A deep link hands off to the Spotify app, which means iOS backgrounds
  // Safari and freezes this page — so the timer below fires into a tab that
  // cannot see playback state. It reliably read "not playing" and advanced,
  // leaving the UI one track AHEAD of the audio; the connect-poll then
  // rewrote the title to match Spotify, which is the "wrong title, then it
  // corrects itself" symptom. (48h to 2026-07-31: 3 confirmations, 0
  // "confirmed playing", 2 spurious advances.) So only judge when we can
  // actually observe: if hidden, wait for the user to come back.
  function _armed() {
    if (document.visibilityState === 'hidden') {
      clientLog('connect', 'deep-link confirm deferred — page hidden', { id });
      document.addEventListener('visibilitychange', function _onVis() {
        if (document.visibilityState === 'hidden') return;
        document.removeEventListener('visibilitychange', _onVis);
        // Give Spotify a beat to report the state it came back with.
        setTimeout(() => _check('visibility'), 1500);
      });
      return;
    }
    _check('timer');
  }
  setTimeout(_armed, _DEEPLINK_CONFIRM_MS);
}

// The ONE place dIdx moves after a failed play, and deliberately a named
// function rather than three inlined copies: the invariant it carries is not
// visible from any single copy. Player.play REPORTS an outcome and never
// touches the queue — when it also advanced, one 404 became three tracks
// dispatched and abandoned in 3.7s. See UNPLAYABLE.
//
// Clearing _playRetried is part of moving on, not an optimisation: the flag
// lives on the track OBJECT, so leaving it set silently denies that track its
// warm-up retry the next time the queue comes round to it. Two of the three
// call sites already did this; the third only differed by oversight.
function _skipToNextTrack(t) {
  delete t._playRetried;
  dIdx++;
  if (dIdx >= allDiscovery.length) dIdx = 0;
  playCurrentTrack();
}

function playCurrentTrack() {
  if (_playLock) {
    const ageMs = Date.now() - _playLockSince;
    // Self-heal a wedged lock. Player.play() awaits fetches, and a stalled
    // connection can leave its promise permanently unsettled — neither the
    // .then nor the .catch below ever runs, and _playLock is never released.
    // Before this, one hung request killed auto-advance for the whole session:
    // every subsequent track-end logged "BLOCKED by _playLock" and playback
    // simply stopped until a page reload. The default fetch deadline at the top
    // of this file now settles most of those on its own; this stays as the
    // backstop for anything that stalls outside a fetch (Spotify SDK, audio
    // element), and for browsers too old for AbortSignal.timeout.
    if (ageMs > _PLAY_LOCK_STALE_MS) {
      clientLog('play', 'forced clear of stale _playLock', { age_ms: ageMs });
    } else {
      clientLog('play', 'playCurrentTrack BLOCKED by _playLock', { age_ms: ageMs });
      return;
    }
  }
  _playLock = true;
  _playLockSince = Date.now();
  const _lockSeq = ++_playLockSeq;
  const _releaseLock = () => { if (_lockSeq === _playLockSeq) _playLock = false; };
  expMarkDirty();
  const t = currentTrack();
  {
    clientLog('play', 'playCurrentTrack', {
      track: t ? `${t.artist} — ${t.name}` : 'NULL',
      id: t?.id, source: t?.source || 'spotify',
      dIdx, queueLen: allDiscovery.length,
    });
  }
  dbg(`playCurrentTrack: ${t ? t.artist+' - '+t.name+' ['+( t.source||'spotify')+'] id='+t.id : 'NO TRACK'}`);
  if (!t) { dbg('no track, returning'); _releaseLock(); return; }
  playDbg('playCurrentTrack:start', { id: t.id, name: t.name, artist: t.artist, source: t.source || 'spotify' });

  // PIN DIG'S INTENT ATOMICALLY with the DOM update below. The connect-
  // poll uses (`_connectTrackId` vs `st.trackId`) + `_lastPlayDispatchAt`
  // to decide whether to suppress stale Spotify state. Without this
  // pre-set, there's a race window between the synchronous DOM update
  // here and the eventual `connect.play()` (which only sets these AFTER
  // Spotify's PUT returns). During that window, a poll fires with
  // `_connectTrackId` still pointing at the OLD track and Spotify
  // reporting OLD too — guard sees no mismatch, poll proceeds, and
  // overwrites the DOM with OLD's name/artist. That was the cover-flash
  // bug. Setting them here closes the window.
  const _dispatchT0 = performance.now();
  if (Player && Player._setConnectTrackId) Player._setConnectTrackId(t.id);
  if (Player) Player._lastPlayDispatchAt = Date.now();
  // Source transitions are the single strongest predictor of playback failure
  // on iOS — bandcamp→spotify lost the Connect device 61% of the time over the
  // 48h to 2026-07-31, against 3.8% for spotify→spotify. That ratio had to be
  // reconstructed by hand from interleaved log lines; emit it directly, with
  // the device-lease state that decides whether this dispatch can work at all.
  const _src = t.source || 'spotify';
  clientLog('intent', 'playCurrentTrack: pinned dispatch', {
    id: t.id, name: (t.name || '').slice(0, 60), source: _src,
    transition: `${_lastDispatchedSource || 'none'}->${_src}`,
    deviceLeaseMs: SpotifyDevice.leaseMs(),
    deviceAlive: SpotifyDevice.isProbablyLive(),
  });
  _lastDispatchedSource = _src;

  const regionTag = '';

  // Re-anchor the progress clock to 0 for the NEW track and snap the bar there
  // immediately — the moment we dispatch, not whenever the next ~3s poll lands.
  // Previously the clock was re-anchored only by the poll and by seek, so after
  // a skip the bar kept showing the PREVIOUS track's progress until a poll
  // corrected it. Duration is unknown until Spotify reports it (the track dict
  // carries none), so we anchor to 0/0; the first poll fills in the real length
  // and the stale-poll guard suppresses any in-flight poll for the old track.
  // Anchor at 0. If we already know the track's length (prefetched alongside
  // the art), pass it so the interpolator can start advancing the bar from 0
  // IMMEDIATELY — instead of holding at 0 until the first clean poll arrives
  // ~1.5-2.5s later (Spotify's /me/player lag + we'd otherwise have no
  // duration). The first matching poll then corrects any small drift.
  _pbarSkipAt = Date.now();
  _pbarLog('SKIP-start', 0, { newId: (t.id || '').slice(0, 10), dIdx,
    source: t.source || 'spotify', path: DIG_IS_IOS ? 'connect' : 'sdk' });
  if (Player && Player._anchorProgress) Player._anchorProgress(0, (_durCache.get(t.id) || 0), false, t.id);
  digPaintProgressInstant(0);
  { const _pt = document.getElementById('player-time'); if (_pt) _pt.textContent = '0:00';
    const _mc = document.getElementById('mc-time-cur'); if (_mc) _mc.textContent = '0:00'; }

  // Top bar + big player page title/artist
  paintTrackInfo(t.name, t.artist);
  document.getElementById('player-region-tag').textContent = regionTag;
  // Show AI reason when present, otherwise region
  const subTag = t._aiReason ? `✦ ${t._aiLens || 'AI'}: ${t._aiReason}` : regionTag;
  document.getElementById('pc-region').textContent = subTag;

  // Set album art immediately to avoid a glitch gap before Spotify's
  // player_state_changed event fires with the real artwork URL
  const source = t.source || 'spotify';
  let artUrl = '';
  if (source === 'bandcamp') {
    // Bandcamp cover (stable bcbits CDN URL stored at ingest); the player
    // backend resolves a fresh one only if the row lacks it.
    artUrl = t.art || '';
  } else {
    // Spotify: use the prefetched + cache-warmed cover if we already resolved
    // it, so the real art is on screen the instant we switch — no ♫ gap, no
    // waiting on the poll. ('' in the cache = resolved-but-no-art → placeholder.)
    const cached = _artCache.get(t.id);
    if (cached) {
      artUrl = cached;
      if (Player && Player._noteArt) Player._noteArt(artUrl);  // keep poll from re-setting it
    }
  }
  paintArt(artUrl);

  // Warm the cover cache: the current track (covers a cache-miss faster than
  // the poll would) plus the next handful of picks, so the upcoming switch is
  // instant. Fire-and-forget — never blocks dispatch.
  {
    const _ahead = (typeof _peekNextContextTracks === 'function') ? _peekNextContextTracks(8) : [];
    void prefetchAlbumArt([t, ..._ahead]);
  }

  // Media session metadata (lock screen, AirPods, control center)
  if ('mediaSession' in navigator) {
    navigator.mediaSession.metadata = new MediaMetadata({
      title: t.name,
      artist: t.artist,
      album: t.album || regionTag,
      artwork: artUrl ? [{ src: artUrl, sizes: '300x300', type: 'image/jpeg' }] : [],
    });
    try { navigator.mediaSession.playbackState = 'playing'; } catch (e) {}
    initMediaSessionHandlers();
    void Player.syncMediaSession();
  }

  // Reset save/dislike buttons (topbar + mobile)
  const btn = document.getElementById('btn-save');
  const mcSave = document.getElementById('mc-save');
  const mcNah = document.getElementById('mc-nah');
  const wasSaved = history.find(h => h.id === t.id && h.status === 'saved');
  btn.textContent = wasSaved ? '♥' : '♡';
  btn.classList.toggle('saved', !!wasSaved);
  mcSave.textContent = wasSaved ? '♥' : '♡';
  mcSave.classList.toggle('saved', !!wasSaved);
  const wasDisliked = history.find(h => h.id === t.id && h.status === 'disliked');
  document.getElementById('btn-nah').classList.toggle('disliked', !!wasDisliked);
  mcNah.classList.toggle('disliked', !!wasDisliked);

  dbg(`calling Player.play for "${t.artist} - ${t.name}" [${t.source||'spotify'}] id=${t.id}`);
  if (typeof window._resetListenAccumulator === 'function') window._resetListenAccumulator(t.id);
  clientLog('play', 'calling Player.play', { id: t.id, spotifyReady: Player.isSpotifyReady(), deviceId: Player.spotifyDeviceId, dispatchPrepMs: Math.round(performance.now() - _dispatchT0) });
  const _playT0 = performance.now();
  Player.play(t).then(ok => {
    clientLog('timing', 'skip→playReturned', {
      id: t.id, ok,
      // Total time from playCurrentTrack dispatch-pin to Player.play resolving.
      // This is what the user perceives as "how long did the next song take
      // to dispatch", excluding Spotify mobile's own audio-start latency
      // (which we can't measure from JS). If this is >1s consistently, the
      // bottleneck is /api/play network/server.
      totalMs: Math.round(performance.now() - _dispatchT0),
      playMs: Math.round(performance.now() - _playT0),
    });
    _releaseLock();
    clientLog('play', `Player.play returned ok=${ok}`, { id: t.id });
    dbg(`Player.play returned: ${ok} for "${t.name}"`);
    playDbg('Player.play:result', { ok, id: t.id, name: t.name });
    // A newer play() owns the player now — the track the user actually asked
    // for is already loading. Anything below (retry, skip, fail counters,
    // history, taste signals) would act on that newer track, not on this one.
    if (ok === SUPERSEDED) return;
    // Handed to the Spotify app. Give it a few seconds, then verify something
    // is actually playing; if not, move to a track that will (on iOS the next
    // pick is usually Bandcamp, which plays in-browser and always works).
    if (ok === DEEPLINK) {
      _confirmDeepLink(t);
      return;
    }
    // Known-hopeless: Spotify has no device and the handshake is spent. The
    // generic failure path below would spend a 700ms warm-up retry on a cause
    // that warming up cannot fix, and count it toward the circuit breaker.
    // Move on once — the picker already knows to prefer Bandcamp from here.
    if (ok === UNPLAYABLE) {
      if (++_consecutiveUnplayable >= _UNPLAYABLE_RUN_LIMIT) {
        clientLog('play', `UNPLAYABLE RUN: ${_consecutiveUnplayable} in a row — stopping`,
          { lastId: t.id });
        _consecutiveUnplayable = 0;
        SpotifyDevice.showAsleepNotice(true);   // the banner's Wake button is the way out
        return;
      }
      _skipToNextTrack(t);
      return;
    }
    if (!ok) {
      console.warn(`[DIG] Play FAILED for "${t.artist} - ${t.name}" [${t.source||'spotify'}] id=${t.id}`);
      // Spotify refused this track with a 403 "Restriction violated"
      // (market-restricted / greyed-out / unavailable). A retry will never
      // clear it, so skip straight to the next track silently — no warmup
      // retry, no circuit-breaker increment. A separate counter guards against
      // an endless run of restricted tracks.
      if (Player.spotifyLastRestricted) {
        _consecutiveRestricted = (_consecutiveRestricted || 0) + 1;
        clientLog('play', 'restricted track — skipping to next', { id: t.id, name: t.name, run: _consecutiveRestricted });
        if (_consecutiveRestricted >= 12) {
          clientLog('play', `RESTRICTED RUN: ${_consecutiveRestricted} in a row — stopping`, { lastId: t.id });
          _consecutiveRestricted = 0;
          return;
        }
        _skipToNextTrack(t);
        return;
      }
      // Spotify Web SDK sometimes returns 404 "Device not found" for a few
      // seconds after a new device registers. Retry the same track once with
      // a brief delay before treating the failure as real.
      if (!t._playRetried) {
        t._playRetried = true;
        clientLog('play', 'retry in 700ms (device may be warming up)', { id: t.id });
        setTimeout(() => playCurrentTrack(), 700);
        return;
      }
      // Circuit breaker: if multiple consecutive plays fail, STOP instead of
      // burning through the entire queue (the cascade bug).
      _consecutivePlayFails = (_consecutivePlayFails || 0) + 1;
      if (_consecutivePlayFails >= 3) {
        console.error(`[DIG] ${_consecutivePlayFails} consecutive play failures — stopping.`);
        clientLog('play', `CIRCUIT BREAKER: ${_consecutivePlayFails} fails`, { lastId: t.id });
        const s = document.getElementById('player-status') || document.getElementById('pc-region');
        if (s) s.textContent = DIG_IS_IOS ? 'open Spotify & sign in' : 'playback error — try refreshing';
        _consecutivePlayFails = 0;
        return;
      }
      _skipToNextTrack(t);
      return;
    }
    delete t._playRetried;
    _consecutivePlayFails = 0; // reset on success
    _consecutiveRestricted = 0; // reset restricted-run guard on success
    _consecutiveUnplayable = 0; // a track played, so the queue is not a dead run
    _hasPlayedThisSession = true;
    addToHistory(t, 'listened');
    _pushPlayed(t, 'dig');
    playDbg('history:listened', { id: t.id, name: t.name });
    if (tailoredMode) recordTasteSignal(t, 'listened');
    if (aiMixMode) _aiNotePlay();
    // Start cross-device heartbeat once playback is confirmed
    _startSessionHeartbeat();
  }).catch(err => {
    _releaseLock();
    clientLog('play', 'Player.play THREW', { err: String(err).slice(0, 200) });
  });
}

// Push a track onto the playback navigation stack. Called from
// playCurrentTrack on every successful DIG play AND from the connect-poll
// external-skip handler when Spotify itself advanced. If the user has been
// pressing prev (cursor < end), a NEW play overwrites the "future" branch
// of the stack — same model as a browser's address bar.
function _pushPlayed(track, source) {
  if (!track || !track.id) return;
  if (_navFromStack) { _navFromStack = false; return; }
  // De-dupe consecutive identical pushes (re-renders, replays of same id)
  const top = playedStack[playedCursor];
  if (top && top.id === track.id) return;
  // Truncate any future entries if user navigated back then plays a fresh one
  if (playedCursor < playedStack.length - 1) {
    playedStack = playedStack.slice(0, playedCursor + 1);
  }
  playedStack.push({
    id: track.id,
    name: track.name || '',
    artist: track.artist || '',
    source,  // 'dig' | 'external'
  });
  if (playedStack.length > _PLAYED_STACK_MAX) {
    const drop = playedStack.length - _PLAYED_STACK_MAX;
    playedStack = playedStack.slice(drop);
    playedCursor = playedStack.length - 1;
  } else {
    playedCursor = playedStack.length - 1;
  }
}

async function prevTrack() {
  if (!allDiscovery.length) return;
  const fromTrack = currentTrack() || {};

  // Standard music-player "previous" semantics:
  //   • If we're past 3 s into the current track → seek to 0 (restart it).
  //   • If we're inside that 3 s window → actually go to the previous track.
  // Lets the user "double-tap previous" to skip back; single-tap restarts.
  const SEEK_TO_RESTART_MS = 3000;
  let curPos = 0;
  try {
    const st = (typeof Player?.getState === 'function') ? await Player.getState() : null;
    curPos = (st && typeof st.position === 'number') ? st.position : 0;
  } catch (e) {}
  if (curPos > SEEK_TO_RESTART_MS) {
    clientLog('prev', 'prevTrack: restart current (pos > 3s)', {
      curPos, trackId: fromTrack.id, name: fromTrack.name,
    });
    // Seek to 0 instead of advancing the stack — same track keeps playing.
    if (typeof Player?.seek === 'function') {
      await Player.seek(0);
    } else {
      // Fallback: re-dispatch play (will start from 0 by default).
      playCurrentTrack();
    }
    return;
  }

  // PREFER the navigation stack — gives "the song you were just hearing"
  // semantics. Falls back to dIdx-1 only if the stack is empty (first play
  // of the session).
  if (playedCursor > 0) {
    playedCursor--;
    const target = playedStack[playedCursor];
    clientLog('prev', 'prevTrack: from stack', {
      mode: currentMode(),
      cursor: playedCursor, stackLen: playedStack.length,
      fromId: fromTrack.id || null, fromName: fromTrack.name || null,
      toId: target.id, toName: target.name, targetSource: target.source,
    });
    // Find the target in the queue or pool
    const idxInQueue = allDiscovery.findIndex(t => t.id === target.id);
    if (idxInQueue >= 0) {
      dIdx = idxInQueue;
    } else {
      // External-Spotify track that DIG doesn't have — splice a stub in
      // at the current position so playCurrentTrack picks it up.
      const stub = { id: target.id, name: target.name, artist: target.artist, source: 'spotify' };
      allDiscovery.splice(dIdx, 0, stub);
    }
    _navFromStack = true;  // suppress the next push (we're navigating, not playing fresh)
    playCurrentTrack();
    return;
  }

  // Fallback: stack is empty (very first interaction) — do the old dIdx-1.
  const fromIdx = dIdx;
  const toIdx = (dIdx - 1 + allDiscovery.length) % allDiscovery.length;
  const toTrack = allDiscovery[toIdx] || {};
  clientLog('prev', 'prevTrack: stack empty, fallback to dIdx-1', {
    mode: currentMode(),
    fromIdx, toIdx,
    fromId: fromTrack.id || null, fromName: fromTrack.name || null,
    toId: toTrack.id || null, toName: toTrack.name || null,
    queueLen: allDiscovery.length,
  });
  dIdx = toIdx;
  playCurrentTrack();
}

// Concurrency guard: only one nextTrack workflow at a time. In AI Mix and
// Journey modes the resolution is async (Spotify search + maybe waiting on a
// queue refill — 2–8s), so without this guard rapid skip taps would race and
// silently cancel each other out, making the player look stuck.
let _nextInFlight = false;
let _nextInFlightSince = 0;
function _clearNextInFlight(why) {
  if (_nextInFlight) {
    clientLog('skip', `clear _nextInFlight (${why})`, { age_ms: Date.now() - _nextInFlightSince });
  }
  _nextInFlight = false;
  _nextInFlightSince = 0;
}

function nextTrack(isSkip = true) {
  if (_nextInFlight) {
    const ageMs = Date.now() - _nextInFlightSince;
    clientLog('skip', 'IGNORED — previous skip still in flight', { age_ms: ageMs });
    // Self-heal: if a previous skip has been "in flight" for >12s something
    // hung. Force-clear and proceed so the user isn't permanently stuck.
    if (ageMs > 12000) {
      console.warn('[DIG skip] forced clear of stale _nextInFlight after', ageMs, 'ms');
      _clearNextInFlight('stale');
    } else {
      // Visible feedback so user knows the press was seen
      const s = document.getElementById('player-status');
      if (s) s.textContent = '… still loading';
      return;
    }
  }
  _nextInFlight = true;
  _nextInFlightSince = Date.now();

  {
    clientLog('skip', isSkip ? 'skip pressed' : 'track ended', {
      mode: journeyMode ? 'journey' : (aiMixMode ? 'aimix' : (tailoredMode ? 'tailored' : 'normal')),
      idx: dIdx, queueLen: allDiscovery.length,
    });
  }

  playDbg(isSkip ? 'nextTrack:skip' : 'nextTrack:ended', { fromIdx: dIdx, isSkip });
  // Mark current as skipped if user pressed skip; capture how much was played.
  const t = currentTrack();
  const pct = (typeof getPlayedPct === 'function') ? getPlayedPct() : null;
  if (t && isSkip) {
    const existing = history.find(h => h.id === t.id);
    if (existing && existing.status === 'listened') {
      existing.status = 'skipped';
      if (pct != null) existing.played_pct = pct;
      saveHistory();
      renderFeed();
    }
    if (tailoredMode) {
      recordTasteSignal(t, 'skip', pct);
      // Penalize anchor proportional to how quickly they skipped
      if (typeof _adjustAnchorFromPct === 'function') _adjustAnchorFromPct(pct, 'skip');
    }
  } else if (t && !isSkip) {
    // Track ended naturally — capture final pct (~100)
    const existing = history.find(h => h.id === t.id);
    if (existing && pct != null) existing.played_pct = Math.max(existing.played_pct || 0, pct);
    if (tailoredMode) {
      recordTasteSignal(t, 'listened', pct ?? 100);
      // Boost anchor — they listened through, this anchor is working
      if (typeof _adjustAnchorFromPct === 'function') _adjustAnchorFromPct(pct ?? 100, 'listened');
    }
    saveHistory();
  }

  if (journeyMode) {
    // Mark engagement on the track the user is leaving
    if (t && typeof _journeyMarkEngagement === 'function') {
      const playedPctNow = (typeof getPlayedPct === 'function') ? getPlayedPct() : null;
      let eng = 'served';
      if (!isSkip) eng = 'finished';
      else if (playedPctNow != null && playedPctNow < 10) eng = 'instant_skip';
      else if (playedPctNow != null && playedPctNow >= 70) eng = 'late_skip';
      else eng = 'mid_skip';
      _journeyMarkEngagement(t, eng);
    }
    pickNextJourneyTrack().then(next => {
      if (next) {
        dIdx++;
        allDiscovery.splice(dIdx, 0, next);
        playCurrentTrack();
      } else {
        document.getElementById('player-status').textContent =
          '🛫 no new tracks on this journey';
      }
    }).finally(() => _clearNextInFlight('journey-resolved'));
    return;
  }

  if (aiMixMode) {
    // AI Mix: pull from the AI-curated queue
    pickNextAiTrack().then(next => {
      if (next) {
        dIdx++;
        allDiscovery.splice(dIdx, 0, next);
        playCurrentTrack();
      } else {
        document.getElementById('player-status').textContent =
          'AI: no new tracks — try another mode';
      }
    }).finally(() => _clearNextInFlight('aimix-resolved'));
    return;
  }

  if (tailoredMode) {
    // Dynamic picking: insert next track into the queue at current position
    const next = pickNextTrack();
    if (next) {
      dIdx++;
      allDiscovery.splice(dIdx, 0, next);
      // try/finally: a throw in playCurrentTrack() would otherwise strand
      // _nextInFlight=true and swallow every skip until the 12s stale-clear.
      // The journey and aimix paths get this guarantee free from .finally().
      try { playCurrentTrack(); } finally { _clearNextInFlight('tailored-resolved'); }
      return;
    }
    // Tailored picker has nothing unheard — fall through to pool-exhausted.
  }

  // Discovery (normal) mode — stratified coverage-gap explorer.
  //
  // The contract (per ARCHITECTURE.md Principle 1): each pick should
  //   (1) DIFFER from recent plays on artist / genre / country, AND
  //   (2) PRIORITISE under-served cells (rare genres, rare countries)
  //       so over time we cover the catalogue rather than circling
  //       around the user's already-explored neighbourhood.
  //
  // Implementation is two stages, both global (no scan-window bias):
  //   STAGE 1 — anti-cluster HARD FILTER: drop any candidate whose
  //     artists OR country OR top genre overlap with the last few plays.
  //     This is a contract guarantee, not a soft penalty: a same-
  //     collaborator track is structurally impossible, not merely
  //     down-weighted.
  //   STAGE 2 — weighted-random sample over coverage gap: each survivor's
  //     weight = (1 / (1 + plays_in_its_genre)) × (1 / (1 + plays_in_its_country)).
  //     Genres/countries you've never touched approach weight 1.0;
  //     saturated cells approach 0. Over many picks this naturally
  //     drives coverage toward an even distribution.
  //
  // The 200-track diversity-shuffled scan window is gone — the picker
  // now sees the full pool every time. No queue-position bias.
  const pick = _pickDiscoveryStratified();
  let found = false;
  if (pick) {
    // Splice the picked track at dIdx so playCurrentTrack uses it.
    dIdx = (dIdx + 1) % Math.max(1, allDiscovery.length);
    allDiscovery.splice(dIdx, 0, pick.track);
    found = true;
    {
      clientLog('picker', 'discovery: stratified', {
        track: `${pick.track.artist} — ${pick.track.name}`.slice(0, 80),
        id: pick.track.id,
        genre: pick.evidence.genre,
        genrePlays: pick.evidence.genrePlays,
        country: pick.evidence.country,
        countryPlays: pick.evidence.countryPlays,
        eligibleN: pick.evidence.eligible,
        filteredN: pick.evidence.filtered,
        sampleN: pick.evidence.sample,
      });
    }
  }
  if (!found) {
    document.getElementById('player-status').textContent =
      'no new tracks — try a different mode';
    _clearNextInFlight('pool-exhausted');
    return;
  }
  // Same exception-safety as the tailored path above.
  try { playCurrentTrack(); } finally { _clearNextInFlight('normal-resolved'); }
}

// === Stratified coverage-gap explorer ===
// Returns { track, evidence } or null if pool exhausted.
function _pickDiscoveryStratified() {
  if (!allTracksPool || !allTracksPool.length) return null;
  const heardIds = new Set(history.map(h => h.id));
  // Eligible = unheard, not session-played
  let eligible = allTracksPool.filter(t =>
    t && t.id && !heardIds.has(t.id) && !playedIds.has(t.id));
  if (!eligible.length) return null;

  // KEEP TO SPOTIFY. Once the handshake has worked, Spotify stays the source
  // and Bandcamp is the fallback — not a co-equal source to interleave with.
  //
  // This is the exact inverse of the deferral it replaces, and the inversion is
  // the point. That one narrowed to BANDCAMP whenever the device looked dead,
  // which fed the very chain that killed it: every Bandcamp track pauses
  // Spotify, a paused backgrounded app is what iOS reclaims, a reclaimed app
  // deregisters its device, and the next Spotify pick then deep-links into a
  // COLD app — which opens the track page and does not play it. Measured
  // 2026-07-31: 18,213 Spotify tracks withheld, 24 Bandcamp played in a row,
  // every deep link after that dead. Every link that DID work landed on a
  // Spotify still resident. Staying on Spotify is what keeps it resident.
  //
  // iOS only: on desktop the Connect device is DIG's own tab, it never sleeps,
  // and interleaving costs nothing — so the narrowing follows the problem
  // rather than becoming a blanket platform rule.
  //
  // Guarded so it can never empty the pool: running discovery dry would be a
  // worse failure than the one this prevents.
  if (DIG_IS_IOS && !DIG_GUEST && !SpotifyDevice.isUnavailable()) {
    const spotifyOnly = eligible.filter(t => !_isBandcampTrack(t));
    if (spotifyOnly.length >= 50) eligible = spotifyOnly;
  }
  SpotifyDevice.pollForReturn();   // no-op unless we are on the fallback
  SpotifyDevice.showAsleepNotice(SpotifyDevice.isUnavailable());

  // STAGE 1 — Anti-cluster filter. Build "recent" sets from last N plays.
  const RECENT_N = 6;
  const recentArtists = new Set();
  const recentCountries = new Set();
  const recentTopGenres = new Set();
  for (let i = 0; i < Math.min(RECENT_N, history.length); i++) {
    const h = history[i];
    if (!h) continue;
    for (const a of _allArtists(h.artist)) recentArtists.add(a);
    if (h.region) recentCountries.add(h.region);
    // history entries don't carry genres directly — look up in pool
    const poolMatch = allTracksPool.find(t => t.id === h.id);
    if (poolMatch && poolMatch.genres && poolMatch.genres.length) {
      recentTopGenres.add(poolMatch.genres[0]);
    }
  }
  const filtered = eligible.filter(t => {
    // Artist overlap with recent N: hard reject.
    for (const a of _allArtists(t.artist)) {
      if (recentArtists.has(a)) return false;
    }
    // Country overlap with recent N: reject UNLESS the country is rare
    // (otherwise we'd lock out e.g. all Italy tracks for 6 plays).
    const country = t.origin_region || t.region;
    if (country && country !== 'Unknown' && recentCountries.has(country)) {
      const playsHere = userCoverage.countries[country] || 0;
      if (playsHere > 20) return false;  // rare countries can bunch; common ones can't
    }
    // Top genre overlap with recent N: same logic as country.
    const topGenre = (t.genres && t.genres[0]) || null;
    if (topGenre && recentTopGenres.has(topGenre)) {
      const playsHere = userCoverage.genres[topGenre] || 0;
      if (playsHere > 20) return false;
    }
    return true;
  });
  // Fall back to eligible if filter is too aggressive (small pool / heavy session)
  const pool = filtered.length >= 50 ? filtered : eligible;

  // STAGE 2 — Weighted random by coverage gap.
  //   weight(t) = (genre_gap × country_gap) raised to a softening power.
  //   genre_gap   = 1 / (1 + min plays across t's genres)
  //   country_gap = 1 / (1 + plays in t's country); Unknown → tiny.
  // Use min() across genres so a track tagged with one rare genre still
  // wins, even if its other tags are common (multi-tag tracks
  // shouldn't be punished for breadth of tagging).
  function _gap(t) {
    let minGenrePlays = Infinity;
    for (const g of (t.genres || [])) {
      const p = userCoverage.genres[g] || 0;
      if (p < minGenrePlays) minGenrePlays = p;
    }
    if (!isFinite(minGenrePlays)) minGenrePlays = 50;  // no genre tag = treat as common
    const country = t.origin_region || t.region;
    let countryPlays;
    if (!country || country === 'Unknown') countryPlays = 200;  // strong dispreference
    else countryPlays = userCoverage.countries[country] || 0;
    const genreGap = 1 / (1 + minGenrePlays);
    const countryGap = 1 / (1 + countryPlays);
    // Softening: raise to 0.6 so the gap from "0 plays" to "5 plays"
    // is meaningful but not absolute.
    let w = Math.pow(genreGap * countryGap, 0.6);
    // Within-country flattening guard. Weighted-random over individual tracks
    // lets a country's biggest genre win by sheer track count — the pool has
    // ~100 rebetiko tracks for Greece vs a handful of entechno, so rebetiko is
    // ~30× likelier even at equal per-track weight. That reproduces the
    // collection skew (every non-Western country collapses to one traditional
    // stereotype). Divide each track's weight by its genre's in-country count
    // (softened, exp 0.8) so genres compete on roughly equal MASS, not track
    // count, surfacing the country's long tail and modern scenes. Greece:
    // rebetiko mass drops from ~20× entechno to ~1.5×.
    const cgCount = _countryGenreCountOf(country, t._genre);
    if (cgCount > 1) w /= Math.pow(cgCount, 0.8);
    return w;
  }

  // Compute weights and a running-sum array for binary-search sampling.
  // Capping the candidate set to a random sample of 1000 keeps this fast
  // even with a 27 K pool; the sample is large enough that the weighted
  // pick dominates over the random sub-sample.
  const SAMPLE = 1000;
  let sample = pool;
  if (pool.length > SAMPLE) {
    sample = [];
    const step = pool.length / SAMPLE;
    for (let i = 0; i < SAMPLE; i++) sample.push(pool[Math.floor(i * step + Math.random() * step)]);
  }
  let totalWeight = 0;
  const weights = sample.map(t => { const w = _gap(t); totalWeight += w; return w; });
  if (totalWeight <= 0) {
    // Degenerate: all gaps are zero. Just random-pick.
    const t = sample[Math.floor(Math.random() * sample.length)];
    return { track: t, evidence: { reason: 'degenerate_random', poolSize: pool.length } };
  }
  let r = Math.random() * totalWeight;
  let chosen = sample[0];
  for (let i = 0; i < sample.length; i++) {
    r -= weights[i];
    if (r <= 0) { chosen = sample[i]; break; }
  }

  // Evidence for the picker log.
  const country = chosen.origin_region || chosen.region || '(none)';
  const minGenrePlays = (chosen.genres || []).reduce(
    (m, g) => Math.min(m, userCoverage.genres[g] || 0), Infinity);
  const countryPlays = userCoverage.countries[country] || 0;
  return {
    track: chosen,
    evidence: {
      eligible: eligible.length,
      filtered: filtered.length,
      pool: pool.length,
      sample: sample.length,
      genre: (chosen.genres || [])[0] || null,
      genrePlays: isFinite(minGenrePlays) ? minGenrePlays : null,
      country,
      countryPlays,
    },
  };
}

function saveCurrentTrack() {
  const t = currentTrack();
  if (!t) {
    clientLog('like', 'save tapped — NO current track');
    return;
  }
  // Toggle: if currently saved, un-save (back to 'listened').
  const isSaved = (history.find(h => h.id === t.id) || {}).status === 'saved';
  clientLog('like', 'save tapped', {
    id: t.id, name: t.name, artist: t.artist, mode: currentMode(),
    branch: isSaved ? 'unsave' : 'save',
  });
  const btnSave = document.getElementById('btn-save');
  const mcSave = document.getElementById('mc-save');
  if (isSaved) {
    btnSave.textContent = '♡';
    btnSave.classList.remove('saved');
    mcSave.textContent = '♡';
    mcSave.classList.remove('saved');
    addToHistory(t, 'listened');  // demote to neutral
    if (tailoredMode) {
      // Cancel the prior +1 save signal by writing a small negative
      tasteSignals = tasteSignals.filter(s => !(s.id === t.id && s.action === 'save'));
    }
    _saveOrUnsave('/unsave', t.artist + ' - ' + t.name, t.id);
    return;
  }
  btnSave.textContent = '♥';
  btnSave.classList.add('saved');
  mcSave.textContent = '♥';
  mcSave.classList.add('saved');
  document.getElementById('btn-nah').classList.remove('disliked');
  document.getElementById('mc-nah').classList.remove('disliked');
  addToHistory(t, 'saved');
  if (tailoredMode) {
    recordTasteSignal(t, 'save');
    if (typeof _boostCurrentAnchor === 'function') _boostCurrentAnchor();
  }
  if (journeyMode && typeof _journeyMarkEngagement === 'function') _journeyMarkEngagement(t, 'loved');
  triggerOverlay('heart-overlay');
  _saveOrUnsave('/save', t.artist + ' - ' + t.name, t.id, t.region);
  _maybePromptSignup();  // first like by an anonymous guest → soft sign-up nudge
}

// Helper: shared fetch for /save + /unsave that also surfaces the
// `needs_relink` flag from the server (set when the stored OAuth token
// lacks user-library-modify, i.e. an existing user signed in before we
// added bidirectional Spotify-library sync).
async function _saveOrUnsave(path, key, id, region) {
  try {
    const params = new URLSearchParams({ track: key });
    if (id) params.set('id', id);
    if (region) params.set('region', region);
    const r = await fetch(`${path}?${params}`);
    const data = await r.json().catch(() => ({}));
    if (data && data.needs_relink) _showRelinkPrompt();
  } catch (e) { /* network failures are harmless — local state is the source of truth */ }
}

// Soft prompt asking the user to re-link Spotify so DIG can write to
// their library. Shown once per browser; dismissible. Stash dismissal
// in localStorage so we don't nag.
function _showRelinkPrompt() {
  if (localStorage.getItem('dig-relink-dismissed') === '1') return;
  if (document.getElementById('dig-relink-banner')) return;  // already shown
  const banner = document.createElement('div');
  banner.id = 'dig-relink-banner';
  banner.style.cssText = 'position:fixed; top:8px; left:50%; transform:translateX(-50%); z-index:9999; background:#1a1a1a; border:1px solid #FF2010; color:#ddd; padding:10px 14px; border-radius:6px; font-size:13px; max-width:90%; box-shadow:0 4px 18px rgba(0,0,0,0.5); display:flex; align-items:center; gap:12px;';
  banner.innerHTML = `
    <span>Saves only land in DIG. <a href="/login" style="color:#FF2010; text-decoration:underline;">Re-link</a> to also save to your <strong>DIG</strong> playlist on Spotify.</span>
    <button id="dig-relink-dismiss" style="background:transparent; border:none; color:#888; font-size:18px; cursor:pointer; line-height:1;">×</button>
  `;
  document.body.appendChild(banner);
  document.getElementById('dig-relink-dismiss').addEventListener('click', () => {
    localStorage.setItem('dig-relink-dismissed', '1');
    banner.remove();
  });
}

// ── Sign-up sheet (magic-link) ──────────────────────────────────────────────
// Anonymous guests build taste freely; the FIRST like opens this once. Likes
// keep working whether or not they sign up. window.DIG_ANON is set from /me, so
// we never nudge a registered/Spotify user.
let _signupSheetEl = null;

function _maybePromptSignup() {
  try {
    if (window.DIG_ANON !== true) return;                 // only anonymous guests
    if (localStorage.getItem('dig-signup-prompted') === '1') return;
    localStorage.setItem('dig-signup-prompted', '1');
    openSignupSheet('first-like');
  } catch (e) {}
}

function openSignupSheet() {
  if (_signupSheetEl) return;
  const ov = document.createElement('div');
  ov.id = 'dig-signup-overlay';
  ov.style.cssText = 'position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.62);display:flex;align-items:flex-end;justify-content:center;';
  ov.innerHTML =
    '<div style="background:#141416;border:1px solid #2a2a2e;border-bottom:none;border-radius:18px 18px 0 0;max-width:440px;width:100%;padding:24px 22px calc(26px + env(safe-area-inset-bottom,0));box-shadow:0 -8px 40px rgba(0,0,0,.6)">' +
      '<div id="dig-signup-body">' +
        '<div style="font-size:20px;font-weight:800;color:#f0f0f0;margin:0 0 6px">Keep your taste ♥</div>' +
        '<div style="color:#9a9a9a;font-size:14px;line-height:1.5;margin:0 0 18px">Save your likes and pick up where you left off on any device. No password — we\'ll email you a sign-in link.</div>' +
        '<input id="dig-signup-email" type="email" inputmode="email" autocomplete="email" placeholder="you@example.com" style="width:100%;font:inherit;color:#f0f0f0;background:#18181b;border:1px solid #2a2a2e;border-radius:10px;padding:12px 13px;margin:0 0 10px;box-sizing:border-box">' +
        '<button id="dig-signup-send" style="width:100%;font:inherit;font-weight:700;border:none;border-radius:10px;padding:13px;background:#FF2010;color:#fff;cursor:pointer">Send me a link</button>' +
        '<div id="dig-signup-msg" style="font-size:13px;min-height:16px;margin-top:8px;color:#ff6b6b"></div>' +
        '<div style="text-align:center;margin-top:10px"><span id="dig-signup-spotify" style="color:#9a9a9a;font-size:13px;cursor:pointer">Have Spotify Premium? <span style="color:#FF2010;text-decoration:underline">Connect it</span> for the full catalogue</span></div>' +
        '<div style="text-align:center;margin-top:12px"><span id="dig-signup-later" style="color:#6a6a6a;font-size:13px;cursor:pointer">Maybe later</span></div>' +
      '</div>' +
    '</div>';
  document.body.appendChild(ov);
  _signupSheetEl = ov;
  ov.addEventListener('click', (e) => { if (e.target === ov) closeSignupSheet(); });
  document.getElementById('dig-signup-later').onclick = closeSignupSheet;
  document.getElementById('dig-signup-send').onclick = submitSignupEmail;
  document.getElementById('dig-signup-spotify').onclick = () => { window.location.href = '/login'; };
  document.getElementById('dig-signup-email').addEventListener('keydown', (e) => { if (e.key === 'Enter') submitSignupEmail(); });
  setTimeout(() => { const i = document.getElementById('dig-signup-email'); if (i) i.focus(); }, 60);
}

function closeSignupSheet() {
  if (_signupSheetEl) { _signupSheetEl.remove(); _signupSheetEl = null; }
}

async function submitSignupEmail() {
  const inp = document.getElementById('dig-signup-email');
  const msg = document.getElementById('dig-signup-msg');
  const btn = document.getElementById('dig-signup-send');
  const email = (inp.value || '').trim();
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) { msg.style.color = '#ff6b6b'; msg.textContent = 'Enter a valid email.'; return; }
  btn.disabled = true; btn.textContent = 'Sending…'; msg.textContent = '';
  try {
    const r = await fetch('/auth/request', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email }) });
    const text = await r.text(); let d = {}; try { d = JSON.parse(text); } catch (_) {}
    if (!r.ok || !d.ok) throw new Error(d.error === 'invalid_email' ? 'That email looks off.' : "Server's busy — try again in a moment.");
    document.getElementById('dig-signup-body').innerHTML =
      '<div style="text-align:center;padding:6px 0">' +
        '<div style="font-size:30px;margin-bottom:8px">✉️</div>' +
        '<div style="font-size:18px;font-weight:800;color:#f0f0f0;margin-bottom:6px">Check your inbox</div>' +
        '<div style="color:#9a9a9a;font-size:14px;line-height:1.5">We sent a sign-in link to <b style="color:#ddd">' + email.replace(/</g, '&lt;') + '</b>. Tap it to save your taste — it expires in 30 minutes.</div>' +
        '<button id="dig-signup-done" style="margin-top:18px;font:inherit;font-weight:700;border:1px solid #2a2a2e;background:transparent;color:#f0f0f0;border-radius:10px;padding:11px 20px;cursor:pointer">Got it</button>' +
      '</div>';
    document.getElementById('dig-signup-done').onclick = closeSignupSheet;
  } catch (e) {
    msg.style.color = '#ff6b6b'; msg.textContent = e.message; btn.disabled = false; btn.textContent = 'Send me a link';
  }
}

function dislikeCurrentTrack() {
  const t = currentTrack();
  if (!t) {
    clientLog('dislike', 'dislike tapped — NO current track');
    return;
  }
  const _isDislikedAlready = (history.find(h => h.id === t.id) || {}).status === 'disliked';
  clientLog('dislike', 'dislike tapped', {
    id: t.id, name: t.name, artist: t.artist, mode: currentMode(),
    branch: _isDislikedAlready ? 'undislike' : 'dislike',
  });
  const btnNah = document.getElementById('btn-nah');
  const mcNah = document.getElementById('mc-nah');
  // Toggle: if currently disliked, un-dislike (back to listened, no skip).
  const isDisliked = (history.find(h => h.id === t.id) || {}).status === 'disliked';
  if (isDisliked) {
    btnNah.classList.remove('disliked');
    mcNah.classList.remove('disliked');
    addToHistory(t, 'listened');  // demote to neutral
    if (tailoredMode) {
      tasteSignals = tasteSignals.filter(s => !(s.id === t.id && s.action === 'dislike'));
    }
    fetch(`/undislike?track=${encodeURIComponent(t.artist + ' - ' + t.name)}`).catch(() => {});
    return;
  }
  btnNah.classList.add('disliked');
  mcNah.classList.add('disliked');
  document.getElementById('btn-save').textContent = '♡';
  document.getElementById('btn-save').classList.remove('saved');
  document.getElementById('mc-save').textContent = '♡';
  document.getElementById('mc-save').classList.remove('saved');
  addToHistory(t, 'disliked');
  if (tailoredMode) recordTasteSignal(t, 'dislike');
  if (journeyMode && typeof _journeyMarkEngagement === 'function') _journeyMarkEngagement(t, 'rejected');
  triggerOverlay('nah-overlay');
  setTimeout(() => nextTrack(false), 500);
}

// ===== FEED/LEDGER =====
function renderFeed() {
  if (!DATA) return;
  if (currentView !== 'list') return;
  const el = document.getElementById('feed');
  const search = (document.getElementById('search').value || '').toLowerCase();

  let items = [];

  if (currentFilter === 'all' || currentFilter === 'listened' || currentFilter === 'skipped' || currentFilter === 'saved' || currentFilter === 'disliked') {
    // Session history — dedupe by lower(artist - track) so the same song
    // across different Spotify track_ids (single vs album release vs remaster)
    // collapses to a single row. We keep the row with the highest-priority
    // status: saved > listened > disliked > skipped > known.
    const STATUS_RANK = { saved: 5, listened: 4, disliked: 3, skipped: 2, known: 1 };
    const byKey = new Map();
    for (const h of history) {
      if (currentFilter !== 'all' && h.status !== currentFilter) continue;
      const key = `${(h.artist || '').toLowerCase()} - ${(h.track || '').toLowerCase()}`;
      const prev = byKey.get(key);
      if (!prev || (STATUS_RANK[h.status] || 0) > (STATUS_RANK[prev.status] || 0)) {
        byKey.set(key, h);
      }
    }
    for (const h of byKey.values()) {
      items.push({
        text: `${h.artist} - ${h.track}`,
        icon: h.status === 'saved' ? '♥' : h.status === 'disliked' ? '✕' : h.status === 'skipped' ? '→' : '▶',
        iconClass: h.status,
        region: h.region,
        id: h.id,
      });
    }
  }

  if (currentFilter === 'all' || currentFilter === 'known') {
    // Full library
    const historyIds = new Set(history.map(h => h.id));
    const historyTracks = new Set(history.map(h => `${h.artist} - ${h.track}`.toLowerCase()));
    for (const k of DATA.known) {
      if (!historyTracks.has(k.toLowerCase())) {
        items.push({ text: k, icon: '', iconClass: '', region: '' });
      }
    }
  }

  if (search) items = items.filter(i => i.text.toLowerCase().includes(search));

  document.getElementById('filter-info').textContent = `${items.length} tracks`;

  const visible = Math.min(_feedLimit, items.length);
  el.innerHTML = items.slice(0, visible).map((i, idx) => {
    const isSaved = i.iconClass === 'saved';
    const isDisliked = i.iconClass === 'disliked';
    return `
    <div class="feed-item" data-idx="${idx}">
      <div class="feed-icon ${i.iconClass}">${i.icon}</div>
      <div class="feed-text">${i.text}</div>
      <div class="feed-actions">
        <button class="feed-btn ${isSaved ? 'active-save' : ''}" data-action="save" data-id="${i.id || ''}" data-text="${i.text}" title="Like">♥</button>
        <button class="feed-btn ${isDisliked ? 'active-dislike' : ''}" data-action="dislike" data-id="${i.id || ''}" data-text="${i.text}" title="Dislike">✕</button>
        <button class="feed-btn" data-action="journey" data-id="${i.id || ''}" data-text="${i.text}" title="Start a journey from here">🛫</button>
      </div>
    </div>`;
  }).join('') + (items.length > visible
    ? `<div class="feed-item feed-more" id="feed-view-more" style="cursor:pointer; justify-content:center; color:#888; padding:14px;">View ${Math.min(_FEED_PAGE, items.length - visible)} more (${items.length - visible} left)</div>`
    : '');

  const more = document.getElementById('feed-view-more');
  if (more) more.addEventListener('click', () => { _feedLimit += _FEED_PAGE; renderFeed(); });

  // Wire up feed action buttons
  el.querySelectorAll('.feed-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const action = btn.dataset.action;
      const id = btn.dataset.id;
      const text = btn.dataset.text;
      feedAction(action, id, text);
    });
  });

  // Wire up row click to play
  el.querySelectorAll('.feed-item').forEach(item => {
    item.style.cursor = 'pointer';
    item.addEventListener('click', () => {
      const id = item.querySelector('.feed-btn')?.dataset.id;
      const text = item.querySelector('.feed-text')?.textContent || '';
      if (!id && !text) return;

      {
        clientLog('ledger-play', 'click', { id, text, poolSize: allDiscovery.length });
      }

      // Path 1: track is in the current discovery pool — instant
      const idx = allDiscovery.findIndex(t => (id && t.id === id) || `${t.artist} - ${t.name}` === text);
      if (idx >= 0) {
        clientLog('ledger-play', 'found in pool', { idx });
        dIdx = idx;
        playCurrentTrack();
        return;
      }

      // Path 2: NOT in pool but we already have a Spotify track id in
      // hand (history rows carry one). Just play it. The previous code
      // ran a Spotify-search resolver here, which silently failed for
      // EVERY ledger entry — every saved/listened track is by definition
      // excluded from the clean-pool /discovery, and the search resolver
      // has additional artist-match guards that reject Vietnamese-with-
      // diacritics and other special-character titles.
      const parts = text.split(' - ');
      const artist = parts[0] || '';
      const track = parts.slice(1).join(' - ') || text;
      if (id) {
        clientLog('ledger-play', 'have id, playing directly', { id, text });
        // Enrich from allTracksPool if we have richer metadata (genres,
        // region, labels) for this id; otherwise build a minimal stub.
        const enriched = (allTracksPool || []).find(t => t.id === id);
        // Infer source from the id form — a `bc:` id is Bandcamp. Hardcoding
        // 'spotify' here mislabels Bandcamp ledger replays: playback still
        // routes correctly (Player.play keys off the bc: prefix, not source),
        // but the instant-art branch reads .source and would skip the cover.
        const stub = enriched || {
          id, name: track, artist,
          source: String(id).startsWith('bc:') ? 'bandcamp' : 'spotify',
        };
        dIdx = (dIdx >= 0 && dIdx < allDiscovery.length) ? dIdx + 1 : 0;
        allDiscovery.splice(dIdx, 0, stub);
        playCurrentTrack();
        return;
      }

      // Path 3: no id (e.g. Library entries from DATA.known are just
      // text). Fall back to the search resolver.
      clientLog('ledger-play', 'NOT in pool, no id — live-resolving via Spotify');
      const s = document.getElementById('player-status');
      if (s) s.textContent = '↻ loading…';
      if (!artist || !track) {
        clientLog('ledger-play', 'bail — could not parse text', { text });
        if (s) s.textContent = '↻ unparseable';
        return;
      }
      resolveAiRecToTrack({
        artist, track,
        search: `track:"${track}" artist:"${artist}"`,
        lens: 'ledger',
        reason: '',
      }).then(resolved => {
        if (!resolved) {
          clientLog('ledger-play', 'live resolve FAILED', { artist, track });
          if (s) s.textContent = '↻ not found on Spotify';
          return;
        }
        clientLog('ledger-play', 'live resolved', { id: resolved.id });
        if (s) s.textContent = '';
        dIdx = (dIdx >= 0 && dIdx < allDiscovery.length) ? dIdx + 1 : 0;
        allDiscovery.splice(dIdx, 0, resolved);
        playCurrentTrack();
      }).catch(e => {
        console.error('[DIG ledger-play] resolve threw', e);
        clientLog('ledger-play', 'resolve threw', { err: String(e) });
        if (s) s.textContent = '↻ error';
      });
    });
  });
}

function feedAction(action, id, text) {
  // Find or create history entry
  let entry = id ? history.find(h => h.id === id) : history.find(h => `${h.artist} - ${h.track}` === text);

  if (!entry) {
    // Parse "artist - track" from text
    const parts = text.split(' - ');
    const artist = parts[0] || text;
    const track = parts.slice(1).join(' - ') || text;
    entry = { track, artist, id: id || '', region: '', status: 'listened',
              time: Date.now(), mode: currentMode() };
    history.unshift(entry);
  }

  if (action === 'journey') {
    // Try to enrich the seed with whatever metadata we have on hand for this track
    const poolMatch = (allTracksPool || []).find(t => t.id === entry.id) || {};
    startJourney({
      artist: entry.artist,
      track: entry.track,
      region: entry.region || poolMatch.region || '',
      year:   poolMatch._year || poolMatch.year || '',
      genres: poolMatch.genres || [],
    });
    return;
  }

  if (action === 'save') {
    // Toggle: if already saved, un-save (back to listened)
    const wasSaved = entry.status === 'saved';
    entry.status = wasSaved ? 'listened' : 'saved';
    const key = `${entry.artist} - ${entry.track}`;
    if (entry.status === 'saved') {
      _saveOrUnsave('/save', key, entry.id);
    } else {
      _saveOrUnsave('/unsave', key, entry.id);
    }
  } else if (action === 'dislike') {
    // Toggle: if already disliked, un-dislike (back to listened)
    entry.status = entry.status === 'disliked' ? 'listened' : 'disliked';
  }

  entry.time = Date.now();
  saveHistory();
  renderFeed();
}

// ===== MAP VIEW =====
let mapDimension = 'region'; // 'region', 'genre', 'year'

function gatherMapData() {
  const userCount = {};
  const savedCount = {};

  // Session history
  for (const h of history) {
    let keys = [];
    if (mapDimension === 'region') keys = [h.region || 'Unknown'];
    else if (mapDimension === 'genre') {
      // Try to find tags from spotify_tracks
      const st = (DATA.spotify_tracks || []).find(t => t.id === h.id);
      keys = st ? st.tags.slice(0, 3) : ['unknown'];
      if (!keys.length) keys = ['unknown'];
    } else if (mapDimension === 'year') {
      const st = (DATA.spotify_tracks || []).find(t => t.id === h.id);
      keys = [st && st.added ? st.added.slice(0, 4) + 's' : 'unknown'];
    }
    for (const k of keys) {
      userCount[k] = (userCount[k] || 0) + 1;
      if (h.status === 'saved') savedCount[k] = (savedCount[k] || 0) + 1;
    }
  }

  // From data.json
  if (mapDimension === 'region' && DATA.user_region_count) {
    for (const [r, c] of Object.entries(DATA.user_region_count))
      userCount[r] = (userCount[r] || 0) + c;
  }
  if (mapDimension === 'year' && DATA.user_decade_count) {
    for (const [d, c] of Object.entries(DATA.user_decade_count))
      userCount[d] = (userCount[d] || 0) + c;
  }
  if (mapDimension === 'genre' && DATA.tags) {
    for (const [tag, c] of DATA.tags)
      userCount[tag] = (userCount[tag] || 0) + c;
  }

  return { userCount, savedCount };
}

function getWorldEstimates() {
  if (mapDimension === 'region') return DATA.world_density || {};
  if (mapDimension === 'genre') {
    // Rough world genre density
    return { 'pop': 100, 'rock': 90, 'electronic': 70, 'hip hop': 65, 'jazz': 50, 'soul': 40,
      'classical': 45, 'r&b': 40, 'country': 35, 'metal': 30, 'folk': 30, 'blues': 25,
      'ambient': 20, 'funk': 20, 'reggae': 18, 'punk': 18, 'disco': 15, 'house': 25,
      'techno': 22, 'world': 30, 'latin': 35, 'singer-songwriter': 30 };
  }
  if (mapDimension === 'year') {
    return { '1950s': 15, '1960s': 30, '1970s': 50, '1980s': 65, '1990s': 75, '2000s': 85, '2010s': 95, '2020s': 100 };
  }
  return {};
}

function getAllKeys() {
  if (mapDimension === 'region') return DATA.regions || [];
  if (mapDimension === 'year') return ['1920s','1930s','1940s','1950s','1960s','1970s','1980s','1990s','2000s','2010s','2020s'];
  if (mapDimension === 'genre') {
    // Return top tags from data + any from session
    const all = new Set((DATA.tags || []).map(t => t[0]));
    return [...all].slice(0, 30);
  }
  return [];
}

function renderMap() {
  if (!DATA) return;
  if (currentView !== 'map') return;
  const el = document.getElementById('map-view');
  const { userCount, savedCount } = gatherMapData();
  const world = getWorldEstimates();
  const allKeys = getAllKeys();

  // Merge any keys from userCount not in allKeys
  const keySet = new Set(allKeys);
  for (const k of Object.keys(userCount)) { if (!keySet.has(k) && k !== 'unknown') { allKeys.push(k); keySet.add(k); } }

  const maxWorld = Math.max(...Object.values(world), 1);
  const maxUser = Math.max(...Object.values(userCount), 1);

  let html = '';

  // Dimension tabs
  html += `<div class="map-dim-tabs">
    <div class="map-dim-tab ${mapDimension==='region'?'active':''}" data-map-dim="region">Region</div>
    <div class="map-dim-tab ${mapDimension==='genre'?'active':''}" data-map-dim="genre">Genre</div>
    <div class="map-dim-tab ${mapDimension==='year'?'active':''}" data-map-dim="year">Year</div>
  </div>`;

  // Context toggle
  html += `<div class="map-context-toggle" data-map-toggle="world">
    ${showWorldContext ? '◉' : '○'} world context
  </div>`;

  // Sort: explored first, then by world estimate
  const sorted = [...allKeys].sort((a, b) => {
    const aU = userCount[a] || 0, bU = userCount[b] || 0;
    if (aU > 0 && bU === 0) return -1;
    if (bU > 0 && aU === 0) return 1;
    return (world[b] || 0) - (world[a] || 0);
  });

  for (const key of sorted) {
    const user = userCount[key] || 0;
    const saved = savedCount[key] || 0;
    const w = world[key] || 0;
    // When context is on, scale user bar relative to world; when off, relative to max user
    const worldPct = showWorldContext ? Math.max(3, (w / maxWorld) * 100) : 0;
    const userPct = user > 0 ? (showWorldContext
      ? Math.max(2, (user / (maxWorld * 1.5)) * 100) // scale against world so you see how small you are
      : Math.max(2, (user / maxUser) * 100)
    ) : 0;

    html += `<div class="map-row">
      <div class="map-row-label ${user > 0 ? 'explored' : ''}">${key}</div>
      <div class="map-row-bar">
        ${showWorldContext ? `<div class="map-row-world" style="width:${worldPct}%"></div>` : ''}
        <div class="map-row-user" style="width:${userPct}%"></div>
      </div>
      <div class="map-row-count">${user || ''}</div>
      <div class="map-row-saved">${saved > 0 ? '♥ ' + saved : ''}</div>
    </div>`;
  }

  // Summary
  const totalExplored = sorted.filter(k => (userCount[k] || 0) > 0).length;
  const totalSaved = Object.values(savedCount).reduce((a, b) => a + b, 0);
  html += `<div style="margin-top:16px;padding-top:12px;border-top:1px solid #111;font-size:10px;color:#333">
    ${totalExplored}/${sorted.length} explored · ${totalSaved} saved
  </div>`;

  el.innerHTML = html;
  _wireMapControls(el);
}

// Delegation, not inline onclick attributes. An `onclick="setMapDim('year')"`
// in generated markup is resolved against the GLOBAL scope when the click
// happens, so it silently requires setMapDim to be a global — which is exactly
// the coupling a module boundary removes. Rebuilding the panel replaces these
// nodes, so the listener goes on the container and survives the re-render.
function _wireMapControls(el) {
  if (el._digWired) return;
  el._digWired = true;
  el.addEventListener('click', (ev) => {
    const t = ev.target && ev.target.closest && ev.target.closest('[data-map-dim],[data-map-toggle]');
    if (!t) return;
    if (t.dataset.mapDim) setMapDim(t.dataset.mapDim);
    else if (t.dataset.mapToggle === 'world') toggleWorldContext();
  });
}

function setMapDim(dim) {
  mapDimension = dim;
  renderMap();
}

function toggleWorldContext() {
  showWorldContext = !showWorldContext;
  renderMap();
}

// ── Genre → likely region (used when MusicBrainz origin is unavailable) ──
// Ordered by specificity — first match wins. Only strong cultural signals.
const GENRE_REGION_HINTS = [
  // East Asia
  [/\bk-?pop\b|k-?hip.?hop|k-?indie|k-?r&b|korean/i,        'South Korea'],
  [/\bj-?pop\b|j-?rock|j-?indie|city.?pop|enka|kayokyoku|jpop|jrock/i, 'Japan'],
  [/\bchinese|mandopop|cantopop|c-?pop\b|erhu\b|guqin|chinese classical/i, 'China'],
  [/\bhong.?kong/i,                                           'Hong Kong'],
  [/\btaiwanese|taiwan/i,                                     'Taiwan'],
  // Southeast Asia
  [/\bgamelan|balinese|javanese|dangdut|indonesian/i,         'Indonesia'],
  [/\bluk.?thung|thai.?pop|thai.?country|mor.?lam/i,         'Thailand'],
  [/\bvietnamese|v-?pop/i,                                    'Vietnam'],
  // South Asia
  [/\bqawwali|ghazal|sufi.*(pakistan|india)|hindi.?film|bollywood|hindustani|carnatic|bhangra/i, 'India'],
  [/\bpakistani|lahori|karachi/i,                             'Pakistan'],
  // Middle East / North Africa
  [/\braï|algerian/i,                                         'Algeria'],
  [/\bgnawa|moroccan/i,                                       'Morocco'],
  [/\begalitarian arabic|arabic\b|khaleeji|egyptian\b/i,      'Middle East'],
  [/\bturkish\b|arabesque.*turkey|türk/i,                    'Turkey'],
  // West Africa
  [/\bafrobeat[s]?\b|highlife|jùjú|fuji\b|afropop.*nigeria|yoruba|igbo/i, 'Nigeria'],
  [/\bmandingue|griot|mande|kora\b|mali\b|senegalese|wolof/i, 'West Africa'],
  // Latin America
  [/\bbossa.?nova|mpb\b|sertanejo|forró|baile.?funk|pagode|axé|frevo/i, 'Brazil'],
  [/\bvallenato|cumbia.*colombia|colombian/i,                 'Colombia'],
  [/\bsalsa\b|son.?cubano|cuban\b|timba/i,                   'Cuba'],
  [/\btango\b|milonga\b|folklore.*(argentin|tucumán)/i,       'Argentina'],
  [/\bcumbia\b|andean|huayno|chicha\b|peruvian/i,             'Peru'],
  // Europe
  [/\bfado\b/i,                                               'Portugal'],
  [/\bflamenco|sevillana|rumba.*(española|catalana)/i,        'Spain'],
  [/\bcanzone.?napoletana|tarantella|neapolitan/i,            'Italy'],
  [/\blaïko|rebetiko|greek/i,                                 'Greece'],
  [/\bschlager\b.*german|german.*schlager|neue.?deutsche/i,   'Germany'],
  [/\bchanson\b|musette\b|french.*jazz|variété/i,             'France'],
  // Appalachian / American
  [/\bappalachian|bluegrass|country.?blues|cajun|zydeco|swamp.?(pop|rock|blues)/i, 'USA'],
];

function inferRegionFromGenres(genres) {
  if (!genres || !genres.length) return null;
  const text = genres.join(' ').toLowerCase();
  for (const [pattern, region] of GENRE_REGION_HINTS) {
    if (pattern.test(text)) return region;
  }
  return null;
}

// ===== EXPLORE VIEW =====
// Unified visualization: every track is a dot. Map by region, genre, vibe, year, or mix.
let expDots = [];        // flat array of {id, name, artist, region, source, labels, genre, year, x, y, tx, ty, r, color, histStatus}
let expRAF = null, expInited = false;
let expCamX = 0, expCamY = 0, expZoom = 1;
let expHovered = null, expDragging = false, expDragStart = null, expCamStartX = 0, expCamStartY = 0;
let expDirty = true; // only redraw when something changed
function expMarkDirty() { expDirty = true; }
let expClusters = []; // [{text,x,y,count,hue}] — one per group in current layout
let expHovCluster = null;
const CLUSTER_THRESH = 2.5; // zoom level below which cluster bubbles show instead of dots
let worldGeoJSON = null; // loaded once, drawn as silhouette in region mode
let worldPath2D = null;  // pre-built Path2D for fast redraws
let expMode = 'genre';   // 'genre', 'region', 'year'
let expSearch = '';
let expShowPool    = true;  // show discovery pool dots
let expShowHistory = false; // show user's listened/saved history dots on top
let expAnimT = 1;        // 0→1 animation progress
let expLabels = [];       // cluster labels [{text, x, y}]
let expTouchDist = 0;    // for pinch zoom

// ── Color from vibe labels ──
function vibeColor(labels, fallbackHue) {
  if (!labels || !labels.energy) {
    // Fallback: use hue from region
    return `hsl(${fallbackHue}, 30%, 25%)`;
  }
  // Energy → lightness
  const energyMap = {'very low': 18, 'low': 25, 'moderate': 35, 'high': 48, 'very high': 60};
  const lightness = energyMap[labels.energy] || 35;

  // Mood → hue (keyword matching)
  const mood = (labels.mood || '').toLowerCase();
  let hue = fallbackHue;
  if (/seren|calm|meditat|peace|tranquil|gentle/.test(mood)) hue = 210;
  else if (/warm|nostalg|golden|amber|cozy/.test(mood)) hue = 35;
  else if (/dark|heavy|aggress|intense|raw|grit/.test(mood)) hue = 330;
  else if (/bright|joy|euphor|uplift|happy|celebr/.test(mood)) hue = 80;
  else if (/myster|ethereal|dream|haunt|eerie/.test(mood)) hue = 270;
  else if (/bitter|melan|sad|sorrow|longing/.test(mood)) hue = 240;
  else if (/playful|funky|groov|bounce/.test(mood)) hue = 50;
  else if (/rebel|punk|chaos|frenet/.test(mood)) hue = 0;
  else if (/spirit|sacred|devot|reveren/.test(mood)) hue = 290;

  const saturation = lightness > 40 ? 65 : 45;
  return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
}

function vibeColorBright(labels, fallbackHue) {
  if (!labels || !labels.energy) return `hsl(${fallbackHue}, 40%, 50%)`;
  const mood = (labels.mood || '').toLowerCase();
  let hue = fallbackHue;
  if (/seren|calm|meditat|peace/.test(mood)) hue = 210;
  else if (/warm|nostalg|golden/.test(mood)) hue = 35;
  else if (/dark|heavy|aggress|intense/.test(mood)) hue = 330;
  else if (/bright|joy|euphor|uplift/.test(mood)) hue = 80;
  else if (/myster|ethereal|dream/.test(mood)) hue = 270;
  else if (/bitter|melan|sad/.test(mood)) hue = 240;
  return `hsl(${hue}, 70%, 60%)`;
}

// ── Extract genre from track ──
// Returns a clean lowercase genre string, or 'unknown' for ungroupable tracks.
function extractGenre(track) {
  // Prefer DB-backed Spotify genres (now backfilled from the artists table)
  if (track && track.genres && track.genres.length > 0) {
    const g = track.genres[0];
    if (g && !_isBadGenre(g)) return g.toLowerCase();
  }
  // Fall back to the search query that found this track
  const query = (track && track.query) || '';
  if (!query) return 'unknown';
  let g = query;
  if (g.startsWith('catalog:'))    g = g.slice(8);
  else if (g.startsWith('hint:'))  g = g.slice(5);
  else if (g.startsWith('ai:'))    g = g.slice(3);
  else if (g.startsWith('random:')) return 'unknown';
  else if (g.startsWith('artist:')) return 'unknown'; // artist-search tracks have no genre signal
  if (g.includes(' year:')) g = g.split(' year:')[0];
  g = g.trim().toLowerCase();
  // Reject strings that still look like search queries or raw titles
  if (_isBadGenre(g)) return 'unknown';
  return g || 'unknown';
}

function _isBadGenre(g) {
  if (!g) return true;
  if (g.startsWith('artist:')) return true;
  if (g.startsWith('crawl:')) return true;  // internal crawler IDs leaked into genres
  if (g === 'unknown' || g === 'random') return true;
  // Looks like a YouTube title or raw search string
  if (g.includes('#') || g.includes('@') || g.includes('http') || g.length > 80) return true;
  return false;
}

// ── Extract year from query field (fallback) ──
function extractYear(query) {
  if (!query) return null;
  const m = query.match(/year:(\d{4})/);
  return m ? parseInt(m[1]) : null;
}

// ── Layout algorithms ──
// Pack clusters tightly using a grid-spiral that adapts to cluster count

function packCenters(items, clusterSize) {
  // Hexagonal packing — tight, no big gaps
  const n = items.length;
  const cols = Math.ceil(Math.sqrt(n * 1.2));
  const spacing = clusterSize;
  const positions = {};
  items.forEach((name, i) => {
    const row = Math.floor(i / cols);
    const col = i % cols;
    const xOff = (row % 2) * spacing * 0.5; // hex offset
    positions[name] = {
      x: (col - cols / 2) * spacing + xOff,
      y: (row - Math.floor(n / cols) / 2) * spacing * 0.87,
    };
  });
  return positions;
}

// ── Unified embedding layout (one map, many lenses) ──
// When TRACK_MAP is loaded, all modes except Year use the same positions.
// Labels change per mode to highlight different dimensions.

function useEmbeddingLayout(dots) {
  // Returns true if embedding positions were applied
  if (!window.TRACK_MAP) return false;
  let applied = 0;
  for (const d of dots) {
    const pos = TRACK_MAP[d.id];
    if (pos) {
      d.tx = pos[0];
      d.ty = pos[1];
      applied++;
    } else {
      // Fallback: jitter around origin
      d.tx = (Math.random() - 0.5) * 60;
      d.ty = (Math.random() - 0.5) * 60;
    }
  }
  return applied > dots.length * 0.3; // consider it working if >30% matched
}

function computeClusterLabels(dots, keyFn, minSize) {
  // Given dots already positioned, compute label positions as centroid of each group
  const groups = {};
  for (const d of dots) {
    const key = keyFn(d);
    if (!groups[key]) groups[key] = { sx: 0, sy: 0, n: 0 };
    groups[key].sx += d.tx;
    groups[key].sy += d.ty;
    groups[key].n++;
  }
  return Object.entries(groups)
    .filter(([_, g]) => g.n >= minSize)
    .map(([name, g]) => ({ text: name, x: g.sx / g.n, y: g.sy / g.n - 5 }));
}

// ── Region → [longitude, latitude] — used to project onto the world silhouette ──
// Projection: canvas_x = lon * 0.67,  canvas_y = -lat * 0.67
const REGION_GEO = {
  // ── North America ──
  'USA': [-100, 39], 'United States': [-100, 39], 'US': [-100, 39],
  'Canada': [-96, 60], 'Mexico': [-102, 24],
  'Cuba': [-79, 22], 'Jamaica': [-77, 18], 'Haiti': [-73, 19],
  'Dominican Republic': [-70, 19], 'Puerto Rico': [-66, 18],
  'Trinidad': [-61, 11], 'Trinidad and Tobago': [-61, 11],
  'Barbados': [-59, 13], 'Bahamas': [-77, 25],
  'Caribbean': [-72, 18],
  'Central America': [-86, 13], 'Guatemala': [-90, 15],
  'Costa Rica': [-84, 10], 'Panama': [-80, 9],
  'Honduras': [-87, 15], 'El Salvador': [-89, 14], 'Nicaragua': [-85, 13],
  // ── South America ──
  'Brazil': [-52, -10], 'Argentina': [-64, -34], 'Colombia': [-74, 4],
  'Chile': [-71, -33], 'Peru': [-76, -9],
  'Venezuela': [-66, 8], 'Bolivia': [-64, -17], 'Ecuador': [-78, -2],
  'Paraguay': [-58, -23], 'Uruguay': [-56, -33],
  'Guyana': [-59, 5], 'Suriname': [-56, 4],
  // ── Western Europe ──
  'UK': [-2, 54], 'United Kingdom': [-2, 54], 'England': [-1, 52],
  'Ireland': [-8, 53], 'Scotland': [-4, 57], 'Wales': [-3, 52],
  'France': [2, 46], 'Germany': [10, 51], 'Italy': [12, 42],
  'Spain': [-4, 40], 'Portugal': [-8, 39],
  'Netherlands': [5, 52], 'Belgium': [4, 51], 'Switzerland': [8, 47],
  'Austria': [14, 47], 'Luxembourg': [6, 50],
  // ── Northern Europe ──
  'Nordic': [15, 63], 'Scandinavia': [15, 63],
  'Sweden': [15, 62], 'Norway': [9, 62], 'Denmark': [10, 56],
  'Finland': [26, 64], 'Iceland': [-19, 65],
  // ── Southern Europe ──
  'Greece': [22, 39], 'Cyprus': [33, 35], 'Malta': [14, 36],
  'Croatia': [16, 45], 'Serbia': [21, 44], 'Slovenia': [15, 46],
  'Bosnia': [18, 44], 'Bosnia and Herzegovina': [18, 44],
  'Albania': [20, 41], 'North Macedonia': [22, 42], 'Kosovo': [21, 43],
  'Montenegro': [19, 43],
  // ── Central / Eastern Europe ──
  'Eastern Europe': [22, 50], 'Poland': [20, 52],
  'Czech Republic': [16, 50], 'Czechia': [16, 50], 'Slovakia': [19, 49],
  'Hungary': [19, 47], 'Romania': [25, 46], 'Bulgaria': [25, 43],
  'Ukraine': [32, 49], 'Belarus': [28, 53],
  'Lithuania': [24, 56], 'Latvia': [25, 57], 'Estonia': [25, 59],
  'Moldova': [29, 47],
  // ── Russia / Caucasus ──
  'Russia': [60, 60], 'Siberia': [100, 60],
  'Azerbaijan': [47, 40], 'Armenia': [45, 40], 'Georgia': [43, 42],
  // ── Middle East ──
  'Middle East': [44, 28], 'Turkey': [35, 39],
  'Iran': [53, 33], 'Iraq': [44, 33],
  'Israel': [35, 31], 'Palestine': [35, 32], 'Lebanon': [36, 34],
  'Jordan': [37, 31], 'Syria': [38, 35], 'Yemen': [48, 16],
  'Saudi Arabia': [45, 24], 'UAE': [54, 24],
  'United Arab Emirates': [54, 24], 'Kuwait': [48, 29],
  'Qatar': [51, 25], 'Bahrain': [50, 26], 'Oman': [57, 22],
  // ── Central Asia ──
  'Central Asia': [63, 44],
  'Kazakhstan': [67, 48], 'Uzbekistan': [63, 41],
  'Kyrgyzstan': [74, 41], 'Tajikistan': [71, 39], 'Turkmenistan': [59, 39],
  'Afghanistan': [67, 33], 'Pakistan': [70, 30],
  // ── North Africa ──
  'North Africa': [10, 28], 'Morocco': [-6, 32], 'Algeria': [3, 28],
  'Tunisia': [9, 34], 'Libya': [17, 27], 'Egypt': [30, 27],
  // ── Sub-Saharan Africa ──
  'West Africa': [-2, 10], 'East Africa': [36, -2],
  'Central Africa': [22, -4], 'Southern Africa': [26, -26],
  'Nigeria': [8, 10], 'Ghana': [-1, 8], 'Senegal': [-14, 14],
  'Guinea': [-12, 11], "Côte d'Ivoire": [-6, 7], 'Ivory Coast': [-6, 7],
  'Cameroon': [12, 6], 'Mali': [-2, 18], 'Burkina Faso': [-2, 12],
  'Niger': [8, 17], 'Chad': [18, 15], 'Sudan': [30, 15],
  'Ethiopia': [40, 9], 'Somalia': [46, 6], 'Eritrea': [39, 15],
  'Kenya': [38, 1], 'Uganda': [32, 1], 'Tanzania': [35, -6],
  'Rwanda': [30, -2], 'Burundi': [30, -3],
  'DR Congo': [24, -4], 'Congo': [15, -1],
  'Angola': [18, -12], 'Zambia': [28, -14], 'Zimbabwe': [30, -20],
  'Mozambique': [35, -18], 'Malawi': [34, -13],
  'South Africa': [25, -29], 'Namibia': [18, -22], 'Botswana': [24, -22],
  'Madagascar': [47, -20], 'Mauritius': [57, -20], 'Comoros': [44, -12],
  // ── South Asia ──
  'India': [80, 22], 'South Asia': [78, 26],
  'Bangladesh': [90, 24], 'Sri Lanka': [81, 8],
  'Nepal': [84, 28], 'Bhutan': [90, 27], 'Maldives': [73, 3],
  'Tibet': [88, 32],
  // ── East Asia ──
  'China': [104, 36], 'Japan': [138, 37],
  'South Korea': [127, 36], 'Korea': [127, 36], 'North Korea': [127, 40],
  'Taiwan': [121, 24], 'Hong Kong': [114, 22], 'Macau': [113, 22],
  'Mongolia': [105, 47],
  // ── Southeast Asia ──
  'Southeast Asia': [108, 10],
  'Thailand': [101, 15], 'Vietnam': [107, 16], 'Cambodia': [105, 12],
  'Laos': [103, 18], 'Myanmar': [96, 20], 'Malaysia': [110, 3],
  'Singapore': [104, 1], 'Indonesia': [118, -5], 'Philippines': [122, 12],
  'Brunei': [115, 4], 'East Timor': [126, -9], 'Timor-Leste': [126, -9],
  // ── Oceania ──
  'Australia': [134, -27], 'New Zealand': [172, -42],
  'Papua New Guinea': [144, -6], 'Fiji': [178, -18],
  'Oceania': [160, -20],
};

// Build cluster summary from settled dot target positions
function buildExpClusters(dots, keyFn, filterFn) {
  const map = {};
  for (const d of dots) {
    if (d.tx === 9999) continue;
    const key = keyFn(d);
    if (filterFn && !filterFn(key)) continue; // e.g. skip 'unknown' in genre overview
    if (!map[key]) map[key] = { text: key, sx: 0, sy: 0, count: 0, hue: d.hue };
    map[key].sx += d.tx;
    map[key].sy += d.ty;
    map[key].count++;
  }
  expClusters = Object.values(map).map(cl => ({
    text: cl.text.length > 28 ? cl.text.slice(0, 26) + '…' : cl.text,
    x: cl.sx / cl.count,
    y: cl.sy / cl.count,
    count: cl.count,
    hue: cl.hue,
  }));
}

function layoutRegion() {
  const dots = expVisibleDots();
  // Project regions onto 2D world map
  const regionCounts = {};
  for (const d of dots) regionCounts[d.region] = (regionCounts[d.region] || 0) + 1;
  const regions = Object.keys(regionCounts);

  // Map geo coords to canvas space: lon → x, lat → y (inverted)
  const regionPos = {};
  for (const r of regions) {
    const geo = REGION_GEO[r];
    if (geo) {
      // Scale: longitude [-180,180] → [-120, 120], latitude [90,-90] → [-60, 60]
      regionPos[r] = { x: geo[0] * 0.67, y: -geo[1] * 0.67 };
    } else {
      // Unknown region — hash to a position
      const h = [...r].reduce((a, c) => a + c.charCodeAt(0), 0);
      regionPos[r] = { x: (h % 200) - 100, y: ((h * 7) % 120) - 60 };
    }
  }

  // dy is a fixed screen-space offset applied at draw time so it never scales with zoom
  expLabels = regions.filter(r => regionCounts[r] >= 2).map(r => ({
    text: r, x: regionPos[r].x, y: regionPos[r].y, dy: -14
  }));

  for (const d of dots) {
    const base = regionPos[d.region] || { x: 0, y: 0 };
    const count = regionCounts[d.region] || 1;
    // Tight spread — keep dots within their country's rough boundary (max ±4 world units)
    const spread = Math.min(4, 1 + Math.sqrt(count) * 0.6);
    d.tx = base.x + (Math.random() - 0.5) * spread;
    d.ty = base.y + (Math.random() - 0.5) * spread;
  }
  buildExpClusters(dots, d => d.region);
  hideNonVisible(dots);
}

function layoutGenre() {
  const dots = expVisibleDots();
  if (useEmbeddingLayout(dots)) {
    // Embedding positions + genre labels at cluster centroids (skip 'unknown' and garbage)
    expLabels = computeClusterLabels(dots, d => d.genre, 2)
      .filter(l => l.text !== 'unknown' && !_isBadGenre(l.text))
      .map(l => ({ ...l, text: l.text.length > 32 ? l.text.slice(0, 30) + '…' : l.text }));
  } else {
    // Fallback: genre embedding coords or hex packing
    const genreGroups = {};
    for (const d of dots) {
      if (!genreGroups[d.genre]) genreGroups[d.genre] = [];
      genreGroups[d.genre].push(d);
    }
    const genres = Object.keys(genreGroups).sort((a, b) => genreGroups[b].length - genreGroups[a].length);
    let genrePos;
    if (GENRE_MAP && GENRE_MAP.coords) {
      genrePos = {};
      const scale = 1.2;
      for (const g of genres) {
        const c = GENRE_MAP.coords[g.toLowerCase()];
        if (c) {
          genrePos[g] = { x: c[0] * scale, y: c[1] * scale };
        } else {
          genrePos[g] = { x: (Math.random() - 0.5) * 100, y: (Math.random() - 0.5) * 100 };
        }
      }
    } else {
      const maxCount = Math.max(...Object.values(genreGroups).map(g => g.length), 1);
      genrePos = packCenters(genres, 18 + Math.sqrt(maxCount) * 2);
    }
    expLabels = genres
      .filter(g => genreGroups[g].length >= 2 && g !== 'unknown' && !_isBadGenre(g))
      .map(g => {
        const display = g.length > 32 ? g.slice(0, 30) + '…' : g;
        return { text: display, x: genrePos[g].x, y: genrePos[g].y - 6 };
      });
    for (const d of dots) {
      const base = genrePos[d.genre] || { x: 0, y: 0 };
      const count = genreGroups[d.genre]?.length || 1;
      const spread = Math.min(14, 3 + Math.sqrt(count) * 2);
      d.tx = base.x + (Math.random() - 0.5) * spread;
      d.ty = base.y + (Math.random() - 0.5) * spread;
    }
  }
  buildExpClusters(dots, d => d.genre, key => key !== 'unknown' && !_isBadGenre(key));
  hideNonVisible(dots);
}


function layoutYear() {
  const dots = expVisibleDots();
  const minY = 1950, maxY = 2029;
  const decades = ['1950s','1960s','1970s','1980s','1990s','2000s','2010s','2020s'];
  expLabels = decades.map((d, i) => ({ text: d, x: -90 + i * 26, y: -50 }));
  for (const d of dots) {
    const year = d.year || (1950 + Math.random() * 80);
    const xNorm = (year - minY) / (maxY - minY);
    d.tx = (xNorm - 0.5) * 190;
    d.ty = (Math.random() - 0.5) * 80;
  }
  buildExpClusters(dots, d => {
    const y = d.year || 2000;
    return Math.floor(y / 10) * 10 + 's';
  });
  hideNonVisible(dots);
}


// Get dots that should participate in the layout based on visible layers
function expVisibleDots() {
  if (!expShowPool && expShowHistory) return expDots.filter(d => d.histStatus);
  if (!expShowPool && !expShowHistory) return [];
  return expDots; // pool on (with or without history overlay)
}

// Move dots outside the visible set offscreen so they don't clutter the layout
function hideNonVisible(visible) {
  const visibleIds = new Set(visible.map(d => d.id));
  for (const d of expDots) {
    if (!visibleIds.has(d.id)) { d.tx = 9999; d.ty = 9999; }
  }
}

// Build a Path2D from world GeoJSON using the same projection as layoutRegion
// lon → x = lon * 0.67,  lat → y = -lat * 0.67
function buildWorldPath(geojson) {
  const path = new Path2D();
  for (const feature of geojson.features) {
    const { type, coordinates } = feature.geometry;
    const polys = type === 'Polygon' ? [coordinates] : coordinates;
    for (const poly of polys) {
      for (const ring of poly) {
        let first = true;
        for (const [lon, lat] of ring) {
          const x = lon * 0.67, y = -lat * 0.67;
          if (first) { path.moveTo(x, y); first = false; }
          else path.lineTo(x, y);
        }
        path.closePath();
      }
    }
  }
  return path;
}

async function loadWorldGeoJSON() {
  if (worldGeoJSON) return;
  try {
    worldGeoJSON = await fetch('world.geojson').then(r => r.json());
    worldPath2D = buildWorldPath(worldGeoJSON);
    expMarkDirty();
  } catch (e) { console.warn('world.geojson failed to load', e); }
}

function applyLayout(mode) {
  if (mode === 'region') { layoutRegion(); loadWorldGeoJSON(); }
  else if (mode === 'year') layoutYear();
  else layoutGenre();
  expAnimT = 0; // start transition
}

// ── Init ──
function initExplore() {
  if (expInited) { applyLayout(expMode); return; }
  expInited = true;

  const container = document.getElementById('explore-view');
  const canvas = document.getElementById('explore-canvas');
  const ctx = canvas.getContext('2d');
  let W = container.clientWidth, H = container.clientHeight - 32; // toolbar height
  canvas.width = W * devicePixelRatio;
  canvas.height = H * devicePixelRatio;
  ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);

  // Build dots from discovery pool
  const histMap = {};
  for (const h of history) histMap[h.id] = h.status;

  const regionHues = {};
  let hueIdx = 0;
  expDots = [];
  for (const [bucket, tracks] of Object.entries(DISC || {})) {
    for (const t of tracks) {
      // Priority: 1) MusicBrainz origin  2) genre inference  3) discovery bucket
      const region = t.origin_region || inferRegionFromGenres(t.genres) || bucket;
      if (!regionHues[region]) regionHues[region] = (hueIdx++ * 137.5) % 360;
      const genre = extractGenre(t);
      const year = t.year ? parseInt(t.year) : extractYear(t.query);
      const hue = regionHues[region];
      expDots.push({
        id: t.id, name: t.name, artist: t.artist,
        region, source: t.source || 'spotify',
        labels: t.labels || null,
        genres: t.genres || [],
        genre, year,
        x: (Math.random() - 0.5) * 200, y: (Math.random() - 0.5) * 200,
        tx: 0, ty: 0, r: 3.5,
        color: vibeColor(t.labels, hue),
        colorBright: vibeColorBright(t.labels, hue),
        hue,
        histStatus: histMap[t.id] || null,
        youtube_id: t.youtube_id, thumbnail: t.thumbnail,
      });
    }
  }

  applyLayout(expMode);
  // Snap to initial positions
  for (const d of expDots) { d.x = d.tx; d.y = d.ty; }
  expAnimT = 1;

  // ── Camera: screen ↔ world ──
  function toWorld(sx, sy) {
    return [(sx - W / 2) / expZoom + expCamX, (sy - H / 2) / expZoom + expCamY];
  }
  function hitTest(sx, sy) {
    const [wx, wy] = toWorld(sx, sy);
    const hitR = 10 / expZoom; // 10px hit area in screen space
    let closest = null, closestDist = hitR * hitR;
    for (const d of expDots) {
      const dx = wx - d.x, dy = wy - d.y;
      const dist = dx * dx + dy * dy;
      if (dist < closestDist) { closest = d; closestDist = dist; }
    }
    return closest;
  }

  // ── Mouse interaction ──
  canvas.addEventListener('mousemove', e => {
    if (expDragging) {
      expCamX = expCamStartX - (e.clientX - expDragStart.x) / expZoom;
      expCamY = expCamStartY - (e.clientY - expDragStart.y) / expZoom;
      expMarkDirty();
      return;
    }
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left, my = e.clientY - rect.top;
    const tt = document.getElementById('explore-tooltip');

    // ── Cluster mode hover ──
    if (expZoom < CLUSTER_THRESH) {
      const prevHovC = expHovCluster;
      expHovCluster = null;
      for (const cl of expClusters) {
        const cx = (cl.x - expCamX) * expZoom + W / 2;
        const cy = (cl.y - expCamY) * expZoom + H / 2;
        const r = Math.max(9, Math.sqrt(cl.count) * 2.8);
        const dx = mx - cx, dy = my - cy;
        if (dx * dx + dy * dy <= (r + 4) * (r + 4)) { expHovCluster = cl; break; }
      }
      if (expHovCluster !== prevHovC) expMarkDirty();
      canvas.style.cursor = expHovCluster ? 'pointer' : 'grab';
      if (expHovCluster) {
        tt.style.display = 'block';
        tt.style.left = Math.min(e.clientX + 16, window.innerWidth - 220) + 'px';
        tt.style.top = Math.min(e.clientY - 10, window.innerHeight - 80) + 'px';
        tt.querySelector('.ett-track').textContent = expHovCluster.text;
        tt.querySelector('.ett-artist').textContent = `${expHovCluster.count} tracks · click to explore`;
        tt.querySelector('.ett-labels').innerHTML = '';
      } else { tt.style.display = 'none'; }
      return;
    }

    // ── Individual dot hover ──
    const prevHov = expHovered;
    expHovered = hitTest(mx, my);
    if (expHovered !== prevHov) expMarkDirty();
    canvas.style.cursor = expHovered ? 'pointer' : 'grab';
    if (expHovered) {
      const d = expHovered;
      tt.style.display = 'block';
      tt.style.left = Math.min(e.clientX + 16, window.innerWidth - 300) + 'px';
      tt.style.top = Math.min(e.clientY - 10, window.innerHeight - 200) + 'px';
      tt.querySelector('.ett-track').textContent = d.name;
      tt.querySelector('.ett-artist').textContent = d.artist + (d.region ? ' · ' + d.region : '');
      const labelsEl = tt.querySelector('.ett-labels');
      if (d.labels) {
        const tags = [d.labels.mood, d.labels.energy, d.labels.texture, d.labels.feel, d.labels.use_case].filter(Boolean);
        labelsEl.innerHTML = tags.map(t => `<span>${t}</span>`).join('');
      } else {
        labelsEl.innerHTML = `<span>${d.genre}</span>`;
      }
    } else {
      tt.style.display = 'none';
    }
  });

  canvas.addEventListener('mousedown', e => {
    expDragging = true;
    expDragStart = { x: e.clientX, y: e.clientY };
    expCamStartX = expCamX;
    expCamStartY = expCamY;
  });

  canvas.addEventListener('mouseup', e => {
    const wasDrag = expDragStart && (Math.abs(e.clientX - expDragStart.x) > 4 || Math.abs(e.clientY - expDragStart.y) > 4);
    expDragging = false;
    // Cluster click — zoom in to that cluster
    if (!wasDrag && expZoom < CLUSTER_THRESH && expHovCluster) {
      expCamX = expHovCluster.x;
      expCamY = expHovCluster.y;
      expZoom = CLUSTER_THRESH + 0.5;
      expHovCluster = null;
      expMarkDirty();
      return;
    }
    if (!wasDrag && expHovered) {
      // Click to play
      const idx = allDiscovery.findIndex(t => t.id === expHovered.id);
      if (idx >= 0) { dIdx = idx; playCurrentTrack(); }
    }
  });

  canvas.addEventListener('mouseleave', () => {
    expDragging = false;
    document.getElementById('explore-tooltip').style.display = 'none';
  });

  canvas.addEventListener('wheel', e => {
    e.preventDefault();
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left, my = e.clientY - rect.top;
    const [wx, wy] = toWorld(mx, my);
    const f = e.deltaY < 0 ? 1.15 : 1 / 1.15;
    const nz = Math.max(0.4, Math.min(40, expZoom * f));
    expCamX = wx - (mx - W / 2) / nz;
    expCamY = wy - (my - H / 2) / nz;
    expZoom = nz;
    if (nz >= CLUSTER_THRESH) expHovCluster = null;
    expMarkDirty();
  }, { passive: false });

  // ── Touch interaction ──
  canvas.addEventListener('touchstart', e => {
    if (e.touches.length === 2) {
      const dx = e.touches[0].clientX - e.touches[1].clientX;
      const dy = e.touches[0].clientY - e.touches[1].clientY;
      expTouchDist = Math.sqrt(dx * dx + dy * dy);
    } else if (e.touches.length === 1) {
      expDragging = true;
      expDragStart = { x: e.touches[0].clientX, y: e.touches[0].clientY };
      expCamStartX = expCamX;
      expCamStartY = expCamY;
    }
  }, { passive: true });

  canvas.addEventListener('touchmove', e => {
    e.preventDefault();
    if (e.touches.length === 2 && expTouchDist > 0) {
      const dx = e.touches[0].clientX - e.touches[1].clientX;
      const dy = e.touches[0].clientY - e.touches[1].clientY;
      const dist = Math.sqrt(dx * dx + dy * dy);
      const f = dist / expTouchDist;
      expZoom = Math.max(0.4, Math.min(40, expZoom * f));
      expTouchDist = dist;
    } else if (e.touches.length === 1 && expDragging) {
      expCamX = expCamStartX - (e.touches[0].clientX - expDragStart.x) / expZoom;
      expCamY = expCamStartY - (e.touches[0].clientY - expDragStart.y) / expZoom;
    }
    expMarkDirty();
  }, { passive: false });

  canvas.addEventListener('touchend', e => {
    if (e.changedTouches.length === 1 && expDragStart) {
      const t = e.changedTouches[0];
      const wasDrag = Math.abs(t.clientX - expDragStart.x) > 8 || Math.abs(t.clientY - expDragStart.y) > 8;
      if (!wasDrag) {
        const rect = canvas.getBoundingClientRect();
        const hit = hitTest(t.clientX - rect.left, t.clientY - rect.top);
        if (hit) {
          const idx = allDiscovery.findIndex(tr => tr.id === hit.id);
          if (idx >= 0) { dIdx = idx; playCurrentTrack(); }
        }
      }
    }
    expDragging = false;
    expTouchDist = 0;
  });

  // ── Search ──
  document.getElementById('explore-search').addEventListener('input', e => {
    expSearch = e.target.value.toLowerCase().trim();
    if (expSearch.length >= 2) {
      const match = expDots.find(d =>
        d.name.toLowerCase().includes(expSearch) ||
        d.artist.toLowerCase().includes(expSearch) ||
        d.genre.toLowerCase().includes(expSearch)
      );
      if (match) { expCamX = match.x; expCamY = match.y; expZoom = Math.max(expZoom, 3); }
    }
    expMarkDirty();
  });

  // ── Mode buttons (Genre / Region / Year) ──
  document.querySelectorAll('.explore-mode[data-mode]').forEach(btn => {
    btn.addEventListener('click', () => {
      // Only touch mode buttons — never strip active from the layer toggles
      document.querySelectorAll('.explore-mode[data-mode]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      expMode = btn.dataset.mode;
      applyLayout(expMode);
      expMarkDirty();
    });
  });

  // ── Pool / History independent toggles ──
  function refreshHistStatuses() {
    const histMap2 = {};
    for (const h of history) histMap2[h.id] = h.status;
    for (const d of expDots) d.histStatus = histMap2[d.id] || null;
  }
  function updateLayerToggles() {
    document.getElementById('tog-pool').classList.toggle('active', expShowPool);
    document.getElementById('tog-hist').classList.toggle('active', expShowHistory);
    refreshHistStatuses();
    applyLayout(expMode);
    expMarkDirty();
  }

  document.getElementById('tog-pool').addEventListener('click', () => {
    expShowPool = !expShowPool;
    // Never allow both off — fall back to pool
    if (!expShowPool && !expShowHistory) { expShowPool = true; return; }
    updateLayerToggles();
  });

  document.getElementById('tog-hist').addEventListener('click', () => {
    expShowHistory = !expShowHistory;
    // Never allow both off — fall back to pool
    if (!expShowPool && !expShowHistory) { expShowPool = true; }
    updateLayerToggles();
  });

  // ── Draw loop ──
  function drawFrame() {
    if (currentView !== 'explore') return;

    // Animation always marks dirty until settled
    if (expAnimT < 1) expDirty = true;

    if (!expDirty) {
      expRAF = requestAnimationFrame(drawFrame);
      return;
    }
    expDirty = false;

    const nW = container.clientWidth, nH = container.clientHeight - 32;
    if (nW !== W || nH !== H) {
      W = nW; H = nH;
      canvas.width = W * devicePixelRatio;
      canvas.height = H * devicePixelRatio;
      ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
    }

    // Animate positions
    if (expAnimT < 1) {
      expAnimT = Math.min(1, expAnimT + 0.04);
      const t = expAnimT * expAnimT * (3 - 2 * expAnimT); // smoothstep
      for (const d of expDots) {
        d.x += (d.tx - d.x) * t * 0.15;
        d.y += (d.ty - d.y) * t * 0.15;
      }
      expDirty = true; // keep going until animation done
    }

    ctx.clearRect(0, 0, W, H);
    ctx.save();
    ctx.translate(W / 2, H / 2);
    ctx.scale(expZoom, expZoom);
    ctx.translate(-expCamX, -expCamY);

    // ── World silhouette (region mode only) ──
    if (expMode === 'region' && worldPath2D) {
      // Solid very-dark-blue landmass so it's clearly visible against pure black
      ctx.fillStyle = 'rgba(22,30,50,1)';
      ctx.fill(worldPath2D);
      // Coastline outline
      ctx.strokeStyle = 'rgba(80,110,160,0.55)';
      ctx.lineWidth = 0.8 / expZoom; // constant ~0.8px on screen
      ctx.stroke(worldPath2D);
    }

    const playingId = currentTrack()?.id;

    // Progressive level-of-detail: fewer dots when zoomed out (they'd just overlap anyway)
    // zoom >=3 = all dots, zoom=1 = every 3rd, zoom=0.4 = every 8th
    const stride = expZoom >= 3 ? 1 : Math.max(1, Math.round(3 / Math.max(0.3, expZoom)));

    // Draw dots — skip entirely in cluster overview mode
    if (expZoom >= CLUSTER_THRESH) expHovCluster = null;
    for (let i = 0; i < expDots.length; i++) {
      if (expZoom < CLUSTER_THRESH) break; // cluster mode: skip all dots
      if (stride > 1 && (i % stride) !== 0) {
        const dd = expDots[i];
        const alwaysShow = dd.id === playingId || dd === expHovered ||
          (expShowHistory && dd.histStatus && dd.histStatus !== 'disliked');
        if (!alwaysShow) continue;
      }
      const d = expDots[i];
      const sx = (d.x - expCamX) * expZoom + W / 2;
      const sy = (d.y - expCamY) * expZoom + H / 2;
      const sR = d.r; // screen-space radius is constant regardless of zoom
      // Cull offscreen
      if (sx + sR < -10 || sx - sR > W + 10 || sy + sR < -10 || sy - sR > H + 10) continue;

      const isHov = expHovered === d;
      const isPlaying = d.id === playingId;
      const matchSearch = expSearch && (
        d.name.toLowerCase().includes(expSearch) ||
        d.artist.toLowerCase().includes(expSearch) ||
        d.genre.toLowerCase().includes(expSearch)
      );
      const dim = expSearch && !matchSearch;

      // Size in screen pixels — divide by expZoom to stay constant as you zoom
      const bothLayers = expShowPool && expShowHistory;
      const isHistDot  = !!d.histStatus; // has the user heard this track?
      let r = d.r;
      if (isHov) r = d.r * 1.8;
      if (isPlaying) r = d.r * 2;
      if (expShowHistory && d.histStatus === 'saved') r = d.r * 1.5;
      // In overlay mode pool-only dots recede slightly
      if (bothLayers && !isHistDot) r = d.r * 0.7;
      const wr = r / expZoom; // world-space radius that produces constant screen size

      ctx.beginPath();
      ctx.arc(d.x, d.y, wr, 0, Math.PI * 2);

      if (dim) {
        ctx.fillStyle = 'rgba(15,15,20,0.15)';
      } else if (bothLayers && !isHistDot) {
        // Pool background dots — faint grey when history is also on
        ctx.fillStyle = 'rgba(50,55,65,0.35)';
      } else if (expShowHistory && d.histStatus === 'saved') {
        ctx.fillStyle = '#FF2010';
      } else if (expShowHistory && d.histStatus === 'disliked') {
        ctx.fillStyle = 'rgba(30,30,30,0.2)';
      } else if (isHov || isPlaying) {
        ctx.fillStyle = d.colorBright;
      } else {
        ctx.fillStyle = d.color;
      }
      ctx.fill();

      // History ring — shown when history layer is active and dot has a status
      if (expShowHistory && isHistDot && d.histStatus !== 'disliked' && !dim) {
        ctx.beginPath();
        ctx.arc(d.x, d.y, (r + 1.2) / expZoom, 0, Math.PI * 2);
        ctx.strokeStyle = d.histStatus === 'saved' ? '#FF2010' : 'rgba(255,32,16,0.3)';
        ctx.lineWidth = (d.histStatus === 'saved' ? 1.5 : 0.8) / expZoom;
        ctx.stroke();
      }

      // Playing indicator
      if (isPlaying && !dim) {
        ctx.beginPath();
        ctx.arc(d.x, d.y, (r + 2.5) / expZoom, 0, Math.PI * 2);
        ctx.strokeStyle = '#fff';
        ctx.lineWidth = 1 / expZoom;
        ctx.stroke();
      }

      // Search highlight
      if (matchSearch) {
        ctx.beginPath();
        ctx.arc(d.x, d.y, (r + 2) / expZoom, 0, Math.PI * 2);
        ctx.strokeStyle = '#fff';
        ctx.lineWidth = 1.2 / expZoom;
        ctx.stroke();
      }
    }

    ctx.restore();

    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';

    if (expZoom < CLUSTER_THRESH) {
      // ── Cluster bubble overview ──
      for (const cl of expClusters) {
        const sx = (cl.x - expCamX) * expZoom + W / 2;
        const sy = (cl.y - expCamY) * expZoom + H / 2;
        if (sx < -80 || sx > W + 80 || sy < -80 || sy > H + 80) continue;

        const r = Math.max(9, Math.sqrt(cl.count) * 2.8);
        const isHov = expHovCluster === cl;

        ctx.globalAlpha = isHov ? 1 : 0.82;
        ctx.beginPath();
        ctx.arc(sx, sy, r, 0, Math.PI * 2);
        ctx.fillStyle = `hsl(${cl.hue},50%,30%)`;
        ctx.fill();
        if (isHov) {
          ctx.strokeStyle = 'rgba(255,255,255,0.8)';
          ctx.lineWidth = 1.5;
          ctx.stroke();
        }

        ctx.globalAlpha = 1;
        if (r >= 10) {
          const fs = Math.min(11, Math.max(7, r * 0.55));
          ctx.font = `500 ${fs}px -apple-system,system-ui,sans-serif`;
          ctx.fillStyle = '#fff';
          const label = cl.text.length > 16 ? cl.text.slice(0, 14) + '…' : cl.text;
          ctx.fillText(label, sx, r > 18 ? sy - 4 : sy);
          if (r > 18) {
            ctx.font = `400 8px -apple-system,system-ui,sans-serif`;
            ctx.fillStyle = 'rgba(255,255,255,0.55)';
            ctx.fillText(cl.count + ' tracks', sx, sy + fs - 1);
          }
        }
      }
    } else {
      // ── Individual dot labels (genre/region cluster labels) ──
      ctx.font = `600 13px -apple-system,system-ui,sans-serif`;
      for (const lbl of expLabels) {
        const sx = (lbl.x - expCamX) * expZoom + W / 2;
        // lbl.dy: optional fixed screen-space vertical nudge (does not scale with zoom)
        const sy = (lbl.y - expCamY) * expZoom + H / 2 + (lbl.dy || 0);
        if (sx < -40 || sx > W + 40 || sy < -20 || sy > H + 20) continue;
        const text = lbl.text;
        const tw = ctx.measureText(text).width;
        ctx.fillStyle = 'rgba(0,0,0,0.55)';
        ctx.fillRect(sx - tw / 2 - 5, sy - 9, tw + 10, 18);
        ctx.fillStyle = 'rgba(255,255,255,0.92)';
        ctx.fillText(text, sx, sy);
      }
    }

    expRAF = requestAnimationFrame(drawFrame);
  }
  drawFrame();
}

function setView(view) {
  currentView = view;
  document.getElementById('view-list').classList.toggle('active', view === 'list');
  document.getElementById('view-map').classList.toggle('active', view === 'map');
  document.getElementById('view-explore').classList.toggle('active', view === 'explore');
  document.getElementById('feed').style.display = view === 'list' ? '' : 'none';
  document.getElementById('map-view').classList.toggle('active', view === 'map');
  document.getElementById('explore-view').classList.toggle('active', view === 'explore');
  if (expRAF && view !== 'explore') { cancelAnimationFrame(expRAF); expRAF = null; }
  if (view === 'list') renderFeed();
  if (view === 'map') renderMap();
  if (view === 'explore') setTimeout(initExplore, 50);
}

// ===== INIT =====
// Check if user is logged in (optional — app works without login)
fetch('/me').then(r => r.json()).then(me => {
  // Anonymous guest = guest mode with no email yet → eligible for the sign-up nudge.
  window.DIG_ANON = (me.guest === true);
  if (me.logged_in && me.user) {
    if (me.user.image) {
      document.getElementById('user-avatar').src = me.user.image;
      document.getElementById('user-btn').style.display = 'block';
      document.getElementById('user-btn').title = me.user.display_name + ' (logout)';
    }
    document.getElementById('login-link').style.display = 'none';
    // Hide mobile sign-in button if logged in
    const mcLogin = document.getElementById('mc-login');
    if (mcLogin) mcLogin.style.display = 'none';
  } else {
    // Show mobile sign-in button if NOT logged in
    const mcLogin = document.getElementById('mc-login');
    if (mcLogin) mcLogin.style.display = '';
    // For anonymous guests, the sign-in affordances open the email sign-up
    // sheet (magic link) instead of the Spotify OAuth flow.
    if (me.guest === true) {
      const ll = document.getElementById('login-link');
      if (ll) { ll.style.display = ''; ll.onclick = (e) => { e.preventDefault(); openSignupSheet(); }; ll.textContent = 'sign up'; }
      if (mcLogin) mcLogin.onclick = (e) => { e.preventDefault(); openSignupSheet(); };
    }
  }
}).catch(() => {
  // Network error — show sign-in just in case
  const mcLogin = document.getElementById('mc-login');
  if (mcLogin) mcLogin.style.display = '';
});

// Guests have no Spotify init to lazily bring up the player, so start the
// Bandcamp <audio> backend now — playback is ready before the first tap.
if (DIG_GUEST) { try { Player.init(); } catch (e) {} }

// Magic-link return: show a quick toast and strip the ?login param from the URL.
(function () {
  const lp = new URLSearchParams(location.search).get('login');
  if (!lp) return;
  const t = document.createElement('div');
  t.style.cssText = 'position:fixed;top:12px;left:50%;transform:translateX(-50%);z-index:10001;background:#141416;border:1px solid #FF2010;color:#f0f0f0;padding:11px 16px;border-radius:10px;font-size:14px;box-shadow:0 6px 24px rgba(0,0,0,.5)';
  t.textContent = lp === 'ok' ? "You're signed in — your taste is saved ♥" : 'That sign-in link expired. Try again.';
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 4500);
  try { history.replaceState({}, '', location.pathname); } catch (e) {}
})();

loadHistory().then(() => {

// ── Helpers ──

function diversityShuffle(tracks) {
  // ROTATING-LENS tournament: every BLOCK_LEN tracks the active "lens" rotates,
  // so no single dimension dominates the whole queue. Within each block, the
  // active lens gets a strong weight and the others contribute lightly.
  //
  // Lenses (cycled in random order each session):
  //   region   — explore varied regions, looser on genre/vibe
  //   genre    — explore varied genres regardless of region
  //   vibe     — explore varied energy/mood regardless of region/genre
  //   era      — explore varied decades
  //   artist   — explore varied artists (anti-clustering)
  //   equal    — original 4-equal-dim mix (baseline diversity)
  //   wander   — pure random (chaos slot — surfaces unexpected things)

  const SAMPLE_SIZE = 60;
  const WINDOW = 25;
  const BLOCK_LEN = 8;

  const arr = [...tracks];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }

  function getDims(t) {
    return {
      region: t.region || '',
      genre:  t._genre && t._genre !== 'unknown' ? t._genre : '__nogenre',
      decade: t._year && t._year !== 'unknown' ? String(t._year).slice(0, 3) + '0s' : '__nodecade',
      vibe:   `${t._energy || ''}|${t._mood || ''}`,
      artist: ((t.artist || '').split(',')[0] || '').trim().toLowerCase() || '__noartist',
    };
  }

  const freq = { region: {}, genre: {}, decade: {}, vibe: {}, artist: {} };
  const windowArr = [];

  function addToWindow(dims) {
    windowArr.push(dims);
    for (const k of ['region', 'genre', 'decade', 'vibe', 'artist']) {
      freq[k][dims[k]] = (freq[k][dims[k]] || 0) + 1;
    }
    if (windowArr.length > WINDOW) {
      const old = windowArr.shift();
      for (const k of ['region', 'genre', 'decade', 'vibe', 'artist']) {
        if (--freq[k][old[k]] <= 0) delete freq[k][old[k]];
      }
    }
  }

  // Per-lens dimension weights
  const LENSES = {
    region:  { region: 4, genre: 1, decade: 1, vibe: 1, artist: 1 },
    genre:   { region: 1, genre: 4, decade: 1, vibe: 1, artist: 1 },
    vibe:    { region: 1, genre: 1, decade: 1, vibe: 4, artist: 1 },
    era:     { region: 1, genre: 1, decade: 4, vibe: 1, artist: 1 },
    artist:  { region: 1, genre: 1, decade: 1, vibe: 1, artist: 4 },
    equal:   { region: 1, genre: 1, decade: 1, vibe: 1, artist: 0 },
    wander:  { region: 0, genre: 0, decade: 0, vibe: 0, artist: 0 }, // pure random
  };
  const LENS_ORDER = (() => {
    // Random rotation each call so the queue feels fresh on every reload.
    const keys = Object.keys(LENSES);
    for (let i = keys.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [keys[i], keys[j]] = [keys[j], keys[i]];
    }
    return keys;
  })();
  console.log(`[DIG] diversity lens rotation: ${LENS_ORDER.join(' → ')} (block=${BLOCK_LEN})`);

  let _lensLog = []; // [{lens, startIdx}]
  function lensFor(idx) {
    const l = LENS_ORDER[Math.floor(idx / BLOCK_LEN) % LENS_ORDER.length];
    if (!_lensLog.length || _lensLog[_lensLog.length - 1].lens !== l) {
      _lensLog.push({ lens: l, startIdx: idx });
    }
    return l;
  }

  function noveltyScore(t, lens) {
    const d = getDims(t);
    const w = LENSES[lens];
    // +weight per dimension absent from window
    return (freq.region[d.region] ? 0 : w.region)
         + (freq.genre[d.genre]   ? 0 : w.genre)
         + (freq.decade[d.decade] ? 0 : w.decade)
         + (freq.vibe[d.vibe]     ? 0 : w.vibe)
         + (freq.artist[d.artist] ? 0 : w.artist)
         + Math.random() * 0.5; // tiebreak (also primary signal for wander)
  }

  const result = [];
  const n = arr.length;

  // Tournament selection with rotating lens. The lens for slot i is selected
  // by lensFor(i) and decides per-dimension weights for noveltyScore.
  for (let i = 0; i < n; i++) {
    const lens = lensFor(i);
    const end = Math.min(i + SAMPLE_SIZE, n);
    let bestJ = i;
    let bestScore = -1;
    for (let j = i; j < end; j++) {
      const s = noveltyScore(arr[j], lens);
      if (s > bestScore) { bestScore = s; bestJ = j; }
    }
    if (bestJ !== i) [arr[i], arr[bestJ]] = [arr[bestJ], arr[i]];
    result.push(arr[i]);
    addToWindow(getDims(arr[i]));
  }
  // Stash lens schedule for telemetry/debug
  window._digLensSchedule = _lensLog;

  // Artist-spacing post-pass: same first-billed artist no closer than 15 tracks.
  // Handles Nusrat × 41 and ensemble variants ("Luca Antignani, X, Y" vs "Luca Antignani, Z").
  const ARTIST_SPACING = 15;
  const spaced = [], postponed = [], lastArtistPos = {};
  for (const t of result) {
    const a = (t.artist || '').split(',')[0].trim().toLowerCase();
    const last = lastArtistPos[a] ?? -ARTIST_SPACING;
    if (spaced.length - last >= ARTIST_SPACING) {
      spaced.push(t);
      lastArtistPos[a] = spaced.length - 1;
    } else {
      postponed.push(t);
    }
  }
  return spaced.concat(postponed);
}

// ── Junk filter (frontend safety pass) ──────────────────────────────────────
// Mirror of lib/track_filter.py for tracks that may have slipped through.
// Catches stock-music slop ("Atmospheric Techno", "cleanmindsounds",
// "Relax a Wave", "Para Dormir", "Sitar Music", "Drum & Bass" as artist).
const _STOCK_WORDS = new Set([
  'ambient','atmospheric','cinematic','epic','orchestral','instrumental',
  'electronic','electric','electronica','techno','trance','house','edm',
  'dnb','darkstep','drumstep','liquid','psytrance','breaks','breakbeat',
  'lofi','lo-fi','chill','chillout','chillhop','hiphop','beats','beat',
  'relax','relaxing','relaxation','calm','calming','soothing','tranquil',
  'peaceful','serene','serenity','bliss','blissful','gentle','soft',
  'sleep','sleeping','sleepy','dream','dreamy','dreams','dreamscape',
  'study','studying','focus','focused','concentration','productivity',
  'work','working','meditation','meditative','mindful','mindfulness',
  'healing','wellness','spa','zen','reiki','yoga','therapy','therapeutic',
  'pure','deep','cosmic','celestial','ethereal','astral','galactic',
  'sounds','sound','tones','tone','waves','wave','frequencies','frequency',
  'music','tunes','sonic','vibes','vibe','mood','moods','mode',
  'background','ambience','soundscape','soundscapes','pads','textures',
  'mind','minds','soul','spirit','breath','breathe','inner',
  'energy','positive','motivational','motivation','inspirational',
  'technology','cybernetic','futuristic','digital','cyber',
  'morning','evening','night','midnight','dawn','dusk','sunset','sunrise',
  'rain','ocean','forest','nature','water','thunder','storm',
]);
const _MULTILANG_RELAX = /\b(?:para\s+(?:dormir|estudiar|relajarse|meditar|trabajar)|som\s+(?:de|do|para)\s+chuva|musica\s+para\s+dormir|m[uú]sica\s+para\s+dormir|pour\s+(?:dormir|[ée]tudier)|zum\s+(?:schlafen|lernen|entspannen)|per\s+(?:dormire|studiare|rilassarsi))\b/i;
const _BARE_GENRE_ARTIST = /^(?:drum\s*[&n]?\s*bass|dnb|d\s*&\s*b|darkstep|drumstep|liquid\s+dnb|psytrance|gabber|hardcore|trance|techno|house|edm|dubstep|tech\s+house|deep\s+house|(?:study|focus|sleep|relax|calm|chill|ambient|lofi|lo-?fi|chillhop|lounge)\s+(?:beats?|music|vibes?|sounds?)|(?:erhu|guqin|guzheng|pipa|qin|koto|sitar|tabla|shamisen|gamelan|shakuhachi|kalimba|hang|didgeridoo|kora|oud|saz|hulusi|dizi|haegeum)\s+(?:music|sounds?|tones?))$/i;
const _NON_STOCK_HINT = /\b(?:feat\.?|featuring|presents?|records?|orchestra|quartet|quintet|trio|sextet|dj|mc|the|of|and|von|de|la|el|los|las|du|der|die|das)\b/i;

function _isStockWordPile(text, threshold) {
  if (!text) return false;
  const s = text.trim().toLowerCase();
  // Smashed compound like "cleanmindsounds"
  const tokens = s.split(/[\s\-_&,.]+/).filter(t => t);
  if (tokens.length === 1 && tokens[0].length >= 8) {
    const frags = ['clean','mind','sound','music','sleep','study','focus','chill',
                   'relax','calm','peace','soft','pure','deep','zen','spa','wellness',
                   'ambient','tranquil','serene','soothing','lofi','beats','vibes',
                   'tones','waves'];
    let hits = 0;
    for (const f of frags) if (tokens[0].includes(f)) hits++;
    if (hits >= 2) return true;
  }
  let stockHits = 0;
  for (const t of tokens) if (_STOCK_WORDS.has(t)) stockHits++;
  if (stockHits >= threshold) {
    const articles = new Set(['a','an','the','of','and','&','in','on','for','to','with']);
    let realNonStock = 0;
    for (const t of tokens) {
      if (!_STOCK_WORDS.has(t) && !articles.has(t)) realNonStock++;
    }
    if (realNonStock <= 1 && !_NON_STOCK_HINT.test(text)) return true;
  }
  return false;
}

function isJunkTrack(t) {
  const name = t.name || '';
  const artist = t.artist || '';
  if (_BARE_GENRE_ARTIST.test(artist)) return true;
  if (_MULTILANG_RELAX.test(name) || _MULTILANG_RELAX.test(artist)) return true;
  if (_isStockWordPile(artist, 2)) return true;
  if (_isStockWordPile(name, 3)) return true;
  // Per-collaborator check
  if (artist.includes(',')) {
    for (const part of artist.split(',')) {
      const p = part.trim();
      if (_BARE_GENRE_ARTIST.test(p)) return true;
      if (_isStockWordPile(p, 2)) return true;
    }
  }
  return false;
}

// Playback-source filter for the discovery queue.
//   ?only=bandcamp / ?only=spotify — force one source (testing / override)
//   ?only=both                     — force ALL sources (same as the default)
// DEFAULT EVERYWHERE = all sources. Deliberately NOT platform-dependent, and
// deliberately NOT a user-facing setting: the pool is small enough that
// narrowing it by source makes discovery feel repetitive, which outweighs the
// playback friction below.
//
// Known tradeoff on iPhone/iPad — do not "fix" this by reverting to
// Bandcamp-only: Spotify playback can't live in the browser on iOS, so it
// remote-controls the Spotify app, which iOS suspends when backgrounded →
// every Spotify skip reopens the app (seamless Spotify needs a NATIVE app,
// not a web page). Bandcamp plays in-browser with zero friction. Mobile used
// to default to Bandcamp-only for exactly this reason; that was reversed on
// purpose to widen the pool. Load /?only=bandcamp for the old friction-free
// mobile behaviour.
const _onlyParam = (new URLSearchParams(location.search).get('only') || '').toLowerCase();
const DIG_ONLY_SOURCE = _onlyParam
  ? (_onlyParam === 'both' ? '' : _onlyParam)
  : '';

function buildDiscoveryQueue(disc) {
  const allTracks = [];
  let junkCount = 0;
  for (const [region, tracks] of Object.entries(disc)) {
    if (!Array.isArray(tracks)) continue;  // /discovery returned an error envelope, not a {region: [...]} map
    for (const t of tracks) {
      if (!t || typeof t.id !== 'string' || !t.id) continue;  // skip malformed entries
      const src = (t.source || '').toLowerCase();
      if (src === 'youtube' || String(t.id || '').startsWith('yt:')) continue;
      if (DIG_ONLY_SOURCE && (src || 'spotify') !== DIG_ONLY_SOURCE) continue;
      if (isJunkTrack(t)) { junkCount++; continue; }
      // Attach quality_score for scoring (comes from server's tracks table)
      if (t.quality_score !== undefined) t._quality = t.quality_score;
      // Priority: 1) MusicBrainz origin  2) genre inference  3) discovery bucket
      t.region = t.origin_region || inferRegionFromGenres(t.genres) || region;
      t._genre = extractGenre(t);
      t._energy = t.labels?.energy || 'unknown';
      t._mood = (t.labels?.mood || '').split(' ')[0] || 'unknown';
      t._year = t.year || t.query?.match(/year:(\d{4})/)?.[1] || 'unknown';
      allTracks.push(t);
    }
  }
  if (junkCount) console.log(`[DIG] junk-filter (frontend safety): suppressed ${junkCount} tracks`);
  allTracksPool = allTracks;
  allDiscovery = diversityShuffle(allTracks);
  const heardIds = new Set(history.map(h => h.id));
  while (dIdx < allDiscovery.length && heardIds.has(allDiscovery[dIdx].id)) dIdx++;
  if (dIdx >= allDiscovery.length) dIdx = 0;
  // Warm the first few covers so even the very first play is instant.
  void prefetchAlbumArt(allDiscovery.slice(dIdx, dIdx + 8));
}

function seedTasteSignals(ledger) {
  const trackById = {};
  for (const t of allTracksPool) trackById[t.id] = t;

  // Use the permanent ledger (liked/disliked across all sessions) as the seed.
  // ledger is already fetched by the caller; fall back to session history if absent.
  let liked = [], disliked = [];
  if (ledger && (ledger.liked || ledger.disliked)) {
    liked = ledger.liked || [];
    disliked = ledger.disliked || [];
  } else {
    for (const h of history) {
      if (h.status === 'saved') liked.push({ track: `${h.artist} - ${h.track}` });
      else if (h.status === 'disliked') disliked.push({ track: `${h.artist} - ${h.track}` });
    }
  }

  // Ledger entries use "artist - name" as key; cross-reference with pool by ID or key
  const trackByKey = {};
  for (const t of allTracksPool) {
    const k = `${(t.artist || '').toLowerCase()} - ${(t.name || '').toLowerCase()}`;
    trackByKey[k] = t;
  }

  const seeds = [
    ...liked.map(e => ({ entry: e, action: 'save', strength: 1.0 })),
    ...disliked.map(e => ({ entry: e, action: 'dislike', strength: -0.8 })),
  ];

  // Place seeds at virtual track indices spread before the current session starts (index 0).
  // Older seeds get more decay; most recent get least. Full strength → 20 tracks ago.
  const total = seeds.length;
  for (let i = 0; i < seeds.length; i++) {
    const { entry, action, strength } = seeds[i];
    const trackKey = (entry.track || '').toLowerCase();
    const t = trackById[entry.id] || trackByKey[trackKey];
    if (!t) continue;

    // Spread across virtual indices -total to -1 (all before session start).
    // Most recent liked/disliked are at the end of the arrays → highest virtual index.
    const virtualIndex = -(total - i);

    tasteSignals.push({
      id: t.id,
      genre: (t._genre || '').toLowerCase(),
      genres: (t.genres || []).map(g => g.toLowerCase()),
      region: t.region, energy: t._energy, mood: t._mood,
      action, strength,
      trackIndex: virtualIndex,
    });
  }

  // Keep most recent 100 (prune oldest seeds if we have many)
  if (tasteSignals.length > 100) tasteSignals = tasteSignals.slice(-100);
}

// ── Load discovery first (critical path), then supplementary data in parallel ──

// One dropped connection used to be terminal — the visitor got a red error
// screen and had to know to reload. Retry twice with a backoff first.
// TWO-PHASE LOAD. Cold start used to block on the FULL pool: /discovery
// returns every unheard track (~28k, ~10 MB raw / ~1.9 MB gzipped) and nothing
// could play until all of it had arrived AND been JSON.parsed — 20-30s on a
// phone. Now we fetch a small region-balanced batch, start playing off that,
// then pull the full pool in the background and swap it in.
// Server side: _bootstrap_sample() in server.py.
const DISCOVERY_BOOTSTRAP_N = 800;
// Delay before the full pool starts downloading. The first track still has to
// resolve its audio (Bandcamp resolve / Spotify token+play), and a 1.9 MB
// background fetch racing those requests would re-create the very bandwidth
// contention this change exists to remove.
const DISCOVERY_UPGRADE_DELAY_MS = 3000;

function fetchDiscovery(attempt = 0, limit = 0) {
  const url = limit > 0 ? `/discovery?limit=${limit}` : '/discovery';
  return fetch(url).then(r => {
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  }).catch(e => {
    if (attempt >= 2) throw e;
    const wait = 800 * Math.pow(2, attempt);   // 0.8s, then 1.6s
    clientLog('firstplay', 'discovery load retrying', { attempt: attempt + 1, limit, err: String(e) },
              { transient: true });
    return new Promise(res => setTimeout(res, wait)).then(() => fetchDiscovery(attempt + 1, limit));
  });
}

function _discTrackCount(disc) {
  let n = 0;
  for (const ts of Object.values(disc || {})) if (Array.isArray(ts)) n += ts.length;
  return n;
}

// Swap the bootstrap batch for the full pool WITHOUT disturbing playback.
// allTracksPool is what the stratified picker samples, so upgrading it is the
// whole point. allDiscovery is the play sequence, and dIdx must keep pointing
// at whatever is currently playing — otherwise the UI and the next skip both
// jump to an unrelated track mid-song.
function upgradeDiscoveryQueue(disc) {
  const cur = allDiscovery[dIdx] || null;
  const poolBefore = allTracksPool.length;
  buildDiscoveryQueue(disc);          // rebuilds allTracksPool, allDiscovery, dIdx
  if (cur) {
    const i = allDiscovery.findIndex(t => t.id === cur.id);
    if (i >= 0) {
      dIdx = i;
    } else {
      // The rebuild's already-heard skip-ahead can drop the playing track.
      // Re-insert it so currentTrack() stays valid until the song ends.
      if (dIdx > allDiscovery.length) dIdx = allDiscovery.length;
      allDiscovery.splice(dIdx, 0, cur);
    }
  }
  // Re-seed taste from the ledger: the first pass ran against the 800-track
  // bootstrap and could only match the few liked/disliked tracks that happened
  // to be in it. Seeds carry a negative trackIndex, live signals a
  // non-negative one, so this drops only the stale seeds.
  if (_pendingLedger) {
    tasteSignals = tasteSignals.filter(s => s.trackIndex >= 0);
    seedTasteSignals(_pendingLedger);
  }
  renderFeed();
  clientLog('firstplay', 'pool upgraded to full', {
    poolBefore, poolAfter: allTracksPool.length,
    queueLen: allDiscovery.length, dIdx, keptCurrent: !!cur,
  });
}

fetchDiscovery(0, DISCOVERY_BOOTSTRAP_N).then(disc => {
  DISC = disc;
  buildDiscoveryQueue(disc);
  renderFeed();
  const bootstrapN = _discTrackCount(disc);
  {
    clientLog('firstplay', 'discovery bootstrap loaded', {
      tracks: allDiscovery.length, requested: DISCOVERY_BOOTSTRAP_N,
    });
  }
  // If the server returned fewer than we asked for, this IS the whole pool
  // (small catalogue, or a guest limited to Bandcamp) — skip the second fetch
  // rather than re-downloading the same rows.
  if (bootstrapN >= DISCOVERY_BOOTSTRAP_N) {
    setTimeout(() => {
      fetchDiscovery(0, 0)
        .then(full => { DISC = full; upgradeDiscoveryQueue(full); })
        .catch(e => {
          // Not transient: playback survives, but this session is stuck on 800
          // tracks out of ~28k for as long as it lasts, and the stratified
          // picker's coverage guarantees are computed against that stub pool.
          // It should surface in the worklist — the retries above are the part
          // that genuinely recovers.
          clientLog('firstplay', 'full pool load failed — staying on bootstrap',
                    { err: String(e) });
        });
    }, DISCOVERY_UPGRADE_DELAY_MS);
  }
  // Try to restore session from another device (must happen AFTER discovery loads
  // so allDiscovery is populated and we can splice the restored track in).
  if (typeof _tryRestoreSession === 'function') {
    _tryRestoreSession().then(restored => {
      if (restored) {
        console.log('[DIG session] restored from another device — press play to continue');
      }
      // Auto-play if the user already pressed play while loading.
      if (typeof _tryConsumePendingPlay === 'function') _tryConsumePendingPlay('discovery-loaded');
    });
  } else {
    if (typeof _tryConsumePendingPlay === 'function') _tryConsumePendingPlay('discovery-loaded');
  }
}).catch(e => {
  console.error('[DIG] discovery load failed:', e);
  clientLog('firstplay', 'discovery load FAILED', { err: String(e), afterRetries: 2 });
  document.getElementById('feed').innerHTML = `<p style="color:#f66;padding:20px">Failed to load tracks: ${e}<br><a href="/" style="color:#f66">Reload</a></p>`;
});

// Supplementary data — loads in parallel, doesn't block playback
Promise.all([
  fetch('data.json').then(r => r.json()).catch(() => ({})),
  fetch('/ledger').then(r => r.json()).catch(() => ({ known: [] })),
  fetch('genre_map.json').then(r => r.json()).catch(() => null),
  fetch('track_map.json').then(r => r.json()).catch(() => null),
  fetch('/api/coverage').then(r => r.json()).catch(() => ({ genres: {}, countries: {} })),
]).then(([data, ledger, genreMap, trackMap, coverage]) => {
  DATA = data;
  DATA.known = ledger.known || [];
  // Kept so upgradeDiscoveryQueue() can re-seed taste against the full pool —
  // this first seeding only sees the 800-track bootstrap batch.
  _pendingLedger = ledger;
  GENRE_MAP = genreMap;
  window.TRACK_MAP = trackMap;
  if (coverage && typeof coverage === 'object') {
    userCoverage = { genres: coverage.genres || {}, countries: coverage.countries || {} };
    console.log(`[DIG coverage] loaded — ${Object.keys(userCoverage.genres).length} genres, ${Object.keys(userCoverage.countries).length} countries`);
  }
  seedTasteSignals(ledger);
}).catch(e => console.warn('[DIG] supplementary data failed:', e));

// Sync localStorage history to server
if (history.length > 0) {
  fetch('/history', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(history),
  }).catch(() => {});
}

}); // end loadHistory().then

// Page switching
function switchPage(page) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.getElementById(page + '-page').classList.add('active');
  document.getElementById('tab-player').classList.toggle('active', page === 'player');
  document.getElementById('tab-ledger').classList.toggle('active', page === 'ledger');
  if (page === 'ledger') {
    if (currentView === 'list') renderFeed();
    else if (currentView === 'map') renderMap();
    else if (currentView === 'explore') setTimeout(initExplore, 50);
  }
}
document.getElementById('tab-player').addEventListener('click', () => switchPage('player'));
document.getElementById('tab-ledger').addEventListener('click', () => switchPage('ledger'));

// View toggle
document.getElementById('view-list').addEventListener('click', () => setView('list'));
document.getElementById('view-map').addEventListener('click', () => setView('map'));
document.getElementById('view-explore').addEventListener('click', () => setView('explore'));

// Filter chips
document.querySelectorAll('.chip').forEach(c => {
  c.addEventListener('click', () => {
    document.querySelectorAll('.chip').forEach(x => x.classList.remove('active'));
    c.classList.add('active');
    currentFilter = c.dataset.f;
    _feedLimit = _FEED_PAGE;  // reset pagination when filter changes
    renderFeed();
  });
});
document.getElementById('search').addEventListener('input', () => {
  _feedLimit = _FEED_PAGE;  // reset pagination on new search
  renderFeed();
});


let _playPending = false;


// State snapshot helper — used in firstplay logs to show why we bailed.
function _firstplayState() {
  return {
    discoveryLen: allDiscovery.length,
    playerReady: Player.isReady(),
    spotifyReady: Player.isSpotifyReady ? Player.isSpotifyReady() : null,
    playLock: _playLock,
    playPending: _playPending,
    currentTrack: document.getElementById('player-track')?.textContent,
  };
}

function handlePlay() {
  clientLog('firstplay', 'handlePlay called', {
    ..._firstplayState(),
    userAgent: navigator.userAgent.slice(0, 80),
    touchDevice: 'ontouchstart' in window,
    spotifySDKExists: typeof Spotify !== 'undefined',
    spotifyPlayerExists: !!(Player && Player.spotifyReady),
  });

  // Block if a play operation is already in progress
  if (_playLock) {
    clientLog('firstplay', 'bail: _playLock active');
    return;
  }

  // Tracks haven't loaded yet — remember intent, auto-play when ready
  if (allDiscovery.length === 0) {
    _playPending = true;
    const s = document.getElementById('player-status');
    if (s) s.textContent = 'loading tracks…';
    clientLog('firstplay', 'bail: discovery empty, _playPending=true (will retry on load)');
    return;
  }

  // Player not initialized — kick it off and KEEP the intent around so the
  // SDK ready handler picks it up whenever it eventually fires (was a single
  // 1500ms retry that silently died on slow connections).
  if (!Player.isReady()) {
    _playPending = true;
    Player.init();
    const s = document.getElementById('player-status');
    if (s) s.textContent = 'connecting Spotify…';
    clientLog('firstplay', 'bail: Player not ready, _playPending=true (waiting for SDK ready event)');
    return;
  }

  // No track has been started yet — begin playback.
  // On iOS Connect mode, session sync may have restored a track name into
  // the UI (so player-track ≠ "DIG"), but nothing is ACTUALLY playing.
  // Use _hasPlayedThisSession to distinguish "track name displayed from sync"
  // from "track is genuinely loaded in the player".
  if (!_hasPlayedThisSession || document.getElementById('player-track').textContent === 'DIG') {
    clientLog('firstplay', 'starting first track', { idx: dIdx, name: allDiscovery[dIdx]?.name, hasPlayed: _hasPlayedThisSession });
    playCurrentTrack();
    return;
  }

  // A track was loaded and has played — toggle pause/resume
  clientLog('firstplay', 'toggling play/pause');
  Player.togglePlay();
}

// Consume any pending intent. Called from BOTH "discovery loaded" and
// "Spotify SDK ready" — whichever fires last triggers the actual play.
function _tryConsumePendingPlay(source) {
  if (!_playPending) return;
  if (allDiscovery.length === 0) {
    clientLog('firstplay', `consume[${source}] held — discovery still empty`);
    return;
  }
  if (!Player.isReady()) {
    clientLog('firstplay', `consume[${source}] held — player not ready yet`);
    return;
  }
  _playPending = false;
  const s = document.getElementById('player-status');
  if (s) s.textContent = '';
  clientLog('firstplay', `consume[${source}] firing playCurrentTrack`, _firstplayState());
  playCurrentTrack();
}
// ── Cross-device session sync ───────────────────────────────────────────────
// Active device writes session state every 5s. New device reads it on load
// and resumes seamlessly — same track, same mode, same position.
let _sessionHeartbeat = null;

function _buildSessionState() {
  const t = currentTrack();
  return {
    track: t ? { id: t.id, name: t.name, artist: t.artist, region: t.region || '',
                 source: t.source || 'spotify', genres: t.genres || [],
                 // The cover travels with the track: without it a receiving
                 // device can only paint half the card.
                 albumArt: t.art || _artCache.get(t.id) || null,
                 _genre: t._genre, _energy: t._energy, _mood: t._mood, _year: t._year } : null,
    mode: journeyMode ? 'journey' : (aiMixMode ? 'aimix' : (tailoredMode ? 'tailored' : 'normal')),
    journey_seed: journeyMode ? journeySeed : null,
    dIdx: dIdx,
    paused: !(Player._playing),
    device_id: Player.spotifyDeviceId || null,
  };
}

function _startSessionHeartbeat() {
  if (_sessionHeartbeat) return;
  _sessionHeartbeat = setInterval(() => {
    const state = _buildSessionState();
    if (!state.track) return;
    fetch('/api/session', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(state),
      keepalive: true,
    }).catch(() => {});
  }, 5000);
}


// Restore session from another device on page load
async function _tryRestoreSession() {
  try {
    const r = await fetch('/api/session');
    const data = await r.json();
    if (!data.state || !data.state.track || data.age_seconds > 300) return false; // 5 min window

    const s = data.state;
    console.log(`[DIG session] found active session (${data.age_seconds}s old):`,
                s.track.artist, '—', s.track.name, 'mode:', s.mode);

    // Restore mode
    if (s.mode === 'tailored' && !tailoredMode) {
      document.getElementById('btn-tailored').click();
    } else if (s.mode === 'aimix' && !aiMixMode) {
      document.getElementById('btn-aimix').click();
    } else if (s.mode === 'journey' && s.journey_seed && !journeyMode) {
      startJourney(s.journey_seed);
    }

    // Restore track — splice it into the queue and play
    if (s.track && s.track.id) {
      const restored = {
        ...s.track,
        _aiReason: 'resumed from another device',
        _aiLens: 'sync',
      };
      dIdx = Math.max(0, Math.min(s.dIdx || 0, allDiscovery.length));
      allDiscovery.splice(dIdx, 0, restored);
      if (typeof _syncMobileModes === 'function') _syncMobileModes();
      // Don't auto-play — wait for user to press play (respects audio autoplay policy)
      paintTrackInfo(s.track.name, s.track.artist);
      paintArt(s.track.albumArt || null);   // art and text together — never half a card
      document.getElementById('pc-region').textContent = '↻ synced — press play';
      return true;
    }
  } catch (e) {
    console.warn('[DIG session] restore failed:', e);
  }
  return false;
}

// ── Live session sync: poll for state from other devices ─────────────────────
// If another device (phone) starts playing, update the laptop's UI to reflect it.
let _sessionPollInterval = null;
let _lastSyncedTrack = null;

function _startSessionPoll() {
  if (_sessionPollInterval) return;
  _sessionPollInterval = setInterval(async () => {
    // Once THIS device has played, it OWNS its player — no remote state may
    // repaint it or touch its queue. The old guard was
    // `_hasPlayedThisSession && _sessionHeartbeat`, and the heartbeat stops on
    // every pause: skipping a track cleared it for a moment, the poll slipped
    // through, and the laptop's track title landed on the phone over the phone's
    // own artwork (Praverb art, Sơn Tùng M-TP title). It also spliced the remote
    // track into the live queue, so the next skip walked into it and the audio
    // stopped. Syncing is for a device that has not started yet; after that it
    // is interference.
    if (_hasPlayedThisSession || _sessionHeartbeat) return;
    try {
      const r = await fetch('/api/session');
      const data = await r.json();
      if (!data.state || !data.state.track || data.age_seconds > 300) return; // 5 min window
      const s = data.state;
      const trackId = s.track.id;
      // Only update if different from what we're showing
      if (trackId === _lastSyncedTrack) return;
      _lastSyncedTrack = trackId;
      // Update UI to reflect the remote device's state. The art goes with the
      // text, always: the session payload carries no cover, so painting the
      // title alone leaves the PREVIOUS track's sleeve above a different song —
      // a card that states two tracks at once. The placeholder is honest; a
      // wrong sleeve is not.
      paintTrackInfo(s.track.name || '', s.track.artist || '');
      paintArt(s.track.albumArt || null);
      document.getElementById('pc-region').textContent = '↻ playing on another device';
      // Restore mode if different
      if (s.mode === 'tailored' && !tailoredMode) {
        document.getElementById('btn-tailored').click();
      } else if (s.mode === 'aimix' && !aiMixMode) {
        document.getElementById('btn-aimix').click();
      }
      // Splice the track into queue so pressing play here picks up seamlessly
      if (s.track.id && allDiscovery.length > 0) {
        const restored = { ...s.track, _aiReason: 'synced from another device', _aiLens: 'sync' };
        dIdx = Math.max(0, Math.min(s.dIdx || 0, allDiscovery.length));
        allDiscovery.splice(dIdx, 0, restored);
      }
      console.log(`[DIG session] synced from remote: ${s.track.artist} — ${s.track.name}`);
    } catch (e) {}
  }, 10000);
}

// Start polling after discovery loads
_startSessionPoll();

// Load taste profile from server (covers all historical saves for tailored mode)
if (typeof _loadTasteProfile === 'function') _loadTasteProfile();

document.getElementById('btn-play').addEventListener('click', handlePlay);
document.getElementById('btn-songback').addEventListener('click', () => prevTrack());
document.getElementById('btn-prev').addEventListener('click', () => Player.seekRelative(-15000));
document.getElementById('btn-fwd').addEventListener('click', () => Player.seekRelative(15000));
document.getElementById('btn-skip').addEventListener('click', () => nextTrack(true));
document.getElementById('btn-save').addEventListener('click', saveCurrentTrack);
document.getElementById('btn-nah').addEventListener('click', dislikeCurrentTrack);
document.getElementById('btn-tailored').addEventListener('click', () => {
  tailoredMode = !tailoredMode;
  if (tailoredMode && aiMixMode) toggleAiMix(false); // mutually exclusive
  const btn = document.getElementById('btn-tailored');
  btn.classList.toggle('tailored-active', tailoredMode);
  btn.title = tailoredMode ? 'Tailored mode ON — songs adapt to your taste' : 'Tailored mode OFF — pure exploration';
  document.getElementById('player-status').textContent = tailoredMode ? 'tailored' : '';
  if (typeof _syncMobileModes === 'function') _syncMobileModes();
});

// ── AI Mix mode (Claude Sonnet curates) ─────────────────────────────────────
let aiMixMode = false;
let aiQueue = []; // [{artist, track, reason, lens, search}]
let aiFetching = false;
let _aiPlaysSinceRefresh = 0;
const AI_REFRESH_EVERY = 5;

// Called from playCurrentTrack on every successful AI Mix track start.
// After AI_REFRESH_EVERY plays, fire a fresh AI batch so Claude sees the
// latest engagement signals + recent-query memory.
function _aiNotePlay() {
  _aiPlaysSinceRefresh++;
  if (_aiPlaysSinceRefresh >= AI_REFRESH_EVERY && !aiFetching) {
    _aiPlaysSinceRefresh = 0;
    console.log(`[DIG AI-Mix] ${AI_REFRESH_EVERY} plays elapsed → triggering fresh batch`);
    // Fetch a new batch of recs (n=8 is a good balance of coverage + latency)
    refillAiQueue(8).then(() => _prebufferAi());
  }
}

function toggleAiMix(force) {
  const next = (force == null) ? !aiMixMode : !!force;
  aiMixMode = next;
  if (aiMixMode && tailoredMode) {
    tailoredMode = false;
    document.getElementById('btn-tailored').classList.remove('tailored-active');
  }
  const btn = document.getElementById('btn-aimix');
  btn.classList.toggle('aimix-active', aiMixMode);
  btn.title = aiMixMode ? 'AI Mix ON — Claude curates each next track' : 'AI Mix OFF';
  document.getElementById('player-status').textContent = aiMixMode ? 'AI mix' : '';
  if (typeof _syncMobileModes === 'function') _syncMobileModes();
  if (aiMixMode) {
    aiQueue = [];
    aiReadyBuffer = [];
    _aiPlaysSinceRefresh = 0;

    // 1) INSTANT FALLBACK: splice in 2 random pool tracks so the very first
    //    skip is playable in <100ms while Claude is thinking.
    _seedAiBufferFromPool(2);

    // 2) SMALL FAST BATCH: ask Claude for just 3 recs (~10s vs ~35s for 10).
    refillAiQueue(3).then(() => {
      _prebufferAi();
      // 3) BACKGROUND TOP-UP: deepen the buffer with the full 10-rec batch.
      refillAiQueue(10).then(() => _prebufferAi());
    });
  } else {
    aiReadyBuffer = [];
  }
}

// Pull N random tracks from the pool, slot them straight into aiReadyBuffer
// as "instant fallback while AI thinks" picks. No AI call, no Spotify search.
function _seedAiBufferFromPool(n) {
  if (!allTracksPool || !allTracksPool.length) return;
  // Avoid tracks the user has already heard
  const heardIds = new Set(history.map(h => h.id));
  const pool = allTracksPool.filter(t => !heardIds.has(t.id));
  if (!pool.length) return;
  for (let i = 0; i < n; i++) {
    const pick = pool[Math.floor(Math.random() * pool.length)];
    aiReadyBuffer.push({
      id:     pick.id,
      name:   pick.name,
      artist: pick.artist,
      region: pick.region || '',
      source: 'spotify',
      genres: pick.genres || [],
      _genre: pick._genre || 'unknown',
      _energy: pick._energy || 'unknown',
      _mood:   pick._mood || 'unknown',
      _year:   pick._year || 'unknown',
      _aiReason: 'random pick — AI is preparing its first batch',
      _aiLens:   'instant',
    });
  }
  console.log(`[DIG AI-Mix] seeded ${n} instant-fallback tracks while Claude thinks`);
}

async function refillAiQueue(n) {
  if (aiFetching) return;
  aiFetching = true;
  try {
    // Send the frontend's in-memory history to close the timing race.
    // Server-side user_history is debounced/async; this snapshot is fresher.
    const recentIds = (history || []).slice(0, 200).map(h => h.id).filter(Boolean);
    const recentArtists = (history || []).slice(0, 200).map(h => h.artist).filter(Boolean);
    // Also include any track in the pre-resolved AI buffer/queue so the
    // backend doesn't propose a duplicate of something we're about to play.
    for (const t of (aiReadyBuffer || [])) {
      if (t.id) recentIds.push(t.id);
      if (t.artist) recentArtists.push(t.artist);
    }
    const r = await fetch('/api/ai-recommend', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        n: n || 10,
        recent_ids: recentIds,
        recent_artists: recentArtists,
      }),
    });
    const data = await r.json();
    if (data.error) {
      console.warn('[DIG AI-Mix]', data.error);
      document.getElementById('player-status').textContent = 'AI: ' + data.error.slice(0, 40);
      return;
    }
    aiQueue.push(...(data.recommendations || []));
    console.log(`[DIG AI-Mix] queued ${data.recommendations?.length || 0} recs (n=${n||10})`,
                data.meta);
  } catch (e) {
    console.error('[DIG AI-Mix] fetch failed:', e);
  } finally {
    aiFetching = false;
  }
}

// ── Artist-name matching (for AI rec resolution) ─────────────────────────────
// Spotify search returns title-fuzzy matches by default. We require the result's
// artist to actually match what Claude asked for, otherwise we end up playing
// "What'cha Got Nigga by Big Steve" when Claude wanted "L.A.X — Caro".
function _normArtist(s) {
  return (s || '')
    .toLowerCase()
    .replace(/[\.\,\&\(\)'"\[\]]/g, '')   // strip punctuation: L.A.X → lax
    .replace(/\s+/g, ' ')
    .trim();
}
function _artistMatches(itemArtists, requestedArtist) {
  const req = _normArtist(requestedArtist);
  if (!req) return false;
  for (const a of (itemArtists || [])) {
    const n = _normArtist(a.name);
    if (!n) continue;
    if (n === req) return true;
    // Allow either side to be a substring of the other (handles "L.A.X" vs "L.A.X.",
    // "Mr. Eazi" vs "Mr Eazi", "The XX" vs "XX") — but require ≥3 chars to avoid
    // matching common short tokens.
    if (req.length >= 3 && (n.includes(req) || req.includes(n))) return true;
  }
  return false;
}

// Resolve an AI recommendation to a real Spotify track via search.
// We require an artist-name match — if none of the candidate items list the
// requested artist, we drop the rec rather than play an unrelated track.
//
// FAST PATH: AI Mix v2 (pool-search backed) returns recs that already have a
// real Spotify ID + full metadata from the dig pool. No search needed.
async function resolveAiRecToTrack(rec) {
  // Pool-search hit — recs already have id+name+artist directly from tracks table
  if (rec && rec.id && rec.artist && (rec.name || rec.track)) {
    return {
      id: rec.id,
      name: rec.name || rec.track,
      artist: rec.artist,
      region: rec.region || '',
      source: rec.source || 'spotify',
      genres: rec.genres || [],
      _genre: ((rec.genres || [])[0] || 'unknown'),
      _energy: (rec.labels && rec.labels.energy) || 'unknown',
      _mood: (rec.labels && rec.labels.mood) || 'unknown',
      _year: rec.year || rec.decade || 'unknown',
      _aiReason: rec._aiReason || rec.reason || '',
      _aiLens: rec._aiLens || rec.lens || '',
    };
  }
  try {
    const tk = await fetch('/token').then(r => r.json());
    if (!tk.access_token) return null;

    async function searchAndPick(query) {
      const url = `https://api.spotify.com/v1/search?q=${encodeURIComponent(query)}&type=track&limit=10&market=from_token`;
      const r = await fetch(url, { headers: { Authorization: 'Bearer ' + tk.access_token } });
      const j = await r.json();
      const items = (j.tracks && j.tracks.items) || [];
      // First try: exact artist match
      for (const item of items) {
        if (_artistMatches(item.artists, rec.artist)) return item;
      }
      return null;
    }

    // Attempt 1: structured query (track:"X" artist:"Y")
    let item = await searchAndPick(rec.search);
    // Attempt 2: looser — just "artist track"
    if (!item) item = await searchAndPick(`${rec.artist} ${rec.track}`);
    // Attempt 3: artist-only — Spotify's "artist:" qualifier scopes results
    if (!item) item = await searchAndPick(`artist:"${rec.artist}" ${rec.track}`);

    if (!item) {
      console.log(`[DIG AI-Mix] no artist-matching result for "${rec.artist} — ${rec.track}"`);
      return null;
    }
    return _spotifyItemToTrack(item, rec);
  } catch (e) {
    console.warn('[DIG AI-Mix] resolve failed:', e);
    return null;
  }
}

function _spotifyItemToTrack(item, rec) {
  const a = item.artists && item.artists[0];
  return {
    id: item.id,
    name: item.name,
    artist: (item.artists || []).map(x => x.name).join(', '),
    album: item.album && item.album.name,
    region: rec.lens === 'region' ? '' : '',  // unknown until enriched; UI will show as-is
    source: 'spotify',
    genres: [],
    _genre: 'unknown',
    _energy: 'unknown',
    _mood: 'unknown',
    _year: (item.album && item.album.release_date && item.album.release_date.slice(0, 4)) || 'unknown',
    _aiReason: rec.reason,
    _aiLens: rec.lens,
  };
}

// Pre-resolved buffer — fully searched + matched Spotify tracks ready to play
// instantly. Kept at TARGET_PREBUFFER size so skips have zero latency.
const TARGET_PREBUFFER = 2;
let aiReadyBuffer = [];      // [resolvedTrack, ...]
let aiPrebuffering = false;

async function _prebufferAi() {
  if (aiPrebuffering) return;
  aiPrebuffering = true;
  try {
    while (aiMixMode && aiReadyBuffer.length < TARGET_PREBUFFER) {
      // Make sure we have recs to resolve
      if (aiQueue.length === 0) {
        if (!aiFetching) await refillAiQueue();
        if (aiQueue.length === 0) break;  // refill failed
      }
      const rec = aiQueue.shift();
      const t = await resolveAiRecToTrack(rec);
      if (t) {
        aiReadyBuffer.push(t);
      } else {
        console.log('[DIG AI-Mix] prebuffer: could not resolve', rec.artist, '-', rec.track);
      }
    }
  } finally {
    aiPrebuffering = false;
  }
}

async function pickNextAiTrack() {
  const heardIds = new Set(history.map(h => h.id));
  // Fast path: a track is already resolved and ready. Skip any that the user
  // has already heard (can happen if AI recs overlap with past plays).
  while (aiReadyBuffer.length > 0) {
    const t = aiReadyBuffer.shift();
    if (t && !heardIds.has(t.id)) {
      void _prebufferAi();
      return t;
    }
  }
  // Slow path: nothing pre-buffered. Resolve on demand.
  console.log('[DIG AI-Mix] cold pick (buffer empty)');
  if (aiQueue.length === 0 && !aiFetching) await refillAiQueue();
  while (aiQueue.length) {
    const rec = aiQueue.shift();
    const t = await resolveAiRecToTrack(rec);
    if (t && !heardIds.has(t.id)) {
      void _prebufferAi();
      return t;
    }
    console.log('[DIG AI-Mix] could not resolve:', rec.artist, '-', rec.track);
  }
  if (aiFetching) {
    document.getElementById('player-status').textContent = 'AI thinking…';
    let waits = 0;
    while (aiFetching && waits < 30) { await new Promise(r => setTimeout(r, 200)); waits++; }
    if (aiQueue.length) return await pickNextAiTrack();
  }
  return null;
}

document.getElementById('btn-aimix').addEventListener('click', () => toggleAiMix());

// ── Journey mode (infinite, seeded) ─────────────────────────────────────────
let journeyMode = false;
let journeySeed = null;          // {artist, track, region, year, genres}
let journeyQueue = [];           // pending recs from current block
let journeyHistory = [];         // [{artist, track, engagement}] across all blocks
let journeyBlockIndex = 0;
let journeyFetching = false;
let _preJourneyMode = null;      // mode before journey started (to restore on exit)

function startJourney(seed) {
  if (!seed || !seed.artist || !seed.track) {
    console.warn('[DIG Journey] startJourney: invalid seed', seed);
    return;
  }
  // If already on a journey, re-seed (branch) — no need to stop first
  if (journeyMode) {
    console.log('[DIG Journey] re-seeding (branching) from current journey');
  } else {
    // Save the current mode so we can restore on exit
    _preJourneyMode = aiMixMode ? 'aimix' : (tailoredMode ? 'tailored' : 'normal');
    console.log('[DIG Journey] saving pre-journey mode:', _preJourneyMode);
  }
  // Don't disable other modes — journey overlays them. The underlying mode
  // is remembered and restored when journey stops.
  journeyMode = true;
  journeySeed = seed;
  journeyQueue = [];
  journeyReadyBuffer = [];
  journeyHistory = [];
  journeyBlockIndex = 0;
  document.getElementById('btn-journey').classList.add('journey-active');
  const seedLabel = `${seed.artist} — ${seed.track}`.slice(0, 50);
  document.getElementById('player-status').textContent = `🛫 ${seedLabel}`;
  if (typeof _syncMobileModes === 'function') _syncMobileModes();
  if (typeof _syncJourneyExitButtons === 'function') _syncJourneyExitButtons();
  console.log('[DIG Journey] starting from', seedLabel);
  refillJourneyQueue().then(() => _prebufferJourney());
}

function stopJourney() {
  journeyMode = false;
  journeySeed = null;
  journeyQueue = [];
  journeyReadyBuffer = [];
  journeyHistory = [];
  journeyBlockIndex = 0;
  document.getElementById('btn-journey').classList.remove('journey-active');
  // Restore the mode that was active before the journey
  const restore = _preJourneyMode || 'normal';
  _preJourneyMode = null;
  if (restore === 'aimix' && !aiMixMode) {
    toggleAiMix(true);
  } else if (restore === 'tailored' && !tailoredMode) {
    tailoredMode = true;
    document.getElementById('btn-tailored').classList.add('tailored-active');
    document.getElementById('player-status').textContent = 'tailored';
  } else {
    document.getElementById('player-status').textContent = '';
  }
  if (typeof _syncMobileModes === 'function') _syncMobileModes();
  if (typeof _syncJourneyExitButtons === 'function') _syncJourneyExitButtons();
  console.log('[DIG Journey] stopped, restored mode:', restore);
}

async function refillJourneyQueue() {
  if (journeyFetching || !journeyMode || !journeySeed) return;
  journeyFetching = true;
  try {
    const recentIds = (history || []).slice(0, 200).map(h => h.id).filter(Boolean);
    // Also include any track already queued up to play, so the backend doesn't
    // resolve a duplicate of something we're about to serve.
    for (const t of (journeyReadyBuffer || [])) {
      if (t && t.id) recentIds.push(t.id);
    }
    const r = await fetch('/api/journey', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        seed: journeySeed,
        block_index: journeyBlockIndex,
        previous_journey: journeyHistory,
        n: 8,
        recent_ids: recentIds,
      }),
    });
    const data = await r.json();
    if (data.error) {
      console.warn('[DIG Journey]', data.error);
      document.getElementById('player-status').textContent = '🛫 ' + data.error.slice(0, 40);
      return;
    }
    journeyQueue.push(...(data.recommendations || []));
    journeyBlockIndex++;
    console.log(`[DIG Journey] block ${journeyBlockIndex} loaded (${data.recommendations?.length || 0} tracks)`,
                data.meta);
  } catch (e) {
    console.error('[DIG Journey] fetch failed:', e);
  } finally {
    journeyFetching = false;
  }
}

let journeyReadyBuffer = [];
let journeyPrebuffering = false;

async function _prebufferJourney() {
  if (journeyPrebuffering) return;
  journeyPrebuffering = true;
  try {
    while (journeyMode && journeyReadyBuffer.length < TARGET_PREBUFFER) {
      if (journeyQueue.length === 0) {
        if (!journeyFetching) await refillJourneyQueue();
        if (journeyQueue.length === 0) break;
      }
      const rec = journeyQueue.shift();
      const t = await resolveAiRecToTrack(rec);
      if (t) {
        t._aiLens = `journey · ${rec.arc || 'expand'}`;
        // Track in journey history so the next block knows what was played
        journeyHistory.push({
          artist: t.artist, track: t.name,
          arc: rec.arc || 'expand',
          engagement: 'served',
        });
        journeyReadyBuffer.push(t);
      } else {
        console.log('[DIG Journey] prebuffer: could not resolve', rec.artist, '-', rec.track);
      }
    }
  } finally {
    journeyPrebuffering = false;
  }
}

async function pickNextJourneyTrack() {
  const heardIds = new Set(history.map(h => h.id));
  let heardSkipped = 0, unresolved = 0, bufferedExamined = 0, queuedExamined = 0;
  while (journeyReadyBuffer.length > 0) {
    const t = journeyReadyBuffer.shift();
    bufferedExamined++;
    if (t && !heardIds.has(t.id)) {
      void _prebufferJourney();
      return t;
    }
    if (t) heardSkipped++;
  }
  console.log('[DIG Journey] cold pick (buffer empty)');
  if (journeyQueue.length === 0 && !journeyFetching) await refillJourneyQueue();
  while (journeyQueue.length) {
    const rec = journeyQueue.shift();
    queuedExamined++;
    const t = await resolveAiRecToTrack(rec);
    if (t && !heardIds.has(t.id)) {
      t._aiLens = `journey · ${rec.arc || 'expand'}`;
      journeyHistory.push({
        artist: t.artist, track: t.name,
        arc: rec.arc || 'expand',
        engagement: 'served',
      });
      void _prebufferJourney();
      return t;
    }
    if (t) { heardSkipped++; console.log('[DIG Journey] skipping already-heard:', t.artist, '-', t.name); }
    else   { unresolved++; console.log('[DIG Journey] could not resolve:', rec.artist, '-', rec.track); }
  }
  {
    clientLog('journey', 'pickNext exhausted — returning null', {
      bufferedExamined, queuedExamined, heardSkipped, unresolved,
      historySize: heardIds.size, fetching: journeyFetching,
    });
  }
  if (journeyFetching) {
    // The /api/journey call typically takes 15–25 s (Claude + pool_search
    // resolution). Wait up to 40 s before falling back.
    document.getElementById('player-status').textContent = '🛫 thinking…';
    let waits = 0;
    while (journeyFetching && waits < 200) { await new Promise(r => setTimeout(r, 200)); waits++; }
    if (journeyQueue.length) return await pickNextJourneyTrack();
  }
  return null;
}

// Annotate journey-history entries with engagement signals as the user reacts.
function _journeyMarkEngagement(track, engagement) {
  if (!journeyMode || !track) return;
  // Find the most recent entry matching this track
  for (let i = journeyHistory.length - 1; i >= 0; i--) {
    const h = journeyHistory[i];
    if (h.artist === track.artist && h.track === track.name) {
      h.engagement = engagement;
      break;
    }
  }
}

// 🛫 always starts or re-seeds a journey from the current track.
// ✕ exit button stops the journey and restores the previous mode.
function _journeyFromCurrentTrack() {
  const t = currentTrack();
  if (!t) {
    document.getElementById('player-status').textContent = '🛫 play a track first';
    return;
  }
  startJourney({
    artist: t.artist,
    track: t.name,
    region: t.region || '',
    year:   t._year || t.year || '',
    genres: t.genres || [],
  });
}

// Show/hide exit buttons based on journey state
function _syncJourneyExitButtons() {
  const show = journeyMode;
  const dExit = document.getElementById('btn-journey-exit');
  const mExit = document.getElementById('mc-journey-exit');
  if (dExit) dExit.style.display = show ? '' : 'none';
  if (mExit) mExit.style.display = show ? '' : 'none';
}

document.getElementById('btn-journey').addEventListener('click', _journeyFromCurrentTrack);
document.getElementById('btn-journey-exit').addEventListener('click', () => { stopJourney(); _syncJourneyExitButtons(); });
document.getElementById('mc-journey-exit').addEventListener('click', () => { stopJourney(); _syncJourneyExitButtons(); _syncMobileModes(); });

// Media Session API — AirPods/headphone controls & lock screen widget
// Re-register on each track (helps Safari/iOS attach handlers after first gesture).
function initMediaSessionHandlers() {
  if (!('mediaSession' in navigator)) return;
  const safe = (action, fn) => {
    try { navigator.mediaSession.setActionHandler(action, fn); } catch (e) {}
  };
  safe('play', () => {
    console.log('[DIG media] action: play');
    clientLog('media', 'play (AirPods/lock screen)');
    void Player.resume();
    // Delay sync so the Spotify SDK has time to process the resume
    setTimeout(() => void Player.syncMediaSession(), 500);
  });
  safe('pause', () => {
    console.log('[DIG media] action: pause');
    clientLog('media', 'pause (AirPods/lock screen)');
    void Player.pause();
    // Delay sync so the Spotify SDK has time to process the pause
    setTimeout(() => void Player.syncMediaSession(), 500);
  });
  safe('nexttrack', () => {
    console.log('[DIG media] action: nexttrack');
    clientLog('media', 'nexttrack (AirPods/lock screen) — DIG handler fired', {
      currentDIdx: dIdx, currentTrackId: (currentTrack() || {}).id || null,
      currentTrackName: (currentTrack() || {}).name || null,
    });
    nextTrack(true);
  });
  // Within first ~15s, -15s seek hits 0 → same track "restarts". Use prev queue slot instead.
  safe('previoustrack', () => {
    void (async () => {
      const _t0 = performance.now();
      console.log('[DIG media] action: previoustrack');
      const cur = currentTrack();
      clientLog('media', 'previoustrack (AirPods/lock screen) — DIG handler fired', {
        currentDIdx: dIdx, currentTrackId: cur?.id || null,
        currentTrackName: cur?.name || null,
      });
      const st = await Player.getState();
      const branch = (st && st.duration >= 1000 && st.position > 15000) ? 'seek-15s' : 'prevTrack';
      clientLog('media', `previoustrack: branching → ${branch}`, {
        position: st?.position, duration: st?.duration,
      });
      if (branch === 'seek-15s') {
        await Player.seekRelative(-15000);
      } else {
        prevTrack();
      }
      void Player.syncMediaSession();
      clientLog('media', 'previoustrack: done', {
        branch, totalMs: Math.round(performance.now() - _t0),
      });
    })();
  });
  // Let the browser / Spotify handle skip-in-track if any; our handlers often stole AirPod double-tap
  // and combined with duration=0 races produced seek-to-start instead of skip.
  try { navigator.mediaSession.setActionHandler('seekforward', null); } catch (e) {}
  try { navigator.mediaSession.setActionHandler('seekbackward', null); } catch (e) {}
}
initMediaSessionHandlers();

// The big play button carried onclick="handlePlay()" in the markup, which is
// resolved against the GLOBAL scope at click time and so quietly required
// handlePlay to be a global. Wiring it here says the same thing somewhere the
// module can see, and keeps behaviour out of the markup.
(() => {
  const btn = document.getElementById('big-play-btn');
  if (btn) btn.addEventListener('click', () => handlePlay());
})();

// Mobile controls — direct handlers (not synthetic .click(), iOS needs real user gesture)
function _flashTap(el) {
  if (!el) return;
  el.classList.remove('tapped');
  void el.offsetWidth;  // restart animation
  el.classList.add('tapped');
  setTimeout(() => el.classList.remove('tapped'), 280);
}
document.getElementById('mc-play').addEventListener('click', handlePlay);
document.getElementById('mc-skip').addEventListener('click', (e) => { _flashTap(e.currentTarget); nextTrack(true); });
document.getElementById('mc-prev').addEventListener('click', (e) => { _flashTap(e.currentTarget); Player.seekRelative(-15000); });
document.getElementById('mc-fwd').addEventListener('click', (e) => { _flashTap(e.currentTarget); Player.seekRelative(15000); });
document.getElementById('mc-songback').addEventListener('click', (e) => { _flashTap(e.currentTarget); prevTrack(); });
document.getElementById('mc-save').addEventListener('click', saveCurrentTrack);
document.getElementById('mc-nah').addEventListener('click', dislikeCurrentTrack);

// Mobile draggable progress bar (tap-to-seek and drag-to-seek)
(function () {
  const bar = document.getElementById('mc-progress');
  if (!bar) return;
  const fill = document.getElementById('mc-progress-fill');
  let dragging = false;

  function _ratioFromEvent(e) {
    const rect = bar.getBoundingClientRect();
    const x = (e.touches ? e.touches[0].clientX : e.clientX) - rect.left;
    return Math.max(0, Math.min(1, x / rect.width));
  }
  function _previewSeek(e) {
    const r = _ratioFromEvent(e);
    fill.style.width = (r * 100) + '%';
  }
  async function _commitSeek(e) {
    const r = _ratioFromEvent(e);
    if (typeof Player === 'undefined' || !Player.getState) return;
    const st = await Player.getState();
    const dur = st && st.duration;
    if (!dur || !isFinite(dur)) return;
    Player.seekTo(Math.floor(r * dur));
  }

  bar.addEventListener('pointerdown', (e) => {
    dragging = true; bar.classList.add('dragging');
    bar.setPointerCapture && bar.setPointerCapture(e.pointerId);
    _previewSeek(e);
    e.preventDefault();
  });
  bar.addEventListener('pointermove', (e) => { if (dragging) _previewSeek(e); });
  bar.addEventListener('pointerup', (e) => {
    if (!dragging) return;
    dragging = false; bar.classList.remove('dragging');
    _commitSeek(e);
  });
  bar.addEventListener('pointercancel', () => { dragging = false; bar.classList.remove('dragging'); });
})();

// Mobile mode buttons — sync state with the desktop topbar buttons
function _syncMobileModes() {
  const mcT = document.getElementById('mc-tailored');
  const mcA = document.getElementById('mc-aimix');
  const mcJ = document.getElementById('mc-journey');
  if (mcT) mcT.classList.toggle('mc-mode-active', tailoredMode);
  if (mcT) mcT.classList.toggle('mc-mode-tailored', true);
  if (mcA) mcA.classList.toggle('mc-mode-active', aiMixMode);
  if (mcA) mcA.classList.toggle('mc-mode-aimix', true);
  if (mcJ) mcJ.classList.toggle('mc-mode-active', journeyMode);
  if (mcJ) mcJ.classList.toggle('mc-mode-journey', true);
}
document.getElementById('mc-tailored').addEventListener('click', () => {
  document.getElementById('btn-tailored').click(); // reuse desktop logic
  _syncMobileModes();
});
document.getElementById('mc-aimix').addEventListener('click', () => {
  document.getElementById('btn-aimix').click();
  _syncMobileModes();
});
document.getElementById('mc-journey').addEventListener('click', () => {
  document.getElementById('btn-journey').click();
  _syncMobileModes();
});
// Sync on page load
_syncMobileModes();

// Progress bar click to seek
document.getElementById('player-progress').addEventListener('click', async (e) => {
  const state = await Player.getState();
  if (!state) return;
  const pct = (e.clientX - e.currentTarget.getBoundingClientRect().left) / e.currentTarget.offsetWidth;
  await Player.seekTo(Math.floor(pct * state.duration));
});

// Wire up auto-next on track end
Player.onTrackEnd(() => nextTrack(false));

// ===== KEYBOARD =====
document.addEventListener('keydown', e => {
  if (e.target.tagName === 'INPUT') return;

  if ((e.metaKey || e.ctrlKey) && e.key === 'ArrowRight') { e.preventDefault(); nextTrack(true); return; }
  if ((e.metaKey || e.ctrlKey) && e.key === 'ArrowLeft') { e.preventDefault(); prevTrack(); return; }
  if (e.key === 'ArrowRight') { e.preventDefault(); Player.seekRelative(15000); return; }
  if (e.key === 'ArrowLeft') { e.preventDefault(); Player.seekRelative(-15000); return; }
  if (e.key === ' ') {
    e.preventDefault();
    document.getElementById('btn-play').click();
    return;
  }
  if (e.key === 'ArrowUp') { e.preventDefault(); saveCurrentTrack(); return; }
  if (e.key === 'ArrowDown') { e.preventDefault(); dislikeCurrentTrack(); return; }
  if (e.key === 's') saveCurrentTrack();
});
