/**
 * The Map view: how much of the world's music this listener has actually
 * touched, per region / genre / decade, against a world-wide baseline.
 *
 * A leaf. It reads three things it does not own — the pool, the play history,
 * and whether its own tab is showing — and it reads them through `wire()`
 * rather than by reaching, so the app can import this without this importing
 * the app back. The pattern is the one Player.wire() established: name the
 * question, not the variable.
 */
import { clientLog } from './log.js';

const app = {
  /** The pool payload: regions, tags, world_density, per-user counts. */
  data: () => null,
  /** Every play, for the "explored" side of each bar. */
  history: () => [],
  /** Is the Map tab the one on screen? Rendering otherwise is wasted work. */
  isVisible: () => false,
};

export function wireMap(impl) {
  Object.assign(app, impl || {});
}

// Which axis the map is sliced on. Owned here because only this view has a
// control for it — it lived in the app's shared state block for no reason
// beyond that being where state went.
let showWorldContext = true;

let mapDimension = 'region'; // 'region', 'genre', 'year'

function gatherMapData() {
  const userCount = {};
  const savedCount = {};

  // Session history
  for (const h of app.history()) {
    let keys = [];
    if (mapDimension === 'region') keys = [h.region || 'Unknown'];
    else if (mapDimension === 'genre') {
      // Try to find tags from spotify_tracks
      const st = (app.data().spotify_tracks || []).find(t => t.id === h.id);
      keys = st ? st.tags.slice(0, 3) : ['unknown'];
      if (!keys.length) keys = ['unknown'];
    } else if (mapDimension === 'year') {
      const st = (app.data().spotify_tracks || []).find(t => t.id === h.id);
      keys = [st && st.added ? st.added.slice(0, 4) + 's' : 'unknown'];
    }
    for (const k of keys) {
      userCount[k] = (userCount[k] || 0) + 1;
      if (h.status === 'saved') savedCount[k] = (savedCount[k] || 0) + 1;
    }
  }

  // From data.json
  if (mapDimension === 'region' && app.data().user_region_count) {
    for (const [r, c] of Object.entries(app.data().user_region_count))
      userCount[r] = (userCount[r] || 0) + c;
  }
  if (mapDimension === 'year' && app.data().user_decade_count) {
    for (const [d, c] of Object.entries(app.data().user_decade_count))
      userCount[d] = (userCount[d] || 0) + c;
  }
  if (mapDimension === 'genre' && app.data().tags) {
    for (const [tag, c] of app.data().tags)
      userCount[tag] = (userCount[tag] || 0) + c;
  }

  return { userCount, savedCount };
}

function getWorldEstimates() {
  if (mapDimension === 'region') return app.data().world_density || {};
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
  if (mapDimension === 'region') return app.data().regions || [];
  if (mapDimension === 'year') return ['1920s','1930s','1940s','1950s','1960s','1970s','1980s','1990s','2000s','2010s','2020s'];
  if (mapDimension === 'genre') {
    // Return top tags from data + any from session
    const all = new Set((app.data().tags || []).map(t => t[0]));
    return [...all].slice(0, 30);
  }
  return [];
}

function renderMap() {
  if (!app.data() || !app.isVisible()) return;
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

export { renderMap, inferRegionFromGenres, gatherMapData, getWorldEstimates };
