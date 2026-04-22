"""
DIG — Coverage-driven exploration recommender.

Replaces Claude-anchored AI Mix. No taste anchoring, no shape queries — the
algorithm's only job is to surface music from places the user has NEVER been.

Mechanism:
  • Pull user's lifetime heard-artist set (from user_history)
  • Compute per-cell exposure: (region, decade) cells the user has been served from
  • Find UNHEARD artists, prioritize cells with ZERO or LOW exposure
  • Sample N tracks: spread across regions, decades, and (where possible) genres
  • Return real Spotify-resolved tracks with a short "why this is new for you" reason

Zero AI calls. Pure SQL. Fast.
"""
from __future__ import annotations

import random
from collections import Counter, defaultdict

from lib.db import fetchall


def _heard_artists(user_id: str) -> set:
    """All lowercased artist names (including collaborators) the user has been served.
    Splits on comma so 'A, B, C' adds all three to the set."""
    rows = fetchall(
        "SELECT DISTINCT artist FROM user_history WHERE user_id = %s",
        (user_id,),
    )
    out = set()
    for r in rows:
        for part in (r["artist"] or "").split(","):
            a = part.strip().lower()
            if a:
                out.add(a)
    return out


def _heard_track_ids(user_id: str) -> set:
    """Spotify IDs the user has been served (for hard track-level exclusion)."""
    rows = fetchall(
        "SELECT DISTINCT track_id FROM user_history WHERE user_id = %s AND track_id IS NOT NULL",
        (user_id,),
    )
    return {r["track_id"] for r in rows if r["track_id"]}


def _cell_exposure(user_id: str) -> Counter:
    """Per-(region, genre, decade) cell exposure. Uses origin_region (MusicBrainz)
    when set, and the first genre tag as the genre dimension. Tracks without
    any genre tag contribute to a (region, '__nogenre', decade) cell."""
    rows = fetchall(
        """
        SELECT COALESCE(NULLIF(t.origin_region, ''), t.region) AS region,
               COALESCE(t.genres[1], '__nogenre') AS genre,
               t.decade, COUNT(*) AS n
        FROM user_history h
        JOIN tracks t ON t.id = h.track_id
        WHERE h.user_id = %s AND COALESCE(t.origin_region, t.region) IS NOT NULL
        GROUP BY 1, 2, 3
        """,
        (user_id,),
    )
    return Counter({(r["region"], r["genre"], r["decade"]): r["n"] for r in rows})


def coverage_explore(user_id: str, n: int = 10) -> dict:
    """Return n tracks the user has never heard, spread across cells they have
    no/low exposure to. Prioritizes new ARTISTS (never heard) over new tracks
    by familiar artists.
    """
    heard_artists = _heard_artists(user_id)
    heard_ids = _heard_track_ids(user_id)
    cell_counts = _cell_exposure(user_id)

    # Trust origin_region (MusicBrainz) over the discovery-pipeline's region
    # whenever it's set — MusicBrainz is the authoritative source. We DON'T
    # require origin_region to string-match region (e.g. "Ukraine" vs
    # "Eastern Europe" are different taxonomies but the same place). If
    # origin_region is set, it just becomes the effective region.
    #
    # Tracks where origin_region is NULL fall back to the dirty region tag
    # and may be wrong — the audit pass will eventually clean those up.
    rows = fetchall(
        """
        SELECT id, name, artist,
               COALESCE(NULLIF(origin_region, ''), region) AS region,
               origin_region,
               decade, year, genres,
               label_energy, label_mood, label_texture, label_feel, label_use_case
        FROM tracks
        WHERE COALESCE(NULLIF(origin_region, ''), region) IS NOT NULL
          AND COALESCE(NULLIF(origin_region, ''), region) != ''
          AND (source != 'youtube' OR source IS NULL)
          AND name IS NOT NULL AND artist IS NOT NULL
        """
    )

    # Bucket candidates by (region, genre, decade) cell.
    # Genre dimension uses the first genre tag; tracks without genres fall into
    # a '__nogenre' bucket (which is less targeted but still participates).
    def all_artists(a):
        """Return set of all lowercased artist names in a collaboration string."""
        return {p.strip().lower() for p in (a or "").split(",") if p.strip()}

    def any_heard(artist_field):
        """True if ANY artist in the collaboration has been heard."""
        for a in all_artists(artist_field):
            if a in heard_artists:
                return True
        return False

    def cell_key(r):
        genre = (r.get("genres") or [None])[0] or "__nogenre"
        return (r["region"], genre, r.get("decade"))

    # Build per-artist pool concentration counts for inverse weighting.
    # An artist with 20 tracks should be 20× LESS likely to be randomly
    # picked than an artist with 1 track — otherwise concentrated artists
    # dominate their cells purely by volume.
    artist_pool_count = Counter()
    for r in rows:
        for a in all_artists(r.get("artist")):
            artist_pool_count[a] += 1

    by_cell = defaultdict(list)
    for r in rows:
        if r["id"] in heard_ids:
            continue
        if any_heard(r["artist"]):
            continue
        by_cell[cell_key(r)].append(r)

    if not by_cell:
        # User has heard every artist + track. Fall back: just unheard tracks regardless of artist.
        unheard = [r for r in rows if r["id"] not in heard_ids]
        return _pack(_random_spread(unheard, n), "exhausted",
                     reason_template="no untouched cells left — sampling fresh tracks across the pool")

    # Order cells: zero-exposure first, then lowest exposure ascending.
    # Within same exposure, randomize so we don't always pick same cell.
    cells_ordered = sorted(by_cell.keys(), key=lambda c: (cell_counts.get(c, 0), random.random()))

    # Spread across regions, genres, and decades so no single dimension dominates.
    picks = []
    used_regions = Counter()
    used_genres = Counter()
    used_decades = Counter()
    used_artists = set()
    MAX_PER_REGION = max(1, n // 4)   # at most ~25% of batch from one region
    MAX_PER_GENRE = max(1, n // 3)    # at most ~33% from one genre
    MAX_PER_DECADE = max(1, n // 3)

    def _weighted_pick(candidates):
        """Pick a track weighted INVERSELY by artist pool concentration.
        An artist with 20 tracks in the pool gets weight 1/20; a unique
        artist gets weight 1/1. This naturally de-emphasizes concentrated
        artists without hard caps — statistically fair."""
        if not candidates:
            return None
        weights = []
        for t in candidates:
            # Use the MAXIMUM concentration across all credited artists
            max_conc = max((artist_pool_count.get(a, 1) for a in all_artists(t.get("artist"))), default=1)
            weights.append(1.0 / max(max_conc, 1))
        total = sum(weights)
        if total == 0:
            return random.choice(candidates)
        r = random.random() * total
        cumulative = 0
        for t, w in zip(candidates, weights):
            cumulative += w
            if r <= cumulative:
                return t
        return candidates[-1]

    # Pass 1: strict spread (different region + genre + decade where possible)
    for cell in cells_ordered:
        if len(picks) >= n:
            break
        region, genre, decade = cell
        if used_regions[region] >= MAX_PER_REGION:
            continue
        if genre != "__nogenre" and used_genres[genre] >= MAX_PER_GENRE:
            continue
        if used_decades[decade] >= MAX_PER_DECADE:
            continue
        # Filter to artists not yet used in this batch
        candidates = [t for t in by_cell[cell] if not (all_artists(t["artist"]) & used_artists)]
        if not candidates:
            candidates = by_cell[cell]
        track = _weighted_pick(candidates)
        if not track:
            continue
        picks.append((track, cell, cell_counts.get(cell, 0)))
        used_regions[region] += 1
        if genre != "__nogenre":
            used_genres[genre] += 1
        used_decades[decade] += 1
        used_artists.update(all_artists(track["artist"]))

    # Pass 2: relax caps if we don't have n picks yet
    if len(picks) < n:
        for cell in cells_ordered:
            if len(picks) >= n:
                break
            region, genre, decade = cell
            candidates = [t for t in by_cell[cell] if not (all_artists(t["artist"]) & used_artists)]
            if not candidates:
                candidates = by_cell[cell]
            track = _weighted_pick(candidates)
            if not track:
                continue
            picks.append((track, cell, cell_counts.get(cell, 0)))
            used_artists.update(all_artists(track["artist"]))

    if not picks:
        return {"recommendations": [], "meta": {"strategy": "coverage", "no_picks": True}}

    return _pack(picks, "coverage")


def _pack(picks, strategy: str, reason_template: str | None = None) -> dict:
    """Convert (track_row, cell, exposure) tuples into the API shape AI Mix expects."""
    recs = []
    for item in picks:
        if isinstance(item, tuple):
            track, cell, exposure = item
        else:
            track, cell, exposure = item, (item.get("region"), "__nogenre", item.get("decade")), 0
        region = track.get("region") or "?"
        genre_tag = ((track.get("genres") or [])[0:1] or [""])[0]
        decade = track.get("decade") or "?"
        cell_desc = f"{region}/{genre_tag or 'untagged'}/{decade}"
        if reason_template:
            reason = reason_template
        elif exposure == 0:
            reason = f"first ever from {cell_desc}"
        else:
            reason = f"new artist from {cell_desc} ({exposure} heard from this cell)"
        recs.append({
            "id":     track["id"],
            "artist": track["artist"],
            "name":   track["name"],
            "region": region,
            "decade": decade,
            "year":   track.get("year"),
            "genres": track.get("genres") or [],
            "labels": {
                "energy":   track.get("label_energy"),
                "mood":     track.get("label_mood"),
                "texture":  track.get("label_texture"),
                "feel":     track.get("label_feel"),
                "use_case": track.get("label_use_case"),
            },
            "source":    "spotify",
            "_aiReason": reason,
            "_aiLens":   "explore",
        })
    return {
        "recommendations": recs,
        "meta": {
            "strategy": strategy,
            "n_returned": len(recs),
            "elapsed_sec": 0.0,  # dominated by SQL roundtrip
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_tokens": 0,
        },
    }


def _random_spread(rows: list, n: int) -> list:
    """Random sample with light region-spread bias."""
    if not rows:
        return []
    by_region = defaultdict(list)
    for r in rows:
        by_region[r.get("region") or "?"].append(r)
    region_keys = list(by_region.keys())
    random.shuffle(region_keys)
    out = []
    while len(out) < n and region_keys:
        for rk in list(region_keys):
            if not by_region[rk]:
                region_keys.remove(rk); continue
            out.append(by_region[rk].pop(random.randint(0, len(by_region[rk]) - 1)))
            if len(out) >= n:
                break
    return [(r, (r.get("region"), r.get("decade")), 0) for r in out]
