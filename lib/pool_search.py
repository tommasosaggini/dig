"""
DIG — Weighted dimensional pool search.

Given a multi-dimensional query (regions, genres, decades, vibe keywords) and a
set of per-dimension weights, score the live `tracks` table and return the
best-matching real tracks.

This is the engine behind AI Mix's "Claude proposes shapes, the pool fills them"
architecture — Claude never names specific artists, so famous-artist bias from
its training data doesn't enter the recommendation set.
"""
from __future__ import annotations

import random
from typing import Iterable

from lib.db import fetchall
from lib.track_key import normalize as _norm_key


VIBE_FIELDS = ("label_energy", "label_mood", "label_texture", "label_feel", "label_use_case")


def search(query: dict,
           weights: dict | None = None,
           exclude_ids: Iterable[str] | None = None,
           exclude_keys: Iterable[str] | None = None,
           exclude_artists: Iterable[str] | None = None,
           limit: int = 1,
           min_score: float = 0.15,
           candidate_cap: int = 3000) -> list:
    """Return up to `limit` tracks ranked by weighted-dimension match.

    query keys:
      regions:        list[str]  e.g. ["Laos","Thailand","Vietnam"]  (exact match on track.region)
      genres:         list[str]  e.g. ["mor lam","luk thung"]        (fuzzy substring on track.genres array)
      decades:        list[str]  e.g. ["1970s","1980s"]              (exact match on track.decade)
      vibe_keywords:  list[str]  e.g. ["modal","drone","trance"]     (substring on label_* fields)

    weights default to equal across whichever dimensions are populated in `query`.
    """
    weights = weights or {}
    exclude_ids = set(exclude_ids or [])
    exclude_keys = set(exclude_keys or [])
    # Artist names normalized to "first comma split lowercased" for cooldown matching
    exclude_artists = set(
        (a or "").split(",")[0].strip().lower()
        for a in (exclude_artists or [])
        if a
    )

    regions = list(query.get("regions") or [])
    genres = [g.lower() for g in (query.get("genres") or []) if g]
    decades = list(query.get("decades") or [])
    vibe_kws = [v.lower() for v in (query.get("vibe_keywords") or []) if v]

    # Auto-balance weights across whichever dims are actually queried.
    dims_present = {
        "region": bool(regions),
        "genre": bool(genres),
        "decade": bool(decades),
        "vibe": bool(vibe_kws),
    }
    n_present = sum(dims_present.values())
    if n_present == 0:
        return []
    default_w = 1.0 / n_present
    w = {
        "region": float(weights.get("region", default_w if dims_present["region"] else 0)),
        "genre":  float(weights.get("genre",  default_w if dims_present["genre"]  else 0)),
        "decade": float(weights.get("decade", default_w if dims_present["decade"] else 0)),
        "vibe":   float(weights.get("vibe",   default_w if dims_present["vibe"]   else 0)),
    }

    # ── SQL pre-filter ────────────────────────────────────────────────────────
    # Pull anything matching at least one dim (UNION-style OR), then score in Python.
    or_clauses, params = [], []
    if regions:
        or_clauses.append("region = ANY(%s)")
        params.append(regions)
    if genres:
        # Any genre in the track's genres array contains any query genre keyword
        like_parts = []
        for g in genres:
            like_parts.append("EXISTS (SELECT 1 FROM unnest(genres) AS gx WHERE LOWER(gx) LIKE %s)")
            params.append(f"%{g}%")
        or_clauses.append("(" + " OR ".join(like_parts) + ")")
    if decades:
        or_clauses.append("decade = ANY(%s)")
        params.append(decades)
    if vibe_kws:
        # Match any vibe keyword against any of the label fields
        like_parts = []
        for kw in vibe_kws:
            for f in VIBE_FIELDS:
                like_parts.append(f"LOWER(COALESCE({f}, '')) LIKE %s")
                params.append(f"%{kw}%")
        or_clauses.append("(" + " OR ".join(like_parts) + ")")

    if not or_clauses:
        return []

    sql = (
        "SELECT id, name, artist, region, decade, year, genres, "
        "label_energy, label_mood, label_texture, label_feel, label_use_case, source "
        "FROM tracks "
        "WHERE (source != 'youtube' OR source IS NULL) "
        f"AND ({' OR '.join(or_clauses)}) "
        f"LIMIT {int(candidate_cap)}"
    )
    candidates = fetchall(sql, tuple(params))

    # ── Score in Python ──────────────────────────────────────────────────────
    scored = []
    for c in candidates:
        if c["id"] in exclude_ids:
            continue
        if _norm_key(c["artist"], c["name"]) in exclude_keys:
            continue
        # Artist-level cooldown — skip any artist heard recently
        if exclude_artists:
            primary = (c.get("artist") or "").split(",")[0].strip().lower()
            if primary and primary in exclude_artists:
                continue

        s = 0.0
        # Region: full credit if the track region is in the query list
        if regions and c.get("region") in regions:
            s += w["region"]
        # Genre: partial credit for each query genre that fuzzy-matches a track genre
        if genres and c.get("genres"):
            track_genres = [(g or "").lower() for g in (c["genres"] or [])]
            hits = 0
            for qg in genres:
                if any(qg in tg or tg in qg for tg in track_genres):
                    hits += 1
            if hits:
                s += w["genre"] * min(1.0, hits / max(len(genres), 1) * 1.5)
        # Decade: full credit on exact match
        if decades and c.get("decade") in decades:
            s += w["decade"]
        # Vibe: count substring hits across all label fields
        if vibe_kws:
            track_vibe = " ".join([(c.get(f) or "") for f in VIBE_FIELDS]).lower()
            if track_vibe:
                hits = sum(1 for kw in vibe_kws if kw in track_vibe)
                if hits:
                    s += w["vibe"] * min(1.0, hits / max(len(vibe_kws), 1) * 1.5)

        # Require multi-dimensional agreement — single-dim matches are noisy.
        # Specifically: vibe-only matches surface false positives like a Tuareg
        # blues track for a "hard techno" query (because label_texture="raw" matched).
        # Score must hit at least 50% of the SUM of provided weights, AND when
        # the query had genre/region populated, genre or region must contribute.
        min_combined = 0.5 * (w["region"] + w["genre"] + w["decade"] + w["vibe"])
        weighted_floor = max(min_score, min_combined)
        if s < weighted_floor:
            continue
        # Anti-vibe-only guard: if the user asked for genres or region, we
        # require genre OR region to contribute a positive amount.
        if (genres or regions):
            region_hit = bool(regions and c.get("region") in regions)
            genre_hit = False
            if genres and c.get("genres"):
                track_genres = [(g or "").lower() for g in (c["genres"] or [])]
                genre_hit = any(qg in tg or tg in qg for qg in genres for tg in track_genres)
            if not (region_hit or genre_hit):
                continue
        scored.append((s, c))

    if not scored:
        return []

    # Sort by score desc, then add slight randomness within the top tier so
    # repeated queries with the same shape don't always return the same track.
    scored.sort(key=lambda x: -x[0])
    top = scored[:max(limit, 5)]
    random.shuffle(top)
    top.sort(key=lambda x: -x[0])  # stable resort, but ties scrambled
    return top[:limit]


def query_to_track(query: dict, weights: dict, exclude_ids, exclude_keys,
                   exclude_artists=None) -> dict | None:
    """Convenience: search and return the single best track in API-friendly form."""
    results = search(query, weights,
                     exclude_ids=exclude_ids, exclude_keys=exclude_keys,
                     exclude_artists=exclude_artists, limit=1)
    if not results:
        return None
    score, t = results[0]
    return {
        "id":     t["id"],
        "name":   t["name"],
        "artist": t["artist"],
        "region": t.get("region") or "",
        "decade": t.get("decade") or "",
        "year":   t.get("year") or "",
        "genres": t.get("genres") or [],
        "labels": {
            "energy":   t.get("label_energy"),
            "mood":     t.get("label_mood"),
            "texture":  t.get("label_texture"),
            "feel":     t.get("label_feel"),
            "use_case": t.get("label_use_case"),
        },
        "source": t.get("source") or "spotify",
        "_pool_score": round(score, 3),
    }
