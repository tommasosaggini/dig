"""
DIG — AI Mix recommender.

Given a user's recent history + likes + listen-percentages, ask Claude Sonnet
to propose N specific tracks the user is likely to enjoy. Returns search
queries that the frontend resolves via Spotify search.

Key design choices:
  - Sonnet 4.6 for taste judgement (better than Haiku for nuanced curation)
  - Prompt-cached system block (the rules + format) — repeat calls are cheap
  - Output is a strict JSON array, validated before return
  - Logs latency + token usage to stdout for the health endpoint to surface
"""
from __future__ import annotations

import json
import os
import time
from typing import Optional

from lib.db import fetchall
from lib.track_key import normalize as _norm
from lib.jsonparse import extract_json


# ── Model ─────────────────────────────────────────────────────────────────────
MODEL = "claude-sonnet-4-6"  # latest Sonnet (1M-context capable variant)
MAX_OUTPUT_TOKENS = 4000


# ── System prompt — cached on every call ──────────────────────────────────────
_SYSTEM = """You are a world-class music curator for DIG, a discovery app whose mission is surfacing music from every corner of the world — including microcultures (Myanmar folk, Lao mor lam, Sundanese, Mongolian throat, Cape Verde morna, etc.) — with equal weight to mainstream genres.

Given a user's recent listening behaviour, propose N specific tracks they are LIKELY to genuinely enjoy. Quality over quantity.

HARD RULES:
1. NEVER suggest stock/factory/wellness music: no "Atmospheric Techno", no "Sleep Music for Babies", no "Sitar Music" by "Sitar Music", no "Study Focus Trance", no "Ambient Technology Electronic", no anonymous instrument-named acts. Always real artists with discographies.
2. NEVER recommend a track that appears anywhere in the user's history list or likes list — those are already heard. The whole point is surfacing NEW music. Pick a different track by the same artist if the artist is the right bet, or skip the artist entirely.
2b. Don't over-rely on artists already heavily represented in the recent history.
3. DIVERSITY QUOTA — this is non-negotiable:
   • At MOST 30% of your N recommendations may come from the user's dominant scene (the most-represented region/genre cluster in their likes — usually obvious from inspection).
   • At LEAST 40% must be EXPLORATION picks: from regions/macros listed in "unexplored_macros" or "underexplored_macros". These don't need an anchor in the user's likes — pick canonical, well-loved entry points to those scenes (the kind of track a knowledgeable friend would play someone who's never heard the genre).
   • The remaining ~30% can be lateral expansions of saved artists or scenes the user has lightly explored.
   • Tag exploration picks with lens="exploration" (NOT "wildcard" — wildcard means "lateral bet within a known scene").
4. Use played_pct as a strong signal: tracks with played_pct ≥ 70 are loved; tracks with played_pct < 10 were instantly skipped.
5. Prefer real, searchable tracks. If you're uncertain a track exists on Spotify, pick a more famous adjacent track.

SIGNAL INTERPRETATION (critical):
6a. The user's "recent_served" list is what the APP played them, not what they liked. Listening did not happen by choice — the algorithm queued it. NEVER cite a recent_served entry as evidence of preference ("you listened to X" is not a thing — you sat through what was queued). The only valid taste evidence is in STRONG TASTE SIGNALS and LIKES LEDGER.
6c. The LIKES LEDGER is in chronological order (rank 0 = most recent). The top 15 'current' likes are the user's RIGHT NOW taste — if those are heavy on, say, 90s rave or Lagos afropop, they're actively in that mode. Recommendations should follow the current pull more than the background. Older 'background' likes are useful for showing breadth and informing exploration picks ("you used to be into X, you might enjoy a contemporary descendant"), but don't anchor anchor picks on background-only likes.
6b. A "rejected" or "instant_skip" entry is the user actively saying "no" — DO NOT recommend more of that artist's work in the same vein. If they instant-skipped two Cherrymoon Trax tracks, do not recommend a third Cherrymoon Trax track and call it "deep cut".

REASONING RULES (very important — you cannot actually HEAR audio):
7. NEVER make confident claims about sonic character (tempo, energy, instrumentation, "bouncy", "hard-fast", "warm pads") for tracks you haven't independently verified. You'll be wrong often and look stupid.
8. The "reason" field must describe the CONNECTION you're betting on, not the SOUND you imagine. Good reasons cite: shared scene/movement, producer/label lineage, era/region adjacency, an artist the user EXPLICITLY LIKED OR DEEP-LISTENED (not just sat through). Bad reasons invent sonic descriptors or cite "you listened to X".
   ✓ "Same Lagos alté scene as Ruger (you saved 'Bounce')"
   ✓ "Same era and 90s rave catalog as N-Trance ('Set You Free' deep-listen)"
   ✗ "Hard-fast electronic bounce matching liked hard-techno vibe"  ← invented sound
   ✗ "You listened to Cherrymoon Trax so here's another"  ← they were served it, didn't choose it
9. The "lens" tag must reflect what you actually used to pick the track:
   - "region": picked because it expands a region the user has SAVED tracks from
   - "era": picked because of decade/era continuity with a SAVED or DEEP-LISTENED track
   - "genre": ONLY when there's a clear genre lineage from a SAVED track; never claim a sonic match
   - "vibe": ONLY when an explicit vibe tag from the user's likes maps cleanly
   - "wildcard": lateral pick within a scene the user already engages with; explain the bet
   - "exploration": deliberate stretch into a region/genre absent from the user's likes — used to fulfill the diversity quota. Reason should be like "you've never explored X — this is the canonical entry point because Y".
10. If you don't know enough about a track to be confident, DO NOT recommend it. Better to return fewer items than to bluff.

OUTPUT FORMAT — return ONLY a valid JSON array, no prose:
[
  {"artist": "Artist Name", "track": "Track Name", "reason": "the connection you are betting on, NOT a guess at how it sounds", "lens": "vibe|region|genre|era|wildcard"},
  ...
]"""


def _extract_json_array(raw: str):
    """First valid JSON array in a possibly-noisy model response, else None."""
    v = extract_json(raw)
    return v if isinstance(v, list) else None


def _build_user_context(user_id: str) -> dict:
    """Pull what Claude needs from the DB. Compact format to stay token-efficient."""
    history = fetchall(
        """
        SELECT track_id, track_name, artist, region, status, played_pct
        FROM user_history
        WHERE user_id = %s
        ORDER BY listened_at DESC
        LIMIT 60
        """,
        (user_id,),
    )

    likes = fetchall(
        """
        SELECT track_key, vibe, added_at
        FROM user_ledger
        WHERE user_id = %s AND status = 'liked'
        ORDER BY added_at DESC
        LIMIT 100
        """,
        (user_id,),
    )

    dislikes = fetchall(
        """
        SELECT track_key
        FROM user_ledger
        WHERE user_id = %s AND status = 'disliked'
        """,
        (user_id,),
    )

    # Categorize each history entry into a single engagement signal.
    # Important: status="listened" with no played_pct means "the app played it
    # and the user didn't immediately reject it" — that's NOT a positive signal,
    # it's the absence of one. Treat as neutral noise.
    def _engagement(status, pct):
        if status == "saved":
            return "loved"
        if status == "disliked":
            return "rejected"
        if status == "skipped":
            if pct is not None and pct < 10:
                return "instant_skip"
            return "skipped"
        # status == "listened"
        if pct is not None and pct >= 70:
            return "deep_listen"   # only signal we trust without explicit save
        if pct is not None and pct < 10:
            return "instant_skip"  # listened-status but actually skipped fast
        return "noise"             # played, no real engagement signal

    history_categorized = []
    for h in history:
        pct = h.get("played_pct")
        eng = _engagement(h["status"], pct)
        history_categorized.append({
            "id": h.get("track_id"),
            "a": h["artist"],
            "t": h["track_name"],
            "r": h["region"],
            "eng": eng,
            "p": (round(pct, 1) if pct is not None else None),
        })

    # Strong signals: things Claude should weight heavily
    strong_signals = [h for h in history_categorized
                      if h["eng"] in ("loved", "deep_listen", "rejected", "instant_skip")]
    # Recently-served (whether engaged or not) — still useful as "don't repeat" context
    recent_served = history_categorized  # all 40

    # Compute coverage map: which broad regions/scenes the user has actually
    # ENGAGED with (saved or deep-listened) vs. blanks. Used to give Claude
    # an explicit "exploration shortlist" so recommendations escape the comfort zone.
    from collections import Counter
    pos_regions = Counter()
    for h in history_categorized:
        if h["eng"] in ("loved", "deep_listen") and h.get("r"):
            pos_regions[h["r"]] += 1
    # Major macro buckets — if the user has 0 positives in a bucket, it's
    # an exploration target.
    MACROS = {
        "North America":   ["USA", "Canada"],
        "Western Europe":  ["UK", "Ireland", "France", "Germany", "Sweden", "Netherlands",
                            "Italy", "Spain", "Portugal", "Greece", "Norway", "Denmark"],
        "Eastern Europe":  ["Russia", "Poland", "Czech Republic", "Hungary", "Romania",
                            "Bulgaria", "Ukraine", "Serbia", "Croatia"],
        "Caribbean/Latin": ["Caribbean", "Cuba", "Trinidad", "Colombia", "Mexico", "Brazil",
                            "Jamaica", "Argentina", "Peru", "Chile", "Venezuela"],
        "Africa":          ["South Africa", "Tanzania", "Kenya", "Nigeria", "Cape Verde",
                            "Ghana", "Senegal", "Morocco", "Ethiopia", "East Africa",
                            "West Africa", "North Africa", "Mali", "Congo"],
        "East Asia":       ["Japan", "China", "Taiwan", "South Korea", "Hong Kong", "Mongolia"],
        "SE Asia":         ["Malaysia", "Thailand", "Vietnam", "Indonesia", "Philippines",
                            "Myanmar", "Laos", "Cambodia", "Singapore"],
        "South Asia":      ["India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal"],
        "Middle East":     ["Turkey", "Iran", "Israel", "Lebanon", "Syria", "Saudi Arabia"],
        "Oceania":         ["Australia", "New Zealand", "Pacific"],
    }
    coverage = {}
    for macro, regs in MACROS.items():
        n = sum(pos_regions.get(r, 0) for r in regs)
        coverage[macro] = n
    unexplored = sorted([m for m, n in coverage.items() if n == 0])
    underexplored = sorted([m for m, n in coverage.items() if 0 < n <= 1])

    # Stamp likes with chronological position (0 = most recently liked) so
    # Claude sees recency at a glance. Bucket into "current" (last 15) /
    # "active" (15-50) / "background" (50+) to make weighting obvious.
    likes_payload = []
    for i, l in enumerate(likes):
        if i < 15:
            recency = "current"     # liked very recently — strongest signal of current taste
        elif i < 50:
            recency = "active"      # liked in this listening era
        else:
            recency = "background"  # older taste; useful for breadth, not the cutting edge
        added_at = l.get("added_at")
        likes_payload.append({
            "rank": i,                              # 0 = most recent
            "recency": recency,
            "key": l["track_key"],
            "vibe": l["vibe"],
            "when": added_at.isoformat() if hasattr(added_at, "isoformat") else added_at,
        })

    return {
        "strong_signals": strong_signals,
        "recent_served": recent_served,
        "likes": likes_payload,
        "dislikes": [d["track_key"] for d in dislikes],
        "coverage": coverage,
        "unexplored_macros": unexplored,
        "underexplored_macros": underexplored,
    }


def ai_recommend(user_id: str, n: int = 10) -> dict:
    """Return {recommendations: [...], meta: {...}} for the given user.

    Errors are caught and surfaced inside the response (never raised) so the
    frontend can degrade gracefully.
    """
    started = time.time()
    try:
        import anthropic
    except ImportError:
        return {"error": "anthropic package not installed", "recommendations": []}

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"error": "ANTHROPIC_API_KEY not set", "recommendations": []}

    ctx = _build_user_context(user_id)
    if not ctx["strong_signals"] and not ctx["likes"]:
        return {
            "error": "no taste data — save a few tracks first",
            "recommendations": [],
        }

    n_explore = max(3, n * 4 // 10)   # ≥40% must be exploration picks
    n_anchor = n - n_explore

    user_prompt = (
        f"Recommend {n} specific tracks for this user.\n\n"
        f"=== STRONG TASTE SIGNALS (the only entries you should weight as preference evidence) ===\n"
        f"Each entry has eng = loved | deep_listen | rejected | instant_skip; p = played_pct 0–100.\n"
        f"  loved        = explicitly saved by the user (strongest positive)\n"
        f"  deep_listen  = listened to ≥70% without saving (soft positive)\n"
        f"  rejected     = explicit dislike (strongest negative — never recommend this artist's same vein)\n"
        f"  instant_skip = skipped within 10% (soft negative)\n"
        f"{json.dumps(ctx['strong_signals'], ensure_ascii=False)}\n\n"
        f"=== COVERAGE MAP — macro regions and how many positive signals the user has there ===\n"
        f"{json.dumps(ctx['coverage'])}\n"
        f"  UNEXPLORED (zero positive signals — STRONG candidates for exploration picks): "
        f"{json.dumps(ctx['unexplored_macros'])}\n"
        f"  UNDEREXPLORED (1 positive signal — also good exploration territory): "
        f"{json.dumps(ctx['underexplored_macros'])}\n\n"
        f"=== RECENTLY SERVED (do NOT recommend any of these — they were already played) ===\n"
        f"{json.dumps([{'a': r['a'], 't': r['t']} for r in ctx['recent_served']], ensure_ascii=False)}\n\n"
        f"=== LIKES LEDGER (chronologically ordered; rank 0 = MOST RECENTLY LIKED) ===\n"
        f"Each entry: {{rank, recency, key, vibe, when}}.\n"
        f"  recency='current'    → liked very recently (last 15) — these reflect the user's CURRENT taste pull. Weight heavily.\n"
        f"  recency='active'     → liked in this listening era (rank 15–49) — solid background of taste.\n"
        f"  recency='background' → older saves (rank 50+) — useful for breadth, but the user has moved on stylistically since.\n"
        f"NEVER recommend any of these (they're already saved).\n"
        f"{json.dumps(ctx['likes'], ensure_ascii=False, default=str)}\n\n"
        f"=== DISLIKES LEDGER (style to avoid) ===\n"
        f"{json.dumps(ctx['dislikes'], ensure_ascii=False)}\n\n"
        f"BUDGET FOR THIS BATCH (HARD requirement):\n"
        f"  • {n_explore} EXPLORATION picks (lens=\"exploration\") from UNEXPLORED or UNDEREXPLORED macros above. "
        f"Pick canonical/well-loved entry points — what a knowledgeable friend would play first to introduce that scene. "
        f"Do NOT cite the user's likes as the reason; the reason is the macro itself ('you have nothing from X — this is the doorway because Y').\n"
        f"  • {n_anchor} ANCHOR picks (lens=region/genre/era/vibe/wildcard) connected to the user's actual saved/deep-listened tracks. "
        f"Spread these across as many different scenes as possible — do NOT stack 4 afropop recs because they have 4 afropop saves.\n\n"
        f"CRITICAL: Do NOT cite a 'recently served' entry as evidence the user likes that artist — the user just sat through it or skipped it, that's not endorsement. Only the STRONG TASTE SIGNALS count as evidence.\n\n"
        f"Return ONLY the JSON array of {n} recommendations. No prose."
    )

    client = anthropic.Anthropic(api_key=api_key)

    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=MAX_OUTPUT_TOKENS,
            system=[
                {"type": "text", "text": _SYSTEM, "cache_control": {"type": "ephemeral"}},
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        return {"error": f"anthropic call failed: {e}", "recommendations": []}

    raw = msg.content[0].text if msg.content else ""
    recs = _extract_json_array(raw)
    if recs is None:
        return {
            "error": "could not parse model output as JSON array",
            "raw": raw[:500],
            "recommendations": [],
        }

    # Build a set of "already heard" keys for hard post-filtering.
    # Format mirrors how the ledger stores keys: "artist - track" lowercased.
    # We strip parenthetical/bracket suffixes ("(Remix)", "[Live]") so a slightly
    # different version is still caught.

    heard_keys = set()
    for h in ctx["recent_served"]:
        heard_keys.add(_norm(h["a"], h["t"]))
    for l in ctx["likes"]:
        # ledger keys are already in "artist - track" lowercased form
        k = (l.get("key") or "").strip().lower()
        if k:
            heard_keys.add(k)
            # Also store version-stripped form
            if " - " in k:
                a, t = k.split(" - ", 1)
                heard_keys.add(_norm(a, t))
    for d_key in ctx["dislikes"]:
        heard_keys.add((d_key or "").strip().lower())

    # Normalize + add a search query for Spotify; drop already-heard.
    out = []
    dropped_heard = []
    for r in recs:
        if not isinstance(r, dict):
            continue
        artist = (r.get("artist") or "").strip()
        track = (r.get("track") or "").strip()
        if not artist or not track:
            continue
        key = _norm(artist, track)
        if key in heard_keys:
            dropped_heard.append(f"{artist} — {track}")
            continue
        out.append({
            "artist": artist,
            "track": track,
            "reason": (r.get("reason") or "").strip(),
            "lens": (r.get("lens") or "wildcard").strip(),
            "search": f'track:"{track}" artist:"{artist}"',
        })

    elapsed = time.time() - started
    usage = getattr(msg, "usage", None)
    meta = {
        "model": MODEL,
        "elapsed_sec": round(elapsed, 2),
        "input_tokens": getattr(usage, "input_tokens", None) if usage else None,
        "output_tokens": getattr(usage, "output_tokens", None) if usage else None,
        "cache_read_tokens": getattr(usage, "cache_read_input_tokens", None) if usage else None,
        "cache_creation_tokens": getattr(usage, "cache_creation_input_tokens", None) if usage else None,
        "n_returned": len(out),
        "strong_signals": len(ctx["strong_signals"]),
        "recent_served": len(ctx["recent_served"]),
        "likes_size": len(ctx["likes"]),
    }

    meta["dropped_already_heard"] = len(dropped_heard)

    # Structured log (picked up by the /api/health endpoint)
    print(f"[AI-MIX] user={user_id} n={len(out)} dropped_heard={len(dropped_heard)} "
          f"elapsed={meta['elapsed_sec']}s "
          f"in={meta['input_tokens']} out={meta['output_tokens']} "
          f"cache_read={meta['cache_read_tokens']}")
    if dropped_heard:
        print(f"[AI-MIX]   dropped: {', '.join(dropped_heard[:5])}"
              f"{' …' if len(dropped_heard) > 5 else ''}")

    return {"recommendations": out, "meta": meta}


# ── Pool-search-backed AI Mix (v2) ───────────────────────────────────────────
# Claude proposes WEIGHTED DIMENSIONAL QUERIES (regions, genres, decades, vibe
# keywords + per-dim weights). For each query, we score the live `tracks` table
# and surface the best real match. Claude never names artists — eliminates the
# famous-name training-data bias.

_AI_MIX_V2_SYSTEM = """You are curating music for the DIG discovery app. The app's whole point is surfacing music that is NOT what Spotify's algorithms would surface — including obscure regional acts, microcultures, and tracks the user would never reach on their own.

CRITICAL: You will NOT name specific artists or tracks. Famous-name bias from your training data would wreck the experience. Instead, for each of N slots you'll output a WEIGHTED DIMENSIONAL QUERY — describe the SHAPE of what should play (regions, genres, decades, vibe keywords + per-dimension weights). Our pool search will resolve each query to a real track from DIG's curated catalog of ~16k tracks across ~80 regions.

OUTPUT FORMAT — return ONLY a JSON array of N items:
[
  {
    "query": {
      "regions":       [str],   // 1–4 region names (see POOL_REGIONS in user prompt)
      "genres":        [str],   // 1–6 genre tags (free-form; we fuzzy-match)
      "decades":       [str],   // 0–3 like ["1970s","1980s"]; omit for "any era"
      "vibe_keywords": [str]    // 0–6 keywords that match against energy/mood/texture/feel labels
    },
    "weights": {"region": 0.0-1.0, "genre": 0.0-1.0, "decade": 0.0-1.0, "vibe": 0.0-1.0},
    "reason": "describe the SHAPE you're asking for and WHY (cite a specific user signal)",
    "lens": "vibe|region|genre|era|wildcard|exploration"
  },
  ...
]

WEIGHT DESIGN — this is where you do the real work:
  • For a SE Asian folk pick: region+genre carry the meaning. weights ~ {region:0.4, genre:0.4, decade:0.1, vibe:0.1}
  • For a dark-techno mood pick: region is irrelevant globally. weights ~ {region:0.0, genre:0.5, vibe:0.4, decade:0.1}
  • For an era-themed pick: weights ~ {region:0.1, genre:0.3, decade:0.5, vibe:0.1}
  • For a "same vibe, different world" pick: weights ~ {region:0.0, genre:0.2, vibe:0.7, decade:0.1}
  • For a deeply specific scene (e.g. Berlin industrial techno): weights ~ {region:0.3, genre:0.5, vibe:0.2}
  Weights should sum to ~1.0. The dimensions you set to 0 are IGNORED in scoring.

QUERY DESIGN:
  • regions: must come from POOL_REGIONS (the user prompt lists them). If you want a region not listed, omit regions and lean on genre+vibe.
  • genres: free-form — write what you mean ("mor lam", "dub techno", "hindustani classical"). We do fuzzy substring matching, so "afrobeats" matches "nigerian afrobeats", "afrobeats fusion", etc.
  • decades: LEAVE EMPTY by default. Only set decades when the user has shown an explicit era preference in their strong signals (e.g. they save almost exclusively 90s rave, or only 70s soul). When in doubt, omit decades — the pool spans every decade and presetting a decade collapses the candidate set 5–10× and makes recommendations feel flat. If you DO set decades, set weight.decade LOW (≤0.1) so it nudges rather than filters.
  • vibe_keywords: SHORT terms that describe character, drawn from how the pool's vibe labels read. Examples that work: "modal", "drone", "trance", "raw", "sparse", "lush", "warm", "industrial", "sunny", "melancholic", "driving", "bouncy", "cinematic", "spiritual". Don't write full sentences — keywords only.

ERA SPREAD:
  • If you generate multiple queries for the same region in a batch, give them DIFFERENT decade settings (or leave decades empty for all) so the user doesn't get 3 tracks all from the same era.
  • Default assumption: the user wants chronological breadth, not era-locked nostalgia.

DIVERSITY QUOTA — non-negotiable:
  • At MOST 30% of N may share a single region/scene. If user has 4 saved afropop tracks, you may include AT MOST 1 afropop slot in the batch.
  • At LEAST 40% must be EXPLORATION picks (lens="exploration") with regions or scenes the user has zero positive signals in. Use the UNEXPLORED list in the user prompt.
  • Aim for spread: across an N=10 batch, hit at least 6 distinct regions/scenes.

SIGNAL INTERPRETATION:
  • LIKES LEDGER is in chronological order (rank 0 = most recent). The 'current' top 15 are right-now taste — weight heavily. 'background' likes are old.
  • STRONG TASTE SIGNALS show explicit engagement (loved/deep_listen/instant_skip/rejected). Use these to calibrate.
  • RECENT_SERVED is what the app played, NOT preference. Don't cite it as evidence.
  • COVERAGE MAP / UNEXPLORED tells you which macros to deliberately stretch into.

NEVER:
  • Name a specific artist or track
  • Recommend stock-music shapes (e.g. don't write {genres:["sleep","study","focus"]})
  • Set weights that don't add up close to 1.0
  • Submit a query with all four dimensions empty"""


_REGION_LIST_CACHE = None
def _pool_region_list():
    """Pull distinct populated regions from the pool, sorted by track count desc."""
    global _REGION_LIST_CACHE
    if _REGION_LIST_CACHE is not None:
        return _REGION_LIST_CACHE
    rows = fetchall(
        "SELECT region, COUNT(*) AS n FROM tracks "
        "WHERE region IS NOT NULL AND region != '' "
        "GROUP BY region ORDER BY n DESC LIMIT 80"
    )
    _REGION_LIST_CACHE = [r["region"] for r in rows]
    return _REGION_LIST_CACHE


def _fetch_recent_user_queries(user_id: str, limit: int = 20) -> list:
    """Return last N queries this user has been served, newest first."""
    rows = fetchall(
        "SELECT query_json, reason, lens FROM user_ai_queries "
        "WHERE user_id = %s ORDER BY ts DESC LIMIT %s",
        (user_id, limit),
    )
    return [
        {"query": r["query_json"], "reason": r.get("reason") or "", "lens": r.get("lens") or ""}
        for r in rows
    ]


def _save_user_queries(user_id: str, queries: list) -> None:
    """Persist freshly-issued queries; keep table bounded by trimming old rows."""
    if not queries:
        return
    from lib.db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for q in queries:
                if not isinstance(q, dict):
                    continue
                cur.execute(
                    "INSERT INTO user_ai_queries (user_id, query_json, reason, lens) "
                    "VALUES (%s, %s::jsonb, %s, %s)",
                    (user_id,
                     json.dumps(q.get("query") or {}, ensure_ascii=False),
                     (q.get("reason") or "")[:500],
                     (q.get("lens") or "")[:40]),
                )
            # Keep only last 50 per user — old ones aren't useful and could bloat the table
            cur.execute(
                "DELETE FROM user_ai_queries WHERE user_id = %s "
                "AND id NOT IN (SELECT id FROM user_ai_queries WHERE user_id = %s "
                "               ORDER BY ts DESC LIMIT 50)",
                (user_id, user_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        # Don't fail the recommendation just because we couldn't persist
    finally:
        conn.close()


def ai_recommend_v2(user_id: str, n: int = 10, frontend_recent_ids: list | None = None,
                    frontend_recent_artists: list | None = None) -> dict:
    """Pool-search-backed AI Mix. Claude outputs weighted queries; pool surfaces real tracks.

    frontend_recent_ids/artists: optional IDs/artist names sent by the browser
    to close the timing race — the in-memory frontend history is fresher than
    the DB-persisted user_history (which is debounced + async).
    """
    started = time.time()
    try:
        import anthropic
    except ImportError:
        return {"error": "anthropic package not installed", "recommendations": []}

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"error": "ANTHROPIC_API_KEY not set", "recommendations": []}

    ctx = _build_user_context(user_id)
    if not ctx["strong_signals"] and not ctx["likes"]:
        return {"error": "no taste data — save a few tracks first", "recommendations": []}

    n_explore = max(3, n * 4 // 10)
    n_anchor = n - n_explore

    # Pull the user's recent query memory so we can tell Claude not to repeat shapes
    recent_queries = _fetch_recent_user_queries(user_id, limit=20)

    user_prompt = (
        f"Recommend {n} tracks via WEIGHTED DIMENSIONAL QUERIES (no artist/track names).\n\n"
        f"=== POOL_REGIONS (use these exact strings in query.regions) ===\n"
        f"{json.dumps(_pool_region_list())}\n\n"
        f"=== YOUR RECENT QUERIES (DO NOT REPEAT THESE SHAPES — propose meaningfully different ones) ===\n"
        f"These are the last {len(recent_queries)} queries you've issued for this user. "
        f"Write NEW queries that differ along at least one of: region, decade, vibe-keywords, or weight emphasis.\n"
        f"{json.dumps([q['query'] for q in recent_queries], ensure_ascii=False)}\n\n"
        f"=== STRONG TASTE SIGNALS ===\n"
        f"{json.dumps(ctx['strong_signals'], ensure_ascii=False)}\n\n"
        f"=== COVERAGE MAP (positive-signal counts per macro region) ===\n"
        f"{json.dumps(ctx['coverage'])}\n"
        f"  UNEXPLORED (zero positives — strong exploration targets): {json.dumps(ctx['unexplored_macros'])}\n"
        f"  UNDEREXPLORED (1 positive): {json.dumps(ctx['underexplored_macros'])}\n\n"
        f"=== RECENT_SERVED (just played; do NOT cite as preference) ===\n"
        f"{json.dumps([{'a': r['a'], 't': r['t']} for r in ctx['recent_served']], ensure_ascii=False)}\n\n"
        f"=== LIKES LEDGER (rank 0 = most recent; current = strong now-taste) ===\n"
        f"{json.dumps(ctx['likes'], ensure_ascii=False, default=str)}\n\n"
        f"=== DISLIKES LEDGER ===\n"
        f"{json.dumps(ctx['dislikes'], ensure_ascii=False)}\n\n"
        f"BUDGET FOR THIS BATCH:\n"
        f"  • {n_explore} EXPLORATION queries (lens=\"exploration\") targeting UNEXPLORED macros\n"
        f"  • {n_anchor} ANCHOR queries connected to current likes (spread across scenes — no stacking)\n\n"
        f"Return ONLY the JSON array of {n} queries."
    )

    client = anthropic.Anthropic(api_key=api_key)
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=MAX_OUTPUT_TOKENS,
            system=[{"type": "text", "text": _AI_MIX_V2_SYSTEM, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        return {"error": f"anthropic call failed: {e}", "recommendations": []}

    raw = msg.content[0].text if msg.content else ""
    queries = _extract_json_array(raw)
    if queries is None:
        return {"error": "could not parse queries from model output", "raw": raw[:500], "recommendations": []}

    # Build heard-keys + heard-ids (don't surface what user has already encountered)

    heard_keys = set()
    heard_ids = set()
    for h in ctx["recent_served"]:
        heard_keys.add(_norm(h["a"], h["t"]))
        if h.get("id"):
            heard_ids.add(h["id"])
    for l in ctx["likes"]:
        k = (l.get("key") or "").strip().lower()
        if k: heard_keys.add(k)
    for k in ctx["dislikes"]:
        heard_keys.add((k or "").strip().lower())
    # Merge in the frontend's in-memory history — it's fresher than the
    # DB user_history because /history POST is async/debounced. This closes
    # the race where the just-played track isn't yet on disk.
    if frontend_recent_ids:
        for tid in frontend_recent_ids:
            if tid: heard_ids.add(tid)

    # Artist cooldown — last 100 plays from server-side history + whatever the
    # frontend says. 100 is generous so an artist heard 30 minutes ago doesn't
    # silently become eligible again.
    recent_artists = set()
    for h in ctx["recent_served"][:100]:
        a = (h.get("a") or "").split(",")[0].strip().lower()
        if a:
            recent_artists.add(a)
    if frontend_recent_artists:
        for a in frontend_recent_artists:
            n = (a or "").split(",")[0].strip().lower()
            if n: recent_artists.add(n)

    # Resolve each query against the pool
    from lib.pool_search import query_to_track
    out = []
    used_ids = set(heard_ids)  # seed within-batch dedup with previously-heard track IDs
    used_artists = set()  # within-batch artist dedup
    no_match = []
    for q in queries:
        if not isinstance(q, dict):
            continue
        query = q.get("query") or {}
        weights = q.get("weights") or {}
        # Combine session-recent artists with within-batch picks
        artist_excl = recent_artists | used_artists
        track = query_to_track(query, weights,
                               exclude_ids=used_ids, exclude_keys=heard_keys,
                               exclude_artists=artist_excl)
        if not track:
            no_match.append({"q": query, "reason": q.get("reason", "")[:80]})
            continue
        used_ids.add(track["id"])
        # Mark this artist used so subsequent queries in the same batch don't pick the same act
        primary_artist = (track["artist"] or "").split(",")[0].strip().lower()
        if primary_artist:
            used_artists.add(primary_artist)
        out.append({
            "id":     track["id"],
            "artist": track["artist"],
            "name":   track["name"],
            "region": track["region"],
            "decade": track.get("decade"),
            "year":   track.get("year"),
            "genres": track.get("genres") or [],
            "labels": track.get("labels") or {},
            "source": track.get("source") or "spotify",
            "_aiReason": q.get("reason", ""),
            "_aiLens":   q.get("lens", "wildcard"),
            "_query":    query,
            "_weights":  weights,
            "_score":    track.get("_pool_score"),
        })

    # Persist the freshly-issued queries so the next call can show them as
    # "do not repeat" context. Save the QUERIES (not the resolved tracks)
    # because shape diversity is what we want to enforce.
    try:
        _save_user_queries(user_id, [q for q in queries if isinstance(q, dict)])
    except Exception:
        pass  # don't let memory persistence break recommendations

    elapsed = time.time() - started
    usage = getattr(msg, "usage", None)
    meta = {
        "model": MODEL,
        "elapsed_sec": round(elapsed, 2),
        "input_tokens": getattr(usage, "input_tokens", None) if usage else None,
        "output_tokens": getattr(usage, "output_tokens", None) if usage else None,
        "cache_read_tokens": getattr(usage, "cache_read_input_tokens", None) if usage else None,
        "n_queries": len(queries),
        "n_resolved": len(out),
        "n_no_match": len(no_match),
        "n_recent_queries_seen": len(recent_queries),
    }
    print(f"[AI-MIX-V2] user={user_id} queries={len(queries)} resolved={len(out)} "
          f"no_match={len(no_match)} recent_q={len(recent_queries)} "
          f"elapsed={meta['elapsed_sec']}s in={meta['input_tokens']} out={meta['output_tokens']}")
    if no_match:
        for nm in no_match[:5]:
            print(f"[AI-MIX-V2]   no match for query: {json.dumps(nm['q'])[:150]}")

    return {"recommendations": out, "meta": meta}


# ── Journey mode ─────────────────────────────────────────────────────────────
# Infinite, seed-anchored exploration. Each call returns one block (~8 tracks)
# structured as a narrative arc: close → expand → stretch. Subsequent blocks
# get the seed plus a record of what played + how the user reacted.

_JOURNEY_SYSTEM = """You are a music curator building an infinite, seed-anchored JOURNEY for the user.

A journey starts from ONE track ("the seed") and unfolds as a sequence of blocks. Each block is a small narrative arc that drifts further from the seed without losing its thread. Your job for each call is to return ONE BLOCK of ~8 tracks as POOL QUERIES — you do not name specific artists or tracks. Our pool search will surface a real track from DIG's catalog for each query.

DECIDE THE SPINE. Look at the seed and decide which dimension is the meaningful through-line:
  • For Chaweewan Dumnern → spine is region/scene (Thai mor lam → Lao → Burmese → Vietnamese → Mainland Chinese → European modal folk that lives in the same world)
  • For a dark techno track → spine is texture/subgenre (Berlin techno → industrial → coldwave → dark ambient → Japanese noise) — region is irrelevant, don't anchor on it
  • For a 60s soul track → spine is era + scene lineage (Stax → Muscle Shoals → UK Northern → Italian beat → modern revival)
  • For a song with strong vibe but generic genre → spine is vibe (lonely-warm-warmer-melancholic across whatever regions/eras best deliver that mood)
You decide. State the spine you chose in the FIRST item's reason.

BLOCK STRUCTURE (~8 slots per call):
  • CLOSE × 3-4 — tightly bound to the seed along the spine. Queries that should resolve to things a fan of the seed would clearly love.
  • EXPAND × 2-3 — adjacent scenes / one degree out. Same spine, looser execution.
  • STRETCH × 1-2 — lateral: preserve ONE dimension of the seed but break others. Skip stretch if there's no meaningful lateral; add more expand picks instead.

CONTINUITY ACROSS BLOCKS:
  • You'll receive `block_index` (0 = first block) and `previous_journey` (previous tracks + engagement)
  • If user instant-skipped multiple from the previous block, narrow the spine — they want more focus
  • If user deep-listened most, you can drift further — they're following the thread

HARD RULES:
1. NEVER name a specific artist or track. Output ONLY dimensional queries.
2. NEVER recommend stock/factory/wellness shapes — don't set genres like "sleep", "study", "spa".
3. Each slot is a query. The pool-search resolves it to a real catalog track while automatically skipping anything already in user history, previous_journey, likes, or dislikes.
4. Tag each slot with arc = "close" | "expand" | "stretch" — the frontend uses this for UI.

OUTPUT FORMAT — return ONLY a JSON array of ~8 items, no prose:
[
  {
    "query": {
      "regions":       [str],   // 0–4 region names from POOL_REGIONS
      "genres":        [str],   // 1–6 genre tags (fuzzy substring match)
      "decades":       [str],   // 0–3 like ["1970s","1980s"]; omit for any era
      "vibe_keywords": [str]    // 0–6 short descriptors (e.g. "modal", "warm", "industrial")
    },
    "weights": {"region": 0.0-1.0, "genre": 0.0-1.0, "decade": 0.0-1.0, "vibe": 0.0-1.0},
    "arc":    "close|expand|stretch",
    "reason": "the connection back to the seed or a previous journey track"
  },
  ...
]

WEIGHT DESIGN:
  • For a region-anchored seed (SE Asian folk): region+genre carry the spine. {region:0.4, genre:0.4, decade:0.1, vibe:0.1}
  • For a vibe-anchored seed (dark techno): region irrelevant. {region:0.0, genre:0.4, vibe:0.5, decade:0.1}
  • For an era-anchored seed (60s soul): weights ~ {region:0.1, genre:0.3, decade:0.5, vibe:0.1}
  • Weights should sum to ~1.0. Dimensions set to 0 are ignored in scoring. Don't set decade high unless the seed explicitly calls for era fidelity."""


def _journey_user_context(user_id: str, exclude_keys: set) -> dict:
    """Lighter context than ai_recommend — just enough to avoid recommending
    things the user has already heard. Doesn't include the full taste profile;
    the seed track is the anchor, not the user's general taste."""
    history = fetchall(
        """
        SELECT track_name, artist
        FROM user_history
        WHERE user_id = %s
        ORDER BY listened_at DESC
        LIMIT 60
        """,
        (user_id,),
    )
    likes = fetchall(
        """
        SELECT track_key
        FROM user_ledger
        WHERE user_id = %s AND status = 'liked'
        ORDER BY added_at DESC
        LIMIT 100
        """,
        (user_id,),
    )
    dislikes = fetchall(
        """
        SELECT track_key
        FROM user_ledger
        WHERE user_id = %s AND status = 'disliked'
        """,
        (user_id,),
    )
    return {
        "recent_served": [{"a": h["artist"], "t": h["track_name"]} for h in history],
        "likes": [l["track_key"] for l in likes],
        "dislikes": [d["track_key"] for d in dislikes],
    }


def journey_recommend(user_id: str, seed: dict, block_index: int = 0,
                      previous_journey: list = None, n: int = 8,
                      frontend_recent_ids: list | None = None) -> dict:
    """Generate one block of a seeded journey.

    Claude outputs pool queries (region/genre/vibe/decade + weights + arc);
    each query is resolved against the live `tracks` table via pool_search,
    with heard_ids + the seed + previous_journey excluded up front. The
    recommendations returned are real catalog tracks (id+name+artist+labels),
    so the frontend skips its Spotify-search fallback.
    """
    started = time.time()
    try:
        import anthropic
    except ImportError:
        return {"error": "anthropic package not installed", "recommendations": []}

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"error": "ANTHROPIC_API_KEY not set", "recommendations": []}

    if not seed or not seed.get("artist") or not seed.get("track"):
        return {"error": "seed must include artist + track", "recommendations": []}

    previous_journey = previous_journey or []

    ctx = _journey_user_context(user_id, set())

    user_prompt = (
        f"Build journey block #{block_index} from this seed.\n\n"
        f"=== SEED TRACK (the anchor — choose the spine from THIS) ===\n"
        f"{json.dumps(seed, ensure_ascii=False)}\n\n"
        f"=== POOL_REGIONS (use these exact strings in query.regions) ===\n"
        f"{json.dumps(_pool_region_list())}\n\n"
        f"=== PREVIOUS JOURNEY BLOCKS (already played) ===\n"
        f"{json.dumps(previous_journey, ensure_ascii=False) if previous_journey else '[] (this is the first block)'}\n\n"
        f"=== USER'S RECENT HISTORY (what's been served — NOT preference) ===\n"
        f"{json.dumps(ctx['recent_served'][:20], ensure_ascii=False)}\n\n"
        f"=== USER'S LIKES LEDGER ===\n"
        f"{json.dumps(ctx['likes'][:20], ensure_ascii=False)}\n\n"
        f"=== USER'S DISLIKES (avoid this style) ===\n"
        f"{json.dumps(ctx['dislikes'], ensure_ascii=False)}\n\n"
        f"Return ONLY the JSON array of {n} queries. "
        f"For block 0, the FIRST item's reason must explicitly state the spine you chose."
    )

    client = anthropic.Anthropic(api_key=api_key)
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=MAX_OUTPUT_TOKENS,
            system=[{"type": "text", "text": _JOURNEY_SYSTEM, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        return {"error": f"anthropic call failed: {e}", "recommendations": []}

    raw = msg.content[0].text if msg.content else ""
    queries = _extract_json_array(raw)
    if queries is None:
        return {"error": "could not parse queries from model output",
                "raw": raw[:500], "recommendations": []}

    # ── Exclusion sets for pool-search ─────────────────────────────────────
    # heard_ids: every track the user has ever seen + anything the frontend
    # tells us it just played (closes the DB-sync race)

    heard_ids = set()
    heard_keys = set()
    # Pull fresh heard_ids directly from user_history — _journey_user_context
    # only returned 60 rows of names, we want the full ID set for dedup.
    try:
        hist_ids = fetchall(
            "SELECT track_id FROM user_history WHERE user_id = %s AND track_id IS NOT NULL",
            (user_id,),
        )
        for r in hist_ids:
            heard_ids.add(r["track_id"])
    except Exception:
        pass
    if frontend_recent_ids:
        for tid in frontend_recent_ids:
            if tid:
                heard_ids.add(tid)
    # Name-based exclusions for tracks in the pool that duplicate something
    # outside the exact id set (reissues, remasters with different ids).
    for h in ctx["recent_served"]:
        heard_keys.add(_norm(h["a"], h["t"]))
    for k in ctx["likes"]:
        heard_keys.add((k or "").lower().strip())
    for k in ctx["dislikes"]:
        heard_keys.add((k or "").lower().strip())
    for p in previous_journey:
        heard_keys.add(_norm(p.get("artist"), p.get("track")))
    heard_keys.add(_norm(seed.get("artist"), seed.get("track")))

    # ── Resolve each query to a real track ─────────────────────────────────
    from lib.pool_search import query_to_track
    out, no_match = [], []
    used_ids = set(heard_ids)
    used_artists = set()
    # Artist cooldown: don't stack multiple journey slots on the same artist
    seed_artist_n = (seed.get("artist") or "").split(",")[0].strip().lower()
    if seed_artist_n:
        used_artists.add(seed_artist_n)
    for q in queries:
        if not isinstance(q, dict):
            continue
        query = q.get("query") or {}
        weights = q.get("weights") or {}
        arc = (q.get("arc") or "expand").strip().lower()
        if arc not in ("close", "expand", "stretch"):
            arc = "expand"
        track = query_to_track(
            query, weights,
            exclude_ids=used_ids, exclude_keys=heard_keys,
            exclude_artists=used_artists,
        )
        if not track:
            no_match.append({"q": query, "arc": arc,
                             "reason": (q.get("reason") or "")[:80]})
            continue
        used_ids.add(track["id"])
        a_norm = (track["artist"] or "").split(",")[0].strip().lower()
        if a_norm:
            used_artists.add(a_norm)
        track["arc"] = arc
        track["lens"] = "journey"
        track["_aiReason"] = (q.get("reason") or "").strip()
        track["_aiLens"] = f"journey · {arc}"
        out.append(track)

    elapsed = time.time() - started
    usage = getattr(msg, "usage", None)
    meta = {
        "model": MODEL,
        "elapsed_sec": round(elapsed, 2),
        "input_tokens": getattr(usage, "input_tokens", None) if usage else None,
        "output_tokens": getattr(usage, "output_tokens", None) if usage else None,
        "cache_read_tokens": getattr(usage, "cache_read_input_tokens", None) if usage else None,
        "n_returned": len(out),
        "no_match": len(no_match),
        "block_index": block_index,
        "seed": f"{seed.get('artist')} — {seed.get('track')}",
    }

    print(f"[JOURNEY] user={user_id} block={block_index} seed='{meta['seed']}' "
          f"n={len(out)} no_match={len(no_match)} elapsed={meta['elapsed_sec']}s "
          f"in={meta['input_tokens']} out={meta['output_tokens']}")
    if out:
        sample = [f"{t['id'][:8]}·{t['arc']}·{(t.get('artist') or '')[:20]}" for t in out[:4]]
        print(f"[JOURNEY]   returned: {sample}")
    if no_match:
        preview = [f"{nm.get('arc')}:{(nm.get('q') or {}).get('genres') or []}" for nm in no_match[:4]]
        print(f"[JOURNEY]   no_match queries: {preview}")
    # Diagnostic: if recs were returned but user still sees "no new tracks",
    # it means the CLIENT rejects them. Log heard-set overlap explicitly.
    if out:
        rec_ids = [t["id"] for t in out]
        overlap = [tid for tid in rec_ids if tid in heard_ids]
        if overlap:
            print(f"[JOURNEY]   WARNING {len(overlap)}/{len(rec_ids)} returned IDs "
                  f"are also in heard_ids — pool_search exclusion leaked!")

    return {"recommendations": out, "meta": meta}
