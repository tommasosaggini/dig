"""
DIG → Instagram curation queue — data access + business logic.

Keeps server.py thin: every /admin/ig/* endpoint is a small wrapper around a
function here. Pure-Python scheduling/selection logic lives here too so it can
be unit-tested without a live DB or HTTP layer (see tests/test_ig_queue.py).

Status lifecycle (see scripts/migrate_ig_post_queue.sql):
    suggested → needs_audio → needs_clip → ready → scheduled → publishing
              → published | failed | skipped
"""
import datetime
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.db import get_conn, fetchall, fetchone, execute

# How long a posted snippet runs. Lives here because the schema default, the
# window picker and the renderer all have to agree — when this was written out
# as a bare 30000 in each of them, changing it meant finding all three plus the
# admin copy, and a miss would show up as a clip that silently disagrees with
# the button that made it. Instagram allows far longer for Reels; the limit is
# attention, not the API.
CLIP_MS = 45000

# Where rendered media + downloaded source audio live (gitignored: media/).
MEDIA_ROOT = os.path.join(ROOT, "media", "ig")

# Cadence config (env-overridable). Default: a post every 2 days at 18:00 UTC.
CADENCE_HOURS = int(os.environ.get("IG_CADENCE_HOURS", "48"))
POST_HOUR_UTC = int(os.environ.get("IG_POST_HOUR_UTC", "18"))

# Statuses that still occupy a track "slot" (block re-suggesting the same track).
ACTIVE_STATUSES = (
    "suggested", "needs_audio", "needs_clip", "ready", "scheduled",
    "publishing", "published",
)

# Columns returned to the dashboard.
_SELECT_COLS = (
    "id, track_id, track_name, artist, artwork_url, audio_source, "
    "audio_path, audio_duration_ms, clip_start_ms, clip_duration_ms, caption, "
    "post_feed, post_story, queue_order, scheduled_at::text, status, error, "
    "ig_media_id, ig_story_media_id, rendered_at::text, published_at::text, "
    "created_at::text, updated_at::text"
)


def item_dir(item_id):
    """Per-item media directory (source.mp3, clip.mp3, feed.mp4, story.mp4, …)."""
    return os.path.join(MEDIA_ROOT, str(item_id))


# ── schema ────────────────────────────────────────────────────────────────────

def ensure_ig_schema():
    """Idempotent DDL — mirrors scripts/migrate_ig_post_queue.sql so a fresh
    deploy self-provisions the table (same pattern as ensure_access_schema)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ig_post_queue (
                    id                BIGSERIAL PRIMARY KEY,
                    track_id          TEXT,
                    track_name        TEXT NOT NULL,
                    artist            TEXT NOT NULL,
                    artwork_url       TEXT,
                    audio_source      TEXT,
                    audio_path        TEXT,
                    audio_duration_ms INTEGER,
                    clip_start_ms     INTEGER,
                    clip_duration_ms  INTEGER NOT NULL DEFAULT 45000,
                    caption           TEXT,
                    post_feed         BOOLEAN NOT NULL DEFAULT TRUE,
                    post_story        BOOLEAN NOT NULL DEFAULT TRUE,
                    queue_order       INTEGER NOT NULL DEFAULT 0,
                    scheduled_at      TIMESTAMPTZ,
                    status            TEXT NOT NULL DEFAULT 'suggested',
                    error             TEXT,
                    ig_media_id       TEXT,
                    ig_story_media_id TEXT,
                    rendered_at       TIMESTAMPTZ,
                    published_at      TIMESTAMPTZ,
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ig_post_queue_status_idx ON ig_post_queue (status)")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ig_post_queue_order_idx ON ig_post_queue (queue_order)")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ig_post_queue_sched_idx ON ig_post_queue (scheduled_at)")
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ig_post_queue_active_track_idx
                ON ig_post_queue (track_id)
                WHERE track_id IS NOT NULL AND status NOT IN ('skipped', 'failed')
                """
            )
        conn.commit()
    finally:
        conn.close()


# ── scheduling ──────────────────────────────────────────────────────────────

def _post_hour_after(dt):
    """First POST_HOUR_UTC moment strictly after dt."""
    candidate = dt.replace(hour=POST_HOUR_UTC, minute=0, second=0, microsecond=0)
    if candidate <= dt:
        candidate += datetime.timedelta(days=1)
    return candidate


def next_slot(last_scheduled, now=None):
    """Next free publish time given the latest already-scheduled time.

    - No prior schedule → the next POST_HOUR_UTC after `now`.
    - Otherwise → CADENCE_HOURS after the last one, but never in the past
      (fast-forward in whole cadence steps so a stale queue doesn't dump).
    Pure function (now injectable) so it's unit-testable.
    """
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if last_scheduled is None:
        return _post_hour_after(now)
    nxt = last_scheduled + datetime.timedelta(hours=CADENCE_HOURS)
    while nxt <= now:
        nxt += datetime.timedelta(hours=CADENCE_HOURS)
    return nxt


def _latest_scheduled_at():
    row = fetchone(
        "SELECT max(scheduled_at) AS m FROM ig_post_queue "
        "WHERE scheduled_at IS NOT NULL AND status IN ('ready','scheduled','publishing','published')"
    )
    return row["m"] if row else None


# ── reads ──────────────────────────────────────────────────────────────────

def list_queue():
    """Whole queue for the dashboard, in the order the admin arranged it.

    queue_order wins for everything still live. It used to sort by status
    bucket first — ready, then needs-clip, then suggested — which quietly
    overrode any hand-picked sequence: a freshly queued run of tracks sorted
    below older ready ones no matter what order you dragged them into, and
    reordering appeared to do nothing. Readiness is a pipeline detail and the
    badge already shows it; running order is an editorial decision and belongs
    to whoever set it.

    Terminal states still sink: published/skipped/failed are history, not
    running order.
    """
    return fetchall(
        f"""
        SELECT {_SELECT_COLS} FROM ig_post_queue
        ORDER BY
          CASE WHEN status IN ('published', 'skipped', 'failed') THEN 1
               ELSE 0 END,
          queue_order ASC,
          COALESCE(scheduled_at, 'infinity'::timestamptz) ASC,
          created_at ASC
        """
    )


def get_item(item_id):
    return fetchone(
        f"SELECT {_SELECT_COLS} FROM ig_post_queue WHERE id = %s", (item_id,))


# ── candidate selection (from the admin's likes) ─────────────────────────────

def _already_queued_track_ids():
    rows = fetchall(
        "SELECT track_id FROM ig_post_queue WHERE track_id IS NOT NULL "
        "AND status <> 'skipped'")
    return {r["track_id"] for r in rows}


def _recently_posted_artists(limit=8):
    rows = fetchall(
        "SELECT lower(artist) AS a FROM ig_post_queue "
        "WHERE status IN ('ready','scheduled','publishing','published') "
        "ORDER BY COALESCE(scheduled_at, created_at) DESC LIMIT %s", (limit,))
    return {r["a"] for r in rows}


def pick_candidates(admin_uid, n=3):
    """Choose up to n liked tracks to suggest, spread across genres and avoiding
    artists posted recently. Returns a list of track dicts (not yet inserted).

    Deterministic-ish: round-robins genres, breaks ties by a stable hash so the
    same run is reproducible (no Math.random reliance), but rotates as the queue
    grows because excluded sets change.
    """
    if not admin_uid:
        return []
    liked = fetchall(
        """
        SELECT t.id, t.name, t.artist, t.genres, t.year, t.album, t.popularity,
               t.label_energy, t.label_mood, t.label_texture, t.label_feel
        FROM user_history h
        JOIN tracks t ON t.id = h.track_id
        WHERE h.user_id = %s AND h.status = 'saved'
        """,
        (admin_uid,),
    )
    if not liked:
        return []

    queued = _already_queued_track_ids()
    recent_artists = _recently_posted_artists()
    pool = [
        t for t in liked
        if t["id"] not in queued
        and (t["artist"] or "").lower() not in recent_artists
    ]
    if not pool:
        return []

    # Bucket by primary genre so the suggestions span the taste, not one cluster.
    buckets = {}
    for t in pool:
        genres = list(t.get("genres") or [])
        key = genres[0] if genres else "_unknown"
        buckets.setdefault(key, []).append(t)
    # Stable ordering inside each bucket (hash of id → reproducible shuffle).
    for key in buckets:
        buckets[key].sort(key=lambda t: _stable_hash(t["id"]))

    # Round-robin across genre buckets, ordering the buckets themselves by a
    # stable hash so we don't always start from the same genre.
    bucket_order = sorted(buckets, key=lambda g: _stable_hash(g))
    out, seen_artists = [], set()
    idx = 0
    while len(out) < n and any(buckets.values()):
        key = bucket_order[idx % len(bucket_order)]
        idx += 1
        bucket = buckets.get(key) or []
        chosen = None
        while bucket:
            cand = bucket.pop(0)
            a = (cand["artist"] or "").lower()
            if a in seen_artists:
                continue
            chosen = cand
            break
        if chosen:
            seen_artists.add((chosen["artist"] or "").lower())
            out.append(chosen)
        if idx > len(bucket_order) * 50:  # safety: avoid infinite loop
            break
    return out


def _stable_hash(s):
    import hashlib
    return int(hashlib.sha1((s or "").encode()).hexdigest()[:12], 16)


# ── writes ──────────────────────────────────────────────────────────────────

def add_item(track_id=None, track_name=None, artist=None, status="suggested",
             caption=None):
    """Insert a queue row. If track_id is given and name/artist omitted, fills
    them from the tracks table. Returns the new id (or None if a duplicate
    active row already exists for that track)."""
    if track_id and (not track_name or not artist):
        row = fetchone("SELECT name, artist FROM tracks WHERE id = %s", (track_id,))
        if row:
            track_name = track_name or row["name"]
            artist = artist or row["artist"]
    track_name = track_name or "(untitled)"
    artist = artist or "(unknown)"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Max queue_order so new items land at the end.
            cur.execute("SELECT COALESCE(max(queue_order), 0) + 1 AS n FROM ig_post_queue")
            order = cur.fetchone()[0]
            try:
                cur.execute(
                    """
                    INSERT INTO ig_post_queue
                        (track_id, track_name, artist, status, caption, queue_order)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (track_id, track_name, artist, status, caption, order),
                )
                new_id = cur.fetchone()[0]
            except Exception:
                conn.rollback()
                return None
        conn.commit()
        return new_id
    finally:
        conn.close()


def _set(item_id, **fields):
    """Generic column setter. Only whitelisted columns are writable."""
    allowed = {
        "track_name", "artist", "artwork_url", "audio_source", "audio_path",
        "audio_duration_ms", "clip_start_ms", "clip_duration_ms", "caption",
        "post_feed", "post_story", "scheduled_at", "status", "error",
        "ig_media_id", "ig_story_media_id", "rendered_at", "published_at",
    }
    sets, params = [], []
    for k, v in fields.items():
        if k not in allowed:
            continue
        sets.append(f"{k} = %s")
        params.append(v)
    if not sets:
        return
    params.append(item_id)
    execute(f"UPDATE ig_post_queue SET {', '.join(sets)} WHERE id = %s", params)


def update_item(item_id, **fields):
    """Dashboard edit: caption / clip window / schedule / format toggles."""
    _set(item_id, **fields)
    return get_item(item_id)


def approve_candidate(item_id):
    """suggested → needs_audio (admin said 'queue this one')."""
    item = get_item(item_id)
    if not item:
        return None
    # If audio is somehow already present, jump straight to clip-picking.
    nxt = "needs_clip" if item.get("audio_path") else "needs_audio"
    _set(item_id, status=nxt, error=None)
    return get_item(item_id)


def skip_item(item_id):
    _set(item_id, status="skipped")
    return get_item(item_id)


def set_audio(item_id, source, path, duration_ms, artwork_url=None):
    """Resolver result → needs_clip."""
    _set(item_id, audio_source=source, audio_path=path,
         audio_duration_ms=duration_ms, artwork_url=artwork_url,
         status="needs_clip", error=None)
    return get_item(item_id)


def set_audio_failed(item_id, error):
    _set(item_id, error=str(error)[:500])  # stays in needs_audio for manual upload


def approve_publish(item_id, when=None):
    """Content-complete → scheduled. Requires audio + a picked clip window.
    Assigns the next cadence slot unless `when` is provided."""
    item = get_item(item_id)
    if not item:
        return {"error": "not_found"}
    if not item.get("audio_path"):
        return {"error": "no_audio"}
    if item.get("clip_start_ms") is None:
        return {"error": "no_clip"}
    scheduled = when or next_slot(_latest_scheduled_at())
    _set(item_id, status="scheduled", scheduled_at=scheduled, error=None)
    return {"ok": True, "item": get_item(item_id)}


def unschedule(item_id):
    """Take a scheduled post back off the calendar.

    Approving is one click and publishing is irreversible, so there has to be a
    way back that is not "ask someone to edit the database". Only works while
    the post is still pending: once it is out on Instagram, unscheduling it
    here would just make our records lie about what is public.
    """
    item = get_item(item_id)
    if not item:
        return {"error": "not_found"}
    if item.get("published_at") or item.get("ig_media_id"):
        return {"error": "already_published"}
    if item.get("status") == "publishing":
        return {"error": "publishing_now"}
    _set(item_id, status="ready", scheduled_at=None, error=None)
    return {"ok": True, "item": get_item(item_id)}


def reorder(ordered_ids):
    """Rewrite queue_order to match the given id sequence."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for pos, iid in enumerate(ordered_ids):
                cur.execute(
                    "UPDATE ig_post_queue SET queue_order = %s WHERE id = %s",
                    (pos, iid))
        conn.commit()
    finally:
        conn.close()


# ── job-queue reads (used by the pipeline scripts) ──────────────────────────

def items_needing_audio(limit=20):
    """Items still missing their source audio — suggestions included.

    A suggestion can't be judged as a post until you can hear the snippet
    and see the card, so fetching audio is part of *preparing* the review, not
    a consequence of approving it. The decision that carries real weight is
    approve-to-publish, and that gate is untouched.
    """
    return fetchall(
        f"SELECT {_SELECT_COLS} FROM ig_post_queue "
        "WHERE status IN ('suggested','needs_audio') AND audio_path IS NULL "
        "ORDER BY queue_order ASC LIMIT %s", (limit,))


def items_needing_render(limit=20):
    """Anything renderable whose media isn't built yet (or has gone stale).

    Rendering is local ffmpeg + Pillow — no API call, no quota, a couple of
    seconds. So it is not worth gating behind approval: if an item has audio
    and a clip window, build it and let the dashboard show a real preview
    instead of a button. Skipped and published items are the only exclusions.
    """
    return fetchall(
        f"SELECT {_SELECT_COLS} FROM ig_post_queue "
        "WHERE status NOT IN ('skipped','published','publishing') "
        "AND clip_start_ms IS NOT NULL "
        "AND audio_path IS NOT NULL AND rendered_at IS NULL "
        "ORDER BY COALESCE(scheduled_at, 'infinity'::timestamptz) ASC LIMIT %s",
        (limit,))


def items_due_for_publish(limit=5):
    return fetchall(
        f"SELECT {_SELECT_COLS} FROM ig_post_queue "
        "WHERE status = 'scheduled' AND rendered_at IS NOT NULL "
        "AND scheduled_at <= now() ORDER BY scheduled_at ASC LIMIT %s", (limit,))
