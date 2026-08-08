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

from lib.db import get_conn, fetchall, fetchone, execute, execute_returning

# How long a posted snippet runs. Lives here because the schema default, the
# window picker and the renderer all have to agree — when this was written out
# as a bare 30000 in each of them, changing it meant finding all three plus the
# admin copy, and a miss would show up as a clip that silently disagrees with
# the button that made it. Instagram allows far longer for Reels; the limit is
# attention, not the API.
CLIP_MS = 45000

# Where rendered media + downloaded source audio live (gitignored: media/).
MEDIA_ROOT = os.path.join(ROOT, "media", "ig")

# Cadence config (env-overridable). Default: one post a day at 18:00 UTC.
CADENCE_HOURS = int(os.environ.get("IG_CADENCE_HOURS", "24"))
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
            # Single-row table: proof the publisher is still being asked to
            # check, independent of whether anything was due. See
            # publisher_health() / pipeline/ig_publish.py._record_heartbeat.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ig_publish_heartbeat (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    last_run_at TIMESTAMPTZ NOT NULL,
                    host TEXT,
                    max_overdue_minutes DOUBLE PRECISION,
                    CHECK (id = 1)
                )
                """
            )
            # Added later: a heartbeat alone says the publisher RAN, not that it
            # could publish. Prod's cron lane shipped without IG_GRAPH_TOKEN and
            # friends, and missing credentials are a dry run — a success path —
            # so it checked in every 15 minutes, found the due item, published
            # nothing and looked perfectly healthy while Lemonade (#16) sat 16
            # hours late.
            cur.execute("ALTER TABLE ig_publish_heartbeat "
                        "ADD COLUMN IF NOT EXISTS can_publish BOOLEAN")
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


def claim_for_publishing(item_id):
    """Atomically move a due item scheduled -> publishing. Returns True iff
    THIS call won the claim.

    Publishing now runs from two independent schedulers — prod's cron, which
    does not sleep, and the laptop's, kept as a backup — so a plain "read
    status, then write status" is a real race: both could see 'scheduled' and
    both call the Graph API, posting the same clip to Instagram twice. The
    UPDATE ... WHERE status = 'scheduled' makes the transition itself the
    check; Postgres serialises concurrent UPDATEs to the same row, so exactly
    one caller's statement matches and gets a row back.
    """
    rows = execute_returning(
        "UPDATE ig_post_queue SET status = 'publishing', error = NULL "
        "WHERE id = %s AND status = 'scheduled' RETURNING id", (item_id,))
    return bool(rows)


def publisher_health():
    """Is anything actually checking for due posts, and is one overdue right now?

    Taitgaral's first post sat 7.5 hours late because the whole pipeline used
    to run from the laptop's cron and the laptop slept through the scheduled
    time — the query that decides what is "due" was correct the entire time,
    nothing was ever wrong with an item, there was simply no process asking.
    That failure mode is invisible unless something surfaces it, so the admin
    page shows both a heartbeat (does the checker still run at all) and any
    post that is late right now, independent of that heartbeat existing.

    `can_publish` is the third question, learned the hard way: a checker that
    runs on time and has no credentials answers "yes I ran" forever while
    publishing nothing.
    """
    hb = fetchone("SELECT last_run_at::text, host, max_overdue_minutes, can_publish "
                  "FROM ig_publish_heartbeat WHERE id = 1")
    overdue = fetchall(
        "SELECT id, track_name, scheduled_at::text, "
        "EXTRACT(EPOCH FROM (now() - scheduled_at)) / 60.0 AS overdue_minutes "
        "FROM ig_post_queue WHERE status = 'scheduled' AND scheduled_at <= now() "
        "ORDER BY scheduled_at ASC"
    )
    return {"heartbeat": hb, "overdue": overdue}


def get_item(item_id):
    return fetchone(
        f"SELECT {_SELECT_COLS} FROM ig_post_queue WHERE id = %s", (item_id,))


# ── candidate selection (from the admin's likes) ─────────────────────────────

# Separators a credit list uses. Comma is the common one, but a Bandcamp or
# SoundCloud credit is as likely to say "&", "x" or "feat." — and the same
# record reaches the pool through more than one of those.
# The trailing \b after an optional dot is wrong and silently half-works:
# "feat." ends on punctuation, so \b never fires there and " feat. Bob" splits
# into "bob" with the dot still attached. Match the word, then eat the dot.
_CREDIT_SPLIT = re.compile(
    r"\s*(?:,|&|/|\+|×|\bx\b|\bfeat\b\.?|\bft\b\.?|\bwith\b|\band\b)\s*",
    re.IGNORECASE)


def track_key(name, artist):
    """Identity of a RECORDING, for telling two rows apart from two copies.

    The artist credit is reduced to a sorted set of names, because Spotify does
    not promise an order and does not keep one: the same Thai posse cut is
    saved twice under

        Jayrun, K6Y, LAZYLOXY, CDGuntee, GUNNER, JV.JARVIS, Nara, …
        Jayrun, Sirpoppa, CDGuntee, LAZYLOXY, NICECNX, GUNNER, …

    — ten identical artists, two different strings, two different ids. Comparing
    the credit as one string made those two separate tracks, so the add-track
    picker offered the same song twice and adding one left its twin sitting
    there looking un-added.

    Pure, so the normalisation is testable without a database, and shared so
    the picker and the already-queued check cannot drift apart on what "the
    same track" means.
    """
    parts = [p.strip() for p in _CREDIT_SPLIT.split((artist or "").lower())]
    credit = ",".join(sorted(p for p in parts if p))
    return (" ".join((name or "").lower().split()), credit)


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


def pick_pool_candidates(n=3, prefer_new_genres=True):
    """Suggest tracks from the POOL rather than from the admin's likes.

    pick_candidates() reads user_history — the admin's Spotify Liked Songs —
    which bounds the Instagram feed to one person's listening on one platform.
    Those likes are dream pop, chillwave, city pop and lo-fi beats, so the feed
    could never post the kuduro, bubbling or chutney the pool has since gained,
    however good that music is. Nothing was wrong with the queue; it was being
    fed through a keyhole.

    This opens the second door. Same guards as the likes path — never an artist
    posted recently, never a track already queued — and the same round-robin
    over genres so a run cannot return five tracks from one scene.

    prefer_new_genres puts genres the feed has NEVER posted first, which is the
    whole point: the account's range should widen as the pool's does.

    Suggestions still land as 'suggested' and still wait for the admin's yes,
    so this widens what gets OFFERED, never what gets published.
    """
    queued = _already_queued_track_ids()
    recent = _recently_posted_artists()
    posted_genres = {r["g"] for r in fetchall(
        "SELECT DISTINCT lower(x) AS g FROM ig_post_queue q "
        "JOIN tracks t ON t.id = q.track_id, unnest(t.genres) x "
        "WHERE q.status NOT IN ('skipped', 'failed')") if r["g"]}

    rows = fetchall(
        """
        SELECT t.id, t.name, t.artist, t.genres, t.year, t.album, t.popularity,
               t.label_energy, t.label_mood, t.label_texture, t.label_feel
        FROM tracks t
        WHERE t.genres IS NOT NULL AND array_length(t.genres, 1) > 0
          AND coalesce(t.name, '') <> '' AND coalesce(t.artist, '') <> ''
        """)
    pool = [t for t in rows
            if t["id"] not in queued
            and (t["artist"] or "").lower() not in recent]
    if not pool:
        return []

    buckets = {}
    for t in pool:
        buckets.setdefault((list(t.get("genres") or []) or ["_unknown"])[0], []).append(t)
    for key in buckets:
        buckets[key].sort(key=lambda t: _stable_hash(t["id"]))

    order = sorted(buckets, key=lambda g: _stable_hash(g))
    if prefer_new_genres:
        order.sort(key=lambda g: (g.lower() in posted_genres, _stable_hash(g)))

    out = []
    while len(out) < n:
        progressed = False
        for g in order:
            if buckets[g]:
                out.append(buckets[g].pop(0))
                progressed = True
                if len(out) >= n:
                    break
        if not progressed:
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
    """Resolver result → needs_clip.

    The resolver's artwork is a yt-dlp video thumbnail — a screen-grab with
    burnt-in titles as often as a sleeve — so it is a LAST RESORT, never an
    overwrite. Re-resolving the audio used to clobber a good cover found by
    lib/cover_art and silently put the YouTube still back on the card, which is
    a bad trade to make for a file the artwork has nothing to do with.
    """
    fields = dict(audio_source=source, audio_path=path,
                  audio_duration_ms=duration_ms, status="needs_clip", error=None)
    current = (get_item(item_id) or {}).get("artwork_url") or ""
    if artwork_url and (not current or "ytimg" in current):
        fields["artwork_url"] = artwork_url
    _set(item_id, **fields)
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


def request_new_source(item_id):
    """Mark an item for re-acquisition from a different upload.

    Does NOT download: yt-dlp and ffmpeg only exist on the studio machine, and
    prod answering "yt-dlp not installed" to a button click is not a useful
    reply. Same shape as render — the dashboard records the intent, the cron on
    the machine that can actually do the work picks it up.

    audio_source is deliberately preserved: the resolver reads it to know which
    candidate was rejected, so the retry advances instead of re-fetching the
    file we just threw away.
    """
    item = get_item(item_id)
    if not item:
        return {"error": "not_found"}
    if item.get("published_at"):
        return {"error": "already_published"}
    _set(item_id, status="needs_audio", audio_path=None,
         rendered_at=None, error=None)
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


def deal_slots(ordered_ids, slot_by_id):
    """Hand a fixed set of publish times back out in a new running order.

    The times belong to the cadence, not to any one post: whatever slots the
    given posts hold are collected, sorted, and dealt out in the order asked
    for. So promoting one post demotes whatever it displaced by exactly one
    slot — the calendar keeps its shape and nothing falls off the end.

    Ids without a slot are skipped (they carry no time to trade). Pure, so the
    dealing is testable without a database — same reason as next_slot().
    """
    live = [int(i) for i in ordered_ids if int(i) in slot_by_id]
    return dict(zip(live, sorted(slot_by_id[i] for i in live)))


def reschedule_slots(ordered_ids):
    """Persist a drag inside the scheduled group.

    Dragging a scheduled post up is a request to publish it *sooner*, which
    queue_order cannot express: once something is scheduled it is held by its
    timestamp, so reordering it looked like it did nothing at all.

    Items already handed to the Graph API ('publishing') keep their time — that
    one is out of our hands — and their slot is not offered to anyone else.

    Returns the new {id: scheduled_at} assignment.
    """
    ids = [int(i) for i in ordered_ids]
    if not ids:
        return {}
    rows = fetchall(
        "SELECT id, scheduled_at, status FROM ig_post_queue "
        "WHERE id = ANY(%s) AND scheduled_at IS NOT NULL AND status = 'scheduled'",
        (ids,))
    current = {r["id"]: r["scheduled_at"] for r in rows}
    assigned = deal_slots(ids, current)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for pos, iid in enumerate(ids):
                cur.execute(
                    "UPDATE ig_post_queue SET queue_order = %s WHERE id = %s",
                    (pos, iid))
            for iid, slot in assigned.items():
                if current[iid] != slot:
                    cur.execute(
                        "UPDATE ig_post_queue SET scheduled_at = %s WHERE id = %s "
                        "AND status = 'scheduled'", (slot, iid))
        conn.commit()
    finally:
        conn.close()
    return assigned


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
