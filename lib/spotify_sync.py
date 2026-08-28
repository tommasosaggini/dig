"""
DIG — pull what Spotify already knows back into DIG's history.

THE HOLE THIS FILLS
-------------------
DIG's history was a record of what DIG *dispatched*, not of what the listener
*heard*. The two diverge constantly on iOS, because the Spotify app is the
playback device and it makes its own choices:

  * a deep link opens an album, DIG adopts one track, Spotify walks the rest
  * an AirPods double-tap lands on a track outside DIG's pool
  * the listener plays something in the Spotify app with DIG closed

None of that reached user_history. `queue.adoptExternalTrack` had a branch for
exactly this case and it recorded nothing: no id in the pool, so a stub went on
the navigation stack "and nowhere else". Measured 2026-08-03 against
/me/player/recently-played: **26 of the last 50 plays were missing**, including
every track around "We Are The People" that the listener asked about by name.

The same asymmetry existed for likes. `/save` mirrors DIG's saves UP to Spotify
(the DIG playlist, or Liked Songs if the user pointed it there), but nothing
ever came back DOWN. A song hearted in the Spotify app was invisible to DIG:
empty heart, and — worse — no positive taste signal, so the picker never
learned from it.

So: two pulls, both merged into the same rows the browser writes.

  recently-played  → user_history  (status 'listened')
  saved tracks     → user_history  (status 'saved') + user_ledger ('liked')

WHY A MERGE AND NOT AN INSERT
-----------------------------
POST /history used to replace the user's whole history with the browser's
localStorage. Anything the server learned on its own survived until the next
sync — minutes. scripts/migrate_spotify_sync.sql adds the (user_id, track_id)
key that turns that write into an upsert, and dig_status_rank() so neither
writer can silently downgrade the other's status. See db_save_history.

WHAT THIS DELIBERATELY DOES NOT DO
----------------------------------
* It does not add external tracks to the `tracks` pool. History is a record of
  listening; the pool is curated, and a mainstream track heard once has not
  earned a place in it. The rows carry name + artist, which is what the feed,
  the artist-coverage counters and the taste profile read.
* It does not detect UN-likes. Spotify offers no removal feed; only additions
  are pulled. Un-liking in DIG still works both ways (db_unsave + mirror_save).
* It does not backfill a pre-existing Liked Songs library. The first run seeds
  the cursor from the newest page and moves on, so a 2,000-song library from
  before DIG doesn't land in the taste profile as one enormous batch.
  scripts/import_likes.py is the deliberate, explicit bulk-import path.

QUOTA
-----
Spotify Development Mode is permanent for this app and its quota is app-wide;
bursts cost ~18h lockouts (see lib/spotify_gate). This costs TWO calls per
sync, gated to one sync per SYNC_MIN_INTERVAL_S per user — an order of
magnitude below what the playback poll already spends.
"""

import datetime
import os
import time

from lib.db import get_conn, fetchall, fetchone

# One sync per user per this many seconds. GET /history triggers a sync, and a
# reload-happy listener would otherwise turn every page open into two Spotify
# calls. Five minutes is well inside recently-played's 50-item window at any
# plausible listening rate (50 tracks ≈ 3 hours).
SYNC_MIN_INTERVAL_S = int(os.environ.get("DIG_SPOTIFY_SYNC_INTERVAL", "300"))

# Pages of 50 Liked Songs to walk in one incremental run. A listener who
# hearted more than this since the last sync gets the rest on the next one.
LIKED_MAX_PAGES = 4

# Runaway guard for the full backfill, not a policy: 500 pages is 25,000
# tracks, past any real library. The walk normally ends on a short page.
LIKED_FULL_MAX_PAGES = 500

# Mirrors STATUS_RANK in web/js/app.js and dig_status_rank() in SQL. Three
# copies is two too many, but they live in three languages; the SQL one is
# authoritative for stored rows and is what actually resolves a conflict.
#
# 'served' is the floor: it means a track was put in front of the listener and
# nothing was measured, so ANY other status carries more information and wins.
# See the header comment on `history` in web/js/app.js for why it exists.
STATUS_RANK = {"saved": 5, "disliked": 5, "listened": 3, "skipped": 2, "served": 1}


def outranks(new_status, old_status):
    """True if `new_status` may overwrite `old_status`.

    Equal ranks overwrite (saved↔disliked toggle each other, later wins) —
    same rule the client applies.
    """
    return STATUS_RANK.get(new_status, 0) >= STATUS_RANK.get(old_status, 0)


def iso_to_ms(value):
    """Spotify timestamp → JS milliseconds, the unit user_history stores.

    Spotify is inconsistent about fractional seconds ('...:12Z' and
    '...:12.345Z' both appear in the same recently-played response), which is
    why this isn't a single strptime.
    """
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def _track_fields(track):
    """(id, name, artist) from a Spotify track object, or None if unusable.

    Local files have no id, and a podcast episode in the recently-played feed
    has no `artists`. Neither belongs in a music history.
    """
    if not isinstance(track, dict):
        return None
    track_id = track.get("id")
    if not track_id:
        return None
    artists = track.get("artists") or []
    names = [a.get("name") for a in artists if isinstance(a, dict) and a.get("name")]
    if not names:
        return None
    return track_id, track.get("name") or "", ", ".join(names)


def recent_rows(items, region_by_id=None, now_ms=None):
    """recently-played items → history rows, newest occurrence per track.

    Pure. `region_by_id` carries the pool's region for tracks DIG does know, so
    a play that arrives via this path still feeds the country-coverage weight
    instead of counting as a blank.

    Duplicates inside one response collapse to the most recent play, because
    user_history is one row per track (the client's model, now the table's).
    """
    region_by_id = region_by_id or {}
    rows = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        fields = _track_fields(item.get("track"))
        if not fields:
            continue
        track_id, name, artist = fields
        played_at = iso_to_ms(item.get("played_at")) or now_ms or int(time.time() * 1000)
        prev = rows.get(track_id)
        if prev and prev["listened_at"] >= played_at:
            continue
        rows[track_id] = {
            "id": track_id,
            "track": name,
            "artist": artist,
            "region": region_by_id.get(track_id) or "",
            # 'listened', not 'served' — and this is the ONE writer outside the
            # accumulator allowed to say so. recently-played only lists a track
            # once it passed ~30s, which is Spotify's own stream threshold and
            # exactly the bar the client's LISTEN_STREAM_MS holds itself to.
            # Same evidence, arriving from Spotify instead of from our own poll.
            "status": "listened",
            "listened_at": played_at,
            # How FAR it got is still unknown, honestly. Spotify does not say,
            # and a fabricated number would poison the completeness signal that
            # quality scoring reads.
            "played_pct": None,
            # Not one of the client's modes (discovery/journey/aimix/tailored)
            # on purpose: these plays are the ones DIG did NOT choose, and the
            # analytics need to be able to tell them apart.
            "mode": "external",
            "source": "spotify",
        }
    return list(rows.values())


def liked_rows(items, region_by_id=None):
    """saved-track items → (history rows, newest added_at ms).

    Pure. Each row carries its own `ledger_key` — 'artist - track' lowercased,
    the same form db_add_known / db_unsave use, so an un-save from either side
    lines up. Keeping the key ON the row rather than in a parallel list is
    deliberate: the caller filters rows by cursor, and a parallel list would
    have to be filtered in lockstep or silently mis-pair keys with tracks.
    """
    region_by_id = region_by_id or {}
    rows = {}
    newest = None
    for item in items or []:
        if not isinstance(item, dict):
            continue
        fields = _track_fields(item.get("track"))
        if not fields:
            continue
        track_id, name, artist = fields
        added_at = iso_to_ms(item.get("added_at"))
        if added_at is not None and (newest is None or added_at > newest):
            newest = added_at
        key = f"{artist} - {name}".strip().lower()
        rows[track_id] = {
            "ledger_key": key if name and artist else None,
            "id": track_id,
            "track": name,
            "artist": artist,
            "region": region_by_id.get(track_id) or "",
            "status": "saved",
            # When they liked it, not when we noticed. A like pulled a week
            # late must not jump to the top of the feed.
            "listened_at": added_at,
            "played_pct": None,
            "mode": "external",
            "source": "spotify",
        }
    return list(rows.values()), newest


def unseen_liked(rows, newest_seen_ms):
    """The liked rows added after the cursor. Pure.

    Rows with no added_at are kept: an unknown date is not evidence that we've
    already got it, and the upsert is idempotent anyway.
    """
    if newest_seen_ms is None:
        return list(rows)
    return [r for r in rows if r.get("listened_at") is None
            or r["listened_at"] > newest_seen_ms]


# ── Persistence ──────────────────────────────────────────────────────────────

def regions_for(track_ids):
    """{track_id: region} for the ids the pool knows. Missing ids are absent."""
    ids = [t for t in (track_ids or []) if t]
    if not ids:
        return {}
    rows = fetchall("SELECT id, region FROM tracks WHERE id = ANY(%s)", (ids,))
    return {r["id"]: r["region"] for r in rows if r.get("region")}


# One upsert, used by both pulls and by POST /history, so there is exactly one
# place that decides how two writers reconcile a row.
#
# COALESCE on the text fields is not cosmetic: the browser posts stub rows for
# tracks it adopted mid-play, and a stub that arrives after a full row must not
# blank the name the pull already resolved.
#
# `mode` is absent from the SET list on purpose. It records how a track was
# FIRST surfaced and a later listen doesn't change that (same rule as
# addToHistory). Filling a NULL from EXCLUDED looked harmless and wasn't: the
# first run relabelled ~40 rows from before mode existed as mode='external',
# i.e. asserted that DIG hadn't chosen tracks it may well have. An unknown
# mode has to stay unknown.
_HISTORY_COLUMNS = """(user_id, track_id, track_name, artist, region, status,
                       listened_at, played_pct, mode, source)"""

# ONE conflict clause, two entry points (single-row and batched). Written
# twice, the two would drift, and the thing that drifts is how two writers
# reconcile a row — the exact invariant this whole feature rests on.
_HISTORY_CONFLICT = """
    ON CONFLICT (user_id, track_id) DO UPDATE SET
        status      = CASE WHEN dig_status_rank(EXCLUDED.status)
                              >= dig_status_rank(user_history.status)
                           THEN EXCLUDED.status ELSE user_history.status END,
        listened_at = GREATEST(COALESCE(EXCLUDED.listened_at, 0),
                               COALESCE(user_history.listened_at, 0)),
        track_name  = COALESCE(NULLIF(EXCLUDED.track_name, ''), user_history.track_name),
        artist      = COALESCE(NULLIF(EXCLUDED.artist, ''),     user_history.artist),
        region      = COALESCE(NULLIF(EXCLUDED.region, ''),     user_history.region),
        played_pct  = COALESCE(EXCLUDED.played_pct, user_history.played_pct),
        source      = COALESCE(user_history.source, EXCLUDED.source)
"""

UPSERT_HISTORY_SQL = (
    f"INSERT INTO user_history {_HISTORY_COLUMNS}\n"
    f"    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)\n"
    f"{_HISTORY_CONFLICT}"
)

UPSERT_HISTORY_BATCH_SQL = (
    f"INSERT INTO user_history {_HISTORY_COLUMNS}\n"
    f"    VALUES %s\n"
    f"{_HISTORY_CONFLICT}"
)


def dedupe_rows(rows):
    """Last row wins per track id. Pure.

    Postgres refuses an ON CONFLICT DO UPDATE that would touch the same row
    twice in ONE statement ("cannot affect row a second time"), so a batched
    upsert must not contain a duplicate key. The pulls already collapse by id;
    this is the guard for anything else that feeds the batch — a streaming
    export, above all, where the same track appears hundreds of times.
    """
    out = {}
    for r in rows:
        if r.get("id"):
            out[r["id"]] = r
    return list(out.values())


def upsert_history(cur, user_id, rows, batch_size=500):
    """Merge rows into user_history. Returns the number written.

    Batched: one round trip per `batch_size` rows. The single-row version was
    631 sequential round trips for one likes backfill over an SSH-tunnelled
    connection, and a streaming-history import is two orders of magnitude
    bigger than that.
    """
    rows = dedupe_rows(rows)
    if not rows:
        return 0
    from psycopg2.extras import execute_values
    values = [
        (user_id, r["id"], r.get("track"), r.get("artist"), r.get("region"),
         r.get("status"), r.get("listened_at"), r.get("played_pct"),
         r.get("mode"), r.get("source"))
        for r in rows
    ]
    execute_values(cur, UPSERT_HISTORY_BATCH_SQL, values, page_size=batch_size)
    return len(values)


def upsert_ledger_liked(cur, user_id, keys):
    """Flag these track keys 'liked' in the ledger.

    Never downgrades: a track the listener disliked in DIG and happens to have
    in an old Spotify playlist keeps its dislike, because the dislike is the
    more recent, more deliberate statement about DIG's own pool.
    """
    seen, values = set(), []
    for key in keys:
        if key and key not in seen:      # same one-row-per-statement rule
            seen.add(key)
            values.append((user_id, key, "liked"))
    if not values:
        return 0
    from psycopg2.extras import execute_values
    execute_values(
        cur,
        """
        INSERT INTO user_ledger (user_id, track_key, status)
        VALUES %s
        ON CONFLICT (user_id, track_key) DO UPDATE SET status = 'liked'
        WHERE user_ledger.status <> 'disliked'
        """,
        values, page_size=500,
    )
    return len(values)


def due_for_sync(last_sync_ms, now_ms, min_interval_s=SYNC_MIN_INTERVAL_S):
    """Rate gate. Pure, so the interval is testable without a clock."""
    if not last_sync_ms:
        return True
    return (now_ms - last_sync_ms) >= min_interval_s * 1000


# ── Spotify I/O ──────────────────────────────────────────────────────────────

def make_user_client(token_info):
    """A gated spotipy client for this user's token.

    Gated deliberately: lib.spotify_gate paces every Spotify call in the whole
    app through one cross-process lane and short-circuits during a 429
    cooldown. A sync that ignored it would be the burst that costs the next
    18 hours.
    """
    from lib.spotify_gate import make_client
    access = token_info.get("access_token") if isinstance(token_info, dict) else token_info
    if not access:
        return None
    return make_client(auth=access)


def sync_user(user_id, token_info, now_ms=None, force=False,
              min_interval_s=SYNC_MIN_INTERVAL_S, client=None,
              full_likes=False):
    """Pull recently-played + Liked Songs for one user and merge them.

    `full_likes` walks the ENTIRE saved-tracks library instead of stopping at
    the cursor. It exists because DIG is meant to be the ledger of what its
    listener likes, and a ledger that starts the day the feature shipped is not
    one. Costs one call per 50 tracks, so a 2,000-song library is 40 calls —
    fine once, ruinous on a 5-minute timer, which is why it is opt-in.

    Returns a summary dict; never raises for a Spotify-side failure — a stale
    history is a much smaller problem than a /history request that 500s.
    """
    now_ms = now_ms or int(time.time() * 1000)
    summary = {"user": user_id, "ran": False, "listened": 0, "liked": 0}
    if not user_id:
        return summary

    row = fetchone(
        "SELECT spotify_liked_cursor, spotify_sync_at FROM users WHERE id = %s",
        (user_id,),
    ) or {}
    if not force and not due_for_sync(row.get("spotify_sync_at"), now_ms, min_interval_s):
        summary["skipped"] = "rate_gate"
        return summary

    sp = client or make_user_client(token_info)
    if sp is None:
        summary["skipped"] = "no_token"
        return summary

    # Claim the slot BEFORE the network calls. Two concurrent /history requests
    # (the app open in two tabs, or a reload racing the cron) would otherwise
    # both pass the gate and both spend the quota.
    _stamp_sync(user_id, now_ms)
    summary["ran"] = True

    played, liked, newest = [], [], None
    try:
        resp = sp.current_user_recently_played(limit=50)
        played = (resp or {}).get("items") or []
    except Exception as exc:
        summary["recent_error"] = repr(exc)[:160]

    cursor = row.get("spotify_liked_cursor")
    try:
        # LIKED_FULL_MAX_PAGES is a runaway guard, not a policy — 500 pages is
        # 25,000 tracks, past any real library. The loop normally ends on a
        # short page (the end of the library) or at the cursor.
        pages = LIKED_FULL_MAX_PAGES if full_likes else LIKED_MAX_PAGES
        raw = []
        for page in range(pages):
            resp = sp.current_user_saved_tracks(limit=50, offset=page * 50)
            items = (resp or {}).get("items") or []
            raw.extend(items)
            if len(items) < 50:
                break
            if full_likes:
                continue          # walking the whole library; the cursor is irrelevant
            # Everything on this page predates the cursor → nothing newer left.
            oldest = min((iso_to_ms(i.get("added_at")) or 0) for i in items)
            if cursor and oldest <= cursor:
                break
        liked_all, newest = liked_rows(raw)
        # A full backfill deliberately ignores the cursor: the point is to
        # reach the likes made BEFORE it, which unseen_liked would filter out.
        liked = liked_all if full_likes else unseen_liked(liked_all, cursor)
        summary["liked_pages"] = len(raw) // 50 + (1 if len(raw) % 50 else 0)
    except Exception as exc:
        summary["liked_error"] = repr(exc)[:160]

    recent_ids = [(_track_fields(i.get("track")) or (None,))[0] for i in played]
    region_by_id = regions_for([i for i in recent_ids if i]
                               + [r["id"] for r in liked])
    listened = recent_rows(played, region_by_id, now_ms)
    for r in liked:
        r["region"] = region_by_id.get(r["id"]) or ""

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            summary["listened"] = upsert_history(cur, user_id, listened)
            summary["liked"] = upsert_history(cur, user_id, liked)
            upsert_ledger_liked(
                cur, user_id, [r["ledger_key"] for r in liked if r.get("ledger_key")])
            if newest is not None and (cursor is None or newest > cursor):
                cur.execute("UPDATE users SET spotify_liked_cursor = %s WHERE id = %s",
                            (newest, user_id))
                summary["liked_cursor"] = newest
        conn.commit()
    except Exception as exc:
        conn.rollback()
        summary["write_error"] = repr(exc)[:160]
    finally:
        conn.close()
    return summary


def _stamp_sync(user_id, now_ms):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET spotify_sync_at = %s WHERE id = %s",
                        (now_ms, user_id))
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()


def sync_all_users(force=False, min_interval_s=SYNC_MIN_INTERVAL_S,
                   full_likes=False, only_user=None):
    """Cron entry point — keeps history complete for a listener who never
    reopens the web app after a session.

    GET /history already syncs on app open, and recently-played holds ~3 hours
    of listening, so this only matters for the case that motivated the whole
    fix: listening entirely inside the Spotify app, for longer than that
    window, without coming back to DIG.

    Token refresh goes through server._user_token_or_refresh — the SAME loader
    the request path uses. Written twice, the two would drift on exactly the
    thing that already bit this app once: scope preservation across refresh
    (a narrowed scope silently persisted breaks SDK playback app-wide). The
    import is lazy because server imports this module.
    """
    import json
    try:
        from server import _user_token_or_refresh
    except Exception as exc:               # not importable → refuse to guess
        return [{"error": f"cannot load token refresher: {exc!r}"[:200]}]

    rows = fetchall("SELECT user_id, token_data FROM user_tokens")
    if only_user:
        rows = [r for r in rows if r["user_id"] == only_user]
    out = []
    for r in rows:
        try:
            token = _user_token_or_refresh(r["user_id"])
            if not token:
                # Fall back to the stored token verbatim: an unrefreshable
                # token may still be unexpired, and a user we can't refresh is
                # not a reason to stop syncing everyone else.
                token = r.get("token_data")
                if isinstance(token, str):
                    token = json.loads(token)
            out.append(sync_user(r["user_id"], token, force=force,
                                 min_interval_s=min_interval_s,
                                 full_likes=full_likes))
        except Exception as exc:
            out.append({"user": r["user_id"], "error": repr(exc)[:160]})
    return out


if __name__ == "__main__":
    import json
    import sys
    from lib.env import load_env
    load_env()
    argv = sys.argv[1:]
    user = None
    if "--user" in argv:
        user = argv[argv.index("--user") + 1]
    for result in sync_all_users(force="--force" in argv or "--full-likes" in argv,
                                 full_likes="--full-likes" in argv,
                                 only_user=user):
        print(json.dumps(result))
