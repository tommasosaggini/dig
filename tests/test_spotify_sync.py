"""History must record what was HEARD, not what DIG dispatched.

DIG wrote a history row only when it sent the play command. On iOS the Spotify
app is the playback device and it makes its own choices constantly — it walks
the rest of an album a deep link opened, an AirPods tap lands outside the pool,
the listener plays something with DIG closed. `adoptExternalTrack` had a branch
for exactly that and it recorded nothing.

Measured 2026-08-03 against /me/player/recently-played: 26 of the last 50 plays
were absent from user_history, including every track around the one the
listener asked about by name. The same asymmetry ran the other way for likes —
DIG pushed saves UP to Spotify and never pulled any back, so a song hearted in
the Spotify app showed an empty heart and fed the picker nothing.

Two fixes, and the tests below split along them:

  * the PULL (lib/spotify_sync) — turn Spotify's own record into history rows
  * the MERGE — POST /history used to DELETE the user's whole history and
    re-insert the browser's localStorage, so anything a second writer learned
    lived for minutes. That is the invariant the static checks defend.

    python3 tests/test_spotify_sync.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib import spotify_sync as S   # noqa: E402


def _item(track_id, name="Song", artists=("Artist",), played_at=None, added_at=None):
    item = {"track": {"id": track_id, "name": name,
                      "artists": [{"name": a} for a in artists]}}
    if played_at:
        item["played_at"] = played_at
    if added_at:
        item["added_at"] = added_at
    return item


# ── The pull ─────────────────────────────────────────────────────────────────

def test_timestamps_parse_with_and_without_fractional_seconds():
    """Spotify mixes both forms inside a single recently-played response.

    A single strptime handles one and silently returns None for the other, and
    a None played_at falls back to "now" — which would date a play from three
    hours ago as if it had just happened and put it at the top of the feed.
    """
    assert S.iso_to_ms("2026-08-02T19:36:12Z") == 1785699372000
    assert S.iso_to_ms("2026-08-02T19:36:12.345Z") == 1785699372345
    assert S.iso_to_ms("") is None
    assert S.iso_to_ms("not a date") is None


def test_unusable_items_are_dropped_not_recorded_blank():
    """Local files carry no id; podcast episodes carry no artists.

    Both appear in recently-played. A row with a NULL track_id cannot be merged
    (it has no key), and one with a blank artist poisons the artist-coverage
    counters, which is the weight that fixed the narrow-pool complaint.
    """
    rows = S.recent_rows([
        _item(None, played_at="2026-08-02T19:00:00Z"),
        {"track": {"id": "podcast1", "name": "Ep 1", "artists": []},
         "played_at": "2026-08-02T19:05:00Z"},
        {"track": None, "played_at": "2026-08-02T19:06:00Z"},
        _item("good", played_at="2026-08-02T19:10:00Z"),
    ])
    assert [r["id"] for r in rows] == ["good"]


def test_a_repeated_track_collapses_to_its_most_recent_play():
    """user_history is one row per track — the client's model since forever
    (addToHistory finds by id and updates in place), and now the table's key.

    Two rows for the same track would violate the unique index and abort the
    whole merge transaction, taking the other 49 plays down with it.
    """
    rows = S.recent_rows([
        _item("t1", played_at="2026-08-02T19:00:00Z"),
        _item("t1", played_at="2026-08-02T21:00:00Z"),
        _item("t1", played_at="2026-08-02T20:00:00Z"),
    ])
    assert len(rows) == 1
    assert rows[0]["listened_at"] == S.iso_to_ms("2026-08-02T21:00:00Z")


def test_played_pct_is_left_unknown_rather_than_invented():
    """Spotify reports that a track played, never how far it got.

    played_pct drives quality scoring and the completeness signal. A default of
    0 would read as an instant skip and a default of 100 as a full listen;
    both are fabrications about tracks we have no measurement for.
    """
    rows = S.recent_rows([_item("t1", played_at="2026-08-02T19:00:00Z")])
    assert rows[0]["played_pct"] is None
    assert rows[0]["status"] == "listened"
    assert rows[0]["mode"] == "external", "must be distinguishable from DIG's own picks"


def test_pool_region_is_carried_so_the_play_still_counts_for_coverage():
    rows = S.recent_rows(
        [_item("known", played_at="2026-08-02T19:00:00Z"),
         _item("unknown", played_at="2026-08-02T19:01:00Z")],
        region_by_id={"known": "West Africa"},
    )
    by_id = {r["id"]: r for r in rows}
    assert by_id["known"]["region"] == "West Africa"
    assert by_id["unknown"]["region"] == ""


def test_multiple_artists_join_the_way_the_pool_writes_them():
    """`_allArtists` in app.js splits on ',' to credit every collaborator.

    Join them any other way ('feat.', ' & ') and a two-artist track counts as
    one unknown name, so the artist-repeat penalty never sees either of them.
    """
    rows = S.recent_rows([_item("t1", artists=("Stephen Marley", "Mos Def"),
                                played_at="2026-08-02T19:00:00Z")])
    assert rows[0]["artist"] == "Stephen Marley, Mos Def"


def test_a_like_is_dated_when_it_was_made_not_when_it_was_noticed():
    """The cron pulls likes up to three hours late, and one that jumped to
    'now' would sit at the top of the feed above music played since."""
    rows, newest = S.liked_rows([_item("t1", name="Hey Baby",
                                       artists=("Stephen Marley",),
                                       added_at="2026-08-02T19:53:00Z")])
    assert rows[0]["status"] == "saved"
    assert rows[0]["listened_at"] == S.iso_to_ms("2026-08-02T19:53:00Z")
    assert newest == S.iso_to_ms("2026-08-02T19:53:00Z")


def test_the_ledger_key_matches_the_form_save_and_unsave_use():
    """db_add_known / db_unsave key on lower('artist - track').

    Any other form writes a second, unreachable ledger row: un-saving in DIG
    would leave the pulled 'liked' flag standing forever.
    """
    rows, _ = S.liked_rows([_item("t1", name="Hey Baby",
                                  artists=("Stephen Marley",),
                                  added_at="2026-08-02T19:53:00Z")])
    assert rows[0]["ledger_key"] == "stephen marley - hey baby"


def test_the_liked_cursor_only_lets_through_what_is_actually_new():
    """/me/tracks is offset-paged and always returns the newest 50. Without the
    cursor every sync would re-write the same 50 likes, and every one of them
    would bump a listened_at the listener never touched."""
    cursor = S.iso_to_ms("2026-08-02T12:00:00Z")
    rows, _ = S.liked_rows([
        _item("old", added_at="2026-08-01T10:00:00Z"),
        _item("new", added_at="2026-08-02T19:00:00Z"),
    ])
    fresh = S.unseen_liked(rows, cursor)
    assert [r["id"] for r in fresh] == ["new"]
    # First-ever run has no cursor: take the page, don't drop it.
    assert len(S.unseen_liked(rows, None)) == 2


def test_a_like_with_no_date_is_kept_not_assumed_seen():
    rows, _ = S.liked_rows([_item("t1", added_at=None)])
    assert rows[0]["listened_at"] is None
    assert len(S.unseen_liked(rows, S.iso_to_ms("2026-08-02T12:00:00Z"))) == 1


# ── Status precedence ────────────────────────────────────────────────────────

def test_an_automatic_listen_can_never_overwrite_an_explicit_save():
    """This is the whole reason the merge is safe to run with two writers.

    The browser holds a track as 'listened' and posts it; meanwhile the pull
    has learned the listener hearted it on Spotify. Without the rank the POST
    would land last and erase the like.
    """
    assert not S.outranks("listened", "saved")
    assert not S.outranks("skipped", "disliked")
    assert S.outranks("saved", "listened")
    # Equal rank overwrites: saved and disliked toggle each other, later wins.
    assert S.outranks("disliked", "saved")
    assert S.outranks("saved", "disliked")


def test_the_rate_gate_is_measured_not_assumed():
    """Development Mode quota is app-wide and bursts cost ~18h lockouts, so a
    reload-happy listener must not turn every page open into two calls."""
    now = 1785699372000
    assert S.due_for_sync(None, now) is True, "never synced → must run"
    assert S.due_for_sync(now - 299_000, now, 300) is False
    assert S.due_for_sync(now - 301_000, now, 300) is True


# ── The merge (static: these guard invariants, not behaviour) ────────────────

def test_the_upsert_resolves_conflicts_by_rank_on_the_track_key():
    sql = S.UPSERT_HISTORY_SQL
    assert "ON CONFLICT (user_id, track_id) DO UPDATE SET" in sql, (
        "DO NOTHING would make every write after the first a silent no-op: a "
        "save would never reach the row, a re-listen would never bump its date"
    )
    assert "dig_status_rank(EXCLUDED.status)" in sql
    assert "GREATEST" in sql, "a re-listen must move listened_at forward, never back"
    # Statuses arrive from two writers with no ordering guarantee, so the
    # resolution has to live in the statement rather than in whoever ran last.
    assert "status      = CASE WHEN" in sql


def test_the_single_row_and_batched_upserts_cannot_drift():
    """Two copies of the conflict clause would drift, and what drifts is how
    two writers reconcile a row — the invariant the whole feature rests on."""
    assert S._HISTORY_CONFLICT in S.UPSERT_HISTORY_SQL
    assert S._HISTORY_CONFLICT in S.UPSERT_HISTORY_BATCH_SQL
    assert "VALUES %s" in S.UPSERT_HISTORY_BATCH_SQL, "execute_values needs the bare placeholder"


def test_a_batch_never_carries_the_same_track_twice():
    """Postgres refuses an ON CONFLICT DO UPDATE that would touch one row twice
    in a single statement, and the whole batch aborts with it.

    A streaming-history export is exactly this shape: the same track appears
    hundreds of times. Last row wins, so the newest play survives.
    """
    rows = S.dedupe_rows([
        {"id": "t1", "listened_at": 1, "status": "listened"},
        {"id": "t2", "listened_at": 2, "status": "listened"},
        {"id": "t1", "listened_at": 3, "status": "saved"},
        {"id": None, "listened_at": 4, "status": "listened"},
    ])
    assert len(rows) == 2
    keep = {r["id"]: r for r in rows}
    assert keep["t1"]["listened_at"] == 3, "the later row must win"


def test_a_full_backfill_reaches_past_the_cursor():
    """The cursor exists to skip likes we already have. A backfill whose whole
    purpose is the likes made BEFORE it must not be filtered by it."""
    src = open(os.path.join(ROOT, "lib", "spotify_sync.py"), encoding="utf-8").read()
    body = src[src.index("    cursor = row.get(\"spotify_liked_cursor\")"):]
    body = body[:body.index("recent_ids =")]
    assert "liked = liked_all if full_likes else unseen_liked(liked_all, cursor)" in body, (
        "the full backfill is being filtered by the incremental cursor"
    )
    assert "LIKED_FULL_MAX_PAGES if full_likes else LIKED_MAX_PAGES" in body, (
        "the full backfill is still capped at the incremental page limit"
    )


def test_post_history_no_longer_deletes_the_users_history():
    """The clobber that made a second writer impossible.

    `DELETE FROM user_history WHERE user_id = %s` meant every row the server
    learned on its own survived until the next sync — minutes. It also meant a
    browser with cleared localStorage wiped the server copy.
    """
    src = open(os.path.join(ROOT, "server.py"), encoding="utf-8").read()
    body = src[src.index("def db_save_history("):]
    body = body[:body.index("\ndef ", 1)]
    assert "DELETE FROM user_history" not in body, (
        "POST /history is replacing the history again — the Spotify pull's "
        "rows will not survive the next client sync"
    )
    assert "UPSERT_HISTORY_SQL" in body, (
        "must share the one upsert with the pull, so there is a single place "
        "that decides how two writers reconcile a row"
    )


def test_get_history_triggers_the_pull_without_blocking_on_it():
    """The whole app boots inside `loadHistory().then(...)`.

    Two gated Spotify calls (1.5s pacing) in front of that is 3-4s before the
    first track paints on every cold open — a worse bug than the one being
    fixed. So the pull runs on a thread and the response comes from the DB.
    """
    src = open(os.path.join(ROOT, "server.py"), encoding="utf-8").read()
    block = src[src.index('if parsed.path == "/history":'):]
    block = block[:block.index("db_get_history(user_id)")]
    assert "_spawn_history_sync(user_id)" in block
    code = "\n".join(ln for ln in block.splitlines()
                     if not ln.lstrip().startswith("#"))
    assert "spotify_sync.sync_user(" not in code, (
        "syncing inline again — /history is on the app's boot path"
    )
    spawn = src[src.index("def _spawn_history_sync("):]
    spawn = spawn[:spawn.index("\n\n\n")]
    assert "threading.Thread" in spawn and "daemon=True" in spawn


def test_the_client_collects_what_the_background_pull_found():
    """Answering from the DB means the pulled rows land after boot.

    Without a re-fetch the listener sees them only on the NEXT page load — and
    a like made in the Spotify app shows an empty heart for the whole session,
    which is half of what this fix was for.
    """
    src = open(os.path.join(ROOT, "web", "js", "app.js"), encoding="utf-8").read()
    assert "setTimeout(_refreshHistoryFromServer" in src
    body = src[src.index("async function _refreshHistoryFromServer("):]
    body = body[:body.index("\nfunction _repaintReactionButtons")]
    assert "RANK[row.status]" in body, (
        "the client must apply the same precedence as dig_status_rank, or the "
        "next POST pushes the weaker status straight back"
    )
    assert "_repaintReactionButtons" in body, (
        "a like pulled from Spotify has to reach the button, not just the array"
    )


def test_the_reaction_buttons_have_one_painter():
    """Two copies of the heart/dislike logic drift, and the one that drifts is
    the one telling the listener whether they liked this."""
    src = open(os.path.join(ROOT, "web", "js", "app.js"), encoding="utf-8").read()
    assert src.count("classList.toggle('saved'") == 1, (
        "the saved-button paint is written more than once"
    )


def test_the_migration_establishes_the_key_the_merge_needs():
    sql = open(os.path.join(ROOT, "scripts", "migrate_spotify_sync.sql"),
               encoding="utf-8").read()
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_user_history_user_track" in sql
    assert "CREATE OR REPLACE FUNCTION dig_status_rank" in sql
    # Duplicates must be collapsed BEFORE the unique index, or the migration
    # aborts on any account that ever recorded the same track twice.
    assert sql.index("DELETE FROM user_history a") < sql.index("CREATE UNIQUE INDEX")


def test_an_external_track_is_recorded_as_it_plays():
    """The branch that used to record nothing.

    The server pull catches these within minutes, but only the browser can
    record them AS THEY HAPPEN — which is what the feed and the in-session
    artist-coverage counters read.
    """
    src = open(os.path.join(ROOT, "web", "js", "app.js"), encoding="utf-8").read()
    body = src[src.index("adoptExternalTrack(trackId"):]
    body = body[:body.index("_pushPlayed(track, 'external')")]
    unknown = body[body.index("track = stub"):]
    assert "addToHistory(track, 'listened')" in unknown, (
        "a track Spotify chose that DIG doesn't have is still being heard"
    )


def test_an_explicit_unsave_can_demote_its_own_row():
    """Rank blocks automatic events, not the listener.

    Tapping ♥ again wrote 'listened', lost to the stored 'saved', and the entry
    stayed saved while the button showed empty — the stale-heart report. The
    next /history POST then pushed 'saved' back over the server's demote.
    """
    src = open(os.path.join(ROOT, "web", "js", "app.js"), encoding="utf-8").read()
    demotes = [ln for ln in src.splitlines() if "demote to neutral" in ln]
    assert len(demotes) == 2, f"expected the unsave and undislike demotes, got {demotes}"
    for line in demotes:
        assert "force: true" in line, f"demotion cannot take effect: {line.strip()}"
    # …and the guard has to honour it, or the call sites are decoration.
    guard = src[src.index("function addToHistory("):]
    guard = guard[:guard.index("saveHistory();")]
    assert "if (force || newRank >= oldRank)" in guard, (
        "the rank guard is ignoring `force` — an explicit un-set is being "
        "treated as if it were an automatic 'listened'"
    )


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except AssertionError as exc:
                failed += 1
                print(f"FAIL {name}: {exc}")
    if failed:
        print(f"\n{failed} failed")
        sys.exit(1)
    print("all spotify-sync checks passed")
