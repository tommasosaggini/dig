"""Unit tests for the Instagram pipeline's pure logic (no DB / no network).

Covers the things most likely to break silently:
  - next_slot() scheduling math (cadence, never-in-the-past fast-forward)
  - deal_slots() — promoting a scheduled post without moving the calendar
  - caption templating (shape + funnel line + hashtags + the headline)
  - the two guards that decide whether we @mention an artist at all

lib.ig_queue imports lib.db at module load but opens NO connection, so this
runs anywhere.

    python3 tests/test_ig_pipeline.py
    pytest tests/test_ig_pipeline.py
"""
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import artist_ig, ig_queue, ig_caption  # noqa: E402

UTC = datetime.timezone.utc


def test_next_slot_from_empty_picks_post_hour():
    now = datetime.datetime(2026, 6, 28, 9, 0, tzinfo=UTC)  # before POST_HOUR_UTC (18)
    slot = ig_queue.next_slot(None, now=now)
    assert slot.hour == ig_queue.POST_HOUR_UTC
    assert slot.date() == now.date()  # same day, later today


def test_next_slot_from_empty_rolls_to_tomorrow_after_hour():
    now = datetime.datetime(2026, 6, 28, 20, 0, tzinfo=UTC)  # after POST_HOUR_UTC
    slot = ig_queue.next_slot(None, now=now)
    assert slot.hour == ig_queue.POST_HOUR_UTC
    assert slot.date() == (now + datetime.timedelta(days=1)).date()


def test_next_slot_advances_by_cadence():
    last = datetime.datetime(2026, 6, 28, 18, 0, tzinfo=UTC)
    now = datetime.datetime(2026, 6, 28, 18, 5, tzinfo=UTC)
    slot = ig_queue.next_slot(last, now=now)
    assert slot == last + datetime.timedelta(hours=ig_queue.CADENCE_HOURS)


def test_next_slot_never_in_the_past():
    # A stale last-slot far in the past must fast-forward past `now`, in whole
    # cadence steps — never dump a backlog by returning a past time.
    last = datetime.datetime(2026, 6, 1, 18, 0, tzinfo=UTC)
    now = datetime.datetime(2026, 6, 28, 9, 0, tzinfo=UTC)
    slot = ig_queue.next_slot(last, now=now)
    assert slot > now
    delta_h = (slot - last).total_seconds() / 3600
    assert delta_h % ig_queue.CADENCE_HOURS == 0  # landed on a cadence boundary


def test_deal_slots_promotes_without_moving_the_calendar():
    # Dragging the last post to the top: it takes the earliest slot and
    # everyone else slides down exactly one. The set of times is unchanged —
    # a promotion must not invent a slot or drop the tail off the end.
    slots = [datetime.datetime(2026, 8, 6 + d, 18, 0, tzinfo=UTC) for d in range(3)]
    current = dict(zip([14, 15, 25], slots))
    assigned = ig_queue.deal_slots([25, 14, 15], current)
    assert assigned[25] == slots[0]
    assert assigned[14] == slots[1]
    assert assigned[15] == slots[2]
    assert sorted(assigned.values()) == slots


def test_deal_slots_ignores_ids_with_no_slot():
    # The unscheduled ones carry no time to trade; offering their nothing to a
    # scheduled post would silently wipe it off the calendar.
    slot = datetime.datetime(2026, 8, 6, 18, 0, tzinfo=UTC)
    assigned = ig_queue.deal_slots([99, 14], {14: slot})
    assert assigned == {14: slot}


def test_headline_carries_song_artist_and_handle():
    assert ig_caption.headline("Maloya sa", "Dany Drack") == "Maloya sa — Dany Drack"
    # Song and artist on the first line, the link on its own beneath it.
    assert ig_caption.headline("Maloya sa", "Dany Drack", "danydrack") \
        == "Maloya sa — Dany Drack\n@danydrack"
    # A handle written with the @ already on it must not become "@@".
    assert ig_caption.headline("Ditto", "NewJeans", "@njz_official") \
        .endswith("\n@njz_official")


def test_ensure_headline_replaces_an_old_headline():
    # The real shape this was written for: an LLM caption that opened with the
    # right words, the wrong dash and no handle.
    old = "Maloya sa - Dany Drack\n\nIsland Vibes\n\n#dig"
    out = ig_caption.ensure_headline(old, "Maloya sa - Version loungue",
                                     "Dany Drack", "danydrack")
    assert out.split("\n")[:2] == ["Maloya sa - Version loungue — Dany Drack",
                                    "@danydrack"]
    assert "Island Vibes" in out          # the body survives
    assert "Maloya sa - Dany Drack" not in out   # the old line does not


def test_ensure_headline_keeps_writing_it_does_not_recognise():
    # First line is a mood, not a headline — prepend rather than overwrite, or
    # the caption loses a line someone wrote.
    out = ig_caption.ensure_headline("candlelit and slow.\n\n#dig", "Unfurl",
                                     "Katatonia", "katatoniaband")
    assert out.split("\n")[:2] == ["Unfurl — Katatonia", "@katatoniaband"]
    assert "candlelit and slow." in out


def test_ensure_headline_is_idempotent():
    # The cron re-runs this over the whole queue every tick; a caption that is
    # already right must come back byte-identical, or every tick is a write.
    once = ig_caption.ensure_headline("", "Puro Teatro", "La Lupe")
    twice = ig_caption.ensure_headline(once, "Puro Teatro", "La Lupe")
    assert once == twice == "Puro Teatro — La Lupe"


def test_template_caption_is_only_the_song_and_the_artist():
    # A caption is the song, the artist, and the handle when we have one.
    # Nothing written, no hashtags — genres and labels are accepted and ignored.
    cap = ig_caption.template_caption(
        "Idioteque", "Radiohead", ["electronic", "idm"],
        {"mood": "playful", "feel": "basement show"})
    assert cap == "Idioteque — Radiohead"
    assert "#" not in cap
    assert ig_caption.BIO_LINE not in cap


def test_template_caption_carries_the_handle_when_known():
    cap = ig_caption.template_caption("Soro", "Salif Keita", ig_handle="salifkeita")
    assert cap == "Soro — Salif Keita\n@salifkeita"


def test_drop_bio_line_leaves_no_gap():
    cap = ("Spooky — Dusty Springfield\n\nsmoky, for a late drive.\n\n"
           + ig_caption.BIO_LINE)
    out = ig_caption.drop_bio_line(cap)
    assert ig_caption.BIO_LINE not in out
    assert "smoky, for a late drive." in out
    assert "\n\n\n" not in out           # no gap left where it stood
    assert ig_caption.drop_bio_line(out) == out


def test_generated_extras_are_stripped_from_an_existing_caption():
    # Exactly the caption sitting on #64 in the queue today.
    cap = ("Perc & Sex — YN Jay\n@ynjay_\n\nplayful, for a basement show.\n\n"
           "#dig #musicdiscovery #nowplaying #augmentedrealityphonk")
    out = ig_caption.drop_generated_extras(
        cap, {"mood": "playful", "feel": "basement show"})
    assert out == "Perc & Sex — YN Jay\n@ynjay_"
    assert ig_caption.drop_generated_extras(out, {"mood": "playful"}) == out


def test_a_hand_written_line_survives_the_sweep():
    # Enforcing a format must not delete Tommaso's own writing.
    cap = "Lemonade — Ria Sean, Randay\n\nVery fresh.\n\n#dig #nowplaying"
    out = ig_caption.drop_generated_extras(cap, {"mood": "sunny", "feel": "car ride"})
    assert out == "Lemonade — Ria Sean, Randay\n\nVery fresh."
    for his in ("Floating about.", "Marching.", "Driving.",
                "I imagine a circus passing by some village in Senegal.",
                "Someone sings it like they mean it."):
        kept = ig_caption.drop_generated_extras(f"S — A\n\n{his}", None)
        assert his in kept, his


def test_a_generated_line_goes_even_after_the_track_was_relabelled():
    # The relabel-from-audio stage rewrites a track's labels after the caption
    # was written, so the stored sentence no longer equals vibe_line(labels)
    # and an exact-match sweep walks straight past it. These four survived the
    # first pass over the real queue for exactly that reason.
    for stale in ("warm, for a garden party.",
                  "mysterious, for a midnight drive.",
                  "bittersweet, for a rooftop sunset.",
                  "rebellious, for a basement show.",
                  "for a rooftop sunset."):
        cap = f"Some Song — Some Artist\n@handle\n\n{stale}"
        out = ig_caption.drop_generated_extras(
            cap, {"mood": "completely", "feel": "different now"})
        assert out == "Some Song — Some Artist\n@handle", stale


def test_the_labelless_fallback_line_is_also_retired():
    # vibe_line(None) wrote this under any track with no labels at all.
    cap = "Song — Artist\n\na gem worth 45 seconds.\n\n#dig"
    assert ig_caption.drop_generated_extras(cap, None) == "Song — Artist"


def test_stable_hash_is_deterministic():
    assert ig_queue._stable_hash("bc:1:2") == ig_queue._stable_hash("bc:1:2")
    assert ig_queue._stable_hash("a") != ig_queue._stable_hash("b")


# ── who we are willing to @mention ──────────────────────────────────────────
# Both guards exist for the same reason: an @mention notifies whoever owns that
# handle. Getting it wrong drags a stranger into a post about someone else.

def test_handle_from_url_takes_profiles_only():
    assert artist_ig.handle_from_url("https://www.instagram.com/clairo/") == "clairo"
    assert artist_ig.handle_from_url("http://instagram.com/oscar.lang?hl=en") == "oscar.lang"
    # A post URL carries the poster's handle nowhere in the path — resolving
    # this to a user called "p" would tag whoever owns @p.
    assert artist_ig.handle_from_url("https://instagram.com/p/Cx123abc/") is None
    assert artist_ig.handle_from_url("https://instagram.com/explore/tags/jazz") is None
    assert artist_ig.handle_from_url("https://twitter.com/clairo") is None
    assert artist_ig.handle_from_url("") is None


def test_pick_artist_refuses_a_tie():
    # Five different acts are called BUKA — a Polish rapper, a Croatian one, a
    # Brooklyn experimental group. MB orders by popularity; taking the top one
    # would be a coin flip on whose Instagram gets tagged.
    buka = [{"name": "Buka", "score": 100, "id": "a"},
            {"name": "Buka", "score": 96, "id": "b"},
            {"name": "BUKA", "score": 93, "id": "c"}]
    assert artist_ig.pick_artist("BUKA", buka) is None
    assert artist_ig.pick_artist("BUKA", buka[:1])["id"] == "a"


def test_pick_artist_takes_a_clear_winner_over_a_namesake():
    # Refusing on the mere existence of a second candidate lost Paramore, whose
    # runner-up is a different band by 17 points. A namesake is not a tie.
    paramore = [{"name": "Paramore", "score": 100, "id": "band"},
                {"name": "Paramore", "score": 83, "id": "indie-duo"}]
    assert artist_ig.pick_artist("Paramore", paramore)["id"] == "band"


def test_pick_artist_ignores_the_generously_scored_wrong_answer():
    # MB returns the closest thing it has even for a name it has never seen,
    # at a score that looks like a match on its own.
    hits = [{"name": "Some Other Band", "score": 88, "id": "x"}]
    assert artist_ig.pick_artist("Djeneba Seck", hits) is None


def test_ensure_headline_does_not_stack_mention_lines():
    # The cron rewrites every caption on every tick. If the handle line were
    # treated as body rather than as part of the headline, each pass would push
    # another @oscar.lang down the post.
    cap = ig_caption.ensure_headline("", "fall into u", "Oscar Lang", "oscar.lang")
    for _ in range(3):
        cap = ig_caption.ensure_headline(cap, "fall into u", "Oscar Lang", "oscar.lang")
    assert cap.count("@oscar.lang") == 1


def test_ensure_headline_migrates_the_old_parenthetical_form():
    old = "fall into u — Oscar Lang (@oscar.lang)\n\ndreamy.\n\n#dig"
    out = ig_caption.ensure_headline(old, "fall into u", "Oscar Lang", "oscar.lang")
    assert out.split("\n")[:2] == ["fall into u — Oscar Lang", "@oscar.lang"]
    assert "(@oscar.lang)" not in out
    assert "dreamy." in out

# ── what counts as the same recording ───────────────────────────────────────

def test_track_key_ignores_credit_order():
    # The bug the add-track picker showed twice: ten identical artists, two
    # orders, two Spotify ids. Spotify does not promise an order and does not
    # keep one, so the credit has to compare as a SET.
    a = "Jayrun, K6Y, LAZYLOXY, CDGuntee, GUNNER, JV.JARVIS, Nara, NICECNX, Sirpoppa, ZENTYARB"
    b = "Jayrun, Sirpoppa, CDGuntee, LAZYLOXY, NICECNX, GUNNER, JV.JARVIS, ZENTYARB, K6Y, Nara"
    assert ig_queue.track_key("\u0e40\u0e23\u0e37\u0e48\u0e2d\u0e07 2024", a) == \
           ig_queue.track_key("\u0e40\u0e23\u0e37\u0e48\u0e2d\u0e07 2024", b)


def test_track_key_survives_the_other_separators():
    # The same record reaches the pool from Spotify, Bandcamp and SoundCloud,
    # and they do not agree on how to join two names.
    k = ig_queue.track_key("Song", "Alice & Bob")
    assert ig_queue.track_key("Song", "Bob, Alice") == k
    assert ig_queue.track_key("Song", "Alice feat. Bob") == k
    assert ig_queue.track_key("Song", "alice  x  BOB") == k


def test_track_key_still_separates_different_line_ups():
    # Sorting must not collapse a solo cut into the collaboration.
    assert ig_queue.track_key("Song", "Alice") != ig_queue.track_key("Song", "Alice, Bob")
    assert ig_queue.track_key("Song A", "Alice") != ig_queue.track_key("Song B", "Alice")


def test_track_key_normalises_whitespace_and_case():
    assert ig_queue.track_key("  Puro   Teatro ", "LA LUPE") == \
           ig_queue.track_key("Puro Teatro", "la lupe")


def test_a_karaoke_release_is_not_this_records_sleeve():
    # iTunes' only hit for "YN Jay Perc & Sex" is a karaoke cover, and _norm
    # strips the brackets — so the title matched 1.00 while the artist matched
    # 0.18, and the post went out wearing "Pristine Karaoke, Vol. 95".
    from lib.cover_art import _plausible
    assert not _plausible(
        "YN Jay", "Perc & Sex",
        "Backing Business",
        "Perc & Sex (Originally Performed by YN Jay) [Karaoke Version]",
        "Pristine Karaoke, Vol. 95")


def test_a_real_release_still_matches_on_title_alone():
    # The title-only branch exists for transliterations, where the artist field
    # cannot possibly match. Rejecting impostors must not close that door.
    from lib.cover_art import _plausible
    assert _plausible("張德蘭", "Blowin' in the Wind", "Teresa Cheung",
                      "Blowin' in the Wind", "Greatest Hits")


def test_karaoke_is_allowed_when_that_is_what_was_asked_for():
    from lib.cover_art import _plausible
    assert _plausible("Pristine Karaoke", "Perc & Sex [Karaoke Version]",
                      "Pristine Karaoke", "Perc & Sex [Karaoke Version]", "")


def _yt(title, seconds):
    return {"title": title, "duration": seconds, "uploader": "ISSAM MUSIC"}


def test_an_official_video_is_not_rejected_for_its_intro():
    # ISSAM's "Nike" runs 2:52 on record and 3:21 as the official video, which
    # a ±12s window called a different recording — so the pipeline reported
    # "no usable YouTube result" for a song on the artist's own channel.
    from lib.ig_audio import _score
    assert _score(_yt("ISSAM - Nike (Official Music Video)", 201),
                  172_000, "ISSAM", "Nike") > -1e8


def test_a_shorter_upload_is_still_rejected():
    # Running short means edit, snippet or sped-up — actually different audio.
    from lib.ig_audio import _score
    assert _score(_yt("ISSAM - Nike (sped up)", 150), 172_000, "ISSAM", "Nike") <= -1e8


def test_the_closer_upload_still_wins():
    from lib.ig_audio import _score
    plain = _score(_yt("ISSAM - Nike", 174), 172_000, "ISSAM", "Nike")
    video = _score(_yt("ISSAM - Nike (Official Music Video)", 201), 172_000,
                   "ISSAM", "Nike")
    assert plain > video


def test_a_punctuated_title_earns_a_normalized_retry():
    # YouTube returned ZERO results for 'ARALIIA !!!<3 SOLO A TI' while the
    # artist's own upload was the first hit for the normalized form — the
    # pipeline said "no youtube source found" for a song sitting in plain
    # sight. An empty first search must retry without the punctuation.
    from lib.ig_audio import _fallback_query
    fb = _fallback_query("ARALIIA !!!<3 SOLO A TI",
                         "ARALIIA", "!!!<3 SOLO A TI")
    assert fb == "araliia 3 solo a ti"


def test_a_clean_query_is_not_searched_twice():
    from lib.ig_audio import _fallback_query
    assert _fallback_query("ISSAM Nike", "ISSAM", "Nike") == ""


def test_a_native_script_upload_passes_on_exact_duration():
    # 'Redpepper Dragonfly' can never appear in '고추 잠자리' — the artist's
    # own upload at the exact release length was gated out by a title check
    # that is structurally blind across scripts. Blind + within ±5s of the
    # release length must pass.
    from lib.ig_audio import _score
    entry = {"title": "고추 잠자리", "duration": 289, "channel": "조용필Official"}
    assert _score(entry, 288_787, "Cho Yong Pil", "Redpepper Dragonfly") > -1e8


def test_blind_matching_stays_tight_on_duration():
    # Without a title match "roughly the right length" is not evidence: the
    # 297s KBS live take and the 330s extended cut must still die, and with
    # no reference duration at all a cross-script title passes nothing.
    from lib.ig_audio import _score
    live = {"title": "조용필 - 고추잠자리 Live", "duration": 297}
    assert _score(live, 288_787, "Cho Yong Pil", "Redpepper Dragonfly") <= -1e8
    noref = {"title": "고추 잠자리", "duration": 289}
    assert _score(noref, None, "Cho Yong Pil", "Redpepper Dragonfly") <= -1e8


def _src(rel):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, rel), encoding="utf-8") as fh:
        return fh.read()


def test_youtube_likes_parse_the_three_upload_shapes():
    from scripts.ingest_youtube_likes import parse_entry
    # A "- Topic" channel is YouTube's own music upload: channel IS the artist.
    assert parse_entry({"title": "Kodo", "channel": "Yoshida Brothers - Topic"}) \
        == ("Yoshida Brothers", "Kodo")
    # "Artist - Title" wins even on a label channel (Majestic Casual uploads
    # someone else's song) — and the tag furniture comes off.
    assert parse_entry({"title": "Aleksandir - I Used To Dream (Official Music Video)",
                        "channel": "Majestic Casual"}) \
        == ("Aleksandir", "I Used To Dream")
    # A bare title on the artist's own channel.
    assert parse_entry({"title": "Daddy Issues", "channel": "The Neighbourhood"}) \
        == ("The Neighbourhood", "Daddy Issues")
    # An em-dash title on the artist's own channel must not duplicate the
    # artist into the song name.
    assert parse_entry({"title": "IVAN VALEEV — NOVELLA",
                        "channel": "IVAN VALEEV"}) == ("IVAN VALEEV", "NOVELLA")
    # A " - " inside a parenthesis is an annotation, not the artist seam.
    a, n = parse_entry({"title": "Snoop Dogg X Dr Dre (Del - 30's Mix)",
                        "channel": "SomeUploader"})
    assert a == "SomeUploader" and n.startswith("Snoop Dogg")


def test_youtube_likes_music_filter_is_keywords_only():
    from scripts.ingest_youtube_likes import is_probably_music
    assert is_probably_music({"title": "Gamma - Mantra", "duration": 142})
    assert not is_probably_music({"title": "Artist interview — full episode",
                                  "duration": 2400})
    # NO duration window: a liked hour-long mix and a 20s interlude are both
    # songs (Tommaso, 2026-08-12) — the dashboard approval is the backstop.
    assert is_probably_music({"title": "Boiler Room Set", "duration": 3600})
    assert is_probably_music({"title": "Gamma - Mantra", "duration": 20})


def test_a_yt_track_id_downloads_its_exact_video():
    # A liked video IS the chosen recording: resolve must go straight at the
    # id, never through the search (which could pick a different upload).
    src = _src("lib/ig_audio.py")
    i = src.index('if track_id.startswith("yt:")')
    assert "_from_youtube_id" in src[i:i + 400]
    j = src.index("def _from_youtube_id")
    assert "watch?v=" in src[j:j + 400]


def test_same_script_mismatch_is_still_rejected():
    # The blind escape must not reopen the original hole: a Latin title that
    # simply is not the song stays gated however good its duration looks.
    from lib.ig_audio import _score
    wrong = {"title": "Some Other Song", "duration": 289}
    assert _score(wrong, 288_787, "Cho Yong Pil", "Redpepper Dragonfly") <= -1e8


def _publish_without_creds(dry_run):
    """Call publish_item with no IG credentials visible. Returns (result, out)."""
    import io, contextlib
    from lib import env as lib_env
    from pipeline import ig_publish
    item = {"id": 16, "track_name": "Lemonade", "artist": "Ria Sean, Randay",
            "post_feed": True, "post_story": True}
    saved = {k: os.environ.pop(k, None) for k in ig_publish.CRED_KEYS}
    # _creds() reads the .env file too, and the studio laptop's real one sits
    # right there — point it at a path that does not exist so this measures the
    # unconfigured box it is about, not the machine running the test.
    real_path = lib_env.ENV_PATH
    lib_env.ENV_PATH = os.path.join(os.path.dirname(real_path), ".env.absent")
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            res = ig_publish.publish_item(item, dry_run=dry_run)
    finally:
        lib_env.ENV_PATH = real_path
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v
    return res, buf.getvalue()


def test_missing_credentials_on_a_due_item_is_an_error_not_a_rehearsal():
    # The bug this exists for: prod's publish lane shipped without the three
    # IG_* variables. Missing credentials took the dry-run branch, which
    # returns a success shape and prints a line identical to a deliberate
    # rehearsal — so prod checked in on time every 15 minutes, found the due
    # item, published nothing, recorded no error and left the status at
    # 'scheduled'. Lemonade (#16) sat 16 hours past its slot that way.
    res, out = _publish_without_creds(dry_run=False)
    assert res.get("error") == "no_creds", res
    assert "MISCONFIGURED" in out
    # Name every variable that is actually missing — the whole failure was
    # nobody being told which half of the config was absent.
    for var in ("IG_GRAPH_TOKEN", "IG_BUSINESS_ACCOUNT_ID", "IG_PUBLIC_MEDIA_BASE"):
        assert var in out


def test_credentials_prefer_the_file_over_a_stale_environment():
    # docker-compose's `env_file:` snapshots .env into the container at create
    # time and never re-reads it, and `docker exec` inherits that snapshot. The
    # token rotates every 30 days, so the environment is the stale copy and the
    # file is the truth — the opposite of lib.env's usual rule, on purpose.
    import tempfile
    from lib import env as lib_env
    from pipeline import ig_publish
    saved = {k: os.environ.get(k) for k in ig_publish.CRED_KEYS}
    real_path = lib_env.ENV_PATH
    fd, path = tempfile.mkstemp(suffix=".env")
    try:
        with os.fdopen(fd, "w") as f:
            f.write("IG_GRAPH_TOKEN=IGAA_fresh\n"
                    "IG_BUSINESS_ACCOUNT_ID='1234'\n")   # quoted: a shell may source this
        lib_env.ENV_PATH = path
        os.environ["IG_GRAPH_TOKEN"] = "IGAA_stale"
        os.environ["IG_BUSINESS_ACCOUNT_ID"] = "9999"
        os.environ["IG_PUBLIC_MEDIA_BASE"] = "https://example.test/m"
        token, ig_id, base = ig_publish._creds()
        assert token == "IGAA_fresh"
        assert ig_id == "1234"
        # Not in the file at all — the environment still answers for it, so a
        # box with no .env keeps working.
        assert base == "https://example.test/m"
    finally:
        lib_env.ENV_PATH = real_path
        os.unlink(path)
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_an_explicit_dry_run_is_still_just_a_rehearsal():
    # --dry-run means "plan only": no credentials needed and nothing wrong.
    res, out = _publish_without_creds(dry_run=True)
    assert res == {"dry_run": True}
    assert "MISCONFIGURED" not in out
    assert "DRY-RUN #16" in out


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"OK: ig pipeline logic passed ({len(fns)} tests)")
