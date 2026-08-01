"""Regression lock for the iPhone "wrong title / Spotify keeps reopening" class.

Reported from the phone (2026-07-31): "when I skip I see some title but the song
playing definitely is not that title, and then the title also sets itself to the
correct song" and "I skip to the next song and Spotify reopens every time, as if
the handshake had never happened".

Three separate defects, all confirmed from 48h of prod logs:

  1. CONTEXT JUMP. Every Connect play hands Spotify DIG_CONNECT_LOOKAHEAD + 1
     track URIs so a locked screen can auto-advance natively. When the suspended
     Spotify app wakes it can resume somewhere ELSE inside that list: DIG
     dispatched 3YuH2kBeqv…, the poll found Spotify on 16wVEQXd8U…, which was
     position 13 of the array we had just sent. The poll classified that as
     `external-skip` — as if the user had hit next on AirPods — and repainted the
     title to match Spotify. That is precisely "wrong title, then it corrects
     itself". A legitimate natural advance is one step and takes a whole track;
     anything else inside our own context must be re-asserted, not followed.

  2. DEEP-LINK PHANTOM ADVANCE. `_confirmDeepLink` checked playback 6s after
     navigating into the Spotify app — by which time iOS had frozen the Safari
     page, so it could not observe anything, read "not playing", and called
     nextTrack(). Over 48h: 3 confirmations, 0 "confirmed playing", 2 spurious
     advances, each leaving the UI one track ahead of the audio.

  3. DEVICE DEATH ACROSS SOURCES. Starting a Bandcamp track pauses the phone's
     Spotify app, and a paused backgrounded app is eligible for iOS to reclaim —
     at which point its Connect device deregisters entirely and the next Spotify
     play 404s into a deep link. Measured: bandcamp->spotify 18 hops, 11 with
     device trouble (61%), 6 deep-linked (33%); spotify->spotify 53 hops, 2
     (3.8%) and 1 (1.9%).

     WHEN iOS reclaims it is not predictable and nothing here may assume it is:
     in the same logs a 1s Bandcamp run lost the device while a 535s one kept it.
     So the fix is not a timer. Spotify stays the default source and DIG only
     leaves it on a PROVEN failure; the device probe, never the lease, decides.

Two earlier answers to (3) were wrong and are locked out by name below, because
both looked reasonable and cost real listening time:

  * deferring Spotify picks whenever the 45s lease had lapsed — a forecast, not
    a fact. It withheld 18,213 Spotify tracks and played 24 consecutive Bandcamp
    ones while the banner sat there.
  * counting `/me/player/devices` length as liveness — an idle 'DIG' web player
    on a Mac at home "proved" the phone was reachable, so DIG promised a play
    the server then had nowhere to send.

Static assertions over all the browser source (see tests/browser_source.py).
They are cheap, they fail loudly if a guard is removed, and each records WHY
the guard exists. What they cannot do is notice that the code, as written,
walks the queue forever — tests/test_playback_behaviour.mjs does that.

    python3 tests/test_ios_playback_divergence.py   # bare, no deps
    pytest tests/test_ios_playback_divergence.py    # if pytest is installed
"""
import os
import re
import sys

from browser_source import ROOT, browser_source



def _app() -> str:
    return browser_source()


def _code_only(src: str) -> str:
    """`src` with `//` and `/* … */` comments removed.

    The comments here quote the very constructs the tests forbid — the fix for a
    bug and the note explaining it name the same symbol — so a bare substring
    search finds the explanation and reports the bug it prevents. Block comments
    matter as much as line ones now that each module opens with a docstring that
    says what it must not do.
    """
    src = re.sub(r"/\*[\s\S]*?\*/", "", src)
    return re.sub(r"(?m)^\s*//.*$", "", src)


def _function_body(src: str, name: str) -> str:
    """The source of `function name(...) { ... }`, brace-matched."""
    start = src.index(f"function {name}")
    depth, i = 0, src.index("{", start)
    for j in range(i, len(src)):
        if src[j] == "{":
            depth += 1
        elif src[j] == "}":
            depth -= 1
            if depth == 0:
                return src[start:j + 1]
    raise AssertionError(f"unbalanced braces in {name}")


def _method_body(src: str, name: str) -> str:
    """The source of an object-literal method `name(args) { … }`, brace-matched.

    The device lifecycle is one object now, so its pieces are methods rather
    than `function _name`. Same brace walk, different opening.
    """
    m = re.search(r"^  " + re.escape(name) + r"\([^)]*\) \{", src, re.M)
    assert m, f"no method {name}(…) in the browser source"
    depth, start = 0, m.start()
    for j in range(m.end() - 1, len(src)):
        if src[j] == "{":
            depth += 1
        elif src[j] == "}":
            depth -= 1
            if depth == 0:
                return src[start:j + 1]
    raise AssertionError(f"unbalanced braces in {name}")


def _bandcamp_handoff() -> str:
    """The Bandcamp branch of Player.play.

    Not _function_body: that looks for `function <name>`, and this one is
    written `Player.play = async function(track)`.
    """
    src = _app()
    i = src.index("Player.play = async function")
    return src[i:src.index("Player._stopConnectPoll = function", i)]


# ── 1. context jump ───────────────────────────────────────────────────────

def test_poll_reasserts_intent_on_a_context_jump():
    """Following Spotify into our own look-ahead is what rewrote the title."""
    src = _app()
    assert "isContextJump" in src, (
        "the context-jump guard is gone: a Spotify app resuming mid-context is "
        "again indistinguishable from an AirPods skip, and the title will "
        "repaint to a track the user never chose"
    )
    jump = src[src.index("const isContextJump"):]
    jump = jump[:jump.index("// Categorise")]
    assert "Player.play(want)" in jump, (
        "a context jump must re-assert DIG's intent, not just be logged"
    )
    assert "return;" in jump, (
        "after re-asserting, the poll must NOT fall through to the repaint"
    )


def test_a_confirmed_track_is_never_second_guessed():
    """The guard may only overrule a track Spotify was never seen playing.

    This replaces an ELAPSED-TIME rule (a step to position 1 counted as natural
    only 30s+ after our dispatch) that broke the AirPods double-tap. On the
    Connect path the Spotify app owns the Now Playing session, so a double-tap
    never reaches DIG — zero `media` events in the 6h to 2026-08-01 22:54, with
    activeSource null on all 18 visibility changes. The tap goes to Spotify,
    Spotify steps to position 1 of our look-ahead, and DIG saw "position 1, only
    5.2s since the last play" and re-asserted the old track (22:47:51). The skip
    worked and DIG undid it, which from the earbuds is a dead button.

    A waking app resumes into the wrong slot INSTEAD of arriving where it was
    sent; a user skips away FROM a track that was playing correctly. So arrival,
    not elapsed time, is what separates them.
    """
    src = _app()
    body = src[src.index("const isContextJump"):]
    body = _code_only(body[:body.index("if (isContextJump")])
    assert "ctxPos > 1" in body, (
        "position 1 is either a natural end-of-track advance or the user pressing "
        "next; both are wanted, and neither may be re-issued"
    )
    assert "_connectTrackConfirmed" in body, (
        "the guard must stop second-guessing once Spotify has been SEEN on DIG's "
        "track — otherwise it re-asserts over the user's own skips"
    )
    assert not re.search(r"msSinceLastPlay\s*[<>]", body), (
        "elapsed time cannot distinguish a botched resume from a fast user skip; "
        "reintroducing it re-breaks the AirPods double-tap"
    )


def test_confirmation_is_earned_by_arrival_and_reset_by_dispatch():
    """The flag must mean 'Spotify was seen here', not 'we asked for this'."""
    src = _app()
    assert re.search(r"st\.trackId === _connectTrackId\) \{\s*\n\s*"
                     r"_contextJumpRecoveries = 0;\s*\n(?:.*\n)*?\s*"
                     r"_connectTrackConfirmed = true;", src), (
        "confirmation must be set by the POLL observing Spotify on the intended "
        "track; setting it on dispatch would make every dispatch self-certifying"
    )
    assert re.search(r"_connectTrackId = trackId;(?:.*\n)*?\s*"
                     r"_connectTrackConfirmed = false;", src), (
        "a new dispatch must clear confirmation, or the guard stays disabled for "
        "the rest of the session after the first confirmed track"
    )


def test_context_jump_recovery_is_bounded():
    """A track Spotify refuses to play must not ping-pong forever."""
    src = _app()
    assert re.search(r"_contextJumpRecoveries\s*<\s*\d", src), (
        "the re-issue loop lost its bound"
    )
    reset = re.search(r"if \([^)]*st\.trackId === _connectTrackId\) \{\s*\n\s*"
                      r"_contextJumpRecoveries = 0;", src)
    assert reset, (
        "the counter must reset on CONFIRMED arrival on the intended track — "
        "resetting on dispatch defeats the bound, since each re-issue returns "
        "ok, clears the counter, and can jump again forever"
    )


def test_track_changed_logs_the_track_it_replaced():
    """_lastState is overwritten by this tick's getState before the log runs.

    Every track-changed line in the 48h to 2026-07-31 logged fromName == toName,
    which made the divergence unreadable in exactly the logs meant to reveal it.
    """
    src = _app()
    log = src[src.index("clientLog('connect', `track-changed:"):]
    log = log[:log.index("});") + 3]
    assert "_clockBefore" in log, (
        "fromName must come from the pre-getState snapshot, not _lastState"
    )
    assert "_lastState?.trackName" not in log, (
        "reading _lastState here yields the NEW track's name for both fields"
    )
    assert "ctxPos" in log, (
        "log the context position — it is what identified this bug"
    )


# ── 2. deep-link phantom advance ──────────────────────────────────────────

def test_only_the_caller_moves_the_queue():
    """Player.play reports outcomes; playCurrentTrack decides what plays next.

    When the unreachable-Spotify branch ALSO advanced (via _onTrackEnd) and
    returned plain false, the caller handled the same failure a second time: its
    700ms warm-up retry fired against a dIdx the first advance had already
    moved, so it dispatched the NEXT track rather than re-trying this one, which
    failed the same way, advanced again, and armed another retry. Measured
    2026-08-01 22:16:05 → 22:16:09 — one 404 became three Spotify tracks
    dispatched and abandoned in 3.7s, each painting its title on the way past.
    That is the "titles come and go" of a failed handshake.
    """
    src = _app()
    branch = src[src.index("SpotifyDevice.lost(data.no_device"):]
    branch = _code_only(branch[:branch.index("return UNPLAYABLE;")])
    assert "_onTrackEnd" not in branch, (
        "the play path must not advance the queue — the caller does, and both "
        "doing it burns a track per failure"
    )
    # The slice ends at the return, so check it in the wider function.
    assert "return UNPLAYABLE;" in src, (
        "the caller cannot tell a hopeless failure from a retryable one without "
        "being told, and would spend a warm-up retry on a missing device"
    )
    # And the caller must act on it without the generic retry.
    handler = src[src.index("if (ok === UNPLAYABLE)"):]
    handler = handler[:handler.index("if (!ok) {")]
    assert "_skipToNextTrack" in handler, (
        "UNPLAYABLE must move on once, through the single owner of dIdx"
    )


def test_advancing_after_a_failure_has_exactly_one_implementation():
    """Three inlined copies of the advance idiom is how they drifted apart.

    The generic failure path used to omit `delete t._playRetried`, which leaves
    the flag set on the track OBJECT and silently denies that track its warm-up
    retry the next time the queue comes round to it.
    """
    src = _app()
    assert src.count("function _skipToNextTrack") == 1
    body = src[src.index("function _skipToNextTrack"):]
    body = body[:body.index("\nfunction ", 1)]
    assert "delete t._playRetried" in body and "dIdx++" in body
    # No failure path may hand-roll the advance any more.
    # Matched by NAME, not by an exact parameter list: pinning the literal
    # `playCurrentTrack() {` made adding an argument look like the function had
    # been deleted, and this check would have passed vacuously on a rename.
    m = re.search(r"function playCurrentTrack\s*\(", src)
    assert m, "playCurrentTrack is gone — this check would pass on nothing"
    play_fn = src[m.start():]
    play_fn = _code_only(play_fn[:play_fn.index("\nfunction ", 1)])
    assert play_fn.count("dIdx++") == 0, (
        "playCurrentTrack must advance only through _skipToNextTrack"
    )


def test_deep_link_does_not_judge_a_frozen_page():
    """A deep link backgrounds Safari; the timer fired into a page that was
    frozen and could not see playback, so it always advanced."""
    body = _function_body(_app(), "_confirmDeepLink")
    assert "visibilityState" in body, (
        "the confirmation must not run while the page is hidden — that is how "
        "the UI ended up one track ahead of the audio"
    )
    assert "visibilitychange" in body, (
        "if hidden, re-arm for when the user returns rather than advancing blind"
    )
    hidden = body[body.index("visibilityState === 'hidden'"):]
    hidden = hidden[:hidden.index("_check('timer')")]
    assert "nextTrack" not in hidden, (
        "the hidden branch must never advance the queue"
    )


# ── 3. device death across sources ────────────────────────────────────────

def test_the_picker_keeps_to_spotify_once_it_works():
    """Spotify is the source; Bandcamp is the fallback. Not co-equal.

    This is the exact INVERSE of the deferral it replaces, and the inversion is
    the point. That one narrowed to BANDCAMP whenever the device looked dead,
    which fed the chain that killed it: every Bandcamp track pauses Spotify, a
    paused backgrounded app is what iOS reclaims, a reclaimed app deregisters
    its device, and the next Spotify pick deep-links into a COLD app that opens
    the track page without playing it. Measured 2026-07-31: 18,213 Spotify
    tracks withheld, 24 Bandcamp in a row, every deep link after that dead.
    """
    body = _function_body(_app(), "_pickDiscoveryStratified")
    assert "!_isBandcampTrack(t)" in body, (
        "while Spotify works the pool must exclude Bandcamp, not prefer it"
    )
    assert "isUnavailable()" in body, (
        "the narrowing must be gated on whether Spotify actually works"
    )
    # And the mirror: not-narrowing-to-Spotify is not the same as narrowing to
    # Bandcamp. The pool is roughly three-fifths Spotify, so merely dropping the
    # preference still serves Spotify most of the time — every pick a guaranteed
    # 404 that burns a title. Reported as "the titles got skipped 3 times".
    assert "_isBandcampTrack(t))" in body and "bandcampOnly" in body, (
        "once Spotify is proven dead the pool must narrow TO Bandcamp, not "
        "merely stop preferring Spotify"
    )


def test_keeping_to_spotify_can_never_empty_the_pool():
    """Running discovery dry would be worse than the failure this prevents."""
    body = _function_body(_app(), "_pickDiscoveryStratified")
    narrow = body[body.index("!_isBandcampTrack(t)"):]
    narrow = narrow[:narrow.index("SpotifyDevice.pollForReturn")]
    assert re.search(r"spotifyOnly\.length\s*>=\s*\d+", narrow), (
        "narrow only while a real Spotify subset remains"
    )


def test_it_is_ios_only():
    """On desktop the Connect device is DIG's own tab and never sleeps, so
    interleaving costs nothing — the narrowing follows the problem instead of
    becoming the blanket platform rule DIG_ONLY_SOURCE warns about."""
    body = _function_body(_app(), "_pickDiscoveryStratified")
    narrow = body[body.index("!_isBandcampTrack(t)") - 300:body.index("!_isBandcampTrack(t)")]
    assert "DIG_IS_IOS" in narrow


def test_the_fallback_keeps_asking_whether_spotify_is_back():
    """Otherwise one failure strands the session on Bandcamp forever."""
    body = _function_body(_app(), "_pickDiscoveryStratified")
    assert "SpotifyDevice.pollForReturn()" in body
    poll = _method_body(_app(), "pollForReturn")
    assert "this.probe(" in poll and "provenUnreachable" in poll


def test_spotify_is_only_paused_when_it_is_actually_playing():
    """The pause is what makes the device reclaimable, so it must be earned.

    It used to fire on every Bandcamp track. Through a run of them it poked a
    dead app over and over — every one of those calls answered
    `nothing_to_pause`, which is the proof it had no work to do.
    """
    body = _bandcamp_handoff()
    assert "spotifyWasPlaying" in body, "pause only when Connect is making sound"
    i = body.index("/api/pause")
    assert "if (spotifyWasPlaying)" in body[:i], "the guard must precede the call"


def test_the_playing_flag_is_read_before_it_is_cleared():
    """_stopConnectPoll sets _connectPlaying = false, and a source test would
    not work here either: _lastDispatchedSource is already the NEW track by the
    time Player.play runs."""
    body = _bandcamp_handoff()
    capture = body.index("const spotifyWasPlaying = _connectPlaying")
    # The CALL, not the name: the comment above the capture mentions
    # _stopConnectPoll and matched first.
    stop = body.index("Player._stopConnectPoll &&")
    assert capture < stop, "capture before the call that clears it"


def test_the_return_poll_is_ios_only_and_evidence_based():
    """What survives of the old deferral: iOS-only, and driven by asking
    Spotify rather than by a timer.

    _shouldDeferSpotifyPicks is GONE — it narrowed to Bandcamp on a lapsed 45s
    lease, i.e. a forecast, and that forecast withheld 18,213 Spotify tracks.
    Only the probe that lets Spotify come BACK remains, and it must stay
    evidence-based for the same reason.
    """
    body = _method_body(_app(), "pollForReturn")
    assert "DIG_IS_IOS" in body, "desktop's Connect device is DIG's own tab"
    assert "provenUnreachable" in body, (
        "keyed off observed failure, never a hardcoded platform rule"
    )
    assert "this.probe(" in body, (
        "without a probe the latch can never clear and Spotify is gone for good"
    )


def test_leaving_spotify_requires_a_real_failure():
    """The latch is set only where a play actually came back with no device."""
    src = _app()
    assert "SpotifyDevice.giveUp(" in src
    setter = src[src.index("SpotifyDevice.giveUp(") - 900:]
    setter = setter[:1000]
    assert "no_device" in setter or "data.error" in setter, (
        "the fallback must be reached from a failed play, not from a timer"
    )


def test_any_sign_of_life_returns_to_spotify():
    """Coming back has to be cheap, or one blip strands the user on Bandcamp."""
    body = _method_body(_app(), "saw")
    assert "provenUnreachable = false" in body, (
        "a landed play, a poll that sees playback, or a probe that finds a "
        "device must all end the fallback"
    )


def test_a_spotify_5xx_is_retried_even_with_no_pinned_device():
    """A 5xx means the device WAS found and its gateway did not answer.

    Flos Virginum, 2026-07-31: the deep link created the device (probe one
    second earlier reported names ["iPhone"], usable 1), then the play returned
    502 after 4.9s. The only retry guard required Player._connectDeviceId to be
    set client-side — and it wasn't, because the SERVER had picked and woken
    the iPhone during its own recovery. So a transient error got no retry at
    all and DIG declared Spotify unreachable while the device sat there
    working. That is "Spotify opens but nothing plays".
    """
    # Anchored on the condition, not a character window around the log line:
    # adding a comment above it silently pushed the condition out of a fixed
    # 400-char slice and failed this for having nothing to look at.
    src = _app()
    assert "spotify_(500|502|503)" in src, "5xx must be treated apart from the 404"
    i = src.index("spotify_(500|502|503)")
    cond = src[src.rindex("if (", 0, i):i]
    assert "Player._connectDeviceId &&" not in cond, (
        "the retry must not depend on a client-pinned device — the server "
        "picks one during recovery and the client never sees it"
    )


def test_the_5xx_retry_targets_the_device_the_server_used():
    """Retrying device-less against a woken device just 404s.

    2026-07-31: 502 on 177ee437…, retry sent `device=-`, 404, "Spotify
    unreachable", Bandcamp — while the probe reported the iPhone present one
    second earlier. The client has no id of its own here; the server picks and
    wakes a device inside its own 404 recovery, so it has to say which.
    """
    src = _app()
    i = src.index("spotify 5xx — device is up but not answering yet")
    block = src[i - 700:i + 700]
    assert "data.device" in block, "use the device the server reported"
    assert "_tryPlay(onDevice)" in block, "and retry against it, not device-less"


def test_the_server_reports_the_device_on_a_failed_play():
    """The client cannot target what the server does not name."""
    server_py = os.path.join(ROOT, "server.py")
    server = open(server_py, encoding="utf-8").read()
    i = server.index('"error": f"spotify_{e.code}"')
    assert '"device": device_id' in server[i:i + 300], (
        "a failed play must say which device it failed on"
    )


def test_the_5xx_retry_is_bounded_and_waits():
    src = _app()
    assert "transientRetried" in src, "one extra attempt per play, never a loop"
    assert re.search(r"const _WOKEN_DEVICE_SETTLE_MS = \d+", src), (
        "a just-woken app needs a beat; the server's 0.4s covers a backgrounded "
        "device, not an evicted one"
    )
    i = src.index("!transientRetried")
    assert "transientRetried = true" in src[i:i + 300], "the flag must be set before retrying"


def test_a_confirmed_deep_link_reclaims_the_queue():
    """A deep link hands Spotify the track's ALBUM as its context, not DIG's.

    2026-07-31: "Bear Day" was deep-linked and played, and when it ended
    Spotify followed with "Float On Baby" by the same artist — ctxPos -1, i.e.
    not among the 19 tracks DIG believed it had queued. The picker never ran.
    A normal Connect play sends DIG's URIs; the deep link cannot, because it
    only fires when that play failed. Once the link is confirmed playing the
    device exists, so the play can be re-issued and the context becomes ours.
    """
    body = _function_body(_app(), "_confirmDeepLink")
    confirmed = body[body.index("if (playing && matched)"):]
    confirmed = confirmed[:confirmed.index("if (playing && !matched)")]
    assert "Player.play(onNow)" in confirmed, (
        "re-issue the play so DIG's URI list replaces the album context"
    )


def test_the_queue_is_reclaimed_at_most_once_per_link():
    """The confirm can run twice — timer, then visibility."""
    body = _function_body(_app(), "_confirmDeepLink")
    assert "let contextReclaimed = false" in body, "per link, not global"
    assert "contextReclaimed = true" in body
    i = body.index("contextReclaimed = true")
    assert "if (!contextReclaimed)" in body[:i]


def test_reclaiming_only_happens_on_the_track_still_showing():
    """If the user skipped on while the link was resolving, re-issuing would
    yank them back to the old track."""
    body = _function_body(_app(), "_confirmDeepLink")
    confirmed = body[body.index("if (!contextReclaimed)"):]
    assert "onNow.id === id" in confirmed


def test_a_cold_start_retries_the_same_track_before_advancing():
    """The deep link is cold-start sensitive, not unreliable.

    Measured 2026-07-31 over every deep link of the day: all 7 fired while at
    least one device was listed began playing; the one fired at `count: 0` did
    not — it opened the track page and sat there, which is "Spotify opens on
    the album of the song, but nothing plays". That first link still leaves the
    app RESIDENT, so the retry lands on a warm Spotify, which is the case that
    works. Advancing to a different track instead discarded the warm-up and
    then failed the next Spotify track for the same missing device.
    """
    body = _function_body(_app(), "_confirmDeepLink")
    assert "_deepLinkAdvances === 1" in body, (
        "the first failure is a cold start; it deserves a retry, not a skip"
    )
    retry = body[body.index("_deepLinkAdvances === 1"):]
    retry = retry[:retry.index("nextTrack(true)")]
    assert "playCurrentTrack()" in retry, "retry the SAME track, not the next one"
    assert "SpotifyDevice.saw(" in retry, (
        "Spotify is warm after the link — writing it off here strands the "
        "session on Bandcamp"
    )


def test_it_still_gives_up_rather_than_looping():
    """One retry. Repeating the link is the 'Spotify reopens every song' bug."""
    body = _function_body(_app(), "_confirmDeepLink")
    assert "nextTrack(true)" in body, "a second failure must move on"
    assert "_deepLinkAdvances >= 3" in body, "and the whole thing stays bounded"


def test_nothing_switches_apps_without_a_tap():
    """The automatic handshake is gone, not merely rate-limited.

    It deep-linked into Spotify on the first no-device play of a session —
    which is the normal state of picking up your phone — so the first thing DIG
    did on being opened was throw the listener into another app. One
    interruption per session was the old bargain; the new one is none, and the
    banner offers the trip instead.
    """
    import os
    from browser_source import JS

    for fname in sorted(os.listdir(JS)):
        if not fname.endswith(".js"):
            continue
        with open(os.path.join(JS, fname), encoding="utf-8") as fh:
            code = _code_only(fh.read())
        for m in re.finditer(r"location\.href\s*=\s*[`'\"]spotify:", code):
            # The only permitted navigation is the one a tap starts.
            around = code[max(0, m.start() - 400):m.start()]
            assert "beginHandshake" in around, (
                f"web/js/{fname} navigates to Spotify outside beginHandshake — "
                "every trip into another app must start with the listener's tap"
            )


def test_the_lease_never_decides_which_source_plays():
    """Device death is not predictable from elapsed time.

    In the 48h to 2026-07-31 a 1s Bandcamp run lost the device and a 535s run
    kept it; time since the last successful play separates the groups no better
    (123s failed, 1372s fine). The lease is now a probe-rate gate and a log
    field, nothing else — no source decision may read it.
    """
    src = _app()
    lease = re.search(r"LEASE_MS\s*=\s*(\d+)", src)
    assert lease, "the device lease constant is gone"
    assert 15000 <= int(lease.group(1)) <= 120000

    picker = _function_body(src, "_pickDiscoveryStratified")
    assert "isProbablyLive()" not in picker, (
        "a lapsed lease means 'not seen lately', never 'Spotify is gone'"
    )


def test_the_player_never_reaches_into_the_queue():
    """web/js/player.js may ask the queue things; it may not touch it.

    Before the split these were bare cross-references inside one 7,500-line
    script, and three of them were written `typeof x === 'function' ? x() : null`
    — not defensiveness but a load-order guess, since the player could not know
    whether the queue had been defined yet. Worse, the player assigned `dIdx`
    directly: it moved the queue's cursor and the queue never knew why, which is
    the shape of every "the UI and the audio disagree" bug in this file.

    Player.wire() replaces all of it. If a queue name turns up in player.js
    again, the seam has been bypassed rather than extended.
    """
    import os
    from browser_source import JS

    with open(os.path.join(JS, "player.js"), encoding="utf-8") as fh:
        src = _code_only(fh.read())
    # Strings too: a log category like 'spotify-history-bounce' is prose about
    # the queue, not a reference to it.
    src = re.sub(r"'(?:[^'\\\n]|\\.)*'", "''", src)
    src = re.sub(r'"(?:[^"\\\n]|\\.)*"', '""', src)
    src = re.sub(r"`(?:[^`\\]|\\.)*`", "``", src)
    for name in ["allDiscovery", "dIdx", "playedStack", "history",
                 "allTracksPool", "userCoverage", "playedIds"]:
        assert not re.search(r"\b" + name + r"\b", src), (
            f"web/js/player.js touches {name}, which belongs to the queue — "
            "add it to the Player.wire() seam instead"
        )


def test_device_state_has_exactly_one_owner():
    """Nothing outside web/js/device.js may touch the device's state.

    This is the invariant the module exists to make checkable, and it could not
    be written before: the state was nine module-level flags in the middle of a
    7,500-line script, written from six places. Two of those reached in and
    assigned `_spotifyDeviceLeaseUntil = 0` directly — one on the Bandcamp
    handoff, one in the deep-link confirm — which is how a lease could go stale
    without anyone deciding it should.

    Callers now report facts (`saw`, `lost`, `endangered`, `giveUp`) and this
    file decides what they mean. The names below are private to it; finding one
    anywhere else means the encapsulation has been worked around rather than
    used, and the flag-per-bug pattern has restarted.
    """
    import os
    from browser_source import JS

    private = ["leaseUntil", "lastProbeAt", "probeInFlight",
               "handshakeUsed", "provenUnreachable", "LEASE_MS", "PROBE_MIN_GAP_MS"]
    for fname in sorted(os.listdir(JS)):
        if not fname.endswith(".js") or fname == "device.js":
            continue
        with open(os.path.join(JS, fname), encoding="utf-8") as fh:
            src = _code_only(fh.read())
        for name in private:
            assert not re.search(r"\b" + name + r"\b", src), (
                f"web/js/{fname} reaches into {name}, which belongs to device.js — "
                "report a fact through SpotifyDevice instead"
            )


def test_probe_is_rate_limited():
    """Spotify's dev quota is tiny — bursts lock the whole app out for ~24h."""
    src = _app()
    assert re.search(r"PROBE_MIN_GAP_MS\s*=\s*\d+", src), (
        "the /api/devices probe lost its rate limit"
    )
    body = _probe_body()
    assert "PROBE_MIN_GAP_MS" in body and "return Promise.resolve(null)" in body, (
        "the rate limit must actually gate the fetch"
    )


def test_bandcamp_start_invalidates_the_lease_and_probes():
    """The pause that starts a Bandcamp track is the moment the device becomes
    reclaimable — the one risky transition we can see coming.

    Without this, a SHORT Bandcamp track leaves the lease fresh, the next
    Spotify track dispatches blind, and the deferral only engages after the
    user has already been deep-linked.
    """
    src = _app()
    branch = src[src.index("try { fetch('/api/pause' + d); }"):]
    branch = branch[:branch.index("Player._bandcamp.play(track)")]
    assert "SpotifyDevice.endangered(" in branch, (
        "the pause IS the moment the device becomes reclaimable; the caller "
        "must say so rather than quietly leaving a stale lease behind"
    )
    # …and the fact must still mean both things.
    body = _method_body(_app(), "endangered")
    assert "leaseUntil = 0" in body, (
        "the lease must not survive the pause that endangers the device"
    )
    assert "this.probe(" in body, (
        "probe while the Bandcamp track plays, so the next Spotify pick reads "
        "an answer rather than a guess"
    )


def test_play_outcomes_move_the_lease():
    src = _app()
    assert "SpotifyDevice.saw('play-ok')" in src, (
        "a play that lands is the primary liveness signal"
    )
    assert "SpotifyDevice.lost(" in src, (
        "a play that 404s must invalidate the lease immediately"
    )
    assert "SpotifyDevice.saw('poll')" in src, (
        "the poll is what keeps the lease fresh across a whole track"
    )


def test_user_is_told_instead_of_being_thrown_into_spotify():
    src = _app()
    assert 'id="spotify-asleep-banner"' in src, (
        "the notice explaining why Bandcamp is playing is gone; without it the "
        "narrowed pool looks like a bug"
    )
    banner = src[src.index('<div id="spotify-asleep-banner">'):]
    banner = banner[:banner.index("</div>")]
    # Markup comments explain the copy and may name files; they are not shown.
    banner = re.sub(r"<!--[\s\S]*?-->", "", banner)
    for jargon in ("Connect", "device", "404", "API", "token", "SDK"):
        assert jargon not in banner, (
            f"'{jargon}' is infra jargon — the notice must describe the outcome"
        )


def test_source_transition_is_logged_directly():
    """The 61%-vs-3.8% correlation had to be reconstructed by hand from
    interleaved lines; it should be one grep."""
    src = _app()
    log = src[src.index("clientLog('intent', 'playCurrentTrack: pinned dispatch'"):]
    log = log[:log.index("});") + 3]
    assert "transition:" in log, "log prev->new source on every dispatch"
    assert "deviceAlive" in log, (
        "the lease state at dispatch time is what explains the outcome"
    )


def _probe_body():
    # Brace-matched, not anchored on the comment that used to follow it —
    # rewording that comment broke this helper with a ValueError.
    return _method_body(_app(), "probe")


def test_liveness_counts_only_devices_the_server_would_play_to():
    """The probe and server.py must agree on what a usable device is.

    2026-07-31, reported as "Spotify reopens every song": a Bandcamp track
    deregistered the iPhone's Connect device, the probe came back
    {count: 1, names: ['DIG'], active: false} — an idle web player on a Mac in
    another building — and `if (devices.length)` read that as proof the phone's
    Spotify was reachable. The lease was revived, the asleep notice cleared, a
    Spotify track was picked, and the server (correctly) had nothing to play it
    on. Deep link, Spotify reopens.
    """
    body = _function_body(_app(), "usableOf")
    assert "devices.filter(" in body, (
        "raw devices.length counts a laptop in another building as liveness"
    )
    assert "is_active" in body and "'Smartphone'" in body, (
        "same rule as _pick_playback_device: active, or a phone"
    )
    assert "is_restricted" in body, "a restricted device rejects control outright"


def test_liveness_is_decided_on_the_filtered_set():
    body = _probe_body()
    assert re.search(r"if \(usable\.length\)\s*\{", body), (
        "filtering and then still testing devices.length would change nothing"
    )


def test_an_empty_usable_set_drops_the_lease():
    body = _probe_body()
    assert "leaseUntil = 0" in body, (
        "no usable device must expire the lease, not merely fail to extend it"
    )


def test_the_probe_still_reports_what_it_saw():
    """The raw count and names are the evidence that diagnosed this; keeping
    only the filtered number would have hidden the Mac entirely."""
    body = _probe_body()
    assert "count: devices.length" in body and "usable: usable.length" in body
    assert "names:" in body


def test_the_server_side_rule_still_matches():
    """Cross-file: if server.py's order changes, this test says so."""
    server_py = os.path.join(ROOT, "server.py")
    server = open(server_py, encoding="utf-8").read()
    pick = server[server.index("def _pick_playback_device"):]
    pick = pick[:pick.index("\ndef ", 1)]
    assert 'd.get("is_active")' in pick and '"Smartphone"' in pick, (
        "the client's probe mirrors this rule — they must change together"
    )
    assert "lambda d: True" not in pick, (
        "the blind fallback is what played a track on a Mac in an empty house"
    )


def test_the_connect_poll_asks_spotify_not_the_active_source():
    """The Connect poll must call spotifyState(), never getState().

    getState() answers "what is DIG's current source doing" and routes to the
    Bandcamp <audio> element whenever activeSource is 'bandcamp'. For the
    CONNECT poll that is the wrong question, and it gave a catastrophic answer:
    after a handshake adoption the Bandcamp track is PAUSED, not stopped, so
    activeSource was still 'bandcamp' and the poll read Spotify as having
    jumped to a bc: id. It "corrected" that by dispatching — the exact bounce
    the adoption exists to avoid.

    Adoption now hands the source over explicitly, but that call sits in a
    try/catch and would fail silently. This is the guarantee that does not
    depend on it.
    """
    src = _code_only(_app())
    start = src.index("_connectPollInterval = setInterval")
    body = src[start:start + 4000]
    assert "Player.spotifyState()" in body, (
        "the Connect poll no longer asks Spotify directly"
    )
    assert "await Player.getState()" not in body, (
        "the Connect poll is reading the ACTIVE SOURCE's state; with a paused "
        "Bandcamp track still active that reports a bc: id as Spotify's"
    )


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"ok   {name}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {name}: {e}")
        except Exception as e:  # noqa: BLE001
            # A crash is a failed check, not a reason to stop. An uncaught
            # ValueError from a .index() lookup aborted this file mid-run and
            # it still printed 8 "ok" lines — a red suite reading as green.
            failed += 1
            print(f"ERROR {name}: {type(e).__name__}: {e}")
    print("\nall iOS-divergence checks passed" if not failed else f"\n{failed} failed")
    sys.exit(1 if failed else 0)


