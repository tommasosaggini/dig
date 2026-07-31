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
     Spotify app; iOS suspends a paused backgrounded app within ~a minute and its
     Connect device deregisters entirely, so the next Spotify play 404s into a
     deep link. Measured: bandcamp->spotify 18 hops, 11 with device trouble (61%),
     6 deep-linked (33%); spotify->spotify 53 hops, 2 (3.8%) and 1 (1.9%). The fix
     is to prefer Bandcamp picks while the device lease has lapsed rather than
     burn the user's skip discovering it.

Static assertions against web/app.html: the file is one ~325 KB inline script
with no module boundary to import, so these are cheap and fail loudly if the
guards are ever removed.

    python3 tests/test_ios_playback_divergence.py   # bare, no deps
    pytest tests/test_ios_playback_divergence.py    # if pytest is installed
"""
import os
import re
import sys

APP = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "web", "app.html")


def _app() -> str:
    with open(APP, encoding="utf-8") as fh:
        return fh.read()


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


def test_context_jump_distinguishes_a_natural_advance():
    """One step after a full track is legitimate; a 13-position leap is not."""
    src = _app()
    body = src[src.index("const isContextJump"):]
    body = body[:body.index("if (isContextJump")]
    assert "ctxPos > 1" in body, (
        "a natural end-of-track advance moves exactly one position; without this "
        "test every locked-screen auto-advance is treated as a bug and re-issued"
    )
    assert "LEGIT_ADVANCE_MIN_MS" in body, (
        "an advance seconds after our dispatch cannot be a finished track"
    )


def test_context_jump_recovery_is_bounded():
    """A track Spotify refuses to play must not ping-pong forever."""
    src = _app()
    assert re.search(r"_contextJumpRecoveries\s*<\s*\d", src), (
        "the re-issue loop lost its bound"
    )
    reset = re.search(r"if \(([^)]*st\.trackId === _connectTrackId)\) "
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

def test_picker_defers_spotify_when_the_app_is_asleep():
    body = _function_body(_app(), "_pickDiscoveryStratified")
    assert "_shouldDeferSpotifyPicks()" in body, (
        "the picker no longer avoids Spotify tracks that can only deep-link"
    )
    assert "_isBandcampTrack" in body, (
        "deferral must fall back to the Bandcamp subset"
    )


def test_deferral_never_empties_the_pool():
    """Running discovery dry is worse than one deep link."""
    body = _function_body(_app(), "_pickDiscoveryStratified")
    guard = body[body.index("_shouldDeferSpotifyPicks()"):]
    guard = guard[:guard.index("} else {")]
    assert re.search(r"bcOnly\.length\s*>=\s*\d+", guard), (
        "deferral must require a real Bandcamp subset before narrowing"
    )


def test_deferral_is_ios_only_and_evidence_based():
    """Not the blanket platform rule the DIG_ONLY_SOURCE comment warns against.

    The evidence source CHANGED on 2026-07-31, and deliberately: deferral used
    to trigger on `_spotifyDeviceProbablyAlive()` — a 45s lease, i.e. a forecast
    — and that withheld 18,213 Spotify tracks while playing 24 consecutive
    Bandcamp ones. Spotify is the default source; leaving it now requires a play
    that actually failed for want of a device (`_spotifyUnavailable`), which is
    a fact rather than a prediction. The bar went up, not down.
    """
    body = _function_body(_app(), "_shouldDeferSpotifyPicks")
    assert "DIG_IS_IOS" in body, "desktop's Connect device is DIG's own tab"
    assert "_spotifyUnavailable" in body, (
        "deferral must key off observed failure, not a hardcoded platform rule"
    )
    assert "_spotifyDeviceProbablyAlive()" not in body, (
        "a lapsed lease means 'not seen lately', never 'Spotify is gone' — "
        "deferring on it is what buried Spotify behind Bandcamp"
    )
    assert "_probeSpotifyDevice" in body, (
        "without a probe the latch can never clear and Spotify is gone for good"
    )


def test_leaving_spotify_requires_a_real_failure():
    """The latch is set only where a play actually came back with no device."""
    src = _app()
    assert "_spotifyUnavailable = true" in src
    setter = src[src.index("_spotifyUnavailable = true") - 900:]
    setter = setter[:1000]
    assert "no_device" in setter or "data.error" in setter, (
        "the fallback must be reached from a failed play, not from a timer"
    )


def test_any_sign_of_life_returns_to_spotify():
    """Coming back has to be cheap, or one blip strands the user on Bandcamp."""
    body = _function_body(_app(), "_markSpotifyDeviceAlive")
    assert "_spotifyIsBack" in body, (
        "a landed play, a poll that sees playback, or a probe that finds a "
        "device must all end the fallback"
    )


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
    assert "_spotifyUnavailable = false" in retry, (
        "Spotify is warm after the link — writing it off here strands the "
        "session on Bandcamp"
    )


def test_it_still_gives_up_rather_than_looping():
    """One retry. Repeating the link is the 'Spotify reopens every song' bug."""
    body = _function_body(_app(), "_confirmDeepLink")
    assert "nextTrack(true)" in body, "a second failure must move on"
    assert "_deepLinkAdvances >= 3" in body, "and the whole thing stays bounded"


def test_the_handshake_is_spent_once_per_session():
    """One interruption is a handshake; repeating it is the 'Spotify reopens
    every song' complaint."""
    src = _app()
    assert "_spotifyHandshakeUsed" in src
    i = src.index("window.location.href = `spotify:track:${trackId}`")
    guard = src[i - 1200:i]
    assert "if (_spotifyHandshakeUsed)" in guard, (
        "the automatic deep link must be gated on the one-shot"
    )


def test_lease_gates_probing_but_the_probe_decides():
    """The lease must NOT be the thing that decides — device death is not
    predictable from elapsed time.

    In the 48h to 2026-07-31 a 1s Bandcamp run lost the device and a 535s run
    kept it; time since the last successful play separates the groups no better
    (123s failed, 1372s fine). So correctness has to come from asking Spotify,
    with the lease only deciding how often we ask. If a future change ever makes
    the lease authoritative — deferring without a probe behind it — this whole
    mitigation degrades into a guess.
    """
    src = _app()
    lease = re.search(r"_DEVICE_LEASE_MS\s*=\s*(\d+)", src)
    assert lease, "the device lease constant is gone"
    ms = int(lease.group(1))
    assert 15000 <= ms <= 120000, (
        f"lease {ms}ms is outside the range where a lapse is noticed within a "
        "track or two; it is a probe gate, not a suspend timer"
    )
    # The lease is no longer an input to the SOURCE decision at all — that is
    # the 2026-07-31 change, and it is the strongest possible form of "the
    # lease must not be authoritative". It survives purely as a probe-rate gate
    # and as the `deviceAlive` field on the dispatch log line.
    body = _function_body(src, "_shouldDeferSpotifyPicks")
    assert "_spotifyDeviceProbablyAlive()" not in body, (
        "the lease is a forecast; it must never decide which source plays"
    )
    assert "_probeSpotifyDevice" in body, (
        "the fallback must keep asking Spotify, or it can never come back"
    )


def test_probe_is_rate_limited():
    """Spotify's dev quota is tiny — bursts lock the whole app out for ~24h."""
    src = _app()
    assert re.search(r"_DEVICE_PROBE_MIN_GAP_MS\s*=\s*\d+", src), (
        "the /api/devices probe lost its rate limit"
    )
    body = _function_body(src, "_probeSpotifyDevice")
    assert "_DEVICE_PROBE_MIN_GAP_MS" in body and "return;" in body, (
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
    assert "_spotifyDeviceLeaseUntil = 0" in branch, (
        "the lease must not survive the pause that endangers the device"
    )
    assert "_probeSpotifyDevice(" in branch, (
        "probe while the Bandcamp track plays, so the next Spotify pick reads "
        "an answer rather than a guess"
    )


def test_play_outcomes_move_the_lease():
    src = _app()
    assert "_markSpotifyDeviceAlive('play-ok')" in src, (
        "a play that lands is the primary liveness signal"
    )
    assert "_markSpotifyDeviceDead(" in src, (
        "a play that 404s must invalidate the lease immediately"
    )
    assert "_markSpotifyDeviceAlive('poll')" in src, (
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
    return _function_body(_app(), "_probeSpotifyDevice")


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
    body = _probe_body()
    assert "devices.filter(" in body, (
        "raw devices.length counts a laptop in another building as liveness"
    )
    assert "is_active" in body and "'Smartphone'" in body, (
        "same rule as _pick_playback_device: active, or a phone"
    )
    assert "is_restricted" in body, "a restricted device rejects control outright"


def test_liveness_is_decided_on_the_filtered_set():
    body = _probe_body()
    assert "if (usable.length) _markSpotifyDeviceAlive" in body, (
        "filtering and then still testing devices.length would change nothing"
    )


def test_an_empty_usable_set_drops_the_lease():
    body = _probe_body()
    assert "_spotifyDeviceLeaseUntil = 0" in body, (
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
    server_py = os.path.join(os.path.dirname(os.path.dirname(APP)), "server.py")
    server = open(server_py, encoding="utf-8").read()
    pick = server[server.index("def _pick_playback_device"):]
    pick = pick[:pick.index("\ndef ", 1)]
    assert 'd.get("is_active")' in pick and '"Smartphone"' in pick, (
        "the client's probe mirrors this rule — they must change together"
    )
    assert "lambda d: True" not in pick, (
        "the blind fallback is what played a track on a Mac in an empty house"
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
