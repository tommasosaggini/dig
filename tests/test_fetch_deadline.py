"""Regression lock for the default fetch deadline in web/app.html.

The wrapper puts an AbortSignal.timeout on every request that doesn't carry its
own signal. That is what un-wedges a hung play — but the default is 15s, and two
endpoints legitimately run far longer:

  /api/ai-recommend  → lib.ai_recommend.ai_recommend / ai_recommend_v2
  /api/journey       → lib.ai_recommend.journey_recommend

Both make a NON-streaming Anthropic call with max_tokens=4000 that generates 8-20
recommendations with prose `reason` fields — routinely 20-50s. At the 15s default
they were aborted every single time, and because both call sites swallow the
rejection with a bare console.error, AI-Mix and Journey just silently stopped
filling their queues with nothing in the server log to show for it.

These assert against the shipped source: the exemption list must keep covering
the LLM endpoints, and the slow deadline must stay above what they actually take.

    python3 tests/test_fetch_deadline.py
    pytest tests/test_fetch_deadline.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP = os.path.join(ROOT, "web", "app.html")

with open(APP, encoding="utf-8") as fh:
    HTML = fh.read()

# The LLM endpoints, and the client call site that must stay in sync with them.
LLM_ENDPOINTS = ["/api/ai-recommend", "/api/journey"]


def _slow_prefixes():
    m = re.search(r"const SLOW_PREFIXES\s*=\s*\[([^\]]*)\]", HTML)
    assert m, "SLOW_PREFIXES is gone from web/app.html — was the deadline wrapper removed?"
    return re.findall(r"'([^']+)'", m.group(1))


def _const_ms(name):
    m = re.search(r"const %s\s*=\s*(\d+)" % name, HTML)
    assert m, "%s is gone from web/app.html" % name
    return int(m.group(1))


def test_llm_endpoints_are_exempt_from_the_default_deadline():
    prefixes = _slow_prefixes()
    for path in LLM_ENDPOINTS:
        assert path in prefixes, (
            "%s is not in SLOW_PREFIXES, so it inherits the %dms default. A "
            "non-streaming Sonnet call with max_tokens=4000 does not finish in "
            "that time — the request aborts and the mode silently dies."
            % (path, _const_ms("DEFAULT_MS"))
        )


def test_the_full_pool_stays_exempt_too():
    assert "/discovery" in _slow_prefixes(), (
        "/discovery lost its exemption — the full ~1.9 MB gzipped pool cannot "
        "reliably land in DEFAULT_MS on mobile."
    )


def test_slow_deadline_is_long_enough_for_a_full_generation():
    slow = _const_ms("SLOW_MS")
    assert slow >= 90_000, (
        "SLOW_MS is %dms. A 4000-token non-streaming generation can run past "
        "that, which puts the LLM endpoints right back where they started."
        % slow
    )
    assert slow > _const_ms("DEFAULT_MS"), "SLOW_MS must exceed DEFAULT_MS"


def test_the_llm_call_sites_still_send_no_signal_of_their_own():
    # The wrapper defers to init.signal, so a call site that grew its own
    # AbortController would quietly opt out of SLOW_MS. If that ever happens the
    # timeout belongs there, not here — this fails to say so.
    for path in LLM_ENDPOINTS:
        i = HTML.find("fetch('%s'" % path)
        assert i > 0, "no fetch call site found for %s" % path
        call = HTML[i:i + 400]
        assert "signal" not in call, (
            "%s now passes its own signal, bypassing SLOW_PREFIXES — move the "
            "long deadline to that call site." % path
        )


def test_stream_resolve_outlasts_an_observed_congestion_spike():
    # Measured in prod on 29/07: server answered /api/bandcamp/resolve in
    # 186-583ms while the client saw 15981ms and 23290ms. At DEFAULT_MS both
    # plays would have been aborted rather than merely late.
    m = re.search(r"const RESOLVE_PREFIXES\s*=\s*\[([^\]]*)\]", HTML)
    assert m, "RESOLVE_PREFIXES is gone from web/app.html"
    prefixes = re.findall(r"'([^']+)'", m.group(1))
    assert "/api/bandcamp/resolve" in prefixes, (
        "stream resolve is back on the %dms default — a congestion spike now "
        "kills the play instead of delaying it" % _const_ms("DEFAULT_MS")
    )
    resolve_ms = _const_ms("RESOLVE_MS")
    assert resolve_ms >= 30_000, "RESOLVE_MS=%d is under the worst spike observed" % resolve_ms
    assert resolve_ms < _const_ms("SLOW_MS"), (
        "resolve is on the play path — it must not inherit the full-pool "
        "deadline, or a wedged play blocks the player for that long"
    )


def test_keepalive_beacons_are_left_alone():
    # clientLog posts with keepalive during page teardown; aborting those loses
    # the last events of a session, which are the ones worth having.
    assert "init.keepalive" in HTML, "the keepalive exemption was dropped"


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("ok   %s" % name)
            except AssertionError as e:
                print("FAIL %s\n     %s" % (name, e))
                fails += 1
            except Exception as e:  # noqa: BLE001
                # A crash is a failed check, not a reason to stop. An uncaught
                # ValueError aborted a sibling file mid-run and it still printed
                # only "ok" lines — a red suite reading as green.
                print("ERROR %s\n     %s: %s" % (name, type(e).__name__, e))
                fails += 1
    sys.exit(1 if fails else 0)
