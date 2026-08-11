"""Regression lock for "guests silently stuck on the 800-track bootstrap".

Found 2026-08-11 in the prod logs: three different guests' full-pool
/discovery fetches all failed with a JSON SyntaxError at EXACTLY byte
15,728,354 — 15 MiB minus the response headers — on both retry attempts,
across two days. The same URL fetched from elsewhere returned 17.3 MB of
valid JSON, so the server and Traefik were innocent: something in those
clients' network path (in-app browsers are the prime suspect — guests
arrive from the Instagram link) caps response bodies at 15 MiB, and the
anon payload had just grown past it.

The client bug was the retry: a deterministic cap cuts the identical fetch
at the identical byte forever, so both retries were guaranteed dead on
arrival and the guest stayed on the 800-track bootstrap slice for the whole
session with nothing in the log naming the cause. The fix: a truncated FULL
fetch falls back to a region-balanced ?limit= slice that fits under any
such cap, and the server logs each /discovery response's raw size so we see
the payload approaching a cap before users do.

    python3 tests/test_discovery_truncation_fallback.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402


def _src(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def test_truncated_full_pool_falls_back_to_a_slice():
    """A SyntaxError on the full fetch must switch to a ?limit= fallback,
    not burn the retries on a byte-identical doomed request."""
    js = _src("web/js/app.js")
    m = re.search(r"function fetchDiscovery[\s\S]{0,2000}?\n\}", js)
    assert m, "fetchDiscovery not found"
    body = m.group(0)
    assert "SyntaxError" in body, (
        "truncation (SyntaxError from r.json()) must be recognized as its own "
        "failure mode — it is deterministic, retrying the same URL cannot help")
    assert "DISCOVERY_CAP_FALLBACK_N" in body, (
        "the truncation path must re-fetch with a slice that fits under a "
        "~15 MiB response cap")


def test_fallback_only_fires_for_the_full_fetch():
    """A truncated ?limit= response is NOT the cap (slices are far smaller);
    that case must take the ordinary retry path, or a flaky network could
    ratchet every client down to a slice."""
    js = _src("web/js/app.js")
    body = re.search(r"function fetchDiscovery[\s\S]{0,2000}?\n\}", js).group(0)
    assert re.search(r"limit === 0 && .*SyntaxError", body), (
        "the fallback must be gated on limit === 0")


def test_fallback_fits_under_the_measured_cap():
    """15 MiB of pool JSON held ~29k tracks when the cap bit (17.3 MB /
    32,181 tracks ≈ 540 B per track), so the fallback slice must stay well
    below that with margin for per-track growth."""
    js = _src("web/js/app.js")
    m = re.search(r"DISCOVERY_CAP_FALLBACK_N = (\d+)", js)
    assert m, "DISCOVERY_CAP_FALLBACK_N missing"
    assert int(m.group(1)) <= 10000, (
        "fallback slice must fit under a 15 MiB response cap with headroom")


def test_server_logs_discovery_payload_size():
    """The operational early warning: every /discovery _evt line carries the
    raw (pre-gzip) byte size, because client-side caps act on raw bytes."""
    py = _src("server.py")
    i = py.find('_evt("discovery",')
    assert i >= 0, "discovery _evt not found"
    assert "raw_bytes" in py[i:i + 600], (
        "/discovery must log raw_bytes — the payload crossing ~15 MiB is how "
        "guests start silently losing the full pool")


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok  {name}")
            except AssertionError as e:
                failed += 1
                print(f"FAIL  {name}: {e}")
    sys.exit(1 if failed else 0)
