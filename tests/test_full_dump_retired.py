"""Stage 4 of sample-not-sync: the full-pool dump no longer exists.

Until 2026-08-11 a limit-less /discovery returned every unheard track — a
payload that grew with ingestion forever (17.3 MB when clients behind ~15 MiB
response caps started losing it mid-body). Every consumer is now on samples:
the client boots on ?limit=800, upgrades to a working set, and refills; a
limit-less request (old cached clients) gets the default working set instead
of the dump; and any requested limit is capped under the measured response
cap. The one thing that genuinely needed the full pool — matching the
permanent ledger against tracks for taste seeding — moved server-side into
db_get_ledger, which resolves each liked/disliked entry against the real
pool and ships the metadata on the entry.

    python3 tests/test_full_dump_retired.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402


def _src(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def _handler_block(py: str) -> str:
    i = py.index('if parsed.path == "/discovery":')
    return py[i:i + 3000]


def test_a_limitless_request_gets_the_default_working_set():
    block = _handler_block(_src("server.py"))
    assert re.search(r"if limit <= 0:\s*\n\s*limit = DISCOVERY_DEFAULT_LIMIT", block), (
        "limit<=0 must be rewritten to the default working set — otherwise a "
        "limit-less /discovery is the unbounded dump again")


def test_every_response_is_sampled():
    """_bootstrap_sample must run unconditionally — no code path may send
    load_discovery's raw output."""
    block = _handler_block(_src("server.py"))
    m = re.search(r"(\s*)disc = _bootstrap_sample\(", block)
    assert m, "the /discovery handler no longer samples at all"
    indent = m.group(1).strip("\n")
    # The sample call sits at the same indent as the limit parsing (not nested
    # under an `if limit > 0:`), so it applies to every request.
    assert f"\n{indent}if limit <= 0:" in block, (
        "sampling appears to be conditional again — a path around "
        "_bootstrap_sample re-creates the full dump")


def test_the_requested_limit_is_capped():
    block = _handler_block(_src("server.py"))
    assert "min(limit, DISCOVERY_LIMIT_CEILING)" in block, (
        "?limit= must be capped: ~540 B/track raw means ~29k tracks crosses "
        "the measured 15 MiB in-app-browser response cap")


def test_the_ledger_resolves_taste_seeds_serverside():
    """The full pool's one real client-side job — matching the permanent
    ledger for taste seeding — must now happen in db_get_ledger."""
    py = _src("server.py")
    i = py.index("def db_get_ledger")
    body = py[i:i + 3000]
    assert "lower(artist || ' - ' || name)" in body, (
        "db_get_ledger must join ledger keys against tracks the way the "
        "client's trackByKey did — lower('artist - name')")
    assert '"seed"' in body or "'seed'" in body, (
        "resolved metadata must ship on the entry as `seed` — the client "
        "falls back to it when the working set has no match")


def test_the_client_uses_the_shipped_seed():
    js = _src("web/js/app.js")
    assert "_seedTrack(entry.seed)" in js, (
        "seedTasteSignals must fall back to the server-resolved seed; "
        "without it, taste silently narrows to whatever fraction of the "
        "ledger lands in the 8k working set")


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
