"""Regression lock for the 2.28 MB /history POST that kept getting cut.

Measured 2026-08-04, three failures in the three minutes after a restart:

    history-sync ok=False err=JSONDecodeError bytes_received=1066158 content_length=2277943
    history-sync ok=False err=JSONDecodeError bytes_received=1895475 content_length=2278432
    history-sync ok=False err=JSONDecodeError bytes_received= 730265 content_length=2278608

Each is a DROPPED WRITE — every save, skip and like in that window never
reached the server. The client was serialising all 11,124 history rows into
every POST, and a mobile uplink cuts a 2.28 MB request mid-string more often
than not.

The defence that was already there — a signature that skipped POSTs carrying no
new state — never made a single request smaller. The first genuinely-new save
still sent the whole library and still got cut. That is why "it already had a
guard" was not an answer.

Two independent changes, because they fail differently:

  DELTA    — post only rows whose merged fields changed. Correct ONLY because
             POST /history upserts on (user_id, track_id); under the old
             DELETE-then-reinsert a subset would have deleted the library.
  CHUNKING — bound every request regardless, for the one case the delta cannot
             shrink: a client holding rows the server has never seen.

    python3 tests/test_history_delta_sync.py
    pytest tests/test_history_delta_sync.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402


def _app() -> str:
    with open(os.path.join(ROOT, "web", "js", "app.js"), encoding="utf-8") as fh:
        return fh.read()


def _server() -> str:
    with open(os.path.join(ROOT, "server.py"), encoding="utf-8") as fh:
        return fh.read()


def _code_only(src: str) -> str:
    src = re.sub(r"/\*[\s\S]*?\*/", "", src)
    return re.sub(r"(?m)^\s*//.*$", "", src)


def _fn(src: str, decl: str) -> str:
    """Brace-matched body of a function declaration."""
    start = src.index(decl)
    depth, i = 0, src.index("{", start)
    for j in range(i, len(src)):
        if src[j] == "{":
            depth += 1
        elif src[j] == "}":
            depth -= 1
            if depth == 0:
                return src[start:j + 1]
    raise AssertionError(f"unbalanced braces in {decl}")


def test_the_whole_history_is_never_posted():
    """The defect itself: JSON.stringify(history) as a request body."""
    code = _code_only(_app())
    assert "body: JSON.stringify(history)" not in code, (
        "posting the entire library is what got truncated at 2.28 MB")
    flush = _fn(code, "async function _flushHistory")
    assert "JSON.stringify(chunk)" in flush, "the body must be a bounded slice"


def test_only_changed_rows_are_sent():
    code = _code_only(_app())
    delta = _fn(code, "function _historyDelta")
    assert "_syncedRowSig.get(h.id) !== _rowSig(h)" in delta, (
        "the delta is what makes the normal POST small")


def test_the_delta_is_diffed_rather_than_marked_at_call_sites():
    """A dirty-flag scheme silently stops syncing whichever mutator someone
    forgets to annotate — and there are four, in three different files' worth
    of concerns: addToHistory, the skip rewrite, the track-end played_pct
    raise, and the reaction-button toggle."""
    code = _code_only(_app())
    assert "_dirtyHistoryIds" not in code, (
        "per-call-site marking cannot be verified to be complete; diffing can")
    assert code.count("saveHistory()") >= 4, (
        "if the number of mutators changed, re-check that the diff still "
        "covers every field they touch")


def test_played_pct_is_rounded_in_the_signature():
    """It arrives as a float that jitters on every progress tick. Compared raw,
    every row is dirty forever and the delta degrades back to a full post."""
    code = _code_only(_app())
    sig = _fn(code, "function _rowSig")
    assert "Math.round(h.played_pct)" in sig
    for field in ("h.status", "h.time"):
        assert field in sig, f"a change to {field} must be detected"


def test_rows_that_came_from_the_server_are_not_echoed_back():
    """Both inbound paths must record what they received.

    loadHistory replaces the whole array at boot; _refreshHistoryFromServer
    merges the Spotify pull a few seconds later. Miss either and the next save
    diffs those rows as unsynced and posts the library straight back — the
    exact request this replaced, and it would lose the race too, since the
    local copy is the older of the two.
    """
    code = _code_only(_app())
    # Scoped to the function bodies, NOT searched across the whole file: the
    # declaration `function _markHistorySynced(rows)` contains the literal text
    # "_markHistorySynced(rows)", so a file-wide substring check passes even
    # when every call site has been deleted. Caught by mutation, not review.
    load = _fn(code, "async function loadHistory")
    assert "_markHistorySynced(history)" in load, "boot load must mark synced"
    pull = _fn(code, "async function _refreshHistoryFromServer")
    assert "_markHistorySynced(rows)" in pull, "the server pull must mark synced"
    flush = _fn(code, "async function _flushHistory")
    assert "_markHistorySynced(chunk)" in flush, "an accepted POST must mark synced"


def test_a_failed_chunk_leaves_its_rows_pending():
    """A lost POST must cost a delay, not the write."""
    code = _code_only(_app())
    flush = _fn(code, "async function _flushHistory")
    assert "if (!res.ok) break;" in flush, (
        "stop on failure; the unsent rows stay dirty by construction")
    assert "_markHistorySynced(chunk)" in flush, (
        "an accepted chunk must be recorded, or it is resent forever")
    marks = flush.index("_markHistorySynced(chunk)")
    breaks = flush.index("if (!res.ok) break;")
    assert breaks < marks, (
        "rows must be marked synced only AFTER the server accepted them")


def test_the_flush_retries_what_it_could_not_send():
    code = _code_only(_app())
    flush = _fn(code, "async function _flushHistory")
    assert "sent !== delta.length" in flush and "setTimeout(_flushHistory" in flush, (
        "without a retry, rows dropped by a cut uplink wait for the next "
        "unrelated user action")


def test_chunks_are_bounded():
    code = _code_only(_app())
    m = re.search(r"_HISTORY_CHUNK\s*=\s*(\d+)", code)
    assert m, "the bound must be explicit"
    assert 0 < int(m.group(1)) <= 1000, (
        "a chunk big enough to truncate defeats the point")


def test_the_subset_post_is_only_safe_because_the_server_merges():
    """The dependency that makes all of this correct.

    If POST /history ever goes back to DELETE-then-reinsert, sending a subset
    stops being an optimisation and starts deleting the listener's library.
    """
    code = _code_only(_server())
    fn = code[code.index("def db_save_history"):]
    fn = fn[:fn.index("\ndef ", 10)]
    assert "DELETE FROM user_history" not in fn, (
        "a full-replace write plus a delta client would erase the library")
    assert "ON CONFLICT" in fn, "the merge is the precondition for the delta"


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
            except Exception as exc:                       # noqa: BLE001
                # A test that raises is a FAILING test, not a stopped run. An
                # unguarded .index() aborted the whole file and grep-for-FAIL
                # reported it as a clean pass.
                failed += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    if failed:
        print(f"\n{failed} failed")
        sys.exit(1)
    print("all history-delta checks passed")
