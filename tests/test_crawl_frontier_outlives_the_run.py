"""Regression lock for "the genre crawler ran hourly for four months and did nothing".

Measured 2026-08-31. `genre_crawl.log` was being written every hour and every
run said the same thing:

    processed=2, new_seeds=0, new_neighbors=0, errors=2, skipped_pre_crawled=5153

Three defects, each of which alone would have been enough:

  1. THE FRONTIER DID NOT SURVIVE THE PROCESS. mb_crawl_state recorded what had
     been crawled; the queue of what to crawl NEXT was a `deque` built at
     startup from data/genre_seeds.json — 5,155 MBIDs — and thrown away at
     exit. Neighbours found mid-run were appended to that deque and written to
     mb_artists as stubs, but nothing recorded that they were unvisited work.
     So the walk covered seeds + one hop, finished on 2026-04-28 at 6,645
     artists, and every run after that re-read the same 5,155 rows, skipped all
     of them, and exited 0. `--max-depth` bounded the PROCESS, not the crawl.

  2. FAILURES WERE RETRIED FOR EVER. load_already_crawled() selected
     `WHERE error IS NULL`, so any MBID that had ever errored was re-requested
     on every subsequent run, hourly, with no attempt count and no backoff. The
     two survivors were plain `not_found` — MusicBrainz does not have them and
     never will.

  3. A 503 WAS ANSWERED IMMEDIATELY. `int(headers.get("Retry-After", 5*(n+1)))`
     takes the header whenever the key is PRESENT, and MusicBrainz sends 0. The
     default never applied; the logs read "backing off 0s" and the retry loop
     fired three requests at an overloaded server in a few milliseconds.

The shared shape is the one dig keeps meeting: the job exits 0, the log has a
reassuring line in it, and the only honest question — did the output move? —
goes unasked. mb_crawl_state.crawled_at even looked FRESH throughout, because
the two permanently-failing rows were re-touched every hour.

    python3 tests/test_crawl_frontier_outlives_the_run.py
"""
import importlib.util
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402

sys.path.insert(0, ROOT)

SCRIPT = os.path.join(ROOT, "scripts", "crawl_genre_seeds.py")

failures = []


def check(label, cond, detail=""):
    if cond:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label} {detail}")
        failures.append(label)


def _src():
    with open(SCRIPT, encoding="utf-8") as fh:
        return fh.read()


def _code_only(text):
    """The source with docstrings and comments stripped.

    These assertions are about what the script DOES. The file explains the
    four-month no-op in its comments, so a plain substring search finds
    "deque" and "WHERE error IS NULL" in the very prose describing their
    removal — a test that fails on its own documentation.
    """
    import io
    import tokenize
    out, prev_end, prev_tok = [], (1, 0), None
    for tok in tokenize.generate_tokens(io.StringIO(text).readline):
        if tok.type == tokenize.COMMENT:
            continue
        # a STRING that stands alone as a statement is a docstring
        if tok.type == tokenize.STRING and prev_tok in (
                None, tokenize.NEWLINE, tokenize.NL, tokenize.INDENT,
                tokenize.DEDENT):
            prev_tok = tok.type
            continue
        out.append(tok.string)
        prev_tok = tok.type
    return " ".join(out)


def _load():
    spec = importlib.util.spec_from_file_location("cgs_under_test", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


src = _src()
code = _code_only(src)
cgs = _load()

print("the frontier lives in the database, not in a deque")
check("no in-process BFS queue rebuilt from the seed file",
      "deque" not in code)
check("work is claimed from mb_crawl_state",
      "FROM mb_crawl_state" in src and "status = 'pending'" in src)
check("neighbours are written down as pending work",
      "def enqueue(" in src and "'pending'" in src)
check("a neighbour is recorded at its true depth regardless of --max-depth",
      "enqueue(cur, nbr_mbid, depth + 1, seed_genre, seed_qid)" in src)
check("--max-depth filters what is CLAIMED",
      "AND depth <= %s" in src)

print("failures end, instead of recurring for ever")
check("permanent errors are named", {"not_found", "mb_400", "mb_410"} <= cgs.PERMANENT_ERRORS)
check("there is an attempt ceiling", isinstance(cgs.MAX_ATTEMPTS, int) and cgs.MAX_ATTEMPTS > 0)
check("retries are spaced, not immediate", "next_attempt_at = now() +" in src)
check("the frontier query respects that spacing",
      "next_attempt_at IS NULL OR next_attempt_at <= now()" in src)
check("nothing selects work by `error IS NULL` any more",
      "WHERE error IS NULL" not in code)

print("a 503 is not answered instantly")


class _Resp:
    """MusicBrainz saying 'overloaded' and 'come back at once' in one breath."""
    status_code = 503
    headers = {"Retry-After": "0"}
    text = ""

    def json(self):
        return {}


slept = []
cgs.requests = type("R", (), {
    "get": staticmethod(lambda *a, **k: _Resp()),
    "RequestException": Exception,
})()
cgs.time = type("T", (), {"sleep": staticmethod(slept.append),
                          "time": staticmethod(lambda: 0.0)})()

out = cgs.mb_lookup("00000000-0000-0000-0000-000000000000", retries=3)
check("Retry-After: 0 does not become sleep(0)", all(s > 0 for s in slept),
      f"(slept {slept})")
check("each wait is at least the schedule we chose",
      slept == [5, 10, 15], f"(slept {slept})")
check("and no single wait exceeds the ceiling",
      all(s <= cgs.MB_MAX_BACKOFF_S for s in slept))
check("exhausting retries is reported as transient, not as success",
      out == {"_error": "exhausted_retries"}, f"(got {out})")
check("and 'exhausted_retries' is NOT terminal",
      "exhausted_retries" not in cgs.PERMANENT_ERRORS)

print()
if failures:
    print(f"FAILED ({len(failures)}): " + ", ".join(failures))
    sys.exit(1)
print("all good")
