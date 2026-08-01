"""The three Anthropic call paths must report the same thing.

lib/ai_recommend.py has three entry points — ai_recommend, ai_recommend_v2 and
journey_recommend — each making one Anthropic call. The block that pulls model,
latency and token usage off the response was copy-pasted three times, and it
had drifted: only ai_recommend captured `cache_creation_input_tokens`.

That is not a cosmetic difference. Cache writes cost ~1.25x base input and
reads ~0.1x, so a prompt that writes on every call and never reads is the exact
signature of a silent cache invalidator — a timestamp in the system prompt, a
per-user id in the cached prefix, a tool list that varies. Two of the three
paths reported only the read side, which is the half that reads as "fine" when
the cache is broken.

These run without network or an API key: they exercise the pure helpers against
a fake response object.

    python3 tests/test_ai_call_plumbing.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib import ai_recommend  # noqa: E402

_RAW = open(os.path.join(ROOT, "lib", "ai_recommend.py"), encoding="utf-8").read()


def _code_only(src: str) -> str:
    """`src` with docstrings and `#` comments removed.

    The comments in that file quote the very constructs these tests forbid —
    a fix and the note explaining it name the same thing — so a bare substring
    search finds the explanation and reports the bug it prevents. Docstrings
    are stripped through the AST rather than by regex, because a triple-quoted
    string is not reliably matchable and getting it wrong here means the check
    silently passes.
    """
    import ast

    tree = ast.parse(src)
    spans = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            doc = node.body[0] if node.body else None
            if (isinstance(doc, ast.Expr) and isinstance(doc.value, ast.Constant)
                    and isinstance(doc.value.value, str)):
                spans.append((doc.lineno, doc.end_lineno))
    lines = src.split("\n")
    for start, end in spans:
        for i in range(start - 1, end):
            lines[i] = ""
    return re.sub(r"#.*$", "", "\n".join(lines), flags=re.M)


SRC = _code_only(_RAW)


class _Usage:
    input_tokens = 1200
    output_tokens = 340
    cache_read_input_tokens = 5000
    cache_creation_input_tokens = 900


class _Block:
    def __init__(self, type_, text=None):
        self.type = type_
        if text is not None:
            self.text = text


class _Msg:
    def __init__(self, content, usage=_Usage()):
        self.content = content
        self.usage = usage


def test_meta_carries_both_halves_of_the_cache():
    meta = ai_recommend._call_meta(_Msg([]), started=0.0)
    assert meta["cache_read_tokens"] == 5000
    assert meta["cache_creation_tokens"] == 900, (
        "cache WRITES are the half that was missing on two of three paths — "
        "without it, a prompt that never hits the cache looks healthy"
    )
    assert meta["input_tokens"] == 1200 and meta["output_tokens"] == 340
    assert meta["model"] == ai_recommend.MODEL


def test_meta_survives_a_response_with_no_usage():
    """Every field is optional-by-getattr; a usage-less response must not raise."""
    meta = ai_recommend._call_meta(_Msg([], usage=None), started=0.0)
    for key in ("input_tokens", "output_tokens",
                "cache_read_tokens", "cache_creation_tokens"):
        assert meta[key] is None, key


def test_caller_fields_are_merged_not_dropped():
    meta = ai_recommend._call_meta(_Msg([]), started=0.0, n_returned=7, seed="x")
    assert meta["n_returned"] == 7 and meta["seed"] == "x"
    assert meta["model"] == ai_recommend.MODEL, "extras must not shadow the common fields"


def test_text_is_taken_by_block_type_not_by_index():
    """`content[0].text` is correct only while nothing enables thinking.

    `content` is a list of BLOCKS and only some carry `.text`. Turn adaptive
    thinking on and block 0 becomes a thinking block — every call site raises
    AttributeError at once, at runtime, on all three paths.
    """
    thinking_first = _Msg([_Block("thinking"), _Block("text", "the answer")])
    assert ai_recommend._first_text(thinking_first) == "the answer"

    assert ai_recommend._first_text(_Msg([])) == ""
    assert ai_recommend._first_text(_Msg([_Block("thinking")])) == ""


def test_no_call_site_indexes_content_directly():
    assert "msg.content[0]" not in SRC, (
        "reading content by index reintroduces the thinking-block break"
    )
    assert SRC.count("_first_text(msg)") == 4, (
        "one definition plus three call sites — all three entry points must "
        "read the response the same way"
    )


def test_the_meta_block_has_exactly_one_implementation():
    assert SRC.count("def _call_meta") == 1
    assert SRC.count("_call_meta(") == 4, (
        "one definition plus three call sites; a fourth hand-rolled meta dict "
        "is how the cache-write field went missing the first time"
    )
    assert "usage = getattr(msg" not in SRC.split("def _call_meta")[0], (
        "a usage block outside the helper means a path has drifted off again"
    )


def test_every_path_logs_both_cache_numbers():
    """Collecting the number and not printing it is the same blind spot."""
    for tag in ("[AI-MIX]", "[AI-MIX-V2]", "[JOURNEY]"):
        i = SRC.index(f'print(f"{tag}')
        stmt = SRC[i:SRC.index(")\n", i)]
        assert "cache_read" in stmt and "cache_write" in stmt, (
            f"{tag} does not log both cache halves — a broken cache is "
            "invisible in a log that only carries reads"
        )


def test_no_parameter_removed_by_the_current_api():
    """These 400 on current models; none were present, and none may return."""
    calls = re.findall(r"client\.messages\.create\(([\s\S]*?)\n        \)", SRC)
    assert len(calls) == 3, f"expected 3 call sites, found {len(calls)}"
    for call in calls:
        for banned in ("temperature", "top_p", "top_k", "budget_tokens", "output_format"):
            assert banned not in call, f"{banned} is rejected by current models"


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
    print("all AI call-plumbing checks passed")
