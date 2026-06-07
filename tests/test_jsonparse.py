"""Regression lock for lib.jsonparse.extract_json — the shared LLM-output JSON
extractor used by ai_recommend, label_discovery, analyze_pool, deep_crawl and
genre_embeddings. Cases mirror every shape those callers feed it.

    python3 tests/test_jsonparse.py     # bare, no deps
    pytest tests/test_jsonparse.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.jsonparse import extract_json  # noqa: E402

CASES = [
    ('[{"a":1},{"b":2}]', [{"a": 1}, {"b": 2}]),                  # clean array
    ('```json\n[{"q":"x"}]\n```', [{"q": "x"}]),                  # fenced array
    ('Here you go:\n[1,2,3]\nhope that helps', [1, 2, 3]),        # prose-wrapped
    ('{"genre":[0.1,-0.2]}', {"genre": [0.1, -0.2]}),            # object
    ('prefix {"a":{"x":1}} trailing junk', {"a": {"x": 1}}),     # object + extra data
    ('{"k":"has ] and } in string"}', {"k": "has ] and } in string"}),  # brackets in strings
    ('[{"terms":["a","b"]}]', [{"terms": ["a", "b"]}]),          # nested arrays
    ('```\n{"a":1}\n```', {"a": 1}),                              # bare fence
    ('no json here', None),
    ('', None),
    (None, None),
]


def test_extract_json_cases():
    wrong = [(raw, extract_json(raw), exp) for raw, exp in CASES if extract_json(raw) != exp]
    assert not wrong, "extract_json mismatches:\n" + "\n".join(
        f"  {raw!r} -> {got!r} (expected {exp!r})" for raw, got, exp in wrong)


if __name__ == "__main__":
    test_extract_json_cases()
    print(f"OK: jsonparse regression lock passed ({len(CASES)} cases)")
