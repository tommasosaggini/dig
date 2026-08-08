"""Repo-integrity gate — cheap, dependency-free, DB-free.

Catches the two classes of bug that have actually bitten this repo:

  1. Syntax errors anywhere in the Python tree (py_compile every file).
  2. Committed code importing a `lib.*` module whose file does not exist
     (the "untracked in-use module" bug — a clean checkout would ImportError
     at startup).

Runnable two ways:
    python3 tests/test_repo_integrity.py     # bare, no deps
    pytest tests/test_repo_integrity.py       # if pytest is installed
"""
import os
import re
import py_compile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKIP_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__", "backups"}


def _py_files():
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fn in filenames:
            if fn.endswith(".py"):
                yield os.path.join(dirpath, fn)


def test_all_python_compiles():
    """Every .py file parses (catches syntax errors before deploy)."""
    failures = []
    for path in _py_files():
        try:
            py_compile.compile(path, doraise=True)
        except py_compile.PyCompileError as exc:
            failures.append(f"{os.path.relpath(path, ROOT)}: {exc}")
    assert not failures, "Python files failed to compile:\n" + "\n".join(failures)


# from lib.foo import ... | import lib.foo | from lib import foo, bar
_RE_FROM_DOTTED = re.compile(r"^\s*from\s+lib\.([a-zA-Z_]\w*)", re.M)
_RE_IMPORT_DOTTED = re.compile(r"^\s*import\s+lib\.([a-zA-Z_]\w*)", re.M)
_RE_FROM_LIB = re.compile(r"^\s*from\s+lib\s+import\s+([^\n#]+)", re.M)


def test_all_lib_imports_resolve():
    """Every `lib.<module>` referenced in code exists as lib/<module>.py.

    This is the regression guard for the untracked-module bug: if someone
    adds `from lib.newthing import x` but never `git add lib/newthing.py`,
    this fails instead of the production container ImportError-ing on boot.
    """
    lib_dir = os.path.join(ROOT, "lib")
    missing = []
    for path in _py_files():
        with open(path, encoding="utf-8") as fh:
            src = fh.read()
        refs = set(_RE_FROM_DOTTED.findall(src)) | set(_RE_IMPORT_DOTTED.findall(src))
        for m in _RE_FROM_LIB.findall(src):
            for name in m.replace("(", "").split(","):
                name = name.strip().split(" as ")[0].strip()
                if name and name.isidentifier():
                    refs.add(name)
        for mod in refs:
            if not os.path.exists(os.path.join(lib_dir, mod + ".py")):
                missing.append(f"{os.path.relpath(path, ROOT)} -> lib.{mod} (lib/{mod}.py missing)")
    assert not missing, "Imports reference non-existent lib modules:\n" + "\n".join(sorted(set(missing)))


def test_no_test_is_defined_after_its_own_runner():
    """A test appended below `if __name__ == "__main__":` never runs.

    Every suite here collects with `for name, fn in globals()` inside that
    block, so collection happens at the point the block executes — anything
    defined after it is invisible. Nothing errors, nothing is skipped aloud;
    the file prints its "all checks passed" line and the new test is simply not
    in it. Found 2026-08-05 in test_ios_playback_divergence.py, where a
    pause-is-not-a-dead-device check written days earlier had never once
    executed, and a fresh one landed in the same hole.

    Appending is the natural way to add a test to these files, so this is a
    trap that will be walked into again rather than a one-off slip.
    """
    import glob
    offenders = []
    for path in sorted(glob.glob(os.path.join(ROOT, "tests", "test_*.py"))):
        with open(path, encoding="utf-8") as fh:
            src = fh.read()
        # The runner is a top-level statement, so anchor to column 0. A plain
        # substring search also matches this very docstring, which quotes the
        # marker — and then reports every test below THIS one as unreachable.
        src_lines = src.splitlines()
        i = next((n for n, ln in enumerate(src_lines)
                  if ln.startswith('if __name__ == "__main__":')), -1)
        if i < 0:
            continue
        after = [ln for ln in src_lines[i:] if ln.startswith("def test_")]
        if after:
            offenders.append(f"{os.path.basename(path)}: {', '.join(a.split('(')[0][4:] for a in after)}")
    assert not offenders, (
        "these tests are defined below the runner that collects them, so they "
        "never execute:\n  " + "\n  ".join(offenders)
    )


def test_a_failed_search_cannot_be_recorded_as_coverage():
    """Every search_tracks() result is None-checked before it is believed.

    search_tracks returns None when the search DID NOT HAPPEN — a 404, a 403,
    a timeout, an exhausted budget. Treating that as an empty result is how
    1,874 catalog cells came to be stamped "explored, nothing there" without
    Spotify ever having answered, and the picker ranks on `explored`, so it
    never went back. This is a static check because pipeline/discover.py runs
    its phases at import and cannot be exercised in a unit test.
    """
    path = os.path.join(ROOT, "pipeline", "discover.py")
    with open(path, encoding="utf-8") as fh:
        lines = fh.readlines()
    offenders = []
    for i, line in enumerate(lines):
        if "search_tracks(" not in line or line.lstrip().startswith("def "):
            continue
        # The guard must be the very next statement — anything else has
        # already used the result before checking whether it is real.
        following = "".join(lines[i + 1:i + 3])
        if "is None" not in following:
            offenders.append(f"discover.py:{i + 1}: {line.strip()}")
    assert not offenders, (
        "these use a search result without checking it happened:\n  "
        + "\n  ".join(offenders))


def test_a_scanned_cell_always_records_what_came_back():
    """mark_cell_explored is never called without `returned=`.

    Without it, "the cell is empty" and "the cell is full of records we
    already own" are the same stored number, which is the other half of the
    same blindness: a productive cell gets written off as barren.
    """
    path = os.path.join(ROOT, "pipeline", "discover.py")
    with open(path, encoding="utf-8") as fh:
        lines = fh.readlines()
    offenders = []
    for i, line in enumerate(lines):
        if "mark_cell_explored(" not in line or "import" in line:
            continue
        # Arguments contain their own parens (`len(new)`), so match to the end
        # of the statement rather than to the first closing bracket.
        stmt = "".join(lines[i:i + 3])
        if "returned=" not in stmt:
            offenders.append(f"discover.py:{i + 1}: {line.strip()}")
    assert not offenders, (
        "these mark a cell scanned without recording what came back:\n  "
        + "\n  ".join(offenders))


if __name__ == "__main__":
    test_all_python_compiles()
    test_all_lib_imports_resolve()
    test_no_test_is_defined_after_its_own_runner()
    test_a_failed_search_cannot_be_recorded_as_coverage()
    test_a_scanned_cell_always_records_what_came_back()
    print("OK: repo integrity (compile + lib-import resolution) passed")
