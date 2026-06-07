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


if __name__ == "__main__":
    test_all_python_compiles()
    test_all_lib_imports_resolve()
    print("OK: repo integrity (compile + lib-import resolution) passed")
