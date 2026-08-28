"""Things about the repo itself that must stay true.

Cheap checks for the class of mistake that is invisible until it is expensive:
a secret that becomes committable, a module nothing loads, a script that
imports something that no longer exists.

    python3 tests/test_repo_hygiene.py
"""
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _git(*args):
    return subprocess.run(["git", "-C", ROOT, *args],
                          capture_output=True, text=True).stdout


def test_no_secret_is_tracked():
    """The one that actually costs money if it slips."""
    tracked = _git("ls-files").split()
    bad = [f for f in tracked
           if re.search(r"(^|/)\.env|secret|credential|\.pem$|\.p12$|token_cache",
                        f, re.I)]
    assert not bad, f"secrets are in git history: {bad}"


def test_every_env_variant_is_ignored():
    """`.env` alone was not enough, and the gap was live.

    Editing or deploying leaves `.env.bak.<timestamp>` beside it. That file was
    NOT ignored on 2026-08-01 — one `git add -A` away from committing the
    Spotify client secret, the cookie secret, the database URL and three API
    keys. The pattern is `.env*` now; this stops it narrowing again.
    """
    probes = [".env", ".env.bak.20260101-000000", ".env.local", ".env.production"]
    for name in probes:
        r = subprocess.run(["git", "-C", ROOT, "check-ignore", "-q", name])
        assert r.returncode == 0, (
            f"{name} would be committed by `git add -A` — widen .gitignore"
        )


def test_no_module_is_orphaned():
    """A browser module nothing loads ships, is tested, and never runs."""
    js = os.path.join(ROOT, "web", "js")
    if not os.path.isdir(js):
        return
    names = [f for f in os.listdir(js) if f.endswith(".js")]
    blob = open(os.path.join(ROOT, "web", "app.html"), encoding="utf-8").read()
    for f in names:
        blob += open(os.path.join(js, f), encoding="utf-8").read()
    for f in names:
        assert f in blob, (
            f"web/js/{f} is loaded by nothing — a refactor half-landed"
        )


def test_python_has_no_unused_imports():
    """They are noise, and they hide a real one going stale."""
    import ast

    offenders = []
    targets = ["server.py"]
    for d in ("lib", "pipeline"):
        targets += [os.path.join(d, f) for f in sorted(os.listdir(os.path.join(ROOT, d)))
                    if f.endswith(".py") and f != "__init__.py"]
    for rel in targets:
        src = open(os.path.join(ROOT, rel), encoding="utf-8").read()
        try:
            tree = ast.parse(src)
        except SyntaxError:
            continue
        body = re.sub(r"^\s*(import|from)\s.*$", "", src, flags=re.M)
        # An import can be the whole point of the statement — server.py probes
        # `import yt_dlp` to find out whether the binary's Python side is
        # installed at all. `# noqa: F401` is the standard way to say so, and a
        # linter that ignores its own escape hatch just teaches people to
        # ignore the linter.
        lines = src.splitlines()
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [(a.asname or a.name.split(".")[0]) for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [a.asname or a.name for a in node.names if a.name != "*"]
            for n in names:
                # `from __future__ import annotations` has no use site by design.
                if n == "annotations":
                    continue
                stmt = lines[node.lineno - 1] if node.lineno <= len(lines) else ""
                if "noqa: F401" in stmt:
                    continue
                if not re.search(r"\b" + re.escape(n) + r"\b", body):
                    offenders.append(f"{rel}: {n}")
    assert not offenders, "unused imports: " + ", ".join(offenders)


def test_dot_env_is_loaded_in_exactly_one_place():
    """Thirty files carried a copy of the loop, in SIX implementations.

    The drift was semantic, not cosmetic: twenty-two used `setdefault` (the
    environment wins) and eight used `os.environ[k] = v` (the FILE wins). So
    `ANTHROPIC_API_KEY=sk-test python3 pipeline/analyze_pool.py` silently ran
    against the key in .env, while the same invocation of pipeline/discover.py
    honoured it — and discover.py's copy carried the comment "allows callers to
    override e.g. ANTHROPIC_API_KEY for testing", stating the intent the other
    eight quietly broke.
    """
    import ast

    offenders = []
    for d in ("pipeline", "scripts", "lib", "."):
        base = os.path.join(ROOT, d)
        for f in sorted(os.listdir(base)):
            if not f.endswith(".py"):
                continue
            rel = os.path.join(d, f).replace("./", "")
            if rel == "lib/env.py":
                continue
            src = open(os.path.join(base, f), encoding="utf-8").read()
            opens_env = re.search(r"open\(\s*_?ENV_PATH", src) or re.search(
                r'\.env["\']\s*\)\s*\n\s*if os\.path\.exists', src)
            if not opens_env:
                continue
            # Opening `.env` is not the offence — LOADING it is. There is
            # exactly one legitimate writer (ig_refresh_token.py rewrites the
            # IG_GRAPH_TOKEN line in place, via a temp file, and reads the
            # values it needs through load_env like everyone else). Judge on
            # whether the file puts what it read into the environment.
            if not re.search(r"os\.environ\s*\[[^\]]+\]\s*=|os\.environ\.setdefault\(", src):
                continue
            offenders.append(rel)
    assert not offenders, (
        "these parse .env themselves instead of calling lib.env.load_env: "
        + ", ".join(offenders)
    )


def test_dot_env_never_overrides_the_real_environment():
    """`.env` is a default, not an override.

    It is what you get when you have not said otherwise — the only reading
    under which per-invocation overrides, CI secrets and `docker run -e` all
    behave. An `os.environ[k] = v` here silently ignores all three.
    """
    src = open(os.path.join(ROOT, "lib", "env.py"), encoding="utf-8").read()
    body = src[src.index("def load_env"):]
    assert "if key not in os.environ" in body or "setdefault" in body, (
        "load_env must not clobber a name the environment already carries"
    )
    assert not re.search(r"os\.environ\[[^\]]+\]\s*=(?!=)", body.replace(
        "os.environ[key] = val", "")) or "if key not in os.environ" in body


def test_the_test_runner_covers_every_suite():
    """A suite the runner does not run is a suite nobody runs."""
    runner = open(os.path.join(ROOT, "tests", "run_all.sh"), encoding="utf-8").read()
    assert "tests/test_*.py" in runner and "tests/test_*.mjs" in runner, (
        "run_all.sh must glob both suite kinds, or adding one in the other "
        "language silently adds nothing"
    )


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
    print("all repo-hygiene checks passed")
