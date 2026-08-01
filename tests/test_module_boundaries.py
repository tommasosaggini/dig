"""Every cross-module reference must be an import.

WHY THIS EXISTS, precisely. Extracting the player moved `_lastDispatchedSource`
into web/js/player.js while the only code that reads and writes it stayed in
app.js. All 17 suites passed and the page then died on the first play with
`Uncaught ReferenceError: _lastDispatchedSource is not defined`.

The suites passed because tests/harness.mjs FLATTENS the module graph into one
scope to run it — a deliberate compromise, documented there, that makes every
module's top-level name visible to every other. It already checked that an
imported name is really exported. It could not check the reverse: a name USED
but never imported, which resolves fine in a flat scope and is a ReferenceError
in a real one.

So this reads the modules as text and does the check the runtime would. It is
the cheapest possible stand-in for real module semantics, and it is aimed at
exactly the mistake an extraction makes.

    python3 tests/test_module_boundaries.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import JS  # noqa: E402

# Provided by the browser or by the SDK loaded in <head>, so a bare reference
# to one is correct rather than a missing import.
AMBIENT = {
    "window", "document", "navigator", "location", "console", "fetch",
    "localStorage", "sessionStorage", "performance", "history", "screen",
    "setTimeout", "setInterval", "clearTimeout", "clearInterval",
    "requestAnimationFrame", "cancelAnimationFrame", "queueMicrotask",
    "Spotify", "MediaMetadata", "Audio", "Image", "Blob", "URL", "FormData",
    "AbortController", "AbortSignal", "IntersectionObserver", "ResizeObserver",
    "Headers", "Request", "Response", "WebSocket", "Worker", "structuredClone",
    "matchMedia", "getComputedStyle", "alert", "confirm", "prompt", "atob", "btoa",
}


def _modules():
    return {f: open(os.path.join(JS, f), encoding="utf-8").read()
            for f in sorted(os.listdir(JS)) if f.endswith(".js")}


def _code_only(src):
    # Strings FIRST, so a `//` inside one cannot be read as a comment opener,
    # and so prose in a log message cannot be read as a reference.
    src = re.sub(r"'(?:[^'\\\n]|\\.)*'", "''", src)
    src = re.sub(r'"(?:[^"\\\n]|\\.)*"', '""', src)
    src = re.sub(r"`(?:[^`\\]|\\.)*`", "``", src)
    src = re.sub(r"/\*[\s\S]*?\*/", "", src)
    # To end of line ANYWHERE, not just comment-only lines: a trailing
    # `// … the queue …` on a code line was read as a use of `queue`.
    src = re.sub(r"//.*$", "", src, flags=re.M)
    # Object-literal KEYS are not references. `host = { currentTrack: … }`
    # declares a property whose name happens to match a function elsewhere;
    # reading it as a use would flag every well-named seam.
    src = re.sub(r"(?m)^(\s*)[A-Za-z_$][\w$]*(\s*:)", r"\1_key_\2", src)
    return src


def _top_level_declared(src):
    """Names this module declares at column 0 — what another module could want."""
    names = set()
    for m in re.finditer(r"^(?:export\s+)?(?:async\s+)?function\*?\s+([A-Za-z_$][\w$]*)",
                         src, re.M):
        names.add(m.group(1))
    for m in re.finditer(r"^(?:export\s+)?(?:const|let|var)\s+([A-Za-z_$][\w$]*)",
                         src, re.M):
        names.add(m.group(1))
    for m in re.finditer(r"^(?:export\s+)?class\s+([A-Za-z_$][\w$]*)", src, re.M):
        names.add(m.group(1))
    return names


def _imported(src):
    names = set()
    for m in re.finditer(r"^import\s+([\s\S]*?)\s+from\s+['\"]", src, re.M):
        clause = m.group(1)
        braced = re.search(r"\{([\s\S]*)\}", clause)
        if braced:
            for part in braced.group(1).split(","):
                t = part.strip()
                if t:
                    names.add(re.split(r"\s+as\s+", t)[-1].strip())
        bare = re.sub(r"\{[\s\S]*\}", "", clause).replace(",", "").strip()
        if bare and not bare.startswith("*"):
            names.add(bare)
    return names


def test_no_module_uses_a_name_it_did_not_import():
    mods = _modules()
    declared = {f: _top_level_declared(s) for f, s in mods.items()}
    offenders = []
    for f, src in mods.items():
        code = _code_only(src)
        used = set(re.findall(r"(?<![.\w$])([A-Za-z_$][\w$]*)", code))
        own = declared[f] | _imported(src) | AMBIENT
        for other, names in declared.items():
            if other == f:
                continue
            for n in names & used - own:
                offenders.append(f"web/js/{f} uses `{n}`, declared in web/js/{other}")
    assert not offenders, (
        "these resolve in the flattened test harness and are a ReferenceError "
        "in the browser:\n  " + "\n  ".join(sorted(set(offenders)))
    )


def test_no_module_declares_a_name_another_also_declares():
    """Two modules owning the same name is a merge waiting to go wrong.

    Harmless under real modules and NOT harmless in the harness, which flattens
    them into one scope — the second declaration would shadow or clash, and the
    tests would then be exercising something the browser never runs.
    """
    mods = _modules()
    seen = {}
    clashes = []
    for f, src in mods.items():
        for n in _top_level_declared(src):
            if n in seen:
                clashes.append(f"`{n}` declared in both {seen[n]} and {f}")
            seen[n] = f
    assert not clashes, "; ".join(clashes)


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
    print("all module-boundary checks passed")
