"""The caching contract for extracted browser modules.

The deploy is scp onto a running server: no build step, no CI, and app.html is
read from disk per request. That makes the HTML always fresh and a plain
`<script src="/js/x.js">` never so — the browser keeps whatever it cached, and
a deploy leaves people running a stale module against new markup. That failure
is invisible, varies per user, and looks exactly like a bug in the new code.

So the URL names the content, and "the content" means the whole import graph.
`_js_module` hashes a module together with everything it imports, transitively;
`_stamp_module_urls` puts that hash on the /js/ src in the served HTML, and the
/js/ route rewrites each `from './x.js'` inside a served module to carry x's
own graph hash. A stamped URL is then cached forever and an unstamped one
revalidates.

Hashing the closure rather than the file is what makes `immutable` safe: a
module's own bytes do not name what a browser ends up running. Stamp app.js
with only its own hash and editing env.js changes nothing about app.js's URL —
a client holding an immutable copy never re-fetches it, and never learns there
is a new env.js either, because the import specifier inside it is stamped too.
Editing a leaf would silently reach nobody.

    python3 tests/test_module_caching.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.join(ROOT, "web")
SERVER = os.path.join(ROOT, "server.py")


def _server():
    with open(SERVER, encoding="utf-8") as fh:
        return fh.read()


def _app():
    with open(os.path.join(WEB, "app.html"), encoding="utf-8") as fh:
        return fh.read()


def test_every_module_reference_gets_stamped():
    """Whatever app.html asks for, the rewriter must actually match."""
    import server  # noqa: E402  — imported late; it reads env at module scope

    html = open(os.path.join(WEB, "app.html"), "rb").read()
    stamped = server._stamp_module_urls(html)

    refs = re.findall(rb'(?:src|href)="(/js/[^"]+\.js)[^"]*"', html)
    for ref in refs:
        assert re.search(
            re.escape(ref) + rb"\?v=[0-9a-f]{8,}", stamped
        ), f"{ref.decode()} was not stamped — it would be cached across deploys"


def test_imported_modules_are_stamped_too():
    """A bare `from './env.js'` never passes through the HTML.

    The browser resolves it itself, so stamping only the HTML leaves every
    imported module falling back to revalidation — a round trip per module on
    each launch, on exactly the mobile path this caching exists to keep quiet.
    """
    import server  # noqa: E402

    served = server._js_module("js/app.js")["body"].decode()
    bare = re.findall(r"""from\s+['"](\./[^'"?]+\.js)['"]""", served)
    assert not bare, f"unstamped import specifiers survive serving: {bare}"
    assert re.search(r"""from\s+['"]\./env\.js\?v=[0-9a-f]{8,}['"]""", served), (
        "app.js imports env.js; the served body must name the version it means"
    )


def test_a_change_to_a_leaf_busts_everything_that_can_reach_it():
    """The hash covers the transitive closure, not one file's bytes.

    If app.js were stamped with only its own hash, editing env.js would change
    nothing about app.js's URL — a client holding an immutable copy would never
    re-fetch it, and because the import specifier inside it is stamped too, it
    would never learn there is a new env.js either. Editing a leaf would reach
    nobody. That is the failure mode that makes `immutable` unsafe, and it is
    silent.
    """
    import server  # noqa: E402

    before = server._js_module("js/app.js")["hash"]
    leaf = os.path.join(WEB, "js", "env.js")
    original = open(leaf, "rb").read()
    try:
        with open(leaf, "wb") as fh:
            fh.write(original + b"\n// cache-bust probe\n")
        server._static_cache.clear()   # the mtime check is second-granularity
        after = server._js_module("js/app.js")["hash"]
    finally:
        with open(leaf, "wb") as fh:
            fh.write(original)
        server._static_cache.clear()
        server._js_graph_cache.clear()
    assert before != after, (
        "editing a leaf module left the entry point's version unchanged; every "
        "client with a cached copy would keep running the old graph forever"
    )


def test_stamping_is_content_derived_not_mtime():
    """A deploy moves mtime on every file; only changed ones may bust.

    scp rewrites everything, so an mtime-derived version would re-download the
    whole graph on every deploy — the precise cost this design exists to avoid.
    """
    src = _server()
    fn = src[src.index("def _js_module"):]
    fn = fn[:fn.index("\ndef ", 1)]
    assert "hashlib.sha1(raw" in fn, (
        "the stamp must be taken over the module's BYTES"
    )
    assert "st_mtime" not in fn and "getmtime" not in fn


def test_a_missing_module_does_not_take_the_page_down():
    """An unreadable file must degrade to an unstamped URL, not a 500."""
    src = _server()
    fn = src[src.index("def _stamp_module_urls"):]
    fn = fn[:fn.index("\ndef ", 1)]
    assert "except OSError" in fn, (
        "stamping runs on the request path for every page load; it must never "
        "be the thing that fails one"
    )


def test_only_a_versioned_url_is_cached_forever():
    src = _server()
    route = src[src.index('if parsed.path.startswith("/js/")'):]
    route = route[:route.index("# Prevent browsers from caching stale HTML")]
    assert "immutable" in route, "a content-named URL should never be re-fetched"
    assert "versioned" in route and '"no-cache"' in route, (
        "an unstamped URL cannot prove which version it is, so it must "
        "revalidate rather than be trusted"
    )


def test_the_route_cannot_escape_the_web_directory():
    """This is the one route that maps a URL onto the filesystem."""
    src = _server()
    route = src[src.index('if parsed.path.startswith("/js/")'):]
    route = route[:route.index("# Prevent browsers from caching stale HTML")]
    assert "normpath" in route and "WEB_DIR" in route, (
        "a path-traversal guard is not optional on a filesystem-backed route"
    )


def test_modules_are_reachable_from_the_page():
    """Every file in web/js must be referenced, directly or by import.

    An orphaned module is a file the tests exercise, the deploy ships, and the
    browser never loads — which is how a refactor silently half-lands.
    """
    js_dir = os.path.join(WEB, "js")
    if not os.path.isdir(js_dir):
        return  # extraction has not started yet
    names = [f for f in os.listdir(js_dir) if f.endswith(".js")]
    if not names:
        return
    blob = _app() + "".join(
        open(os.path.join(js_dir, f), encoding="utf-8").read() for f in names
    )
    for f in names:
        assert f in blob, f"web/js/{f} is not referenced by the page or any module"


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
    print("all module-caching checks passed")
