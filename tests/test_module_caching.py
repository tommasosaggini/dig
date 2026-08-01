"""The caching contract for extracted browser modules.

The deploy is scp onto a running server: no build step, no CI, and app.html is
read from disk per request. That makes the HTML always fresh and a plain
`<script src="/js/x.js">` never so — the browser keeps whatever it cached, and
a deploy leaves people running a stale module against new markup. That failure
is invisible, varies per user, and looks exactly like a bug in the new code.

So the URL names the content. `_stamp_module_urls` rewrites every /js/ src in
the served HTML to carry the file's content hash; the /js/ route then caches a
stamped URL forever and revalidates an unstamped one. A changed module gets a
new URL and is fetched. An unchanged one is not requested at all — which
matters more than it sounds, because the alternative (revalidate every launch)
spends a round trip per module on a phone, on the one path where latency is
most felt.

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


def test_stamping_is_content_derived_not_mtime():
    """A deploy moves mtime on every file; only changed ones may bust."""
    src = _server()
    fn = src[src.index("def _stamp_module_urls"):]
    fn = fn[:fn.index("\ndef ", 1)]
    assert "_static_entry" in fn or "sha1" in fn, (
        "the stamp must come from the bytes; an mtime-derived one re-downloads "
        "every module on every deploy and defeats the point"
    )
    assert "st_mtime" not in fn


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
