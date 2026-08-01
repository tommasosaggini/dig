"""Where the browser code lives, for the static suites.

It used to live in one place — a single inline <script> in web/app.html — and
every test hardcoded that path. When the script moved to web/js/, six suites
broke at once for a reason that had nothing to do with what they assert.

So the location is a decision made here, once. A static test almost always
wants "all the browser source", markup included: the assertions mix code
(a guard, a constant) with markup (a banner's wording, an element id), and
which file a given thing lives in is exactly the detail these tests should not
depend on.
"""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.join(ROOT, "web")
JS = os.path.join(WEB, "js")


def module_files() -> list:
    """Every browser module, in a stable order (load order is not implied)."""
    if not os.path.isdir(JS):
        return []
    return [os.path.join(JS, f) for f in sorted(os.listdir(JS)) if f.endswith(".js")]


def browser_source() -> str:
    """app.html and every module, concatenated.

    Joined with a newline so a construct at the end of one file and one at the
    start of the next can never be matched as if they were adjacent.
    """
    parts = []
    with open(os.path.join(WEB, "app.html"), encoding="utf-8") as fh:
        parts.append(fh.read())
    for path in module_files():
        with open(path, encoding="utf-8") as fh:
            parts.append(fh.read())
    return "\n".join(parts)
