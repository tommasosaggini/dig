"""Load `.env` into os.environ, once, the same way everywhere.

Thirty files carried a copy of this loop — pipeline steps, scripts, and the
server — in SIX distinct implementations, and the drift was semantic rather
than cosmetic:

    os.environ.setdefault(k, v)     22 files — the environment wins
    os.environ[k] = v                8 files — the FILE wins

So `ANTHROPIC_API_KEY=sk-test python3 pipeline/analyze_pool.py` silently ran
against the key in `.env` instead of the one on the command line, while the
same invocation of `pipeline/discover.py` honoured it — whose copy carried the
comment "allows callers to override e.g. ANTHROPIC_API_KEY for testing",
stating the intent that the other eight quietly broke.

THE ENVIRONMENT WINS. `.env` is a default, not an override: it is what you get
when you have not said otherwise, which is the only reading under which
per-invocation overrides, CI secrets and `docker run -e` all behave.
"""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(ROOT, ".env")

_loaded = False


def _parse_line(line):
    """One `.env` line → (key, value), or None if it carries neither.

    Shared by load_env() and read_env_file() so the two cannot drift — the
    drift between six copies of this parsing is the whole reason this module
    exists.
    """
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        return None
    # `export FOO=bar` is valid in a file that is also sourced by a shell, and
    # every hand-rolled copy of this loop would have set a variable literally
    # named "export FOO".
    if line.startswith("export "):
        line = line[len("export "):].lstrip()
    key, _, val = line.partition("=")
    key = key.strip()
    if not key:
        return None
    val = val.strip()
    # Strip one matching pair of surrounding quotes — again, a file that a
    # shell may source can carry them, and every copy of this loop would have
    # included the quotes in the value.
    if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
        val = val[1:-1]
    return key, val


def read_env_file(keys, path=None):
    """What the FILE says about `keys`, ignoring os.environ entirely.

    The deliberate opposite of load_env, for the rare value where the
    environment is not a caller's intent but a stale snapshot — see
    pipeline/ig_publish.py:_creds(), where docker-compose's `env_file:` bakes a
    rotating Instagram token into the container at create time and never reads
    the file again. Sets nothing; returns only the names it actually found, so
    a caller can fall back to os.environ for the rest.
    """
    target = path or ENV_PATH
    wanted = set(keys)
    found = {}
    try:
        with open(target, encoding="utf-8") as fh:
            for line in fh:
                kv = _parse_line(line)
                if kv and kv[0] in wanted:
                    found[kv[0]] = kv[1]
    except OSError:
        pass  # no file (a real 12-factor deploy) — caller falls back
    return found


def load_env(path=None, force=False):
    """Fill in anything `.env` defines that the environment has not.

    Idempotent: importing modules that each call this — which is most of
    them — costs one file read, not thirty. Missing file is not an error;
    production passes real environment variables and has no `.env` at all.

    Returns the number of names actually set, so a caller that cares can tell
    "loaded nothing because it was all already set" from "found no file".
    """
    global _loaded
    if _loaded and not force:
        return 0
    _loaded = True

    target = path or ENV_PATH
    if not os.path.exists(target):
        return 0

    n = 0
    with open(target, encoding="utf-8") as fh:
        for line in fh:
            kv = _parse_line(line)
            if not kv:
                continue
            key, val = kv
            if key not in os.environ:
                os.environ[key] = val
                n += 1
    return n
