"""DIG — .env loader.

Single source for populating os.environ from the repo-root .env file. Imported
by lib.db and every pipeline/script entry point (previously each re-implemented
this loop inline ~25 times, with two divergent semantics).
"""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_env(path=None):
    """Populate os.environ from KEY=VALUE lines in .env (repo root by default).

    Existing environment variables win (setdefault): the container's
    docker-compose env takes precedence over the bind-mounted .env file.
    """
    env_path = path or os.path.join(ROOT, ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
