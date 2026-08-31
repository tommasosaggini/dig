#!/usr/bin/env python3
"""Is the DATABASE reachable — not "is something listening on the port".

Five cron scripts on this laptop asked `nc -z 127.0.0.1 5433` and treated a
bound socket as a working tunnel. On 2026-08-31 that cost half an hour of
writes. `ssh -fN` binds the local port and forks as soon as the LISTENER is
up, which is before the forwarded channel is usable — and `ExitOnForwardFailure`
only covers the bind, not a channel that never comes up or dies later. So the
11:30 run, firing while the laptop's network was still returning "No route to
host", opened a listener with nothing behind it and reported "tunnel up on
5433". `nc -z` then passed at 11:45; every stage of ig_cron skipped the repair
and failed with "connection refused"; and only at 12:00 — when ssh's own
keepalive finally gave up and released the port — did a run rebuild it.

A connection that actually carries a query is the only check that tells those
two states apart. Exit 0 = the database answered. Exit 1 = it did not, reason
on stderr.

Deliberately not lib.db.get_conn(): this needs a SHORT connect_timeout (a
probe that hangs for the kernel default is not a probe), and it must not
inherit the session GUCs get_conn() sets for real work.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env


def probe(timeout: int = 5) -> str | None:
    """Return None if the database answered, else the reason it did not."""
    load_env()
    url = os.environ.get("DATABASE_URL")
    if not url:
        return "DATABASE_URL not set"
    try:
        import psycopg2
    except ImportError:
        return "psycopg2 not installed"
    try:
        conn = psycopg2.connect(url, connect_timeout=timeout)
    except Exception as e:
        return f"{type(e).__name__}: {str(e).strip().splitlines()[0]}"
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    except Exception as e:
        return f"{type(e).__name__}: {str(e).strip().splitlines()[0]}"
    finally:
        conn.close()
    return None


if __name__ == "__main__":
    reason = probe(int(os.environ.get("PG_PROBE_TIMEOUT", "5")))
    if reason:
        print(reason, file=sys.stderr)
        sys.exit(1)
    sys.exit(0)
