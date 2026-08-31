"""Regression lock for "the tunnel was up, and nothing could reach the database".

Measured 2026-08-31, the first full day after the dig database was taken off
the public internet (a DOCKER-USER DROP on 5433; the laptop's jobs reach it
through `ssh -L` from then on).

The mechanism:

  11:30  ig_cron finds no listener, runs `ssh -fN`, prints "tunnel up on 5433".
         The laptop had just woken; its network was still returning "No route
         to host" to a sibling job in the same minute. `ssh -fN` forks once the
         local port is BOUND — which is before the forwarded channel works, and
         ExitOnForwardFailure covers only the bind.
  11:45  `nc -z 127.0.0.1 5433` passes, because a listener genuinely exists.
         The run skips the repair. Every stage fails "connection refused".
  12:00  ssh's own keepalive finally gives up, releases the port, and the next
         run rebuilds a tunnel that works.

Half an hour of writes, and the guard reported healthy throughout. `nc -z`
answers "is something listening", which is not the question any of these jobs
has. The question is "does the database answer", and only a query answers it.

Five scripts carried their own copy of that guard, which is why the one script
that had learned to clear a stale tunnel first (ig_cron.sh) fixed nothing for
the other four — and cleared it on the branch where the port was already known
to be dead, i.e. never on the branch that mattered.

Locked below: the probe must see through a bound-but-dead listener, and no
cron script may go back to asking the port.

    python3 tests/test_db_reachability_is_not_a_port_check.py
"""
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_source import ROOT  # noqa: E402

sys.path.insert(0, ROOT)

from lib.pg_probe import probe  # noqa: E402

CRON_SCRIPTS = [
    "dig_sync_cron.sh",
    "ig_cron.sh",
    "ig_likes_sync.sh",
    "audio_analyze_cron.sh",
    "redate_cron.sh",
]

failures = []


def check(label, cond, detail=""):
    if cond:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label} {detail}")
        failures.append(label)


def _dead_listener(port, seconds=12):
    """A socket that binds, accepts, and then says nothing — the 11:45 state."""
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(8)
    held = []

    def serve():
        end = time.time() + seconds
        while time.time() < end:
            try:
                srv.settimeout(1)
                held.append(srv.accept()[0])   # accept, then never answer
            except socket.timeout:
                pass
        srv.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    return srv


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


print("a bound-but-dead listener is not a reachable database")
port = _free_port()
_dead_listener(port)
time.sleep(0.3)

# The old guard, reproduced exactly: this is what every script used to ask.
probe_sock = socket.socket()
probe_sock.settimeout(3)
nc_z_passes = probe_sock.connect_ex(("127.0.0.1", port)) == 0
probe_sock.close()
check("the port answers, so `nc -z` would have passed", nc_z_passes)

os.environ["DATABASE_URL"] = f"postgresql://dig:x@127.0.0.1:{port}/dig"
started = time.time()
reason = probe(timeout=4)
elapsed = time.time() - started
check("probe() refuses it anyway", reason is not None, f"(got {reason!r})")
check("and bounds its own wait", elapsed < 12, f"(took {elapsed:.1f}s)")

print("a closed port is refused too")
os.environ["DATABASE_URL"] = f"postgresql://dig:x@127.0.0.1:{_free_port()}/dig"
check("probe() returns a reason, not an exception", probe(timeout=3) is not None)

print("no cron script asks the port instead of the database")
for name in CRON_SCRIPTS:
    with open(os.path.join(ROOT, name), encoding="utf-8") as fh:
        body = "".join(l for l in fh if not l.lstrip().startswith("#"))
    check(f"{name}: no `nc -z` guard", "nc -z" not in body)
    check(f"{name}: no private `ssh -fN`", "ssh -fN" not in body)
    check(f"{name}: sources the shared helper", "lib/pg_tunnel.sh" in body)
    check(f"{name}: calls pg_tunnel_ensure", "pg_tunnel_ensure" in body)

print("the helper itself keeps its two hard-won properties")
with open(os.path.join(ROOT, "lib", "pg_tunnel.sh"), encoding="utf-8") as fh:
    helper = fh.read()
body = "".join(l for l in helper.splitlines(True) if not l.lstrip().startswith("#"))
check("binds 127.0.0.1 explicitly, so it cannot half-bind a dual stack",
      '-L "127.0.0.1:${port}:${target}"' in body)
check("waits on the probe rather than a fixed sleep",
      "while [ \"$waited\" -lt \"$wait_s\" ]" in body and "pg_probe" in body)
check("evicts by port holder, not by command-line match",
      "lsof -t -nP -iTCP" in body)
check("and only ever kills ssh", 'if [ "$holder_cmd" = "ssh" ]' in body)

print()
if failures:
    print(f"FAILED ({len(failures)}): " + ", ".join(failures))
    sys.exit(1)
print("all good")
