# DIG — one implementation of "make sure the database is reachable".
#
# Sourced by every cron script on this laptop. There were five hand-rolled
# copies of this block (dig_sync_cron, ig_cron, ig_likes_sync, audio_analyze_cron,
# redate_cron) and they had already drifted: only ig_cron.sh had learned to
# pkill a stale tunnel first, and it ran that pkill only on the branch where
# the port was already known to be dead — i.e. never on the branch where it
# would have mattered. Same shape as the six copies of the .env loop that
# lib/env.py exists to end.
#
# Two things every copy got wrong, both of which cost writes on 2026-08-31:
#
#   1. `nc -z 127.0.0.1 5433` tests that SOMETHING is listening locally. It
#      cannot distinguish a working tunnel from a listener with a dead channel
#      behind it, which is exactly what `ssh -fN` leaves when it forks while
#      the network is still coming up. See lib/pg_probe.py for the incident.
#      The check here is a real SELECT 1.
#
#   2. `ssh -fN … && sleep 2` treats "ssh forked" as "the tunnel works". It
#      does not; the fork happens at bind time. We poll the probe instead of
#      guessing at a sleep.
#
# Requires the caller to have resolved $PYTHON (every one of these scripts
# already walks a Python ladder, because cron's PATH finds the system 3.9
# which has no psycopg2).

# Is the database answering right now?
pg_probe() {
  PG_PROBE_TIMEOUT="${PG_PROBE_TIMEOUT:-5}" \
    "${PYTHON:-python3}" "${DIR}/lib/pg_probe.py" 2>/dev/null
}

# Make the database reachable, or fail loudly. Returns 0 only when a query
# has actually round-tripped.
pg_tunnel_ensure() {
  local port="${PG_TUNNEL_PORT:-5433}"
  local target="${PG_TUNNEL_TARGET:-10.0.3.2:5432}"
  local host="${PG_TUNNEL_HOST:-root@91.99.188.232}"
  local wait_s="${PG_TUNNEL_WAIT_S:-45}"

  # Fast path: already working. Costs one local connection.
  if pg_probe; then
    return 0
  fi

  echo "--- db unreachable, rebuilding tunnel ---"

  # Tear down first: the port may be held by a listener whose channel is dead
  # — the state that made the old `nc -z` guard lie — and ssh will not bind
  # over it, so without eviction the repair silently no-ops.
  #
  # Evict by WHO HOLDS THE PORT, not by matching a command line. The old
  # `pkill -f "5433:10.0.3.2:5432"` only matched a tunnel spelled exactly that
  # way, so a stale ssh from an older revision of these scripts (different
  # flags, or the unqualified -L this file used to emit) survived the pkill
  # and kept the port. lsof answers the question we actually have.
  #
  # Only ever kill ssh. If something else on this laptop owns 5433 we must
  # fail loudly rather than shoot an unrelated process.
  local holder_pid holder_cmd
  for holder_pid in $(lsof -t -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null); do
    holder_cmd="$(ps -o comm= -p "$holder_pid" 2>/dev/null | xargs basename 2>/dev/null)"
    if [ "$holder_cmd" = "ssh" ]; then
      echo "    evicting stale tunnel (pid $holder_pid)"
      kill "$holder_pid" 2>/dev/null
    else
      echo "    port $port is held by ${holder_cmd:-?} (pid $holder_pid), not ours — refusing to kill it"
    fi
  done
  sleep 1

  # Bind 127.0.0.1 EXPLICITLY, and keep DATABASE_URL on 127.0.0.1 to match.
  # Unqualified `-L 5433:…` makes ssh bind every loopback address it can, and
  # a PARTIAL bind is not a failure it exits on: caught in testing on
  # 2026-08-31 holding [::1]:5433 alone while something else held
  # 127.0.0.1:5433, printing "Address already in use" and carrying on. Which
  # half a client gets then depends on how its resolver orders localhost —
  # another way to be half-connected and not know it.
  ssh -fN -o ExitOnForwardFailure=yes -o BatchMode=yes \
      -o ConnectTimeout=10 \
      -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
      -L "127.0.0.1:${port}:${target}" "$host" 2>&1

  # Poll rather than sleep. A laptop waking from sleep needs a few seconds of
  # network before ssh can finish the channel; a genuinely broken tunnel never
  # will, and the deadline is what separates them.
  local waited=0
  while [ "$waited" -lt "$wait_s" ]; do
    if pg_probe; then
      echo "tunnel up on ${port} (after ${waited}s)"
      return 0
    fi
    sleep 3
    waited=$((waited + 3))
  done

  # Leave nothing behind that a later run could mistake for a live tunnel.
  # (It no longer could — every caller probes — but a dead listener squatting
  # on the port would block the next repair's bind.)
  pkill -f "${port}:${target}" 2>/dev/null
  local reason
  reason="$(PG_PROBE_TIMEOUT=5 "${PYTHON:-python3}" "${DIR}/lib/pg_probe.py" 2>&1 >/dev/null)"
  echo "$(date '+%F %T') FATAL: db unreachable after ${wait_s}s — ${reason}"
  return 1
}
