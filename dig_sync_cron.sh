#!/bin/bash
# DIG — pull Spotify listened/liked into DIG's ledger.
#
# SEPARATE from dig_cron.sh on purpose, for three reasons:
#
#   1. dig_cron.sh is not in any crontab (its log stops 2026-04-28), so
#      anything appended to it does not run.
#   2. Even when it does run, it aborts on `pre_flight_or_exit` the moment
#      Spotify is in a quota cooldown — and it is the LAST thing in the file,
#      so any earlier hang skips it too. The sync is 2 calls; it must not be
#      hostage to a discovery pipeline that spends thousands.
#   3. Cadence. Discovery wants to run every few hours. This wants to run
#      often, because /me/player/recently-played holds only the last FIFTY
#      plays — measured, not assumed: paging back with `before` returns an
#      empty page immediately. Miss that window and the listening is gone for
#      good; there is no API that returns it later.
#
# At 50 items per window, every 30 minutes tolerates 100 tracks/hour of
# listening before anything can be lost. Cost is 2 API calls per run, paced
# through lib/spotify_gate like everything else.
#
#   */30 * * * * /Users/tommasosaggini/Sites/dig/dig_sync_cron.sh >> /Users/tommasosaggini/Sites/dig/dig_sync.log 2>&1
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
export PYTHONUNBUFFERED=1

if [ -f "$DIR/venv/bin/python3" ]; then
  PYTHON="$DIR/venv/bin/python3"
elif [ -x "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3" ]; then
  PYTHON="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"
elif [ -x "/Library/Frameworks/Python.framework/Versions/3.11/bin/python3" ]; then
  PYTHON="/Library/Frameworks/Python.framework/Versions/3.11/bin/python3"
else
  PYTHON="$(command -v python3 || echo /usr/bin/python3)"
fi

# The DB is reached through an SSH tunnel; without it every run is a silent
# no-op. Same pattern as ig_likes_sync.sh.
PG_TUNNEL_PORT="${PG_TUNNEL_PORT:-5433}"
PG_TUNNEL_TARGET="${PG_TUNNEL_TARGET:-10.0.3.2:5432}"
PG_TUNNEL_HOST="${PG_TUNNEL_HOST:-root@91.99.188.232}"

if ! nc -z 127.0.0.1 "$PG_TUNNEL_PORT" 2>/dev/null; then
  ssh -fN -o ExitOnForwardFailure=yes -o BatchMode=yes \
      -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
      -L "${PG_TUNNEL_PORT}:${PG_TUNNEL_TARGET}" "$PG_TUNNEL_HOST" \
    && sleep 2 || { echo "$(date '+%F %T') FATAL: no db tunnel. Aborting sync."; exit 1; }
fi

# --force bypasses the per-user 5-minute rate gate, which exists to stop a
# reload-happy browser from spending quota. A 30-minute cron is the pace the
# gate was protecting against, not something it needs to throttle.
echo "===== SPOTIFY LEDGER SYNC: $(date '+%Y-%m-%d %H:%M:%S') ====="
"$PYTHON" -m lib.spotify_sync --force
echo "===== DONE: $(date '+%Y-%m-%d %H:%M:%S') ====="
