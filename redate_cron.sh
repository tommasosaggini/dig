#!/bin/bash
# DIG — recover original release years for Bandcamp reissues, from MusicBrainz.
#
# See scripts/redate_bandcamp_reissues.py for WHY. In one line: Bandcamp's
# release_date is the date the page went up, so reissued back-catalogue enters
# the pool wearing a modern year and the era axis (lib/era.py) counts it as
# evidence the 2020s are over-supplied. This walks the backlog and puts the
# real years back.
#
# Cadence: hourly. The batch is bounded by MusicBrainz's 1 req/sec, not by
# anything of ours — 300 pairs is ~10 minutes of requests, and there are
# ~53,800 pairs, so a full sweep is about three weeks. MB is free and
# unmetered and this spends ZERO Spotify quota, so the only cost is patience.
#
#   0 * * * * /Users/tommasosaggini/Sites/dig/redate_cron.sh >> /Users/tommasosaggini/Sites/dig/redate.log 2>&1
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
export PYTHONUNBUFFERED=1

# Cron's PATH is minimal and /usr/bin/python3 is the system 3.9, which lacks
# psycopg2 and cannot parse the `str | None` syntax lib/era.py uses. Same
# ladder as dig_cron.sh.
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
# no-op that still marks pairs checked. Same pattern as dig_sync_cron.sh.
PG_TUNNEL_PORT="${PG_TUNNEL_PORT:-5433}"
PG_TUNNEL_TARGET="${PG_TUNNEL_TARGET:-10.0.3.2:5432}"
PG_TUNNEL_HOST="${PG_TUNNEL_HOST:-root@91.99.188.232}"

if ! nc -z 127.0.0.1 "$PG_TUNNEL_PORT" 2>/dev/null; then
  ssh -fN -o ExitOnForwardFailure=yes -o BatchMode=yes \
      -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
      -L "${PG_TUNNEL_PORT}:${PG_TUNNEL_TARGET}" "$PG_TUNNEL_HOST" \
    && sleep 2 || { echo "$(date '+%F %T') FATAL: no db tunnel. Aborting."; exit 1; }
fi

# Never stack two of these: the pacing inside is per-process, and a second
# copy would double our rate against MusicBrainz. The script guards against
# its MB-using SIBLINGS; this guards against itself.
if pgrep -f "redate_bandcamp_reissues" >/dev/null 2>&1; then
  echo "$(date '+%F %T') a re-dating run is still in flight — skipping this slot."
  exit 0
fi

echo "===== BANDCAMP RE-DATE: $(date '+%Y-%m-%d %H:%M:%S') ====="
"$PYTHON" scripts/redate_bandcamp_reissues.py --limit "${1:-300}"
echo "===== DONE: $(date '+%Y-%m-%d %H:%M:%S') ====="
