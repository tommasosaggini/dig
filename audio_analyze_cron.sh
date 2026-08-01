#!/bin/bash
# Keep the pool audio analysis running across days.
#
# Measuring 42k tracks takes ~6 days of continuous downloading at ~285/hr, so
# the job has to survive laptop sleeps, wifi changes, and crashes. It is fully
# resumable (it only picks tracks with no features yet), so the recovery
# strategy is simply: if it isn't running, start it.
#
# Saves go first — they define the taste profile the recommender matches
# against — then the rest of the pool, best tracks first.
#
#   */10 * * * * /Users/tommasosaggini/Sites/dig/audio_analyze_cron.sh >> /Users/tommasosaggini/Sites/dig/audio_analyze.log 2>&1
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
export PYTHONUNBUFFERED=1

# Already running? Leave it alone — two workers would race on the same rows.
if pgrep -f "audio_analyze.py" > /dev/null; then
  exit 0
fi

if [ -f "$DIR/venv/bin/python3" ]; then
  PYTHON="$DIR/venv/bin/python3"
elif [ -x "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3" ]; then
  PYTHON="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"
else
  PYTHON="$(command -v python3 || echo /usr/bin/python3)"
fi

PG_TUNNEL_PORT="${PG_TUNNEL_PORT:-5433}"
if ! nc -z 127.0.0.1 "$PG_TUNNEL_PORT" 2>/dev/null; then
  ssh -fN -o ExitOnForwardFailure=yes -o BatchMode=yes \
      -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
      -L "${PG_TUNNEL_PORT}:10.0.3.2:5432" root@91.99.188.232 \
    && sleep 2 || { echo "FATAL: no db tunnel"; exit 1; }
fi

echo "===== AUDIO ANALYSIS: $(date '+%Y-%m-%d %H:%M:%S') ====="
# Saves first (they shape the taste profile), then the wider pool.
"$PYTHON" pipeline/audio_analyze.py --scope saves
"$PYTHON" pipeline/audio_analyze.py --scope all --limit 2000
echo "===== PAUSED: $(date '+%Y-%m-%d %H:%M:%S') ====="
