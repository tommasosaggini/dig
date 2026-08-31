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

# cron's PATH has no Homebrew, and yt-dlp shells out to ffmpeg/ffprobe to
# extract mp3. Without this every download died in postprocessing — and because
# a failure marks the track analysed, each one was burned permanently.
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

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

# The database is not reachable from the internet; everything here goes
# through an SSH tunnel. ONE implementation, in lib/pg_tunnel.sh — this
# block used to be copy-pasted into five scripts and its `nc -z` check
# could not tell a working tunnel from a listener with a dead channel
# behind it. That cost half an hour of writes on 2026-08-31.
. "$DIR/lib/pg_tunnel.sh"
pg_tunnel_ensure || { echo "Aborting audio analysis."; exit 1; }

echo "===== AUDIO ANALYSIS: $(date '+%Y-%m-%d %H:%M:%S') ====="
# Saves first (they shape the taste profile), then the wider pool.
# nice: this runs for days on a laptop that is also being used. yt-dlp shells
# out to ffmpeg for mp3 extraction on every track, and nothing waits on the
# result, so it should always yield to interactive work.
nice -n 15 "$PYTHON" pipeline/audio_analyze.py --scope saves
nice -n 15 "$PYTHON" pipeline/audio_analyze.py --scope all --limit 2000
echo "===== PAUSED: $(date '+%Y-%m-%d %H:%M:%S') ====="
