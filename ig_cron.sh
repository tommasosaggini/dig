#!/bin/bash
# DIG → Instagram curation pipeline driver.
# Separate from dig_cron.sh (discovery) on purpose: this path costs ZERO Spotify
# quota and must keep running even when discovery is in a Spotify cooldown.
#
# Suggested cron (hourly is plenty for an every-2-days cadence):
#   17 * * * * /Users/tommasosaggini/Sites/dig/ig_cron.sh >> /Users/tommasosaggini/Sites/dig/ig_cron.log 2>&1
#
# Stages:
#   1. propose   — top up suggestions from the admin's likes (admin approves in UI)
#   2. resolve   — download full audio for approved items (Bandcamp → yt-dlp)
#   3. render    — build clip.mp3 + cards + feed/story mp4 for scheduled items
#   4. publish   — push due, rendered items to Instagram (DRY-RUN until creds set)

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
export PYTHONUNBUFFERED=1

# Same Python auto-detect as dig_cron.sh (cron PATH is minimal).
if [ -f "$DIR/venv/bin/python3" ]; then
  PYTHON="$DIR/venv/bin/python3"
elif [ -x "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3" ]; then
  PYTHON="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"
elif command -v python3 &>/dev/null; then
  PYTHON="python3"
else
  PYTHON="/usr/bin/python3"
fi

"$PYTHON" -c "import sys; assert sys.version_info >= (3, 10); import psycopg2" || {
  echo "FATAL: $PYTHON unusable (missing psycopg2 or <3.10). Aborting ig_cron."
  exit 1
}

echo ""
echo "===== DIG IG RUN: $(date '+%Y-%m-%d %H:%M:%S') ====="

echo "--- propose ---"
"$PYTHON" pipeline/ig_propose.py 2>&1 || echo "(propose failed)"

echo "--- resolve audio ---"
"$PYTHON" pipeline/ig_audio_resolver.py 2>&1 || echo "(resolve failed)"

echo "--- render ---"
"$PYTHON" pipeline/ig_render.py 2>&1 || echo "(render failed — ffmpeg/Pillow installed?)"

echo "--- publish ---"
"$PYTHON" pipeline/ig_publish.py 2>&1 || echo "(publish failed)"

echo "===== IG DONE: $(date '+%Y-%m-%d %H:%M:%S') ====="
