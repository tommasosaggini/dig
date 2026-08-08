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
#   2. caption   — headline + the artist's Instagram @mention where one exists
#   3. resolve   — download full audio for approved items (Bandcamp → yt-dlp)
#   4. render    — build clip.mp3 + cards + feed/story mp4 for scheduled items
#   5. publish   — push due, rendered items to Instagram (DRY-RUN until creds set)

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
export PYTHONUNBUFFERED=1

# cron runs with PATH=/usr/bin:/bin:/usr/sbin:/sbin — Homebrew is NOT on it.
# ffmpeg/ffprobe live in /usr/local/bin (Intel) or /opt/homebrew/bin (Apple
# silicon), so every render failed with "ffmpeg not found on PATH" while the
# same command worked fine by hand. The Python auto-detect below exists for
# exactly this reason; ffmpeg needed it too.
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

# Only one run at a time. This is on a */2 cron but a render pass takes minutes,
# so without a lock every tick launched another overlapping run: the moment
# ffmpeg started working the pile-up drove load average past 200 and macOS
# clamped the CPU to 20%. mkdir is the atomic test-and-set that always exists;
# macOS has no flock(1).
LOCK="$DIR/.ig_cron.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  # Reap a lock left behind by a crash/reboot rather than wedging forever.
  if [ -f "$LOCK/pid" ] && ! kill -0 "$(cat "$LOCK/pid")" 2>/dev/null; then
    rm -rf "$LOCK"
    mkdir "$LOCK" 2>/dev/null || exit 0
  else
    exit 0
  fi
fi
echo $$ > "$LOCK/pid"
trap 'rm -rf "$LOCK"' EXIT INT TERM

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

# The database lives on prod and is not exposed publicly, so everything here
# runs through an SSH tunnel. Tunnels die — laptop sleeps, wifi changes,
# network moves. Re-open it if the port isn't answering, otherwise every stage
# below fails with "connection refused" and the queue silently stops moving.
PG_TUNNEL_PORT="${PG_TUNNEL_PORT:-5433}"
PG_TUNNEL_TARGET="${PG_TUNNEL_TARGET:-10.0.3.2:5432}"
PG_TUNNEL_HOST="${PG_TUNNEL_HOST:-root@91.99.188.232}"

if ! nc -z 127.0.0.1 "$PG_TUNNEL_PORT" 2>/dev/null; then
  echo "--- db tunnel down, reopening ---"
  pkill -f "${PG_TUNNEL_PORT}:${PG_TUNNEL_TARGET}" 2>/dev/null
  ssh -fN -o ExitOnForwardFailure=yes -o BatchMode=yes \
      -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
      -L "${PG_TUNNEL_PORT}:${PG_TUNNEL_TARGET}" "$PG_TUNNEL_HOST" \
    && sleep 2 && echo "tunnel up on ${PG_TUNNEL_PORT}" \
    || { echo "FATAL: could not open db tunnel. Aborting ig_cron."; exit 1; }
fi

echo "--- refresh ig token ---"
"$PYTHON" pipeline/ig_refresh_token.py 2>&1 || echo "(token refresh failed)"

echo "--- propose ---"
"$PYTHON" pipeline/ig_propose.py 2>&1 || echo "(propose failed)"

# Caption headline + the artist's @mention. Straight after propose so the
# dashboard shows the finished caption while the post is still under review,
# and cheap on repeats: every artist, hit or miss, is cached.
echo "--- artist instagram links ---"
"$PYTHON" pipeline/ig_artist_ig.py 2>&1 || echo "(artist link failed)"

echo "--- resolve audio ---"
"$PYTHON" pipeline/ig_audio_resolver.py 2>&1 || echo "(resolve failed)"

echo "--- autoclip ---"
"$PYTHON" pipeline/ig_autoclip.py 2>&1 || echo "(autoclip failed)"

# Once a track has audio on disk its labels can be measured rather than guessed
# from the title. Runs before render because the cover art is generated FROM
# those labels — relabelling after would leave the picture describing the wrong
# song. Only touches queued tracks; the wider pool keeps its text labels.
echo "--- relabel from audio ---"
"$PYTHON" pipeline/ig_relabel_audio.py 2>&1 || echo "(audio relabel failed)"

echo "--- cover art ---"
"$PYTHON" pipeline/ig_cover_art.py 2>&1 || echo "(cover art lookup failed)"

echo "--- render ---"
# nice: rendering is the only genuinely CPU-hungry stage and nothing waits on
# it, so it should always lose to whatever the machine is actually being used
# for. Belt-and-braces with the -threads cap inside mux_video().
nice -n 15 "$PYTHON" pipeline/ig_render.py 2>&1 || echo "(render failed — ffmpeg/Pillow installed?)"

# Rendering happens here (residential IP for yt-dlp, ffmpeg installed); prod
# only serves. Instagram fetches the mp4 over HTTPS and the dashboard streams
# it, so both need the files to actually be on the server — push after every
# render or an approved post is a video that exists only on this laptop.
echo "--- sync media to prod ---"
./ig_sync_media.sh 2>&1 || echo "(media sync failed)"

# Prod publishes; this laptop owns the credentials (ig_refresh_token.py rotates
# the token here and rewrites this .env only). Push them alongside the media —
# same "prod needs this to do its half" step, and a no-op when unchanged.
echo "--- sync credentials to prod ---"
./ig_sync_env.sh 2>&1 || echo "(credential sync failed)"

echo "--- publish ---"
"$PYTHON" pipeline/ig_publish.py 2>&1 || echo "(publish failed)"

echo "===== IG DONE: $(date '+%Y-%m-%d %H:%M:%S') ====="
