#!/bin/bash
# Refresh DIG's copy of the admin's Spotify Liked Songs.
#
# Deliberately NOT part of ig_cron.sh. That script is quota-free by design so
# the Instagram pipeline keeps running through a Spotify cooldown; this one
# talks to Spotify, and the dev quota is small enough that a burst has locked
# the app out for ~24h before. Once a day, paginated 50 at a time, is plenty:
# the IG queue only needs to know about likes, not within minutes.
#
# Without this, the pool only refreshes when you happen to sign in to DIG with
# Spotify (server.py kicks off an import in the OAuth callback), so anything
# liked since your last login never becomes a candidate post.
#
#   0 7 * * * /Users/tommasosaggini/Sites/dig/ig_likes_sync.sh >> /Users/tommasosaggini/Sites/dig/ig_likes_sync.log 2>&1
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
export PYTHONUNBUFFERED=1

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
pg_tunnel_ensure || { echo "Aborting likes sync."; exit 1; }

ADMIN="$(grep '^ADMIN_UID' "$DIR/.env" | cut -d= -f2- | tr -d ' \r')"
[ -n "$ADMIN" ] || { echo "FATAL: ADMIN_UID not set in .env"; exit 1; }

echo "===== LIKES SYNC: $(date '+%Y-%m-%d %H:%M:%S') ====="
"$PYTHON" scripts/import_likes.py --user "$ADMIN"
# YouTube likes ride the same daily sync: reading the LL playlist costs no
# API quota at all (yt-dlp + the resolve stage's cookie file), and a failure
# there must not take the Spotify import down with it.
echo "--- youtube likes ---"
"$PYTHON" scripts/ingest_youtube_likes.py --limit 50 2>&1 || echo "(youtube likes failed)"
echo "===== DONE: $(date '+%Y-%m-%d %H:%M:%S') ====="
