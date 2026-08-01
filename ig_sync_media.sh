#!/bin/bash
# Push rendered IG media to prod.
#
# The split: this machine renders (yt-dlp needs a residential IP, ffmpeg and
# Pillow are installed here), prod serves. Instagram creates a media container
# by fetching a public URL — it does not accept an upload — so feed.mp4 has to
# be reachable at IG_PUBLIC_MEDIA_BASE before ig_publish.py can post it.
#
# Safe to run repeatedly: rsync only sends what changed.
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
REMOTE="${IG_SYNC_REMOTE:-root@91.99.188.232}"
REMOTE_DIR="${IG_SYNC_DIR:-/srv/dig/app/media/ig/}"

[ -d "$DIR/media/ig" ] || { echo "no media/ig yet — nothing to sync"; exit 0; }

# --delete is deliberately NOT used: prod may still be serving media for a post
# whose local files were cleaned up, and a stale file is harmless where a
# missing one breaks a live Reel.
rsync -az --timeout=120 \
      -e "ssh -o BatchMode=yes -o ConnectTimeout=15" \
      "$DIR/media/ig/" "$REMOTE:$REMOTE_DIR"
rc=$?

if [ $rc -eq 0 ]; then
  echo "media synced to $REMOTE:$REMOTE_DIR"
else
  echo "rsync failed (rc=$rc) — prod still has the previous media"
fi
exit $rc
