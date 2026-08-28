#!/bin/bash
# DIG — unattended genre-coverage backfill.
#
# Alternates the two halves of the engine in bounded batches until a deadline:
#
#   source_genre_artists.py   who plays the genres we cannot serve  (free APIs)
#   ingest_genre_artists.py   what those people have on Bandcamp    (paced)
#   sync_genre_vocab.py       recount coverage so progress is visible
#
# Batches rather than one long run, because every stage is resumable and a
# short batch that finishes is worth more than a long one that gets killed
# holding state in memory. Each stage records its own progress, so stopping
# this script at any moment loses at most the current batch.
#
# Only ever ONE of each stage at a time: the pacing inside them is per-process,
# so two concurrent sourcing runs would silently double the request rate on
# MusicBrainz and Discogs and get the whole night rate-limited.
#
#   ./scripts/backfill_night.sh 6        # run for six hours
set -u
cd "$(dirname "$0")/.."

HOURS="${1:-6}"
LOG_DIR="${BACKFILL_LOG_DIR:-/tmp/dig_backfill}"
mkdir -p "$LOG_DIR"
DEADLINE=$(( $(date +%s) + ${HOURS%.*} * 3600 ))

PY=python3
[ -x "venv/bin/python3" ] && PY="venv/bin/python3"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# Never stack on top of a run already in flight.
while pgrep -f "source_genre_artists|ingest_genre_artists" >/dev/null 2>&1; do
  log "waiting for an in-flight run to finish…"
  sleep 60
done

round=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  round=$((round + 1))
  remaining=$(( (DEADLINE - $(date +%s)) / 60 ))
  [ "$remaining" -lt 5 ] && break
  log "── round $round (${remaining} min left) ─────────────────────────"

  # Sourcing is cheap, un-metered and FAST — one round produced 2,536 artists
  # in 40 minutes, and 7,900 were queued for ingest before the ingest had done
  # 300. Bandcamp's 4-8s pacing is the real bottleneck, so sourcing gets a
  # short top-up and the rest of the round goes to converting what it found.
  budget=$(( remaining < 30 ? remaining / 3 : 15 ))
  [ "$budget" -lt 1 ] && budget=1
  log "sourcing artists for the worst-covered genres (${budget} min)"
  "$PY" -u scripts/source_genre_artists.py --limit 150 --max-minutes "$budget" \
      >> "$LOG_DIR/sourcing.log" 2>&1
  log "  $(tail -3 "$LOG_DIR/sourcing.log" | head -1)"

  remaining=$(( (DEADLINE - $(date +%s)) / 60 ))
  [ "$remaining" -lt 5 ] && break
  budget=$(( remaining < 75 ? remaining - 2 : 70 ))
  [ "$budget" -lt 1 ] && budget=1
  log "ingesting tracks for sourced artists (${budget} min)"
  "$PY" -u scripts/ingest_genre_artists.py --limit 600 --max-minutes "$budget" \
      >> "$LOG_DIR/ingest.log" 2>&1
  log "  $(grep -E 'tracks from|Bandcamp pushed back|cooldown' "$LOG_DIR/ingest.log" | tail -1)"

  "$PY" -u scripts/sync_genre_vocab.py --no-fetch --gaps 0 >> "$LOG_DIR/coverage.log" 2>&1
  log "  $(grep 'served' "$LOG_DIR/coverage.log" | tail -1)"
done

log "══ backfill window finished after $round round(s) ══"
"$PY" -u scripts/sync_genre_vocab.py --no-fetch --gaps 0 2>&1 | tail -6
