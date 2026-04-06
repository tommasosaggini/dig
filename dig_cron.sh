#!/bin/bash
# DIG — Automated discovery & catalog growth
# Run this via cron or launchd to keep the catalog growing daily.
#
# Recommended cron (every 4 hours):
#   0 */4 * * * /Users/tommasosaggini/Sites/dig/dig_cron.sh >> /Users/tommasosaggini/Sites/dig/cron.log 2>&1

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

PYTHON="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo ""
echo "===== DIG CRON RUN: $TIMESTAMP ====="

# 1. YouTube discovery — runs independently, no Spotify dependency
echo ""
echo "--- YouTube discovery ---"
$PYTHON discover_youtube.py 2>&1 || echo "(youtube discovery failed)"

# 2. Merge YouTube into main pool
echo ""
echo "--- Merging YouTube ---"
$PYTHON discover_youtube.py --merge 2>&1 || echo "(merge failed)"

# 3. Catalog scan — probe 60 cells for pool sizes (may be rate-limited)
echo ""
echo "--- Catalog scan ---"
$PYTHON catalog.py 60 2>&1 || echo "(catalog scan failed, likely rate-limited)"

# 4. Spotify discovery — fetch new tracks
echo ""
echo "--- Spotify discovery ---"
$PYTHON discover.py 2>&1 || echo "(spotify discovery failed)"

# 5. Sync exploration data
echo ""
echo "--- Syncing catalog ---"
$PYTHON catalog.py --sync 2>&1 || echo "(sync failed)"

echo ""
echo "===== DIG CRON DONE: $(date '+%Y-%m-%d %H:%M:%S') ====="
