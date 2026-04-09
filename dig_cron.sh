#!/bin/bash
# DIG — Unified discovery pipeline
# Single script that runs all discovery sources at regular intervals.
# Manages a shared Spotify API budget to avoid rate limits.
#
# Cron (every 3 hours):
#   0 */3 * * * /Users/tommasosaggini/Sites/dig/dig_cron.sh >> /Users/tommasosaggini/Sites/dig/cron.log 2>&1

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Force unbuffered Python output so cron logs show progress in real time
export PYTHONUNBUFFERED=1

# Auto-detect Python
if [ -f "$DIR/venv/bin/python3" ]; then
  PYTHON="$DIR/venv/bin/python3"
elif command -v python3 &>/dev/null; then
  PYTHON="python3"
else
  PYTHON="/usr/bin/python3"
fi

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
echo ""
echo "===== DIG DISCOVERY RUN: $TIMESTAMP ====="

# Reset the shared API budget for this run
$PYTHON -c "import sys; sys.path.insert(0, '$DIR'); from lib.api_budget import reset; reset()"

# 1. YouTube discovery — no Spotify dependency, always runs
echo ""
echo "--- YouTube channel mining ---"
$PYTHON pipeline/discover_youtube.py 2>&1 || echo "(youtube discovery failed)"

echo ""
echo "--- Merging YouTube into pool ---"
$PYTHON pipeline/discover_youtube.py --merge 2>&1 || echo "(merge failed)"

# 2. Spotify genre/region discovery — uses shared budget
echo ""
echo "--- Spotify discovery ---"
$PYTHON pipeline/discover.py 2>&1 || echo "(spotify discovery failed or rate-limited)"

# 3. Artist graph crawl — uses remaining shared budget
echo ""
echo "--- Artist discovery ---"
$PYTHON pipeline/discover_artists.py 2>&1 || echo "(artist discovery failed or rate-limited)"

# 4. AI labeling (uses Anthropic, not Spotify)
echo ""
echo "--- AI labeling ---"
$PYTHON pipeline/label_discovery.py 2>&1 || echo "(labeling failed)"

# 5. Gap analysis — plan next run's priorities
echo ""
echo "--- Gap analysis ---"
$PYTHON pipeline/analyze_pool.py 2>&1 || echo "(analysis failed)"

# Summary
POOL=$($PYTHON -c "
import sys; sys.path.insert(0, '$DIR')
from lib.db import fetchone
row = fetchone('SELECT COUNT(*) AS n FROM tracks')
print(row['n'] if row else '?')
" 2>/dev/null || echo "?")

BUDGET=$($PYTHON -c "import sys; sys.path.insert(0, '$DIR'); from lib.api_budget import get_used; print(get_used())" 2>/dev/null || echo "?")

echo ""
echo "===== DONE: $(date '+%Y-%m-%d %H:%M:%S') | Pool: $POOL tracks | Spotify calls: $BUDGET ====="
