#!/bin/bash
# Push the Instagram credentials this laptop holds to prod's .env.
#
# Prod is the primary publisher (see the "Instagram publish" lane in
# /etc/cron.d/dig): render and resolve genuinely need ffmpeg/yt-dlp and stay
# here, but publishing is the Graph API plus already-synced media, and prod
# does not sleep. That lane was deployed WITHOUT the three credentials it
# needs, and publish_item() treats missing credentials as "dry run" — a
# success path. So for weeks prod woke every 15 minutes, correctly found the
# due item, printed "DRY-RUN #16: would publish …", wrote no error and left the
# status at 'scheduled'. Nothing was published and nothing looked broken.
# Lemonade (#16) sat 16 hours past its slot that way.
#
# The token is the reason this is a sync and not a one-time copy:
# pipeline/ig_refresh_token.py rotates IG_GRAPH_TOKEN every 30 days and
# rewrites THIS machine's .env only. A credential copied to prod by hand would
# go stale within 60 days and fail exactly the same silent way.
#
# Runs on every ig_cron pass and is a no-op when the values already match, so
# it also re-seeds a rebuilt box instead of waiting for the next rotation.
# Values travel over stdin, never as argv — ssh command arguments are visible
# in `ps` on the remote host.
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
REMOTE="${IG_SYNC_REMOTE:-root@91.99.188.232}"
REMOTE_ENV="${IG_SYNC_ENV_PATH:-/srv/dig/app/.env}"
KEYS='IG_GRAPH_TOKEN|IG_BUSINESS_ACCOUNT_ID|IG_PUBLIC_MEDIA_BASE'

[ -f "$DIR/.env" ] || { echo "no local .env — nothing to sync"; exit 0; }

if ! grep -qE "^(${KEYS})=" "$DIR/.env"; then
  echo "no IG credentials in local .env — nothing to sync"
  exit 0
fi

# The remote upsert. Reports key NAMES only: this output goes to a log file.
read -r -d '' UPSERT <<'PY'
import os, sys
path = sys.argv[1]
want = {}
for line in sys.stdin.read().splitlines():
    k, _, v = line.partition("=")
    if k:
        want[k.strip()] = v.strip()
lines = open(path).read().splitlines() if os.path.exists(path) else []
changed, seen = [], set()
for i, line in enumerate(lines):
    k = line.partition("=")[0].strip()
    if k in want:
        seen.add(k)
        if line != f"{k}={want[k]}":
            lines[i] = f"{k}={want[k]}"
            changed.append(k)
for k, v in want.items():
    if k not in seen:
        lines.append(f"{k}={v}")
        changed.append(k)
if not changed:
    print("  prod credentials already current.")
    sys.exit(0)
# Same atomic swap as ig_refresh_token.py: a half-written .env would take down
# every other part of dig that reads it, not just Instagram.
tmp = path + ".tmp"
with open(tmp, "w") as f:
    f.write("\n".join(lines) + "\n")
os.replace(tmp, path)
print("  prod .env updated: " + " ".join(sorted(changed)))
PY

# The program travels base64-encoded so no amount of quoting inside it can be
# re-interpreted by the remote shell; stdin stays free to carry the values.
B64="$(printf '%s' "$UPSERT" | base64 | tr -d '\n')"

grep -E "^(${KEYS})=" "$DIR/.env" \
  | ssh -o BatchMode=yes -o ConnectTimeout=15 "$REMOTE" \
        "python3 -c 'import base64,sys; exec(base64.b64decode(\"$B64\"))' '$REMOTE_ENV'"
rc=$?

# A credential push that cannot reach prod must not fail the cron run: the rest
# of the pipeline is unaffected, and prod keeps whatever it already had.
[ $rc -eq 0 ] || echo "  credential sync failed (rc=$rc) — prod keeps its previous .env"
exit 0
