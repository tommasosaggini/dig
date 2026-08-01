#!/usr/bin/env bash
# Runs every test in this directory. No dependencies: the .py suites are bare
# asserts with a __main__ runner, the .mjs suites drive the shipped browser
# script through harness.mjs.
#
#   tests/run_all.sh          all of them
#   tests/run_all.sh playback only files matching "playback"
#
# There are two kinds of test here and they are not redundant:
#
#   *.py   STATIC assertions against web/app.html and server.py. Cheap, and
#          each one records why an invariant exists — the comment is half the
#          value. They cannot catch a logic error.
#   *.mjs  BEHAVIOURAL. They boot the real script against a synthetic browser
#          and drive it. These catch what static text cannot: a queue that
#          walks forever, a guard that overrules the user, an outcome handled
#          twice.
set -uo pipefail
cd "$(dirname "$0")/.."

filter="${1:-}"
fail=0
total=0

run_one() {
  local f="$1" cmd="$2"
  [[ -n "$filter" && "$f" != *"$filter"* ]] && return 0
  total=$((total + 1))
  printf '%-46s ' "$f"
  local out
  # A hung test is a failing test: an unbounded loop in the app under test is
  # the most valuable thing here, and it must not take the suite down with it.
  if out=$(cd . && perl -e 'alarm shift; exec @ARGV' 120 $cmd "$f" 2>&1); then
    printf 'ok\n'
  else
    printf 'FAIL\n'
    printf '%s\n' "$out" | sed 's/^/    /' | tail -25
    fail=$((fail + 1))
  fi
}

for f in tests/test_*.py; do run_one "$f" "python3"; done
for f in tests/test_*.mjs; do run_one "$f" "node --max-old-space-size=1500"; done

echo
if [[ $fail -gt 0 ]]; then
  echo "$fail of $total suites failed"
  exit 1
fi
echo "all $total suites passed"
