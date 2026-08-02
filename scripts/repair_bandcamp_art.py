#!/usr/bin/env python3
"""Find and repair Bandcamp cover URLs that no longer load.

A pool row stores `art_url` derived from the `art_id` seen at ingest. Bandcamp
lets an artist replace the artwork, which mints a NEW art_id and retires the
old one — so the stored URL 404s while the album is perfectly fine. The player
then shows its ♫ placeholder over a track that HAS a cover.

Found 2026-08-02 by driving the real app: "03MF (Original Mix)" played with the
placeholder while a live resolve was answering a2055129838_10.jpg, which loads.
The player now falls back to the resolver's cover when the stored one fails,
but that is a workaround applied on every play forever. This is the fix: the
row was wrong, so correct the row.

    python3 scripts/repair_bandcamp_art.py --check            # measure only
    python3 scripts/repair_bandcamp_art.py --check --limit 500
    python3 scripts/repair_bandcamp_art.py --fix              # repair for real

`--check` is the default and writes nothing. Both modes are resumable and safe
to interrupt: each row is committed on its own.

BANDCAMP IS SOMEONE ELSE'S SERVICE. Checking a cover is a HEAD against a CDN
(cheap, no API); re-resolving hits their tralbum API, so that only happens for
rows that actually failed, and both are rate-limited. Nothing here runs
concurrently on purpose.
"""
import argparse
import random
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from lib import bandcamp                      # noqa: E402
from lib.db import execute, fetchall          # noqa: E402

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 DIG/1.0"
HEAD_PAUSE_S = 0.15      # ~7 covers/sec against a CDN
RESOLVE_PAUSE_S = 1.2    # their API, not their CDN — go slowly


def cover_loads(url):
    """True / False / None (could not tell — network trouble, not a verdict)."""
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            # Bandcamp answers a retired art id with 404, but a CDN edge can
            # also hand back a tiny HTML error body with a 200. Treat anything
            # that is not an image as gone.
            ctype = (r.headers.get("Content-Type") or "").lower()
            return r.status == 200 and ctype.startswith("image/")
    except urllib.error.HTTPError as e:
        return False if e.code in (403, 404, 410) else None
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix", action="store_true",
                    help="re-resolve and UPDATE rows whose cover is gone")
    ap.add_argument("--check", action="store_true", help="measure only (default)")
    ap.add_argument("--limit", type=int, default=400,
                    help="how many rows to examine (0 = all)")
    ap.add_argument("--seed", type=int, default=None,
                    help="sample seed, so a measurement can be repeated")
    args = ap.parse_args()
    fix = args.fix and not args.check

    rows = fetchall(
        "SELECT id, name, artist, art_url FROM tracks "
        "WHERE source = 'bandcamp' AND art_url IS NOT NULL AND art_url <> ''"
    )
    total = len(rows)
    if args.limit and args.limit < total:
        # A random sample, not the first N: rows are roughly ingest-ordered and
        # the first N would measure one crawl rather than the pool.
        random.seed(args.seed)
        rows = random.sample(rows, args.limit)
    print(f"{total} bandcamp rows carry a cover; examining {len(rows)}"
          f"{' and REPAIRING' if fix else ' (check only)'}")

    dead, fixed, unknown, unfixable = [], 0, 0, 0
    for i, r in enumerate(rows, 1):
        # Progress OUTSIDE the dead-row branch, and flushed. The first version
        # nested it under "this cover is dead", so a healthy run printed one
        # header line and nothing else for half an hour — indistinguishable
        # from a hang, which is exactly what it was taken for.
        if i % 250 == 0:
            print(f"  … {i}/{len(rows)} examined, {len(dead)} dead, {fixed} repaired",
                  flush=True)
        ok = cover_loads(r["art_url"])
        time.sleep(HEAD_PAUSE_S)
        if ok is None:
            unknown += 1
            continue
        if ok:
            continue
        dead.append(r)
        print(f"  DEAD {r['id']}  {(r['artist'] or '')[:24]} — {(r['name'] or '')[:28]}", flush=True)
        if not fix:
            continue
        # bc:<band_id>:<track_id> — the same shape /api/bandcamp/resolve parses.
        parts = str(r["id"]).split(":")
        if len(parts) != 3:
            unfixable += 1
            continue
        res = bandcamp.resolve_stream(parts[1], parts[2])
        time.sleep(RESOLVE_PAUSE_S)
        fresh = (res or {}).get("art")
        if not res.get("ok") or not fresh or fresh == r["art_url"]:
            # No better answer than the one already stored. Leave it: an empty
            # art_url would lose the placeholder's only clue that this row is
            # the broken kind, and the player's fallback still covers the play.
            unfixable += 1
            print(f"       no replacement ({res.get('error') or 'same url'})")
            continue
        execute("UPDATE tracks SET art_url = %s WHERE id = %s", (fresh, r["id"]))
        fixed += 1
        print(f"       -> {fresh}")

    checked = len(rows) - unknown
    pct = (100.0 * len(dead) / checked) if checked else 0.0
    print(f"\nexamined {len(rows)} ({unknown} inconclusive) — {len(dead)} dead "
          f"covers = {pct:.1f}% of {checked} checked")
    if checked:
        print(f"extrapolates to ~{int(total * len(dead) / checked)} of {total} rows")
    if fix:
        print(f"repaired {fixed}, no replacement available for {unfixable}")
    else:
        print("nothing was written — rerun with --fix to repair")


if __name__ == "__main__":
    main()
