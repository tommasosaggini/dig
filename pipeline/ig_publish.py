#!/usr/bin/env python3
"""
DIG → Instagram: publisher (Phase 4).

Publishes due, rendered items to Instagram via the Graph API: feed Reel +
optional Story. SAFE TO RUN NOW — without credentials it prints a dry-run plan
and changes nothing, so it can sit in cron before Meta app review completes.

Requires, when live:
  - IG_GRAPH_TOKEN          long-lived token with instagram_business_content_publish
  - IG_BUSINESS_ACCOUNT_ID  the IG business account id
  - IG_PUBLIC_MEDIA_BASE    public https base that serves media/ig/<id>/feed.mp4
                            (Instagram fetches the video server-side, so the URL
                            MUST be publicly reachable — e.g. https://diiiiiiiig.xyz/ig-media)

Graph publishing is a container→poll→publish flow. See docs/IG_SETUP.md.

Usage:
    python3 pipeline/ig_publish.py            # publish everything due
    python3 pipeline/ig_publish.py --id 12    # publish one
    python3 pipeline/ig_publish.py --dry-run  # force plan-only
"""
import datetime
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import requests
from lib.env import load_env
load_env()
from lib import ig_queue

# Instagram API *with Instagram Login* — the variant that needs no linked
# Facebook Page and whose tokens are the `IGAA…` kind minted from the app's
# Instagram product. It is served from graph.instagram.com; graph.facebook.com
# belongs to the Facebook-Login variant and rejects these tokens outright.
GRAPH = os.environ.get("IG_GRAPH_BASE", "https://graph.instagram.com/v23.0")


CRED_KEYS = ("IG_GRAPH_TOKEN", "IG_BUSINESS_ACCOUNT_ID", "IG_PUBLIC_MEDIA_BASE")


def _creds():
    """The three credentials, with `.env` OUTRANKING the environment.

    This is a deliberate, local inversion of lib.env's rule that the
    environment wins — the one case where the environment is not a caller's
    intent but a stale copy. On prod, docker-compose passes `env_file:
    ./app/.env`, which snapshots the file into the container's environment at
    CREATE time and never looks again; `docker exec` (how the publish cron
    runs) inherits that snapshot. IG_GRAPH_TOKEN is rotated every 30 days by
    pipeline/ig_refresh_token.py and pushed to prod by ig_sync_env.sh, so a few
    weeks after any container recreate the snapshot holds a token the file has
    already replaced. Instagram answers an expired token with `{"error":
    {"message":"API access blocked."}}` — which reads as a sanction on the app,
    not an expired credential, and cost us a hunt for a Meta enforcement action
    that did not exist.

    So for these three names the file on disk is the truth when it defines
    them; anything it does not define still comes from the environment, which
    keeps `IG_GRAPH_TOKEN=… python3 pipeline/ig_publish.py` working on a box
    with no .env at all.
    """
    from lib.env import read_env_file
    on_disk = read_env_file(CRED_KEYS)
    return tuple(on_disk.get(k) or os.environ.get(k) for k in CRED_KEYS)


# Publishing used to be reachable only from the laptop's cron, alongside render
# and resolve — the stages that genuinely need ffmpeg/yt-dlp and so genuinely
# have to run there. Publish needs neither; it is the Graph API plus already-
# synced media, both of which prod already has. That coupling is what let
# Taitgaral's first post sit 7.5 hours late: the laptop slept through the
# scheduled time and nothing else was ever asking. This heartbeat is the record
# that something DID ask, and how overdue an item was when it finally ran, so a
# repeat is visible instead of silently absorbed into "well it went out
# eventually".
def _record_heartbeat(overdue_minutes=None, can_publish=None):
    from lib.db import execute
    import socket
    try:
        # Table is owned by lib.ig_queue.ensure_ig_schema(), run at server
        # startup — not created here, so a publish run before the server has
        # ever started (a bare cron on a fresh box) still needs it to exist.
        ig_queue.ensure_ig_schema()
        execute("""
            INSERT INTO ig_publish_heartbeat (id, last_run_at, host, max_overdue_minutes,
                                              can_publish)
            VALUES (1, now(), %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                last_run_at = EXCLUDED.last_run_at,
                host = EXCLUDED.host,
                max_overdue_minutes = EXCLUDED.max_overdue_minutes,
                can_publish = EXCLUDED.can_publish
        """, (socket.gethostname(), overdue_minutes, can_publish))
    except Exception as e:
        # A missed heartbeat write must not stop a publish from happening.
        print(f"  (heartbeat write failed: {e})")


def _media_url(base, item_id, fname):
    return f"{base.rstrip('/')}/{item_id}/{fname}"


def _create_container(ig_id, token, **params):
    params["access_token"] = token
    r = requests.post(f"{GRAPH}/{ig_id}/media", data=params, timeout=60)
    r.raise_for_status()
    return r.json()["id"]


def _wait_ready(container_id, token, tries=30, delay=4):
    """Poll container status until FINISHED (video processing isn't instant)."""
    for _ in range(tries):
        r = requests.get(f"{GRAPH}/{container_id}",
                         params={"fields": "status_code", "access_token": token},
                         timeout=30)
        r.raise_for_status()
        code = r.json().get("status_code")
        if code == "FINISHED":
            return True
        if code == "ERROR":
            raise RuntimeError("container processing ERROR")
        time.sleep(delay)
    raise RuntimeError("container never reached FINISHED")


def _publish_container(ig_id, token, container_id):
    r = requests.post(f"{GRAPH}/{ig_id}/media_publish",
                      data={"creation_id": container_id, "access_token": token},
                      timeout=60)
    r.raise_for_status()
    return r.json()["id"]


def check_item(item):
    """Create a media container and stop — publishes nothing.

    Instagram fetches the video from IG_PUBLIC_MEDIA_BASE server-side and
    transcodes it before anything appears on the profile, so a container that
    reaches FINISHED proves the whole risky half of publishing: the token,
    the account id, the public URL, and the file's codec/duration/aspect. The
    container simply expires unused (24h) if never published.
    """
    token, ig_id, base = _creds()
    if not (token and ig_id and base):
        print("  missing creds — set IG_GRAPH_TOKEN / IG_BUSINESS_ACCOUNT_ID / "
              "IG_PUBLIC_MEDIA_BASE")
        return {"error": "no_creds"}
    iid = item["id"]
    url = _media_url(base, iid, "feed.mp4")
    print(f"  #{iid} {item['track_name']} — {item['artist']}")
    print(f"     url: {url}")
    try:
        cid = _create_container(ig_id, token, media_type="REELS",
                                video_url=url,
                                caption=item.get("caption") or "")
        print(f"     container: {cid}  (created — nothing published)")
        _wait_ready(cid, token)
        print("     status: FINISHED — Instagram fetched and accepted the video")
        return {"container": cid, "ok": True}
    except Exception as e:
        detail = ""
        resp = getattr(e, "response", None)
        if resp is not None:
            detail = f" | {resp.status_code} {resp.text[:300]}"
        print(f"     FAILED: {e!r}{detail}")
        return {"error": str(e)}


def publish_item(item, dry_run=False):
    token, ig_id, base = _creds()
    iid = item["id"]
    if dry_run or not (token and ig_id and base):
        print(f"  DRY-RUN #{iid}: would publish '{item['track_name']} — "
              f"{item['artist']}'  feed={item['post_feed']} story={item['post_story']}")
        # "I was told to plan only" and "I meant to publish and cannot" are the
        # same code path but not the same event. Prod ran the second one every
        # 15 minutes for weeks, printing a line indistinguishable from a
        # deliberate rehearsal, while a scheduled post went nowhere. Name it.
        if not dry_run:
            missing = [n for n, v in (("IG_GRAPH_TOKEN", token),
                                      ("IG_BUSINESS_ACCOUNT_ID", ig_id),
                                      ("IG_PUBLIC_MEDIA_BASE", base)) if not v]
            print(f"  MISCONFIGURED: this item is DUE and was not published — "
                  f"missing {', '.join(missing)}. On the studio laptop the "
                  f"credentials live in .env; prod is seeded from it by "
                  f"./ig_sync_env.sh, which ig_cron.sh runs every pass.")
            return {"error": "no_creds"}
        return {"dry_run": True}

    # Two schedulers can now see the same due item (prod's cron and the
    # laptop's, kept as a backup) — claim it atomically so only one of them
    # actually calls the Graph API. Losing the claim is not an error: it means
    # the item is being handled, just not by this process.
    if not ig_queue.claim_for_publishing(iid):
        print(f"  #{iid} already claimed by another run — skipping.")
        return {"skipped": "claimed_elsewhere"}

    d = ig_queue.item_dir(iid)
    result = {}
    try:
        if item.get("post_feed") and os.path.exists(os.path.join(d, "feed.mp4")):
            cid = _create_container(
                ig_id, token, media_type="REELS",
                video_url=_media_url(base, iid, "feed.mp4"),
                caption=item.get("caption") or "")
            _wait_ready(cid, token)
            media_id = _publish_container(ig_id, token, cid)
            ig_queue.update_item(iid, ig_media_id=media_id)
            result["feed"] = media_id
            print(f"  published feed #{iid} → {media_id}")

        if item.get("post_story") and os.path.exists(os.path.join(d, "story.mp4")):
            cid = _create_container(
                ig_id, token, media_type="STORIES",
                video_url=_media_url(base, iid, "story.mp4"))
            _wait_ready(cid, token)
            media_id = _publish_container(ig_id, token, cid)
            ig_queue.update_item(iid, ig_story_media_id=media_id)
            result["story"] = media_id
            print(f"  published story #{iid} → {media_id}")

        ig_queue.update_item(
            iid, status="published",
            published_at=datetime.datetime.now(datetime.timezone.utc))
        return result
    except Exception as e:
        ig_queue.update_item(iid, status="failed", error=str(e)[:500])
        print(f"  FAILED #{iid}: {e!r}")
        return {"error": str(e)}


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--check", action="store_true",
                    help="create a container to validate the media, publish nothing")
    args = ap.parse_args()

    if args.id:
        item = ig_queue.get_item(args.id)
        items = [item] if item else []
    else:
        items = ig_queue.items_due_for_publish()

    overdue = None
    if items and not args.id:
        # ig_queue's _SELECT_COLS casts scheduled_at to text (fine for JSON
        # over the wire, not directly subtractable), so parse it back here.
        now = datetime.datetime.now(datetime.timezone.utc)
        deltas = []
        for it in items:
            raw = it.get("scheduled_at")
            if not raw:
                continue
            sched = datetime.datetime.fromisoformat(raw)
            if sched.tzinfo is None:
                sched = sched.replace(tzinfo=datetime.timezone.utc)
            deltas.append((now - sched).total_seconds() / 60.0)
        if deltas:
            overdue = max(deltas)
            if overdue > 30:
                print(f"  WARNING: most overdue item is {overdue:.0f} min past "
                     f"its scheduled time — the checker was not run for a while.")
    if not args.id:
        _record_heartbeat(overdue, can_publish=all(_creds()))

    if not items:
        print("nothing due to publish." if not args.check
              else "no item selected — pass --id.")
        return
    for it in items:
        if args.check:
            check_item(it)
        else:
            publish_item(it, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
