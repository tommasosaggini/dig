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

ENV_PATH = os.path.join(ROOT, ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

import requests
from lib import ig_queue

GRAPH = "https://graph.facebook.com/v21.0"


def _creds():
    return (os.environ.get("IG_GRAPH_TOKEN"),
            os.environ.get("IG_BUSINESS_ACCOUNT_ID"),
            os.environ.get("IG_PUBLIC_MEDIA_BASE"))


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


def publish_item(item, dry_run=False):
    token, ig_id, base = _creds()
    iid = item["id"]
    if dry_run or not (token and ig_id and base):
        print(f"  DRY-RUN #{iid}: would publish '{item['track_name']} — "
              f"{item['artist']}'  feed={item['post_feed']} story={item['post_story']}")
        return {"dry_run": True}

    d = ig_queue.item_dir(iid)
    ig_queue.update_item(iid, status="publishing", error=None)
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
    args = ap.parse_args()

    if args.id:
        item = ig_queue.get_item(args.id)
        items = [item] if item else []
    else:
        items = ig_queue.items_due_for_publish()

    if not items:
        print("nothing due to publish.")
        return
    for it in items:
        publish_item(it, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
