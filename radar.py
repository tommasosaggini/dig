#!/usr/bin/env python3
"""
music-radar: find new music fast from your seeds.

Usage:
  python3 radar.py                    # dig — discover from your profile
  python3 radar.py "query"            # search for something specific
  python3 radar.py --like "artist - track" [--vibe chill fast ...]
  python3 radar.py --know "artist - track"   # mark as known (skip in future)
  python3 radar.py --nah "artist - track"    # mark as disliked
  python3 radar.py --ledger                  # view your full ledger
  python3 radar.py --add-seed "query"
  python3 radar.py --add-tag "tag"
  python3 radar.py --add-label "label"
  python3 radar.py --profile
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import sys
import os
import random
from datetime import datetime
from urllib.parse import quote_plus

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

DIR = os.path.dirname(os.path.abspath(__file__))
PROFILE_PATH = os.path.join(DIR, "profile.json")
LEDGER_PATH = os.path.join(DIR, "ledger.json")

DEFAULT_PROFILE = {
    "seeds": [
        "ria sean lemonade",
        "bad channel B",
        "elon bass luciano bradini mean one",
        "closed paradis secrets",
        "dj bafog tattoos on my ribs",
        "gontiti",
    ],
    "tags": ["electronic", "experimental", "club", "southeast-asian", "hard-techno", "ambient"],
    "labels": ["international black", "240kmh", "bangkit", "better listen records", "wrwtfww"],
}


# --- Ledger ---

def load_ledger():
    if os.path.exists(LEDGER_PATH):
        with open(LEDGER_PATH) as f:
            return json.load(f)
    return {"known": [], "liked": [], "disliked": [], "vibes": {}}


def save_ledger(ledger):
    with open(LEDGER_PATH, "w") as f:
        json.dump(ledger, f, indent=2)


def is_known(title, ledger=None):
    """Check if a track/artist is already in the ledger (known, liked, or disliked)."""
    if ledger is None:
        ledger = load_ledger()
    title_low = title.lower()
    all_entries = (
        [k.lower() for k in ledger.get("known", [])]
        + [l["track"].lower() for l in ledger.get("liked", [])]
        + [d["track"].lower() for d in ledger.get("disliked", [])]
    )
    for entry in all_entries:
        # fuzzy: if either contains the other
        if entry in title_low or title_low in entry:
            return True
    return False


def filter_known(results):
    """Remove results that match known/liked/disliked entries."""
    ledger = load_ledger()
    filtered = []
    skipped = 0
    for r in results:
        check = r["title"]
        if r.get("artist"):
            check = f"{r['title']} {r['artist']}"
        if is_known(check, ledger):
            skipped += 1
        else:
            filtered.append(r)
    if skipped:
        print(f"  (filtered {skipped} known tracks)")
    return filtered


def cmd_like(args):
    """--like "artist - track" [--vibe chill fast ...]"""
    ledger = load_ledger()
    # Split on --vibe
    track_parts = []
    vibes = []
    in_vibe = False
    for a in args:
        if a == "--vibe":
            in_vibe = True
        elif in_vibe:
            vibes.append(a)
        else:
            track_parts.append(a)
    track = " ".join(track_parts)
    entry = {"track": track, "date": datetime.now().isoformat()[:10]}
    if vibes:
        entry["vibe"] = vibes
    ledger["liked"].append(entry)
    # Also add to known
    if track.lower() not in [k.lower() for k in ledger["known"]]:
        ledger["known"].append(track)
    save_ledger(ledger)
    v = f" [{', '.join(vibes)}]" if vibes else ""
    print(f"  ♥ Liked: {track}{v}")
    print(f"  ({len(ledger['liked'])} liked, {len(ledger['known'])} known)")


def cmd_know(args):
    """--know "artist - track" — mark as known without liking."""
    ledger = load_ledger()
    track = " ".join(args)
    if track.lower() not in [k.lower() for k in ledger["known"]]:
        ledger["known"].append(track)
        save_ledger(ledger)
        print(f"  ✓ Known: {track}")
    else:
        print(f"  Already known: {track}")
    print(f"  ({len(ledger['known'])} known total)")


def cmd_nah(args):
    """--nah "artist - track" — mark as disliked."""
    ledger = load_ledger()
    track = " ".join(args)
    ledger["disliked"].append({"track": track, "date": datetime.now().isoformat()[:10]})
    if track.lower() not in [k.lower() for k in ledger["known"]]:
        ledger["known"].append(track)
    save_ledger(ledger)
    print(f"  ✗ Nah: {track}")
    print(f"  ({len(ledger['disliked'])} disliked, {len(ledger['known'])} known)")


def cmd_show_ledger():
    ledger = load_ledger()
    print("\n📒 LEDGER\n")
    print(f"Known: {len(ledger.get('known', []))} tracks")
    print(f"Liked: {len(ledger.get('liked', []))} tracks")
    print(f"Disliked: {len(ledger.get('disliked', []))} tracks")

    if ledger.get("liked"):
        print("\n♥ LIKED:")
        for l in ledger["liked"]:
            v = f" [{', '.join(l['vibe'])}]" if l.get("vibe") else ""
            print(f"  {l['track']}{v}")

    if ledger.get("disliked"):
        print("\n✗ DISLIKED:")
        for d in ledger["disliked"]:
            print(f"  {d['track']}")

    if ledger.get("known"):
        # Show known that aren't in liked or disliked
        liked_tracks = {l["track"].lower() for l in ledger.get("liked", [])}
        disliked_tracks = {d["track"].lower() for d in ledger.get("disliked", [])}
        just_known = [k for k in ledger["known"] if k.lower() not in liked_tracks and k.lower() not in disliked_tracks]
        if just_known:
            print(f"\n~ KNOWN (no opinion):")
            for k in just_known:
                print(f"  {k}")
    print()


# --- Profile ---

def load_profile():
    if os.path.exists(PROFILE_PATH):
        with open(PROFILE_PATH) as f:
            return json.load(f)
    save_profile(DEFAULT_PROFILE)
    return DEFAULT_PROFILE


def save_profile(profile):
    with open(PROFILE_PATH, "w") as f:
        json.dump(profile, f, indent=2)


# --- Search functions ---

def search_youtube(query, max_results=5):
    url = f"https://www.youtube.com/results?search_query={quote_plus(query + ' music')}"
    resp = requests.get(url, headers=HEADERS, timeout=10)
    results = []
    matches = re.findall(r'"videoId":"(.+?)".*?"text":"(.+?)"', resp.text)
    seen = set()
    for vid_id, title in matches:
        if vid_id not in seen and len(results) < max_results:
            if any(skip in title.lower() for skip in ["ad", "subscribe", "channel"]):
                continue
            seen.add(vid_id)
            results.append({
                "title": title,
                "url": f"https://youtube.com/watch?v={vid_id}",
                "source": "youtube"
            })
    return results


def search_bandcamp(query, max_results=5):
    url = f"https://bandcamp.com/search?q={quote_plus(query)}&item_type=t"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        for item in soup.select(".searchresult")[:max_results]:
            heading = item.select_one(".heading a")
            subhead = item.select_one(".subhead")
            if heading:
                results.append({
                    "title": heading.text.strip(),
                    "artist": subhead.text.strip() if subhead else "?",
                    "url": heading["href"].split("?")[0],
                    "source": "bandcamp"
                })
        return results
    except Exception:
        return []


def search_soundcloud(query, max_results=5):
    url = f"https://soundcloud.com/search/sounds?q={quote_plus(query)}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        results = []
        matches = re.findall(r'href="(/[^"]+)"[^>]*>([^<]+)</a>', resp.text)
        seen = set()
        for path, text in matches:
            if "/" in path and len(path.split("/")) >= 2 and path not in seen:
                if any(skip in path for skip in ["/search", "/discover", "/charts", "/you/", "javascript"]):
                    continue
                if len(text.strip()) > 2 and len(results) < max_results:
                    seen.add(path)
                    results.append({
                        "title": text.strip(),
                        "url": f"https://soundcloud.com{path}",
                        "source": "soundcloud"
                    })
        return results
    except Exception:
        return []


def search_bandcamp_label(label, max_results=5):
    url = f"https://bandcamp.com/search?q={quote_plus(label)}&item_type=b"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        for item in soup.select(".searchresult")[:2]:
            link = item.select_one(".heading a")
            if link:
                label_url = link["href"].split("?")[0]
                try:
                    lresp = requests.get(label_url + "/music", headers=HEADERS, timeout=10)
                    lsoup = BeautifulSoup(lresp.text, "html.parser")
                    for album in lsoup.select(".music-grid-item a")[:max_results]:
                        title = album.select_one(".title")
                        if title:
                            results.append({
                                "title": title.text.strip(),
                                "url": album.get("href", ""),
                                "source": f"bandcamp label:{label}",
                            })
                except Exception:
                    pass
        return results
    except Exception:
        return []


def explore_bandcamp_tags(tags, max_results=5):
    results = []
    for tag in tags[:3]:
        url = f"https://bandcamp.com/tag/{quote_plus(tag)}?sort_field=date"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")
            for item in soup.select(".item_list .item")[:max_results]:
                title_el = item.select_one(".itemtext")
                artist_el = item.select_one(".itemsubtext")
                link = item.select_one("a")
                if title_el and link:
                    results.append({
                        "title": title_el.text.strip(),
                        "artist": artist_el.text.strip() if artist_el else "?",
                        "url": link.get("href", ""),
                        "source": f"bandcamp #{tag}",
                    })
        except Exception:
            continue
    random.shuffle(results)
    return results[:max_results]


# --- Display ---

def print_result(r, idx=None):
    prefix = f"  {idx}." if idx else "  •"
    if "bandcamp" in r.get("source", ""):
        src = "BC"
    elif "youtube" in r.get("source", ""):
        src = "YT"
    elif "soundcloud" in r.get("source", ""):
        src = "SC"
    else:
        src = r["source"][:6]

    artist = r.get("artist", "")
    title = r["title"]
    if artist:
        print(f"{prefix} [{src}] {title} — {artist}")
    else:
        print(f"{prefix} [{src}] {title}")
    print(f"      {r['url']}")


# --- Main modes ---

def run_search(queries, max_per_source=3):
    print("\n🎵 MUSIC RADAR — SEARCH\n")
    all_results = []
    for q in queries:
        print(f"→ {q}")
        print("-" * 50)
        results = (
            search_youtube(q, max_per_source)
            + search_bandcamp(q, max_per_source)
            + search_soundcloud(q, max_per_source)
        )
        results = filter_known(results)
        for i, r in enumerate(results, 1):
            print_result(r, i)
        all_results.extend(results)
        print()
    print(f"Found {len(all_results)} new results.\n")
    return all_results


def run_dig():
    profile = load_profile()
    ledger = load_ledger()
    print("\n🎵 MUSIC RADAR — DIG\n")

    all_results = []

    # 1. Seeds
    seeds = profile.get("seeds", [])
    if seeds:
        pick = random.sample(seeds, min(3, len(seeds)))
        for seed in pick:
            print(f"→ Seed: {seed}")
            print("-" * 50)
            results = (
                search_youtube(seed, 3)
                + search_bandcamp(seed, 2)
                + search_soundcloud(seed, 3)
            )
            results = filter_known(results)
            for r in results:
                print_result(r)
            all_results.extend(results)
            print()

    # 2. From liked tracks
    liked = ledger.get("liked", [])
    if liked:
        recent = liked[-5:]
        pick = random.sample(recent, min(2, len(recent)))
        for like in pick:
            q = like["track"]
            print(f"→ From liked: {q}")
            print("-" * 50)
            results = (
                search_youtube(q, 3)
                + search_bandcamp(q, 2)
                + search_soundcloud(q, 3)
            )
            results = filter_known(results)
            for r in results:
                print_result(r)
            all_results.extend(results)
            print()

    # 3. Labels
    labels = profile.get("labels", [])
    if labels:
        pick = random.sample(labels, min(2, len(labels)))
        for label in pick:
            print(f"→ Label: {label}")
            print("-" * 50)
            results = search_bandcamp_label(label, 4)
            results = filter_known(results)
            for r in results:
                print_result(r)
            all_results.extend(results)
            print()

    # 4. Tags
    tags = profile.get("tags", [])
    if tags:
        pick = random.sample(tags, min(3, len(tags)))
        print(f"→ Fresh from tags: {', '.join(pick)}")
        print("-" * 50)
        results = explore_bandcamp_tags(pick, 6)
        results = filter_known(results)
        for r in results:
            print_result(r)
        all_results.extend(results)
        print()

    print(f"{'=' * 50}")
    print(f"Found {len(all_results)} new tracks. Go dig.\n")
    return all_results


# --- CLI ---

if __name__ == "__main__":
    args = sys.argv[1:]

    if not args or args[0] == "--dig":
        run_dig()
    elif args[0] == "--like":
        cmd_like(args[1:])
    elif args[0] == "--know":
        cmd_know(args[1:])
    elif args[0] == "--nah":
        cmd_nah(args[1:])
    elif args[0] == "--ledger":
        cmd_show_ledger()
    elif args[0] == "--add-seed":
        profile = load_profile()
        seed = " ".join(args[1:])
        profile["seeds"].append(seed)
        save_profile(profile)
        print(f"  + Added seed: {seed}")
    elif args[0] == "--add-tag":
        profile = load_profile()
        profile["tags"].append(args[1])
        save_profile(profile)
        print(f"  + Added tag: {args[1]}")
    elif args[0] == "--add-label":
        profile = load_profile()
        label = " ".join(args[1:])
        profile["labels"].append(label)
        save_profile(profile)
        print(f"  + Added label: {label}")
    elif args[0] == "--profile":
        print(json.dumps(load_profile(), indent=2))
    elif args[0] == "--help":
        print(__doc__)
    else:
        run_search(args)
