#!/usr/bin/env python3
"""
Re-label queued tracks from the recording instead of the track title.

The pool labeller reads "NewJeans — Ditto" and reaches for what k-pop usually
is; measured across 30k tracks that bias is stark (k-pop is tagged "euphoric"
3.7x more than its runner-up, where no mood dominates overall). Ditto is hazy
and nostalgic, and got "euphoric / crowded dancefloor / party peak".

Once a track is queued for Instagram its full audio is on disk, so its labels
can be grounded in measurements — tempo, brightness, dynamics, transients —
rather than genre reputation. Those labels drive both the generated cover and
the caption, so this runs before render.

Only touches tracks that have audio. The 40k-track pool keeps its text labels;
this is a targeted upgrade where the evidence exists.

    python3 pipeline/ig_relabel_audio.py            # queued, not yet audio-labelled
    python3 pipeline/ig_relabel_audio.py --force    # redo them anyway
    python3 pipeline/ig_relabel_audio.py --dry-run  # show the diff, write nothing
"""
import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import audio_features, ig_queue
from lib.db import execute, fetchall

FIELDS = ("energy", "mood", "texture", "feel", "use_case")

ENERGY = ["very low", "low", "moderate", "high", "very high"]
MOOD = ["serene", "melancholic", "euphoric", "dark", "warm", "mysterious",
        "rebellious", "playful", "spiritual", "bittersweet", "aggressive",
        "dreamy", "joyful", "haunting", "tender", "chaotic", "nostalgic",
        "triumphant", "anxious", "peaceful"]
TEXTURE = ["warm analog", "crisp digital", "hazy lo-fi", "lush orchestral",
           "raw distorted", "shimmering synths", "deep bass", "acoustic wood",
           "metallic industrial", "ethereal pads", "punchy drums", "airy vocals",
           "gritty fuzz", "clean electric", "dense layered", "sparse minimal",
           "bright brass", "dark strings", "percussive tribal",
           "glitchy electronic"]
FEEL = ["midnight drive", "sunday morning", "rainy afternoon", "desert highway",
        "crowded dancefloor", "empty cathedral", "forest walk", "rooftop sunset",
        "basement show", "ocean waves", "city night", "mountain peak",
        "candlelit room", "festival main stage", "train journey", "garden party",
        "winter cabin", "summer beach", "foggy street", "stargazing"]
USE_CASE = ["deep focus", "party peak", "cooking dinner", "late night alone",
            "road trip", "morning coffee", "workout", "meditation", "reading",
            "falling asleep", "house cleaning", "dinner party", "studying",
            "commute", "creative work", "pre-game", "yoga", "shower",
            "background chill", "emotional processing"]


def ensure_schema():
    execute("ALTER TABLE tracks ADD COLUMN IF NOT EXISTS "
            "audio_labeled_at TIMESTAMPTZ")


def build_prompt(entries):
    lines = []
    for e in entries:
        lines.append(
            f"{e['id']} | {e['artist']} — {e['name']}"
            + (f" [{e['album']}]" if e.get("album") else "")
            + f"\n    MEASURED FROM THE AUDIO: {e['sound']}")
    return f"""You are labelling songs you can hear. For each track you are given
its title and, crucially, MEASUREMENTS TAKEN FROM THE ACTUAL RECORDING.

Weight the measurements above what you assume from the artist or genre. A k-pop
single measured as slow, hazy and soft-attacked is nostalgic and dreamy, NOT
euphoric — the genre's reputation is not evidence about this recording. Where
your memory of the song and the measurements disagree, trust the measurements
for energy and texture, and let them steer mood and feel too.

Pick ONLY from these exact values:
- energy: {" | ".join(ENERGY)}
- mood (exactly one): {" | ".join(MOOD)}
- texture (1-2, comma separated): {" | ".join(TEXTURE)}
- feel (exactly one): {" | ".join(FEEL)}
- use_case (exactly one): {" | ".join(USE_CASE)}

Return ONLY valid JSON, no markdown:
{{"track_id": {{"energy":"...","mood":"...","texture":"...","feel":"...","use_case":"..."}}}}

Tracks:
{chr(10).join(lines)}"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ensure_schema()
    items = [i for i in ig_queue.list_queue()
             if i.get("audio_path") and os.path.exists(i["audio_path"])
             and i["status"] not in ("skipped", "published")]
    if not items:
        print("no queued items with audio.")
        return

    ids = [i["track_id"] for i in items if i.get("track_id")]
    known = {r["id"]: r for r in fetchall(
        "SELECT id, name, artist, album, genres, label_energy, label_mood, "
        "label_texture, label_feel, label_use_case, audio_labeled_at "
        "FROM tracks WHERE id = ANY(%s)", (ids,))}

    entries, by_id = [], {}
    for it in items:
        row = known.get(it.get("track_id"))
        if not row:
            continue
        if row.get("audio_labeled_at") and not args.force:
            continue
        feats = audio_features.analyse(it["audio_path"])
        if not feats:
            print(f"  ! could not analyse {row['name']}")
            continue
        entries.append({"id": row["id"], "name": row["name"],
                        "artist": row["artist"], "album": row.get("album") or "",
                        "sound": audio_features.describe(feats)})
        by_id[row["id"]] = (row, it)

    if not entries:
        print("nothing to re-label (use --force to redo).")
        return

    from pipeline.label_discovery import client, LABEL_MODEL
    resp = client.chat.completions.create(
        model=LABEL_MODEL,
        messages=[{"role": "user", "content": build_prompt(entries)}],
    )
    text = (resp.choices[0].message.content or "").strip()
    if text.startswith("```"):
        text = text.split("```")[1].lstrip("json").strip()
    out = json.loads(text)

    changed = 0
    for tid, new in out.items():
        pair = by_id.get(tid)
        if not pair:
            continue
        row, item = pair
        diffs = [(f, row.get(f"label_{f}"), new.get(f)) for f in FIELDS
                 if (row.get(f"label_{f}") or "") != (new.get(f) or "")]
        print(f"  {row['name'][:32]} — {row['artist'][:20]}")
        print(f"     heard: {next(e['sound'] for e in entries if e['id'] == tid)}")
        for f, old, nv in diffs:
            print(f"     {f:<9} {str(old):<30} → {nv}")
        if not diffs:
            print("     (unchanged)")
        if diffs and not args.dry_run:
            changed += 1
            execute("UPDATE tracks SET label_energy=%s, label_mood=%s, "
                    "label_texture=%s, label_feel=%s, label_use_case=%s, "
                    "audio_labeled_at=now() WHERE id=%s",
                    tuple(new.get(f) or row.get(f"label_{f}") for f in FIELDS)
                    + (tid,))
            # The cover is generated FROM these labels, so it is now stale.
            ig_queue.update_item(item["id"], rendered_at=None)
        elif not args.dry_run:
            execute("UPDATE tracks SET audio_labeled_at=now() WHERE id=%s", (tid,))
        print()

    print(f"{changed} track(s) relabelled"
          + ("  (dry run — nothing written)" if args.dry_run else
             "; their covers will rebuild on the next render."))


if __name__ == "__main__":
    main()
